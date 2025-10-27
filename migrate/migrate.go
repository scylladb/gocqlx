// Copyright (C) 2017 ScyllaDB
// Use of this source code is governed by a ALv2-style
// license that can be found in the LICENSE file.

package migrate

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/gocql/gocql"

	"github.com/scylladb/gocqlx/v3"
	"github.com/scylladb/gocqlx/v3/qb"
)

// DefaultAwaitSchemaAgreement controls whether checking for cluster schema agreement
// is disabled or if it is checked before each file or statement is applied.
// The default is not checking before each file or statement but only once after every
// migration has been run.
var DefaultAwaitSchemaAgreement = AwaitSchemaAgreementDisabled

type awaitSchemaAgreement int

// Options for checking schema agreement.
const (
	AwaitSchemaAgreementDisabled awaitSchemaAgreement = iota
	AwaitSchemaAgreementBeforeEachFile
	AwaitSchemaAgreementBeforeEachStatement
)

// ShouldAwait decides whether to await schema agreement for the configured DefaultAwaitSchemaAgreement option above.
func (as awaitSchemaAgreement) ShouldAwait(stage awaitSchemaAgreement) bool {
	return as == stage
}

const (
	infoSchema = `CREATE TABLE IF NOT EXISTS gocqlx_migrate (
	name text,
	checksum text,
	done int,
	start_time timestamp,
	end_time timestamp,
	PRIMARY KEY(name)
)`
	selectInfo = "SELECT * FROM gocqlx_migrate"
)

// Info contains information on migration applied on a database.
type Info struct {
	StartTime time.Time
	EndTime   time.Time
	Name      string
	Checksum  string
	Done      int
}

// List provides a listing of applied migrations.
func List(ctx context.Context, session gocqlx.Session) ([]*Info, error) {
	if err := ensureInfoTable(ctx, session); err != nil {
		return nil, err
	}

	q := session.ContextQuery(ctx, selectInfo, nil)

	var v []*Info
	if err := q.SelectRelease(&v); err == gocql.ErrNotFound {
		return nil, nil
	} else if err != nil {
		return v, err
	}

	sort.Slice(v, func(i, j int) bool {
		return v[i].Name < v[j].Name
	})

	return v, nil
}

// Pending provides a listing of pending migrations.
func Pending(ctx context.Context, session gocqlx.Session, f fs.FS) ([]*Info, error) {
	applied, err := List(ctx, session)
	if err != nil {
		return nil, err
	}

	// Create a set of applied migration names
	appliedNames := make(map[string]struct{}, len(applied))
	for _, migration := range applied {
		appliedNames[migration.Name] = struct{}{}
	}

	fm, err := fs.Glob(f, "*.cql")
	if err != nil {
		return nil, fmt.Errorf("list migrations: %w", err)
	}

	pending := make([]*Info, 0)

	for _, name := range fm {
		baseName := filepath.Base(name)
		// Check if the migration is not in the applied set
		if _, exists := appliedNames[baseName]; !exists {
			c, err := fileChecksum(f, name)
			if err != nil {
				return nil, fmt.Errorf("calculate checksum for %q: %w", name, err)
			}

			info := &Info{
				Name:      baseName,
				StartTime: time.Now(),
				Checksum:  c,
			}

			pending = append(pending, info)
		}
	}

	return pending, nil
}

func ensureInfoTable(ctx context.Context, session gocqlx.Session) error {
	return session.ContextQuery(ctx, infoSchema, nil).ExecRelease()
}

// Migrate is a wrapper around FromFS.
// It executes migrations from a directory on disk.
//
// Deprecated: use FromFS instead
func Migrate(ctx context.Context, session gocqlx.Session, dir string) error {
	return FromFS(ctx, session, os.DirFS(dir))
}

// FromFS executes new CQL files from a file system abstraction (io/fs.FS).
// The provided FS has to be a flat directory containing *.cql files.
//
// It supports code based migrations, see Callback and CallbackFunc.
// Any comment in form `-- CALL <name>;` will trigger an CallComment callback.
func FromFS(ctx context.Context, session gocqlx.Session, f fs.FS) error {
	// get database migrations
	dbm, err := List(ctx, session)
	if err != nil {
		return fmt.Errorf("list migrations: %s", err)
	}

	// get file migrations
	fm, err := fs.Glob(f, "*.cql")
	if err != nil {
		return fmt.Errorf("list migrations: %w", err)
	}
	if len(fm) == 0 {
		return fmt.Errorf("no migration files found")
	}
	sort.Strings(fm)

	// verify migrations
	if len(dbm) > len(fm) {
		return fmt.Errorf("database is ahead")
	}

	for i := 0; i < len(dbm); i++ {
		if dbm[i].Name != fm[i] {
			return fmt.Errorf("inconsistent migrations found, expected %q got %q at %d", dbm[i].Name, fm[i], i)
		}
		c, err := fileChecksum(f, fm[i])
		if err != nil {
			return fmt.Errorf("calculate checksum for %q: %s", fm[i], err)
		}
		if dbm[i].Checksum != c {
			return fmt.Errorf("file %q was tampered with, expected md5 %s", fm[i], dbm[i].Checksum)
		}
	}

	// apply migrations
	if len(dbm) > 0 {
		last := len(dbm) - 1
		if err := applyMigration(ctx, session, f, fm[last], dbm[last].Done); err != nil {
			return fmt.Errorf("apply migration %q: %s", fm[last], err)
		}
	}

	for i := len(dbm); i < len(fm); i++ {
		if err := applyMigration(ctx, session, f, fm[i], 0); err != nil {
			return fmt.Errorf("apply migration %q: %s", fm[i], err)
		}
	}

	if err = session.AwaitSchemaAgreement(ctx); err != nil {
		return fmt.Errorf("awaiting schema agreement: %s", err)
	}

	return nil
}

// applyMigration executes a single migration file by parsing and applying its statements.
// It handles three types of content in migration files:
//   - SQL statements: executed against the database
//   - Callback commands: processed via registered callback handlers (format: -- CALL function_name;)
//   - Regular comments: silently skipped (format: -- any comment text)
//
// The function maintains migration state by tracking the number of completed statements,
// allowing for resumption of partially completed migrations.
//
// Parameters:
//   - ctx: context for cancellation and timeouts
//   - session: database session for executing statements
//   - f: filesystem containing the migration file
//   - path: path to the migration file within the filesystem
//   - done: number of statements already completed (for resuming partial migrations)
//
// Returns an error if the migration fails at any point.
func applyMigration(ctx context.Context, session gocqlx.Session, f fs.FS, path string, done int) error {
	file, err := f.Open(path)
	if err != nil {
		return err
	}

	b, err := io.ReadAll(file)
	_ = file.Close()
	if err != nil {
		return err
	}

	info := Info{
		Name:      filepath.Base(path),
		StartTime: time.Now(),
		Checksum:  checksum(b),
	}

	stmt, names := qb.Insert("gocqlx_migrate").Columns(
		"name",
		"checksum",
		"done",
		"start_time",
		"end_time",
	).ToCql()

	update := session.ContextQuery(ctx, stmt, names)
	defer update.Release()

	if DefaultAwaitSchemaAgreement.ShouldAwait(AwaitSchemaAgreementBeforeEachFile) {
		if err = session.AwaitSchemaAgreement(ctx); err != nil {
			return fmt.Errorf("awaiting schema agreement: %s", err)
		}
	}

	i := 0
	r := bytes.NewBuffer(b)
	for {
		stmt, err := r.ReadString(';')
		if err == io.EOF {
			if strings.TrimSpace(stmt) != "" {
				// handle missing semicolon after last statement
				err = nil
			} else {
				break
			}
		}
		if err != nil {
			return err
		}
		i++

		if i <= done {
			continue
		}

		if Callback != nil && i == 1 {
			if err := Callback(ctx, session, BeforeMigration, info.Name); err != nil {
				return fmt.Errorf("before migration callback: %s", err)
			}
		}

		if DefaultAwaitSchemaAgreement.ShouldAwait(AwaitSchemaAgreementBeforeEachStatement) {
			if err = session.AwaitSchemaAgreement(ctx); err != nil {
				return fmt.Errorf("awaiting schema agreement: %s", err)
			}
		}

		// trim new lines and all whitespace characters
		stmt = strings.TrimSpace(stmt)

		// Process statement based on its type
		if cb := isCallback(stmt); cb != "" {
			// Handle callback commands (e.g., "-- CALL function_name;")
			if Callback == nil {
				return fmt.Errorf("statement %d: missing callback handler while trying to call %s", i, cb)
			}
			if err := Callback(ctx, session, CallComment, cb); err != nil {
				return fmt.Errorf("callback %s: %s", cb, err)
			}
		} else if stmt != "" && !isComment(stmt) {
			// Execute SQL statements (skip empty statements and comments)
			q := session.ContextQuery(ctx, stmt, nil).RetryPolicy(nil)
			if err := q.ExecRelease(); err != nil {
				return fmt.Errorf("statement %d: %s", i, err)
			}
		}
		// Regular comments and empty statements are silently skipped

		// update info
		info.Done = i
		info.EndTime = time.Now()
		if err := update.BindStruct(info).Exec(); err != nil {
			return fmt.Errorf("migration statement %d: %s", i, err)
		}
	}
	if i == 0 {
		return fmt.Errorf("no migration statements found in %q", info.Name)
	}

	if Callback != nil && i > done {
		if err := Callback(ctx, session, AfterMigration, info.Name); err != nil {
			return fmt.Errorf("after migration callback: %s", err)
		}
	}

	return nil
}

var cbRegexp = regexp.MustCompile("^-- *CALL +(.+);$")

func isCallback(stmt string) (name string) {
	s := cbRegexp.FindStringSubmatch(stmt)
	if len(s) == 0 {
		return ""
	}
	return s[1]
}

// isComment returns true if the statement is a SQL comment that should be ignored.
// It distinguishes between regular comments (which should be skipped) and
// callback commands (which should be processed).
func isComment(stmt string) bool {
	return strings.HasPrefix(stmt, "--") && isCallback(stmt) == ""
}
