// Copyright (C) 2017 ScyllaDB
// Use of this source code is governed by a ALv2-style
// license that can be found in the LICENSE file.

package migrate

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/gocql/gocql"
	"github.com/scylladb/gocqlx"
	"github.com/scylladb/gocqlx/qb"
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
	Name      string
	Checksum  string
	Done      int
	StartTime time.Time
	EndTime   time.Time
}

// List provides a listing of applied migrations.
func List(ctx context.Context, session *gocql.Session) ([]*Info, error) {
	if err := ensureInfoTable(ctx, session); err != nil {
		return nil, err
	}

	var v []*Info
	err := gocqlx.Select(&v, session.Query(selectInfo).WithContext(ctx))
	if err == gocql.ErrNotFound {
		return nil, nil
	}

	sort.Slice(v, func(i, j int) bool {
		return v[i].Name < v[j].Name
	})

	return v, err
}

func ensureInfoTable(ctx context.Context, session *gocql.Session) error {
	return gocqlx.Query(session.Query(infoSchema).WithContext(ctx), nil).ExecRelease()
}

// Migrate reads the cql files from a directory and applies required migrations.
func Migrate(ctx context.Context, session *gocql.Session, dir string) error {
	// get database migrations
	dbm, err := List(ctx, session)
	if err != nil {
		return fmt.Errorf("failed to list migrations: %s", err)
	}

	// get file migrations
	fm, err := filepath.Glob(filepath.Join(dir, "*.cql"))
	if err != nil {
		return fmt.Errorf("failed to list migrations in %q: %s", dir, err)
	}
	if len(fm) == 0 {
		return fmt.Errorf("no migration files found in %q", dir)
	}
	sort.Strings(fm)

	// verify migrations
	if len(dbm) > len(fm) {
		return fmt.Errorf("database is ahead of %q", dir)
	}

	for i := 0; i < len(dbm); i++ {
		if dbm[i].Name != filepath.Base(fm[i]) {
			fmt.Println(dbm[i].Name, filepath.Base(fm[i]), i)
			return errors.New("inconsistent migrations")
		}
		c, err := fileChecksum(fm[i])
		if err != nil {
			return fmt.Errorf("failed to calculate checksum for %q: %s", fm[i], err)
		}
		if dbm[i].Checksum != c {
			return fmt.Errorf("file %q was tempered with, expected md5 %s", fm[i], dbm[i].Checksum)
		}
	}

	// apply migrations
	if len(dbm) > 0 {
		last := len(dbm) - 1
		if err := applyMigration(ctx, session, fm[last], dbm[last].Done); err != nil {
			return fmt.Errorf("failed to apply migration %q: %s", fm[last], err)
		}
	}

	for i := len(dbm); i < len(fm); i++ {
		if err := applyMigration(ctx, session, fm[i], 0); err != nil {
			return fmt.Errorf("failed to apply migration %q: %s", fm[i], err)
		}
	}

	if err = session.AwaitSchemaAgreement(ctx); err != nil {
		return fmt.Errorf("awaiting schema agreement failed: %s", err)
	}

	return nil
}

func applyMigration(ctx context.Context, session *gocql.Session, path string, done int) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}

	b, err := ioutil.ReadAll(f)
	f.Close()
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

	iq := gocqlx.Query(session.Query(stmt).WithContext(ctx), names)
	defer iq.Release()

	if DefaultAwaitSchemaAgreement.ShouldAwait(AwaitSchemaAgreementBeforeEachFile) {
		if err = session.AwaitSchemaAgreement(ctx); err != nil {
			return fmt.Errorf("awaiting schema agreement failed: %s", err)
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
				return fmt.Errorf("before migration callback failed: %s", err)
			}
		}

		if DefaultAwaitSchemaAgreement.ShouldAwait(AwaitSchemaAgreementBeforeEachStatement) {
			if err = session.AwaitSchemaAgreement(ctx); err != nil {
				return fmt.Errorf("awaiting schema agreement failed: %s", err)
			}
		}

		// execute
		q := gocqlx.Query(session.Query(stmt).RetryPolicy(nil).WithContext(ctx), nil)
		if err := q.ExecRelease(); err != nil {
			return fmt.Errorf("statement %d failed: %s", i, err)
		}

		// update info
		info.Done = i
		info.EndTime = time.Now()
		if err := iq.BindStruct(info).Exec(); err != nil {
			return fmt.Errorf("migration statement %d failed: %s", i, err)
		}
	}
	if i == 0 {
		return fmt.Errorf("no migration statements found in %q", info.Name)
	}

	if Callback != nil && i > done {
		if err := Callback(ctx, session, AfterMigration, info.Name); err != nil {
			return fmt.Errorf("after migration callback failed: %s", err)
		}
	}

	return nil
}
