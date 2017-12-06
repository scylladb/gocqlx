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
	"time"

	"github.com/gocql/gocql"
)


var migratorTable MigratorTable

func SetMigratorTable(mt MigratorTable) {
	migratorTable = mt
}

// List provides a listing of applied migrations.
func List(ctx context.Context, session *gocql.Session) (v []*Info, err error) {
	if migratorTable == nil {
		mt, err := NewCassandraSessionMigrator(session)
		if err != nil {
			return nil, err
		}
		return mt.List(ctx)
	}
	return migratorTable.List(ctx)
}

// Migrate reads the cql files from a directory and applies required migrations.
func Migrate(ctx context.Context, session *gocql.Session, dir string) error {
	// get database migrations
	if migratorTable == nil {
		var err error
		if migratorTable, err = NewCassandraSessionMigrator(session); err != nil {
			return err
		}
	}
	dbm, err := migratorTable.List(ctx)
	if err != nil {
		return fmt.Errorf("failed to list migrations: %s", err)
	}

	// get file migrations
	fm, err := filepath.Glob(filepath.Join(dir, "*.cql"))
	if err != nil {
		return fmt.Errorf("failed to list migrations in %q: %s", dir, err)
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
	}

	for i := 0; i < len(dbm); i++ {
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
		if err := applyMigration(ctx, migratorTable, fm[last], dbm[last].Done); err != nil {
			return fmt.Errorf("failed to apply migration %q: %s", fm[last], err)
		}
	}

	for i := len(dbm); i < len(fm); i++ {
		if err := applyMigration(ctx, migratorTable, fm[i], 0); err != nil {
			return fmt.Errorf("failed to apply migration %q: %s", fm[i], err)
		}
	}

	return nil
}

func applyMigration(ctx context.Context, mt MigratorTable, path string, done int) error {
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

	i := 1
	r := bytes.NewBuffer(b)
	for {
		stmt, err := r.ReadString(';')
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if i <= done {
			i++
			continue
		}

		info.Done = i
		if err = mt.Execute(ctx, stmt, info); err != nil {
			return err
		}

		i++
	}

	return nil
}
