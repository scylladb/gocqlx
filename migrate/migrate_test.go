// Copyright (C) 2017 ScyllaDB
// Use of this source code is governed by a ALv2-style
// license that can be found in the LICENSE file.

// +build all integration

package migrate_test

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/scylladb/gocqlx/v2"
	. "github.com/scylladb/gocqlx/v2/gocqlxtest"
	"github.com/scylladb/gocqlx/v2/migrate"
)

var migrateSchema = `
CREATE TABLE IF NOT EXISTS gocqlx_test.migrate_table (
    testint int,
    testuuid timeuuid,
    PRIMARY KEY(testint, testuuid)
)
`

var insertMigrate = `INSERT INTO gocqlx_test.migrate_table (testint, testuuid) VALUES (%d, now())`

func recreateTables(tb testing.TB, session gocqlx.Session) {
	tb.Helper()

	if err := session.ExecStmt("DROP TABLE IF EXISTS gocqlx_test.gocqlx_migrate"); err != nil {
		tb.Fatal(err)
	}
	if err := session.ExecStmt(migrateSchema); err != nil {
		tb.Fatal(err)
	}
	if err := session.ExecStmt("TRUNCATE gocqlx_test.migrate_table"); err != nil {
		tb.Fatal(err)
	}
}

func TestMigration(t *testing.T) {
	session := CreateSession(t)
	defer session.Close()
	recreateTables(t, session)

	ctx := context.Background()

	t.Run("init", func(t *testing.T) {
		dir := makeMigrationDir(t, 2)
		defer os.Remove(dir)

		if err := migrate.Migrate(ctx, session, dir); err != nil {
			t.Fatal(err)
		}
		if c := countMigrations(t, session); c != 2 {
			t.Fatal("expected 2 migration got", c)
		}
	})

	t.Run("update", func(t *testing.T) {
		dir := makeMigrationDir(t, 4)
		defer os.Remove(dir)

		if err := migrate.Migrate(ctx, session, dir); err != nil {
			t.Fatal(err)
		}
		if c := countMigrations(t, session); c != 4 {
			t.Fatal("expected 4 migration got", c)
		}
	})

	t.Run("ahead", func(t *testing.T) {
		dir := makeMigrationDir(t, 2)
		defer os.Remove(dir)

		if err := migrate.Migrate(ctx, session, dir); err == nil || !strings.Contains(err.Error(), "ahead") {
			t.Fatal("expected error")
		} else {
			t.Log(err)
		}
	})

	t.Run("tempered with file", func(t *testing.T) {
		dir := makeMigrationDir(t, 4)
		defer os.Remove(dir)

		appendMigrationFile(t, dir, 3, "\nSELECT * FROM bla;\n")

		if err := migrate.Migrate(ctx, session, dir); err == nil || !strings.Contains(err.Error(), "tempered") {
			t.Fatal("expected error")
		} else {
			t.Log(err)
		}
	})
}

func TestMigrationNoSemicolon(t *testing.T) {
	session := CreateSession(t)
	defer session.Close()
	recreateTables(t, session)

	if err := session.ExecStmt(migrateSchema); err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()

	dir := makeMigrationDir(t, 1)
	defer os.Remove(dir)

	f, err := os.OpenFile(filepath.Join(dir, "0.cql"), os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Fprintf(f, insertMigrate, 0) // note no ; at the end
	f.Close()

	if err := migrate.Migrate(ctx, session, dir); err != nil {
		t.Fatal(err)
	}
	if c := countMigrations(t, session); c != 2 {
		t.Fatal("expected 2 migration got", c)
	}
}

func TestIsCallback(t *testing.T) {
	table := []struct {
		Name string
		Stmt string
		Cb   string
	}{
		{
			Name: "CQL statement",
			Stmt: "SELECT * from X;",
		},
		{
			Name: "CQL comment",
			Stmt: "-- Item",
		},
		{
			Name: "CALL without space",
			Stmt: "--CALL Foo;",
			Cb:   "Foo",
		},
		{
			Name: "CALL with space",
			Stmt: "-- CALL Foo;",
			Cb:   "Foo",
		},
		{
			Name: "CALL with many spaces",
			Stmt: "--   CALL Foo;",
			Cb:   "Foo",
		},
		{
			Name: "CALL with many spaces 2",
			Stmt: "--   CALL   Foo;",
			Cb:   "Foo",
		},
		{
			Name: "CALL with unicode",
			Stmt: "-- CALL α;",
			Cb:   "α",
		},
	}

	for i := range table {
		test := table[i]
		t.Run(test.Name, func(t *testing.T) {
			if migrate.IsCallback(test.Stmt) != test.Cb {
				t.Errorf("IsCallback(%s)=%s, expected %s", test.Stmt, migrate.IsCallback(test.Stmt), test.Cb)
			}
		})
	}
}

func TestMigrationCallback(t *testing.T) {
	var (
		beforeCalled int
		afterCalled  int
		inCalled     int
	)
	migrate.Callback = func(ctx context.Context, session gocqlx.Session, ev migrate.CallbackEvent, name string) error {
		switch ev {
		case migrate.BeforeMigration:
			beforeCalled += 1
		case migrate.AfterMigration:
			afterCalled += 1
		case migrate.CallComment:
			inCalled += 1
		}
		return nil
	}

	defer func() {
		migrate.Callback = nil
	}()

	reset := func() {
		beforeCalled = 0
		afterCalled = 0
		inCalled = 0
	}

	assertCallbacks := func(t *testing.T, before, afer, in int) {
		if beforeCalled != before {
			t.Fatalf("expected %d before calls got %d", before, beforeCalled)
		}
		if afterCalled != afer {
			t.Fatalf("expected %d after calls got %d", afer, afterCalled)
		}
		if inCalled != in {
			t.Fatalf("expected %d in calls got %d", in, inCalled)
		}
	}

	session := CreateSession(t)
	defer session.Close()
	recreateTables(t, session)

	if err := session.ExecStmt(migrateSchema); err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()

	t.Run("init", func(t *testing.T) {
		dir := makeMigrationDir(t, 2)
		defer os.Remove(dir)
		reset()

		if err := migrate.Migrate(ctx, session, dir); err != nil {
			t.Fatal(err)
		}
		assertCallbacks(t, 2, 2, 0)
	})

	t.Run("no duplicate calls", func(t *testing.T) {
		dir := makeMigrationDir(t, 4)
		defer os.Remove(dir)
		reset()

		if err := migrate.Migrate(ctx, session, dir); err != nil {
			t.Fatal(err)
		}
		assertCallbacks(t, 2, 2, 0)
	})

	t.Run("in calls", func(t *testing.T) {
		dir := makeMigrationDir(t, 4)
		defer os.Remove(dir)
		reset()

		appendMigrationFile(t, dir, 4, "\n-- CALL Foo;\n")
		appendMigrationFile(t, dir, 5, "\n-- CALL Bar;\n")

		if err := migrate.Migrate(ctx, session, dir); err != nil {
			t.Fatal(err)
		}
		assertCallbacks(t, 2, 2, 2)
	})
}

func makeMigrationDir(tb testing.TB, n int) (dir string) {
	tb.Helper()

	dir, err := ioutil.TempDir("", "gocqlx_migrate")
	if err != nil {
		tb.Fatal(err)
	}

	for i := 0; i < n; i++ {
		path := migrateFilePath(dir, i)
		cql := []byte(fmt.Sprintf(insertMigrate, i) + ";")
		if err := ioutil.WriteFile(path, cql, os.ModePerm); err != nil {
			os.Remove(dir)
			tb.Fatal(err)
		}
	}

	return dir
}

func countMigrations(tb testing.TB, session gocqlx.Session) int {
	tb.Helper()

	var v int
	if err := session.Query("SELECT COUNT(*) FROM gocqlx_test.migrate_table", nil).Get(&v); err != nil {
		tb.Fatal(err)
	}
	return v
}

func appendMigrationFile(tb testing.TB, dir string, i int, text string) {
	path := migrateFilePath(dir, i)
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_APPEND, os.ModePerm)
	if err != nil {
		tb.Fatal(err)
	}
	if _, err := f.WriteString(text); err != nil {
		tb.Fatal(err)
	}
}

func migrateFilePath(dir string, i int) string {
	return filepath.Join(dir, fmt.Sprint(i, ".cql"))
}
