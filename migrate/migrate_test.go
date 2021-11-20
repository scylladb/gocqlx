// Copyright (C) 2017 ScyllaDB
// Use of this source code is governed by a ALv2-style
// license that can be found in the LICENSE file.

//go:build all || integration
// +build all integration

package migrate_test

import (
	"context"
	"fmt"
	"io/fs"
	"strings"
	"testing"

	"github.com/psanford/memfs"
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
		if err := migrate.FromFS(ctx, session, makeTestFS(2)); err != nil {
			t.Fatal(err)
		}
		if c := countMigrations(t, session); c != 2 {
			t.Fatal("expected 2 migration got", c)
		}
	})

	t.Run("update", func(t *testing.T) {
		if err := migrate.FromFS(ctx, session, makeTestFS(4)); err != nil {
			t.Fatal(err)
		}
		if c := countMigrations(t, session); c != 4 {
			t.Fatal("expected 4 migration got", c)
		}
	})

	t.Run("ahead", func(t *testing.T) {
		err := migrate.FromFS(ctx, session, makeTestFS(2))
		if err == nil || !strings.Contains(err.Error(), "ahead") {
			t.Fatal("expected error")
		} else {
			t.Log(err)
		}
	})

	t.Run("tempered with file", func(t *testing.T) {
		f := makeTestFS(4)
		writeFile(f, 3, "SELECT * FROM bla;")

		if err := migrate.FromFS(ctx, session, f); err == nil || !strings.Contains(err.Error(), "tempered") {
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

	f := makeTestFS(0)
	f.WriteFile("0.cql", []byte(fmt.Sprintf(insertMigrate, 0)+";"+fmt.Sprintf(insertMigrate, 1)), fs.ModePerm)

	ctx := context.Background()
	if err := migrate.FromFS(ctx, session, f); err != nil {
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
		f := makeTestFS(2)
		reset()

		if err := migrate.FromFS(ctx, session, f); err != nil {
			t.Fatal(err)
		}
		assertCallbacks(t, 2, 2, 0)
	})

	t.Run("no duplicate calls", func(t *testing.T) {
		f := makeTestFS(4)
		reset()

		if err := migrate.FromFS(ctx, session, f); err != nil {
			t.Fatal(err)
		}
		assertCallbacks(t, 2, 2, 0)
	})

	t.Run("in calls", func(t *testing.T) {
		f := makeTestFS(4)
		writeFile(f, 4, "\n-- CALL Foo;\n")
		writeFile(f, 5, "\n-- CALL Bar;\n")
		reset()

		if err := migrate.FromFS(ctx, session, f); err != nil {
			t.Fatal(err)
		}
		assertCallbacks(t, 2, 2, 2)
	})
}

func countMigrations(tb testing.TB, session gocqlx.Session) int {
	tb.Helper()

	var v int
	if err := session.Query("SELECT COUNT(*) FROM gocqlx_test.migrate_table", nil).Get(&v); err != nil {
		tb.Fatal(err)
	}
	return v
}

func makeTestFS(n int) *memfs.FS {
	f := memfs.New()
	for i := 0; i < n; i++ {
		writeFile(f, i, fmt.Sprintf(insertMigrate, i)+";")
	}
	return f
}

func writeFile(f *memfs.FS, i int, text string) {
	f.WriteFile(fmt.Sprint(i, ".cql"), []byte(text), fs.ModePerm)
}
