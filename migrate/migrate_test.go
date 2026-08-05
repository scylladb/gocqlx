// Copyright (C) 2017 ScyllaDB
// Use of this source code is governed by a ALv2-style
// license that can be found in the LICENSE file.

//go:build all || integration
// +build all integration

package migrate_test

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/psanford/memfs"

	"github.com/scylladb/gocqlx/v3"
	"github.com/scylladb/gocqlx/v3/gocqlxtest"
	"github.com/scylladb/gocqlx/v3/migrate"
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

func TestPending(t *testing.T) {
	session := gocqlxtest.CreateSession(t)
	defer session.Close()
	recreateTables(t, session)

	ctx := context.Background()

	t.Run("pending", func(t *testing.T) {
		defer recreateTables(t, session)

		f := memfs.New()
		writeFile(t, f, 0, fmt.Sprintf(insertMigrate, 0)+";")

		pending, err := migrate.Pending(ctx, session, f)
		if err != nil {
			t.Fatal(err)
		}
		if len(pending) != 1 {
			t.Fatal("expected 2 pending migrations got", len(pending))
		}

		err = migrate.FromFS(ctx, session, f)
		if err != nil {
			t.Fatal(err)
		}

		pending, err = migrate.Pending(ctx, session, f)
		if err != nil {
			t.Fatal(err)
		}
		if len(pending) != 0 {
			t.Fatal("expected no pending migrations got", len(pending))
		}

		for i := 1; i < 3; i++ {
			writeFile(t, f, i, fmt.Sprintf(insertMigrate, i)+";")
		}

		pending, err = migrate.Pending(ctx, session, f)
		if err != nil {
			t.Fatal(err)
		}
		if len(pending) != 2 {
			t.Fatal("expected 2 pending migrations got", len(pending))
		}
	})
}

func TestMigration(t *testing.T) {
	session := gocqlxtest.CreateSession(t)
	defer session.Close()
	recreateTables(t, session)

	ctx := context.Background()

	t.Run("init", func(t *testing.T) {
		if err := migrate.FromFS(ctx, session, makeTestFS(t, 2)); err != nil {
			t.Fatal(err)
		}
		if c := countMigrations(t, session); c != 2 {
			t.Fatal("expected 2 migration got", c)
		}
	})

	t.Run("update", func(t *testing.T) {
		if err := migrate.FromFS(ctx, session, makeTestFS(t, 4)); err != nil {
			t.Fatal(err)
		}
		if c := countMigrations(t, session); c != 4 {
			t.Fatal("expected 4 migration got", c)
		}
	})

	t.Run("ahead", func(t *testing.T) {
		err := migrate.FromFS(ctx, session, makeTestFS(t, 2))
		if err == nil || !strings.Contains(err.Error(), "ahead") {
			t.Fatal("expected error")
		} else {
			t.Log(err)
		}
	})

	t.Run("tempered with file", func(t *testing.T) {
		f := makeTestFS(t, 4)
		writeFile(t, f, 3, "SELECT * FROM bla;")

		if err := migrate.FromFS(ctx, session, f); err == nil || !strings.Contains(err.Error(), "tampered") {
			t.Fatal("expected error")
		} else {
			t.Log(err)
		}
	})
}

func TestMigrationNoSemicolon(t *testing.T) {
	session := gocqlxtest.CreateSession(t)
	defer session.Close()
	recreateTables(t, session)

	if err := session.ExecStmt(migrateSchema); err != nil {
		t.Fatal(err)
	}

	f := makeTestFS(t, 0)
	err := f.WriteFile("0.cql", []byte(fmt.Sprintf(insertMigrate, 0)+";"+fmt.Sprintf(insertMigrate, 1)), fs.ModePerm)
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	if err := migrate.FromFS(ctx, session, f); err != nil {
		t.Fatal(err)
	}
	if c := countMigrations(t, session); c != 2 {
		t.Fatal("expected 2 migration got", c)
	}
}

func TestMigrationWithTrailingComment(t *testing.T) {
	session := gocqlxtest.CreateSession(t)
	defer session.Close()
	recreateTables(t, session)

	if err := session.ExecStmt(migrateSchema); err != nil {
		t.Fatal(err)
	}

	f := makeTestFS(t, 0)
	// Create a migration with a trailing comment (this should reproduce the issue)
	migrationContent := fmt.Sprintf(insertMigrate, 0) + "; -- ttl 1 hour"
	err := f.WriteFile("0.cql", []byte(migrationContent), fs.ModePerm)
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	if err := migrate.FromFS(ctx, session, f); err != nil {
		t.Fatal("Migration should succeed with trailing comment, but got error:", err)
	}
	if c := countMigrations(t, session); c != 1 {
		t.Fatal("expected 1 migration got", c)
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

func TestIsComment(t *testing.T) {
	table := []struct {
		Name      string
		Stmt      string
		IsComment bool
	}{
		{
			Name:      "CQL statement",
			Stmt:      "SELECT * from X;",
			IsComment: false,
		},
		{
			Name:      "Regular comment",
			Stmt:      "-- This is a comment",
			IsComment: true,
		},
		{
			Name:      "Comment with additional text",
			Stmt:      "-- ttl 1 hour",
			IsComment: true,
		},
		{
			Name:      "Callback command (not a regular comment)",
			Stmt:      "-- CALL Foo;",
			IsComment: false,
		},
		{
			Name:      "Callback with spaces (not a regular comment)",
			Stmt:      "--   CALL Bar;",
			IsComment: false,
		},
		{
			Name:      "Empty statement",
			Stmt:      "",
			IsComment: false,
		},
		{
			Name:      "Whitespace only",
			Stmt:      "   ",
			IsComment: false,
		},
	}

	for i := range table {
		test := table[i]
		t.Run(test.Name, func(t *testing.T) {
			result := migrate.IsComment(test.Stmt)
			if result != test.IsComment {
				t.Errorf("IsComment(%q) = %v, expected %v", test.Stmt, result, test.IsComment)
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
			beforeCalled++
		case migrate.AfterMigration:
			afterCalled++
		case migrate.CallComment:
			inCalled++
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
		t.Helper()

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

	session := gocqlxtest.CreateSession(t)
	defer session.Close()
	recreateTables(t, session)

	if err := session.ExecStmt(migrateSchema); err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()

	t.Run("init", func(t *testing.T) {
		f := makeTestFS(t, 2)
		reset()

		if err := migrate.FromFS(ctx, session, f); err != nil {
			t.Fatal(err)
		}
		assertCallbacks(t, 2, 2, 0)
	})

	t.Run("no duplicate calls", func(t *testing.T) {
		f := makeTestFS(t, 4)
		reset()

		if err := migrate.FromFS(ctx, session, f); err != nil {
			t.Fatal(err)
		}
		assertCallbacks(t, 2, 2, 0)
	})

	t.Run("in calls", func(t *testing.T) {
		f := makeTestFS(t, 4)
		writeFile(t, f, 4, "\n-- CALL Foo;\n")
		writeFile(t, f, 5, "\n-- CALL Bar;\n")
		reset()

		if err := migrate.FromFS(ctx, session, f); err != nil {
			t.Fatal(err)
		}
		assertCallbacks(t, 2, 2, 2)
	})
}

func TestMigrationRecordsProgressAfterContextCanceled(t *testing.T) {
	session := gocqlxtest.CreateSession(t)
	defer session.Close()
	recreateTables(t, session)

	f := memfs.New()
	err := f.WriteFile("0.cql", []byte(fmt.Sprintf(insertMigrate, 0)+";\n-- CALL cancel;\n-- CALL after_cancel;\n"+fmt.Sprintf(insertMigrate, 1)+";"), fs.ModePerm)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	afterCancelCalls := 0
	beforeCalls := 0
	afterMigrationCalls := 0
	migrate.Callback = func(callbackCtx context.Context, session gocqlx.Session, ev migrate.CallbackEvent, name string) error {
		if ev == migrate.BeforeMigration {
			beforeCalls++
		}
		if ev == migrate.AfterMigration {
			afterMigrationCalls++
		}
		if ev == migrate.CallComment && name == "cancel" {
			cancel()
			if err := callbackCtx.Err(); err != nil {
				return fmt.Errorf("callback context was canceled: %w", err)
			}
		}
		if ev == migrate.CallComment && name == "after_cancel" {
			afterCancelCalls++
		}
		return nil
	}
	defer func() {
		migrate.Callback = nil
	}()

	err = migrate.FromFS(ctx, session, f)
	if !errors.Is(err, context.Canceled) || !strings.Contains(err.Error(), "context ended before statement 3") {
		t.Fatalf("FromFS() error=%v expected context canceled", err)
	}

	if done := migrationDone(t, session, "0.cql"); done != 2 {
		t.Fatalf("migration done=%d expected 2", done)
	}
	if count := countMigrations(t, session); count != 1 {
		t.Fatalf("migration statements executed=%d expected 1", count)
	}
	if afterCancelCalls != 0 {
		t.Fatalf("post-cancellation callback calls=%d expected 0", afterCancelCalls)
	}
	if beforeCalls != 1 {
		t.Fatalf("before migration callback calls=%d expected 1", beforeCalls)
	}
	if afterMigrationCalls != 0 {
		t.Fatalf("after migration callback calls=%d expected 0", afterMigrationCalls)
	}

	if err := migrate.FromFS(context.Background(), session, f); err != nil {
		t.Fatalf("resume migration: %v", err)
	}
	if done := migrationDone(t, session, "0.cql"); done != 4 {
		t.Fatalf("resumed migration done=%d expected 4", done)
	}
	if count := countMigrations(t, session); count != 2 {
		t.Fatalf("migration statements executed after resume=%d expected 2", count)
	}
	if afterCancelCalls != 1 {
		t.Fatalf("post-cancellation callback calls after resume=%d expected 1", afterCancelCalls)
	}
	if beforeCalls != 2 {
		t.Fatalf("before migration callback calls after resume=%d expected 2", beforeCalls)
	}
	if afterMigrationCalls != 1 {
		t.Fatalf("after migration callback calls after resume=%d expected 1", afterMigrationCalls)
	}
}

func TestMigrationBeforeCallbackUsesParentContext(t *testing.T) {
	session := gocqlxtest.CreateSession(t)
	defer session.Close()
	recreateTables(t, session)

	f := memfs.New()
	err := f.WriteFile("0.cql", []byte(fmt.Sprintf(insertMigrate, 0)+";"), fs.ModePerm)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var callbackErr error
	migrate.Callback = func(callbackCtx context.Context, session gocqlx.Session, ev migrate.CallbackEvent, name string) error {
		if ev == migrate.BeforeMigration {
			cancel()
			callbackErr = callbackCtx.Err()
		}
		return nil
	}
	defer func() {
		migrate.Callback = nil
	}()

	err = migrate.FromFS(ctx, session, f)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("FromFS() error=%v expected context canceled", err)
	}
	if !errors.Is(callbackErr, context.Canceled) {
		t.Fatalf("callback context error=%v expected context canceled", callbackErr)
	}
	if count := countMigrations(t, session); count != 0 {
		t.Fatalf("migration statements executed=%d expected 0", count)
	}
}

func TestMigrationAfterCallbackUsesParentContext(t *testing.T) {
	session := gocqlxtest.CreateSession(t)
	defer session.Close()
	recreateTables(t, session)

	f := memfs.New()
	err := f.WriteFile("0.cql", []byte("-- CALL cancel;"), fs.ModePerm)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	afterCalls := 0
	migrate.Callback = func(callbackCtx context.Context, session gocqlx.Session, ev migrate.CallbackEvent, name string) error {
		switch ev {
		case migrate.CallComment:
			cancel()
		case migrate.AfterMigration:
			afterCalls++
			err := callbackCtx.Err()
			if err == nil {
				return errors.New("after migration context was not canceled")
			}
			return err
		}
		return nil
	}
	defer func() {
		migrate.Callback = nil
	}()

	err = migrate.FromFS(ctx, session, f)
	if !errors.Is(err, context.Canceled) || !strings.Contains(err.Error(), "after migration callback: context canceled") {
		t.Fatalf("FromFS() error=%v expected canceled AfterMigration callback", err)
	}
	if done := migrationDone(t, session, "0.cql"); done != 1 {
		t.Fatalf("migration done=%d expected 1", done)
	}
	if afterCalls != 1 {
		t.Fatalf("after migration callback calls=%d expected 1", afterCalls)
	}
	if count := countMigrations(t, session); count != 0 {
		t.Fatalf("migration statements executed=%d expected 0", count)
	}

	if err := migrate.FromFS(context.Background(), session, f); err != nil {
		t.Fatalf("resume migration: %v", err)
	}
	if afterCalls != 1 {
		t.Fatalf("after migration callback calls after resume=%d expected 1", afterCalls)
	}
}

func TestMigrationCQLStatementUsesDetachedContext(t *testing.T) {
	ctx := newManualDeadlineContext(context.Background())
	deadlineObserved := make(chan bool, 1)
	cluster := gocqlxtest.CreateCluster()
	cluster.QueryObserver = queryObserverFunc(func(queryCtx context.Context, query gocql.ObservedQuery) {
		if !strings.Contains(query.Statement, "INSERT INTO gocqlx_test.migrate_table") {
			return
		}
		_, hasDeadline := queryCtx.Deadline()
		deadlineObserved <- hasDeadline
		ctx.expire()
	})
	session := gocqlxtest.CreateSessionFromCluster(t, cluster)
	defer session.Close()
	recreateTables(t, session)

	f := memfs.New()
	if err := f.WriteFile("0.cql", []byte(fmt.Sprintf(insertMigrate, 0)+";\n"+fmt.Sprintf(insertMigrate, 1)+";"), fs.ModePerm); err != nil {
		t.Fatal(err)
	}

	err := migrate.FromFS(ctx, session, f)
	if !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "context ended before statement 2") {
		t.Fatalf("FromFS() error=%v expected deadline exceeded before statement 2", err)
	}
	select {
	case hasDeadline := <-deadlineObserved:
		if hasDeadline {
			t.Fatal("CQL statement context retained parent deadline")
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting to observe CQL statement")
	}
	if done := migrationDone(t, session, "0.cql"); done != 1 {
		t.Fatalf("migration done=%d expected 1", done)
	}
	if count := countMigrations(t, session); count != 1 {
		t.Fatalf("migration statements executed=%d expected 1", count)
	}
}

func TestMigrationFinishesStartedOperationAfterDeadline(t *testing.T) {
	session := gocqlxtest.CreateSession(t)
	defer session.Close()
	recreateTables(t, session)

	f := memfs.New()
	err := f.WriteFile("0.cql", []byte(fmt.Sprintf(insertMigrate, 0)+";\n-- CALL wait_for_deadline;\n"+fmt.Sprintf(insertMigrate, 1)+";"), fs.ModePerm)
	if err != nil {
		t.Fatal(err)
	}

	ctx := newManualDeadlineContext(context.Background())
	migrate.Callback = func(callbackCtx context.Context, session gocqlx.Session, ev migrate.CallbackEvent, name string) error {
		if ev == migrate.CallComment && name == "wait_for_deadline" {
			ctx.expire()
			if _, ok := callbackCtx.Deadline(); ok {
				return errors.New("callback context retained parent deadline")
			}
			if err := callbackCtx.Err(); err != nil {
				return fmt.Errorf("callback context ended: %w", err)
			}
		}
		return nil
	}
	defer func() {
		migrate.Callback = nil
	}()

	err = migrate.FromFS(ctx, session, f)
	if !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "context ended before statement 3") {
		t.Fatalf("FromFS() error=%v expected deadline exceeded before statement 3", err)
	}
	if done := migrationDone(t, session, "0.cql"); done != 2 {
		t.Fatalf("migration done=%d expected 2", done)
	}
	if count := countMigrations(t, session); count != 1 {
		t.Fatalf("migration statements executed=%d expected 1", count)
	}

	if err := migrate.FromFS(context.Background(), session, f); err != nil {
		t.Fatalf("resume migration: %v", err)
	}
	if done := migrationDone(t, session, "0.cql"); done != 3 {
		t.Fatalf("resumed migration done=%d expected 3", done)
	}
	if count := countMigrations(t, session); count != 2 {
		t.Fatalf("migration statements executed after resume=%d expected 2", count)
	}
}

type manualDeadlineContext struct {
	context.Context
	deadline   time.Time
	done       chan struct{}
	expireOnce sync.Once
}

func newManualDeadlineContext(parent context.Context) *manualDeadlineContext {
	return &manualDeadlineContext{
		Context:  parent,
		deadline: time.Now().Add(time.Hour),
		done:     make(chan struct{}),
	}
}

func (c *manualDeadlineContext) Deadline() (time.Time, bool) {
	return c.deadline, true
}

func (c *manualDeadlineContext) Done() <-chan struct{} {
	return c.done
}

func (c *manualDeadlineContext) Err() error {
	select {
	case <-c.done:
		return context.DeadlineExceeded
	default:
		return nil
	}
}

func (c *manualDeadlineContext) expire() {
	c.expireOnce.Do(func() {
		close(c.done)
	})
}

type queryObserverFunc func(context.Context, gocql.ObservedQuery)

func (f queryObserverFunc) ObserveQuery(ctx context.Context, query gocql.ObservedQuery) {
	f(ctx, query)
}

func countMigrations(tb testing.TB, session gocqlx.Session) int {
	tb.Helper()

	var v int
	if err := session.Query("SELECT COUNT(*) FROM gocqlx_test.migrate_table", nil).Get(&v); err != nil {
		tb.Fatal(err)
	}
	return v
}

func migrationDone(tb testing.TB, session gocqlx.Session, name string) int {
	tb.Helper()

	var v int
	if err := session.Query("SELECT done FROM gocqlx_test.gocqlx_migrate WHERE name = ?", nil).Bind(name).Get(&v); err != nil {
		tb.Fatal(err)
	}
	return v
}

func makeTestFS(tb testing.TB, n int) *memfs.FS {
	tb.Helper()
	f := memfs.New()
	for i := 0; i < n; i++ {
		writeFile(tb, f, i, fmt.Sprintf(insertMigrate, i)+";")
	}
	return f
}

func writeFile(tb testing.TB, f *memfs.FS, i int, text string) {
	tb.Helper()
	err := f.WriteFile(fmt.Sprint(i, ".cql"), []byte(text), fs.ModePerm)
	if err != nil {
		tb.Fatal(err)
	}
}
