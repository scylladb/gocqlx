// Copyright (C) 2017 ScyllaDB
// Use of this source code is governed by a ALv2-style
// license that can be found in the LICENSE file.

// +build all integration

package table_test

import (
	"testing"
	"time"

	. "github.com/scylladb/gocqlx/v2/gocqlxtest"
	"github.com/scylladb/gocqlx/v2/qb"
	"github.com/scylladb/gocqlx/v2/table"
)

func TestRewriteRows(t *testing.T) {
	session := CreateSession(t)
	defer session.Close()

	if err := session.ExecStmt(`CREATE TABLE gocqlx_test.rewrite_table (testtext text PRIMARY KEY)`); err != nil {
		t.Fatal("create table:", err)
	}

	tbl := table.New(table.Metadata{
		Name:    "gocqlx_test.rewrite_table",
		Columns: []string{"testtext"},
		PartKey: []string{"testtext"},
	})

	// Insert data with 500ms TTL
	q := tbl.InsertBuilder().TTL(500 * time.Millisecond).Query(session)
	if err := q.Bind("a").Exec(); err != nil {
		t.Fatal("insert:", err)
	}
	if err := q.Bind("b").Exec(); err != nil {
		t.Fatal("insert:", err)
	}
	if err := q.Bind("c").Exec(); err != nil {
		t.Fatal("insert:", err)
	}

	// Rewrite data without TTL
	if err := table.RewriteRows(session, tbl); err != nil {
		t.Fatal("rewrite:", err)
	}

	// Wait and check if data persisted
	time.Sleep(time.Second)

	var n int
	if err := qb.Select(tbl.Name()).CountAll().Query(session).Scan(&n); err != nil {
		t.Fatal("scan:", err)
	}
	if n != 3 {
		t.Fatal("expected 3 entries")
	}
}
