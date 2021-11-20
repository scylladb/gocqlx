// Copyright (C) 2017 ScyllaDB
// Use of this source code is governed by a ALv2-style
// license that can be found in the LICENSE file.

//go:build all || integration
// +build all integration

package dbutil_test

import (
	"testing"
	"time"

	"github.com/scylladb/gocqlx/v2/dbutil"
	. "github.com/scylladb/gocqlx/v2/gocqlxtest"
	"github.com/scylladb/gocqlx/v2/qb"
	"github.com/scylladb/gocqlx/v2/table"
)

func TestRewriteTableTTL(t *testing.T) {
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
	if err := dbutil.RewriteTable(session, tbl, tbl, nil); err != nil {
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

func TestRewriteTableClone(t *testing.T) {
	session := CreateSession(t)
	defer session.Close()

	if err := session.ExecStmt(`CREATE TABLE gocqlx_test.rewrite_table_clone_src (testtext text PRIMARY KEY, testint int)`); err != nil {
		t.Fatal("create table:", err)
	}

	src := table.New(table.Metadata{
		Name:    "gocqlx_test.rewrite_table_clone_src",
		Columns: []string{"testtext", "testint"},
		PartKey: []string{"testtext"},
	})

	if err := session.ExecStmt(`CREATE TABLE gocqlx_test.rewrite_table_clone_dst (testtext text PRIMARY KEY, testfloat float)`); err != nil {
		t.Fatal("create table:", err)
	}

	dst := table.New(table.Metadata{
		Name:    "gocqlx_test.rewrite_table_clone_dst",
		Columns: []string{"testtext", "testfloat"},
		PartKey: []string{"testtext"},
	})

	// Insert data
	q := src.InsertBuilder().Query(session)
	if err := q.Bind("a", 1).Exec(); err != nil {
		t.Fatal("insert:", err)
	}
	if err := q.Bind("b", 2).Exec(); err != nil {
		t.Fatal("insert:", err)
	}
	if err := q.Bind("c", 3).Exec(); err != nil {
		t.Fatal("insert:", err)
	}

	transformer := func(m map[string]interface{}) {
		m["testfloat"] = float32(m["testint"].(int))
	}

	// Rewrite data
	if err := dbutil.RewriteTable(session, dst, src, transformer); err != nil {
		t.Fatal("rewrite:", err)
	}

	var n int
	if err := qb.Select(dst.Name()).CountAll().Query(session).Scan(&n); err != nil {
		t.Fatal("scan:", err)
	}
	if n != 3 {
		t.Fatal("expected 3 entries")
	}
	var f float32
	if err := dst.GetQuery(session, "testfloat").Bind("a").Scan(&f); err != nil {
		t.Fatal("scan:", err)
	}
	if f != 1 {
		t.Fatal("expected 1")
	}
}
