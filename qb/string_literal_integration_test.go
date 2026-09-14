// Copyright (C) 2017 ScyllaDB
// Use of this source code is governed by a ALv2-style
// license that can be found in the LICENSE file.

//go:build all || integration
// +build all integration

package qb_test

import (
	"testing"

	"github.com/scylladb/gocqlx/v3/gocqlxtest"
	"github.com/scylladb/gocqlx/v3/qb"
	"github.com/scylladb/gocqlx/v3/table"
)

func TestStringLiteralHelpersIntegration(t *testing.T) {
	session := gocqlxtest.CreateSession(t)
	defer session.Close()

	const taskTableName = "qb_string_literal_tasks"
	if err := session.ExecStmt(`DROP TABLE IF EXISTS ` + taskTableName); err != nil {
		t.Fatal("drop task table:", err)
	}
	if err := session.ExecStmt(`CREATE TABLE ` + taskTableName + ` (
		id int PRIMARY KEY,
		status text,
		note text,
		tags set<text>,
		attrs map<text, text>
	)`); err != nil {
		t.Fatal("create task table:", err)
	}
	if err := session.ExecStmt(`CREATE INDEX ` + taskTableName + `_tags_idx ON ` + taskTableName + ` (tags)`); err != nil {
		t.Fatal("create tags index:", err)
	}
	if err := session.ExecStmt(`CREATE INDEX ` + taskTableName + `_attrs_keys_idx ON ` + taskTableName + ` (keys(attrs))`); err != nil {
		t.Fatal("create attrs keys index:", err)
	}

	taskTable := table.New(table.Metadata{
		Name:    taskTableName,
		Columns: []string{"id", "status", "note", "tags", "attrs"},
		PartKey: []string{"id"},
	})

	err := qb.Insert(taskTableName).
		Columns("id", "tags", "attrs").
		StringLitColumn("status", "O'Brien").
		StringLitColumn("note", "initial").
		Query(session).
		BindMap(qb.M{
			"id":    1,
			"tags":  []string{"urgent", "O'Brien"},
			"attrs": map[string]string{"O'Brien": "owner"},
		}).
		ExecRelease()
	if err != nil {
		t.Fatal("insert string literals:", err)
	}

	var status string
	err = qb.Select(taskTableName).
		Columns("status").
		Where(qb.EqLit("id", "1")).
		Query(session).
		Get(&status)
	if err != nil {
		t.Fatal("select inserted status:", err)
	}
	if status != "O'Brien" {
		t.Fatalf("status = %q, want %q", status, "O'Brien")
	}

	err = qb.Update(taskTableName).
		SetLitString("status", "RUNNING").
		SetLitString("note", "ready").
		Where(qb.EqLit("id", "1")).
		Query(session).
		ExecRelease()
	if err != nil {
		t.Fatal("update string literals:", err)
	}

	applied, err := taskTable.UpdateBuilder("status").
		If(qb.EqLitString("status", "RUNNING")).
		Query(session).
		BindMap(qb.M{
			"id":     1,
			"status": "DONE",
		}).
		ExecCASRelease()
	if err != nil {
		t.Fatal("table update IF string literal:", err)
	}
	if !applied {
		t.Fatal("table update IF string literal was not applied")
	}

	applied, err = qb.Update(taskTableName).
		SetLitString("note", "matched").
		Where(qb.EqLit("id", "1")).
		If(
			qb.NeLitString("status", "RUNNING"),
			qb.GtLitString("status", "A"),
			qb.GtOrEqLitString("status", "DONE"),
			qb.LtLitString("status", "ZZZ"),
			qb.LtOrEqLitString("status", "DONE"),
		).
		Query(session).
		ExecCASRelease()
	if err != nil {
		t.Fatal("conditional string literal comparisons:", err)
	}
	if !applied {
		t.Fatal("conditional string literal comparisons were not applied")
	}

	var id int
	err = qb.Select(taskTableName).
		Columns("id").
		Where(qb.ContainsLitString("tags", "O'Brien")).
		Query(session).
		Get(&id)
	if err != nil {
		t.Fatal("select CONTAINS string literal:", err)
	}
	if id != 1 {
		t.Fatalf("CONTAINS id = %d, want 1", id)
	}

	err = qb.Select(taskTableName).
		Columns("id").
		Where(qb.ContainsKeyLitString("attrs", "O'Brien")).
		Query(session).
		Get(&id)
	if err != nil {
		t.Fatal("select CONTAINS KEY string literal:", err)
	}
	if id != 1 {
		t.Fatalf("CONTAINS KEY id = %d, want 1", id)
	}

	const keyTableName = "qb_string_literal_keys"
	if err := session.ExecStmt(`DROP TABLE IF EXISTS ` + keyTableName); err != nil {
		t.Fatal("drop key table:", err)
	}
	if err := session.ExecStmt(`CREATE TABLE ` + keyTableName + ` (
		id text PRIMARY KEY,
		status text
	)`); err != nil {
		t.Fatal("create key table:", err)
	}

	err = qb.Insert(keyTableName).
		StringLitColumn("id", "O'Brien").
		StringLitColumn("status", "matched").
		Query(session).
		ExecRelease()
	if err != nil {
		t.Fatal("insert text primary key:", err)
	}

	err = qb.Select(keyTableName).
		Columns("status").
		Where(qb.InLitStrings("id", "missing", "O'Brien")).
		Query(session).
		Get(&status)
	if err != nil {
		t.Fatal("select IN string literals:", err)
	}
	if status != "matched" {
		t.Fatalf("IN status = %q, want %q", status, "matched")
	}
}
