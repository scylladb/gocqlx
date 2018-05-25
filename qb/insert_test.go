// Copyright (C) 2017 ScyllaDB
// Use of this source code is governed by a ALv2-style
// license that can be found in the LICENSE file.

package qb

import (
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
)

func TestInsertBuilder(t *testing.T) {
	table := []struct {
		B *InsertBuilder
		N []string
		S string
	}{

		// Basic test for insert
		{
			B: Insert("cycling.cyclist_name").Columns("id", "user_uuid", "firstname"),
			S: "INSERT INTO cycling.cyclist_name (id,user_uuid,firstname) VALUES (?,?,?) ",
			N: []string{"id", "user_uuid", "firstname"},
		},
		// Change table name
		{
			B: Insert("cycling.cyclist_name").Columns("id", "user_uuid", "firstname").Into("Foobar"),
			S: "INSERT INTO Foobar (id,user_uuid,firstname) VALUES (?,?,?) ",
			N: []string{"id", "user_uuid", "firstname"},
		},
		// Add columns
		{
			B: Insert("cycling.cyclist_name").Columns("id", "user_uuid", "firstname").Columns("stars"),
			S: "INSERT INTO cycling.cyclist_name (id,user_uuid,firstname,stars) VALUES (?,?,?,?) ",
			N: []string{"id", "user_uuid", "firstname", "stars"},
		},
		// Add a named column
		{
			B: Insert("cycling.cyclist_name").Columns("id", "user_uuid", "firstname").NamedColumn("stars", "stars_name"),
			S: "INSERT INTO cycling.cyclist_name (id,user_uuid,firstname,stars) VALUES (?,?,?,?) ",
			N: []string{"id", "user_uuid", "firstname", "stars_name"},
		},
		// Add a literal column
		{
			B: Insert("cycling.cyclist_name").Columns("id", "user_uuid", "firstname").LitColumn("stars", "stars_lit"),
			S: "INSERT INTO cycling.cyclist_name (id,user_uuid,firstname,stars) VALUES (?,?,?,stars_lit) ",
			N: []string{"id", "user_uuid", "firstname"},
		},
		// Add TTL
		{
			B: Insert("cycling.cyclist_name").Columns("id", "user_uuid", "firstname").TTL(time.Second),
			S: "INSERT INTO cycling.cyclist_name (id,user_uuid,firstname) VALUES (?,?,?) USING TTL 1 ",
			N: []string{"id", "user_uuid", "firstname"},
		},
		{
			B: Insert("cycling.cyclist_name").Columns("id", "user_uuid", "firstname").TTLNamed("ttl"),
			S: "INSERT INTO cycling.cyclist_name (id,user_uuid,firstname) VALUES (?,?,?) USING TTL ? ",
			N: []string{"id", "user_uuid", "firstname", "ttl"},
		},
		// Add TIMESTAMP
		{
			B: Insert("cycling.cyclist_name").Columns("id", "user_uuid", "firstname").Timestamp(time.Date(2005, 05, 05, 0, 0, 0, 0, time.UTC)),
			S: "INSERT INTO cycling.cyclist_name (id,user_uuid,firstname) VALUES (?,?,?) USING TIMESTAMP 1115251200000000 ",
			N: []string{"id", "user_uuid", "firstname"},
		},
		{
			B: Insert("cycling.cyclist_name").Columns("id", "user_uuid", "firstname").TimestampNamed("ts"),
			S: "INSERT INTO cycling.cyclist_name (id,user_uuid,firstname) VALUES (?,?,?) USING TIMESTAMP ? ",
			N: []string{"id", "user_uuid", "firstname", "ts"},
		},
		// Add IF NOT EXISTS
		{
			B: Insert("cycling.cyclist_name").Columns("id", "user_uuid", "firstname").Unique(),
			S: "INSERT INTO cycling.cyclist_name (id,user_uuid,firstname) VALUES (?,?,?) IF NOT EXISTS ",
			N: []string{"id", "user_uuid", "firstname"},
		},
	}

	for _, test := range table {
		stmt, names := test.B.ToCql()
		if diff := cmp.Diff(test.S, stmt); diff != "" {
			t.Error(diff)
		}
		if diff := cmp.Diff(test.N, names); diff != "" {
			t.Error(diff)
		}
	}
}
