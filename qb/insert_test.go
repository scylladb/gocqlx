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
		// Basic test for insert JSON
		{
			B: Insert("cycling.cyclist_name").Columns("id", "user_uuid", "firstname").Json(),
			S: "INSERT INTO cycling.cyclist_name JSON ?",
			N: nil,
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
		// Add TIMEOUT
		{
			B: Insert("cycling.cyclist_name").Columns("id", "user_uuid", "firstname").Timeout(time.Second),
			S: "INSERT INTO cycling.cyclist_name (id,user_uuid,firstname) VALUES (?,?,?) USING TIMEOUT 1s ",
			N: []string{"id", "user_uuid", "firstname"},
		},
		{
			B: Insert("cycling.cyclist_name").Columns("id", "user_uuid", "firstname").TimeoutNamed("to"),
			S: "INSERT INTO cycling.cyclist_name (id,user_uuid,firstname) VALUES (?,?,?) USING TIMEOUT ? ",
			N: []string{"id", "user_uuid", "firstname", "to"},
		},
		// Add TupleColumn
		{
			B: Insert("cycling.cyclist_name").TupleColumn("id", 2),
			S: "INSERT INTO cycling.cyclist_name (id) VALUES ((?,?)) ",
			N: []string{"id[0]", "id[1]"},
		},
		{
			B: Insert("cycling.cyclist_name").TupleColumn("id", 2).Columns("user_uuid"),
			S: "INSERT INTO cycling.cyclist_name (id,user_uuid) VALUES ((?,?),?) ",
			N: []string{"id[0]", "id[1]", "user_uuid"},
		},
		// Add IF NOT EXISTS
		{
			B: Insert("cycling.cyclist_name").Columns("id", "user_uuid", "firstname").Unique(),
			S: "INSERT INTO cycling.cyclist_name (id,user_uuid,firstname) VALUES (?,?,?) IF NOT EXISTS ",
			N: []string{"id", "user_uuid", "firstname"},
		},
		// Add FuncColumn
		{
			B: Insert("cycling.cyclist_name").FuncColumn("id", Now()),
			S: "INSERT INTO cycling.cyclist_name (id) VALUES (now()) ",
			N: nil,
		},
		{
			B: Insert("cycling.cyclist_name").FuncColumn("id", Now()).Columns("user_uuid"),
			S: "INSERT INTO cycling.cyclist_name (id,user_uuid) VALUES (now(),?) ",
			N: []string{"user_uuid"},
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
