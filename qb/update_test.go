// Copyright (C) 2017 ScyllaDB
// Use of this source code is governed by a ALv2-style
// license that can be found in the LICENSE file.

package qb

import (
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
)

func TestUpdateBuilder(t *testing.T) {
	w := EqNamed("id", "expr")

	table := []struct {
		B *UpdateBuilder
		N []string
		S string
	}{
		// Basic test for update
		{
			B: Update("cycling.cyclist_name").Set("id", "user_uuid", "firstname").Where(w),
			S: "UPDATE cycling.cyclist_name SET id=?,user_uuid=?,firstname=? WHERE id=? ",
			N: []string{"id", "user_uuid", "firstname", "expr"},
		},
		// Change table name
		{
			B: Update("cycling.cyclist_name").Set("id", "user_uuid", "firstname").Where(w).Table("Foobar"),
			S: "UPDATE Foobar SET id=?,user_uuid=?,firstname=? WHERE id=? ",
			N: []string{"id", "user_uuid", "firstname", "expr"},
		},
		// Add SET
		{
			B: Update("cycling.cyclist_name").Set("id", "user_uuid", "firstname").Where(w).Set("stars"),
			S: "UPDATE cycling.cyclist_name SET id=?,user_uuid=?,firstname=?,stars=? WHERE id=? ",
			N: []string{"id", "user_uuid", "firstname", "stars", "expr"},
		},
		// Add SET literal
		{
			B: Update("cycling.cyclist_name").SetLit("user_uuid", "literal_uuid").Where(w).Set("stars"),
			S: "UPDATE cycling.cyclist_name SET user_uuid=literal_uuid,stars=? WHERE id=? ",
			N: []string{"stars", "expr"},
		},
		// Add SET SetFunc
		{
			B: Update("cycling.cyclist_name").SetFunc("user_uuid", Fn("someFunc", "param_0", "param_1")).Where(w).Set("stars"),
			S: "UPDATE cycling.cyclist_name SET user_uuid=someFunc(?,?),stars=? WHERE id=? ",
			N: []string{"param_0", "param_1", "stars", "expr"},
		},
		// Add SET Add
		{
			B: Update("cycling.cyclist_name").Add("total").Where(w),
			S: "UPDATE cycling.cyclist_name SET total=total+? WHERE id=? ",
			N: []string{"total", "expr"},
		},
		// Add SET AddNamed
		{
			B: Update("cycling.cyclist_name").AddNamed("total", "inc").Where(w),
			S: "UPDATE cycling.cyclist_name SET total=total+? WHERE id=? ",
			N: []string{"inc", "expr"},
		},
		// Add SET AddLit
		{
			B: Update("cycling.cyclist_name").AddLit("total", "1").Where(w),
			S: "UPDATE cycling.cyclist_name SET total=total+1 WHERE id=? ",
			N: []string{"expr"},
		},
		// Add SET Remove
		{
			B: Update("cycling.cyclist_name").Remove("total").Where(w),
			S: "UPDATE cycling.cyclist_name SET total=total-? WHERE id=? ",
			N: []string{"total", "expr"},
		},
		// Add SET RemoveNamed
		{
			B: Update("cycling.cyclist_name").RemoveNamed("total", "dec").Where(w),
			S: "UPDATE cycling.cyclist_name SET total=total-? WHERE id=? ",
			N: []string{"dec", "expr"},
		},
		// Add SET RemoveLit
		{
			B: Update("cycling.cyclist_name").RemoveLit("total", "1").Where(w),
			S: "UPDATE cycling.cyclist_name SET total=total-1 WHERE id=? ",
			N: []string{"expr"},
		},
		// Add WHERE
		{
			B: Update("cycling.cyclist_name").Set("id", "user_uuid", "firstname").Where(w, Gt("firstname")),
			S: "UPDATE cycling.cyclist_name SET id=?,user_uuid=?,firstname=? WHERE id=? AND firstname>? ",
			N: []string{"id", "user_uuid", "firstname", "expr", "firstname"},
		},
		// Add IF
		{
			B: Update("cycling.cyclist_name").Set("id", "user_uuid", "firstname").Where(w).If(Gt("firstname")),
			S: "UPDATE cycling.cyclist_name SET id=?,user_uuid=?,firstname=? WHERE id=? IF firstname>? ",
			N: []string{"id", "user_uuid", "firstname", "expr", "firstname"},
		},
		// Add TTL
		{
			B: Update("cycling.cyclist_name").Set("id", "user_uuid", "firstname").Where(w).TTL(time.Second),
			S: "UPDATE cycling.cyclist_name USING TTL 1 SET id=?,user_uuid=?,firstname=? WHERE id=? ",
			N: []string{"id", "user_uuid", "firstname", "expr"},
		},
		{
			B: Update("cycling.cyclist_name").Set("id", "user_uuid", "firstname").Where(w).TTLNamed("ttl"),
			S: "UPDATE cycling.cyclist_name USING TTL ? SET id=?,user_uuid=?,firstname=? WHERE id=? ",
			N: []string{"ttl", "id", "user_uuid", "firstname", "expr"},
		},
		// Add TIMESTAMP
		{
			B: Update("cycling.cyclist_name").Set("id", "user_uuid", "firstname").Where(w).Timestamp(time.Date(2005, 05, 05, 0, 0, 0, 0, time.UTC)),
			S: "UPDATE cycling.cyclist_name USING TIMESTAMP 1115251200000000 SET id=?,user_uuid=?,firstname=? WHERE id=? ",
			N: []string{"id", "user_uuid", "firstname", "expr"},
		},
		{
			B: Update("cycling.cyclist_name").Set("id", "user_uuid", "firstname").Where(w).TimestampNamed("ts"),
			S: "UPDATE cycling.cyclist_name USING TIMESTAMP ? SET id=?,user_uuid=?,firstname=? WHERE id=? ",
			N: []string{"ts", "id", "user_uuid", "firstname", "expr"},
		},
		// Add IF EXISTS
		{
			B: Update("cycling.cyclist_name").Set("id", "user_uuid", "firstname").Where(w).Existing(),
			S: "UPDATE cycling.cyclist_name SET id=?,user_uuid=?,firstname=? WHERE id=? IF EXISTS ",
			N: []string{"id", "user_uuid", "firstname", "expr"},
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
