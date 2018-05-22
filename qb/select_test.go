// Copyright (C) 2017 ScyllaDB
// Use of this source code is governed by a ALv2-style
// license that can be found in the LICENSE file.

package qb

import (
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestSelectBuilder(t *testing.T) {
	w := EqNamed("id", "expr")

	table := []struct {
		B *SelectBuilder
		N []string
		S string
	}{
		// Basic test for select *
		{
			B: Select("cycling.cyclist_name"),
			S: "SELECT * FROM cycling.cyclist_name ",
		},
		// Basic test for select columns
		{
			B: Select("cycling.cyclist_name").Columns("id", "user_uuid", "firstname"),
			S: "SELECT id,user_uuid,firstname FROM cycling.cyclist_name ",
		},
		// Add a SELECT AS column
		{
			B: Select("cycling.cyclist_name").Columns("id", "user_uuid", As("firstname", "name")),
			S: "SELECT id,user_uuid,firstname AS name FROM cycling.cyclist_name ",
		},
		// Basic test for select distinct
		{
			B: Select("cycling.cyclist_name").Distinct("id"),
			S: "SELECT DISTINCT id FROM cycling.cyclist_name ",
		},
		// Change table name
		{
			B: Select("cycling.cyclist_name").From("Foobar"),
			S: "SELECT * FROM Foobar ",
		},
		// Add WHERE
		{
			B: Select("cycling.cyclist_name").Where(w, Gt("firstname")),
			S: "SELECT * FROM cycling.cyclist_name WHERE id=? AND firstname>? ",
			N: []string{"expr", "firstname"},
		},
		// Add GROUP BY
		{
			B: Select("cycling.cyclist_name").Columns("MAX(stars) as max_stars").GroupBy("id"),
			S: "SELECT id,MAX(stars) as max_stars FROM cycling.cyclist_name GROUP BY id ",
		},
		// Add ORDER BY
		{
			B: Select("cycling.cyclist_name").Where(w).OrderBy("firstname", ASC),
			S: "SELECT * FROM cycling.cyclist_name WHERE id=? ORDER BY firstname ASC ",
			N: []string{"expr"},
		},
		// Add LIMIT
		{
			B: Select("cycling.cyclist_name").Where(w).Limit(10),
			S: "SELECT * FROM cycling.cyclist_name WHERE id=? LIMIT 10 ",
			N: []string{"expr"},
		},
		// Add PER PARTITION LIMIT
		{
			B: Select("cycling.cyclist_name").Where(w).LimitPerPartition(10),
			S: "SELECT * FROM cycling.cyclist_name WHERE id=? PER PARTITION LIMIT 10 ",
			N: []string{"expr"},
		},
		// Add ALLOW FILTERING
		{
			B: Select("cycling.cyclist_name").Where(w).AllowFiltering(),
			S: "SELECT * FROM cycling.cyclist_name WHERE id=? ALLOW FILTERING ",
			N: []string{"expr"},
		},
		// Add COUNT all
		{
			B: Select("cycling.cyclist_name").CountAll().Where(Gt("stars")),
			S: "SELECT count(*) FROM cycling.cyclist_name WHERE stars>? ",
			N: []string{"stars"},
		},
		// Add COUNT with GROUP BY
		{
			B: Select("cycling.cyclist_name").Count("stars").GroupBy("id"),
			S: "SELECT id,count(stars) FROM cycling.cyclist_name GROUP BY id ",
		},
		// Add Min
		{
			B: Select("cycling.cyclist_name").Min("stars"),
			S: "SELECT min(stars) FROM cycling.cyclist_name ",
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
