// Copyright (C) 2017 ScyllaDB
// Use of this source code is governed by a ALv2-style
// license that can be found in the LICENSE file.

package qb

import (
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
)

func TestDeleteBuilder(t *testing.T) {
	w := EqNamed("id", "expr")

	table := []struct {
		B *DeleteBuilder
		N []string
		S string
	}{
		// Basic test for delete
		{
			B: Delete("cycling.cyclist_name").Where(w),
			S: "DELETE FROM cycling.cyclist_name WHERE id=? ",
			N: []string{"expr"},
		},
		// Change table name
		{
			B: Delete("cycling.cyclist_name").Where(w).From("Foobar"),
			S: "DELETE FROM Foobar WHERE id=? ",
			N: []string{"expr"},
		},
		// Add column
		{
			B: Delete("cycling.cyclist_name").Where(w).Columns("stars"),
			S: "DELETE stars FROM cycling.cyclist_name WHERE id=? ",
			N: []string{"expr"},
		},
		// Add WHERE
		{
			B: Delete("cycling.cyclist_name").Where(w, Gt("firstname")),
			S: "DELETE FROM cycling.cyclist_name WHERE id=? AND firstname>? ",
			N: []string{"expr", "firstname"},
		},
		// Add IF
		{
			B: Delete("cycling.cyclist_name").Where(w).If(Gt("firstname")),
			S: "DELETE FROM cycling.cyclist_name WHERE id=? IF firstname>? ",
			N: []string{"expr", "firstname"},
		},
		// Add TIMESTAMP
		{
			B: Delete("cycling.cyclist_name").Where(w).Timestamp(time.Date(2005, 05, 05, 0, 0, 0, 0, time.UTC)),
			S: "DELETE FROM cycling.cyclist_name USING TIMESTAMP 1115251200000000 WHERE id=? ",
			N: []string{"expr"},
		},
		{
			B: Delete("cycling.cyclist_name").Where(w).TimestampNamed("ts"),
			S: "DELETE FROM cycling.cyclist_name USING TIMESTAMP ? WHERE id=? ",
			N: []string{"ts", "expr"},
		},
		// Add IF EXISTS
		{
			B: Delete("cycling.cyclist_name").Where(w).Existing(),
			S: "DELETE FROM cycling.cyclist_name WHERE id=? IF EXISTS ",
			N: []string{"expr"},
		},
	}

	for _, test := range table {
		stmt, names := test.B.ToCql()
		if diff := cmp.Diff(test.S, stmt); diff != "" {
			t.Error(diff)
		}
		if diff := cmp.Diff(test.N, names); diff != "" {
			t.Error(diff, names)
		}
	}
}
