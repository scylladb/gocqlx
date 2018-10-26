// Copyright (C) 2017 ScyllaDB
// Use of this source code is governed by a ALv2-style
// license that can be found in the LICENSE file.

package table

import (
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestTableGet(t *testing.T) {
	table := []struct {
		M Metadata
		C []string
		N []string
		S string
	}{
		{
			M: Metadata{
				Name:    "table",
				Columns: []string{"a", "b", "c", "d"},
				PartKey: []string{"a"},
				SortKey: []string{"b"},
			},
			N: []string{"a", "b"},
			S: "SELECT * FROM table WHERE a=? AND b=? ",
		},
		{
			M: Metadata{
				Name:    "table",
				Columns: []string{"a", "b", "c", "d"},
				PartKey: []string{"a"},
			},
			N: []string{"a"},
			S: "SELECT * FROM table WHERE a=? ",
		},
		{
			M: Metadata{
				Name:    "table",
				Columns: []string{"a", "b", "c", "d"},
				PartKey: []string{"a"},
			},
			C: []string{"d"},
			N: []string{"a"},
			S: "SELECT d FROM table WHERE a=? ",
		},
	}

	for _, test := range table {
		stmt, names := New(test.M).Get(test.C...)
		if diff := cmp.Diff(test.S, stmt); diff != "" {
			t.Error(diff)
		}
		if diff := cmp.Diff(test.N, names); diff != "" {
			t.Error(diff, names)
		}
	}
}

func TestTableSelect(t *testing.T) {
	table := []struct {
		M Metadata
		C []string
		N []string
		S string
	}{
		{
			M: Metadata{
				Name:    "table",
				Columns: []string{"a", "b", "c", "d"},
				PartKey: []string{"a"},
				SortKey: []string{"b"},
			},
			N: []string{"a"},
			S: "SELECT * FROM table WHERE a=? ",
		},
		{
			M: Metadata{
				Name:    "table",
				Columns: []string{"a", "b", "c", "d"},
				PartKey: []string{"a"},
				SortKey: []string{"b"},
			},
			C: []string{"d"},
			N: []string{"a"},
			S: "SELECT d FROM table WHERE a=? ",
		},
	}

	for _, test := range table {
		stmt, names := New(test.M).Select(test.C...)
		if diff := cmp.Diff(test.S, stmt); diff != "" {
			t.Error(diff)
		}
		if diff := cmp.Diff(test.N, names); diff != "" {
			t.Error(diff, names)
		}
	}

	// run SelectBuilder on the same data set
	for _, test := range table {
		stmt, names := New(test.M).SelectBuilder(test.C...).ToCql()
		if diff := cmp.Diff(test.S, stmt); diff != "" {
			t.Error(diff)
		}
		if diff := cmp.Diff(test.N, names); diff != "" {
			t.Error(diff, names)
		}
	}
}

func TestTableInsert(t *testing.T) {
	table := []struct {
		M Metadata
		N []string
		S string
	}{
		{
			M: Metadata{
				Name:    "table",
				Columns: []string{"a", "b", "c", "d"},
				PartKey: []string{"a"},
				SortKey: []string{"b"},
			},
			N: []string{"a", "b", "c", "d"},
			S: "INSERT INTO table (a,b,c,d) VALUES (?,?,?,?) ",
		},
	}

	for _, test := range table {
		stmt, names := New(test.M).Insert()
		if diff := cmp.Diff(test.S, stmt); diff != "" {
			t.Error(diff)
		}
		if diff := cmp.Diff(test.N, names); diff != "" {
			t.Error(diff, names)
		}
	}
}

func TestTableUpdate(t *testing.T) {
	table := []struct {
		M Metadata
		C []string
		N []string
		S string
	}{
		{
			M: Metadata{
				Name:    "table",
				Columns: []string{"a", "b", "c", "d"},
				PartKey: []string{"a"},
				SortKey: []string{"b"},
			},
			C: []string{"d"},
			N: []string{"d", "a", "b"},
			S: "UPDATE table SET d=? WHERE a=? AND b=? ",
		},
	}

	for _, test := range table {
		stmt, names := New(test.M).Update(test.C...)
		if diff := cmp.Diff(test.S, stmt); diff != "" {
			t.Error(diff)
		}
		if diff := cmp.Diff(test.N, names); diff != "" {
			t.Error(diff, names)
		}
	}
}

func TestTableDelete(t *testing.T) {
	table := []struct {
		M Metadata
		C []string
		N []string
		S string
	}{
		{
			M: Metadata{
				Name:    "table",
				Columns: []string{"a", "b", "c", "d"},
				PartKey: []string{"a"},
				SortKey: []string{"b"},
			},
			N: []string{"a", "b"},
			S: "DELETE FROM table WHERE a=? AND b=? ",
		},
		{
			M: Metadata{
				Name:    "table",
				Columns: []string{"a", "b", "c", "d"},
				PartKey: []string{"a"},
			},
			N: []string{"a"},
			S: "DELETE FROM table WHERE a=? ",
		},
		{
			M: Metadata{
				Name:    "table",
				Columns: []string{"a", "b", "c", "d"},
				PartKey: []string{"a"},
			},
			C: []string{"d"},
			N: []string{"a"},
			S: "DELETE d FROM table WHERE a=? ",
		},
	}

	for _, test := range table {
		stmt, names := New(test.M).Delete(test.C...)
		if diff := cmp.Diff(test.S, stmt); diff != "" {
			t.Error(diff)
		}
		if diff := cmp.Diff(test.N, names); diff != "" {
			t.Error(diff, names)
		}
	}
}
