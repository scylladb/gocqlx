// Copyright (C) 2017 ScyllaDB
// Use of this source code is governed by a ALv2-style
// license that can be found in the LICENSE file.

package table

import (
	"sync"
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/scylladb/gocqlx/v3/qb"
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
				Name:    "tbl",
				Columns: []string{"a", "b", "c", "d"},
				PartKey: []string{"a"},
				SortKey: []string{"b"},
			},
			N: []string{"a", "b"},
			S: "SELECT * FROM tbl WHERE a=? AND b=? ",
		},
		{
			M: Metadata{
				Name:    "tbl",
				Columns: []string{"a", "b", "c", "d"},
				PartKey: []string{"a"},
			},
			N: []string{"a"},
			S: "SELECT * FROM tbl WHERE a=? ",
		},
		{
			M: Metadata{
				Name:    "tbl",
				Columns: []string{"a", "b", "c", "d"},
				PartKey: []string{"a"},
			},
			C: []string{"d"},
			N: []string{"a"},
			S: "SELECT d FROM tbl WHERE a=? ",
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

	// run GetBuilder on the same data set
	for _, test := range table {
		stmt, names := New(test.M).GetBuilder(test.C...).ToCql()
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
				Name:    "tbl",
				Columns: []string{"a", "b", "c", "d"},
				PartKey: []string{"a"},
				SortKey: []string{"b"},
			},
			N: []string{"a"},
			S: "SELECT * FROM tbl WHERE a=? ",
		},
		{
			M: Metadata{
				Name:    "tbl",
				Columns: []string{"a", "b", "c", "d"},
				PartKey: []string{"a"},
				SortKey: []string{"b"},
			},
			C: []string{"d"},
			N: []string{"a"},
			S: "SELECT d FROM tbl WHERE a=? ",
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
		C []string
		N []string
		S string
	}{
		{
			M: Metadata{
				Name:    "tbl",
				Columns: []string{"a", "b", "c", "d"},
				PartKey: []string{"a"},
				SortKey: []string{"b"},
			},
			N: []string{"a", "b", "c", "d"},
			S: "INSERT INTO tbl (a,b,c,d) VALUES (?,?,?,?) ",
		},
		{
			M: Metadata{
				Name:    "table",
				Columns: []string{"a", "b", "c", "d"},
				PartKey: []string{"a"},
				SortKey: []string{"b"},
			},
			C: []string{"a", "b", "c"},
			N: []string{"a", "b", "c"},
			S: "INSERT INTO table (a,b,c) VALUES (?,?,?) ",
		},
	}

	for _, test := range table {
		stmt, names := New(test.M).Insert(test.C...)
		if diff := cmp.Diff(test.S, stmt); diff != "" {
			t.Error(diff)
		}
		if diff := cmp.Diff(test.N, names); diff != "" {
			t.Error(diff, names)
		}
	}

	// run InsertBuilder on the same data set
	for _, test := range table {
		stmt, names := New(test.M).InsertBuilder(test.C...).ToCql()
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
				Name:    "tbl",
				Columns: []string{"a", "b", "c", "d"},
				PartKey: []string{"a"},
				SortKey: []string{"b"},
			},
			C: []string{"d"},
			N: []string{"d", "a", "b"},
			S: "UPDATE tbl SET d=? WHERE a=? AND b=? ",
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

	// run UpdateBuilder on the same data set
	for _, test := range table {
		stmt, names := New(test.M).UpdateBuilder(test.C...).ToCql()
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
				Name:    "tbl",
				Columns: []string{"a", "b", "c", "d"},
				PartKey: []string{"a"},
				SortKey: []string{"b"},
			},
			N: []string{"a", "b"},
			S: "DELETE FROM tbl WHERE a=? AND b=? ",
		},
		{
			M: Metadata{
				Name:    "tbl",
				Columns: []string{"a", "b", "c", "d"},
				PartKey: []string{"a"},
			},
			N: []string{"a"},
			S: "DELETE FROM tbl WHERE a=? ",
		},
		{
			M: Metadata{
				Name:    "tbl",
				Columns: []string{"a", "b", "c", "d"},
				PartKey: []string{"a"},
			},
			C: []string{"d"},
			N: []string{"a"},
			S: "DELETE d FROM tbl WHERE a=? ",
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

	// run DeleteBuilder on the same data set
	for _, test := range table {
		stmt, names := New(test.M).DeleteBuilder(test.C...).ToCql()
		if diff := cmp.Diff(test.S, stmt); diff != "" {
			t.Error(diff)
		}
		if diff := cmp.Diff(test.N, names); diff != "" {
			t.Error(diff, names)
		}
	}
}

func TestTableQuotedName(t *testing.T) {
	tbl := New(Metadata{
		Name:    `ks.tableName`,
		Columns: []string{"a", "b", "c", "d"},
		PartKey: []string{"a"},
		SortKey: []string{"b"},
	})
	stmtOnly := func(stmt string, _ []string) string {
		return stmt
	}

	tests := []struct {
		name string
		got  string
		want string
	}{
		{
			name: "get",
			got:  stmtOnly(tbl.Get()),
			want: `SELECT * FROM ks."tableName" WHERE a=? AND b=? `,
		},
		{
			name: "select",
			got:  stmtOnly(tbl.Select()),
			want: `SELECT * FROM ks."tableName" WHERE a=? `,
		},
		{
			name: "select all",
			got:  stmtOnly(tbl.SelectAll()),
			want: `SELECT * FROM ks."tableName" `,
		},
		{
			name: "insert",
			got:  stmtOnly(tbl.Insert()),
			want: `INSERT INTO ks."tableName" (a,b,c,d) VALUES (?,?,?,?) `,
		},
		{
			name: "update",
			got:  stmtOnly(tbl.Update("d")),
			want: `UPDATE ks."tableName" SET d=? WHERE a=? AND b=? `,
		},
		{
			name: "delete",
			got:  stmtOnly(tbl.Delete()),
			want: `DELETE FROM ks."tableName" WHERE a=? AND b=? `,
		},
	}

	for _, tt := range tests {
		if diff := cmp.Diff(tt.want, tt.got); diff != "" {
			t.Errorf("%s stmt mismatch (-want +got):\n%s", tt.name, diff)
		}
	}
}

func TestTableConcurrentUsage(t *testing.T) {
	table := []struct {
		Name string
		M    Metadata
		C    []string
		N    []string
		S    string
	}{
		{
			Name: "Full select",
			M: Metadata{
				Name:    "tbl",
				Columns: []string{"a", "b", "c", "d"},
				PartKey: []string{"a"},
				SortKey: []string{"b"},
			},
			N: []string{"a", "b"},
			S: "SELECT * FROM tbl WHERE a=? AND b=? ",
		},
		{
			Name: "Sub select",
			M: Metadata{
				Name:    "tbl",
				Columns: []string{"a", "b", "c", "d"},
				PartKey: []string{"a"},
				SortKey: []string{"b"},
			},
			C: []string{"d"},
			N: []string{"a", "b"},
			S: "SELECT d FROM tbl WHERE a=? AND b=? ",
		},
	}

	parallelCount := 3
	// run SelectBuilder on the data set in parallel
	for _, test := range table {
		var wg sync.WaitGroup
		testTable := New(test.M)
		wg.Add(parallelCount)
		for i := 0; i < parallelCount; i++ {
			go func() {
				defer wg.Done()
				stmt, names := testTable.SelectBuilder(test.C...).
					Where(qb.Eq("b")).ToCql()
				if diff := cmp.Diff(test.S, stmt); diff != "" {
					t.Error(diff)
				}
				if diff := cmp.Diff(test.N, names); diff != "" {
					t.Error(diff, names)
				}
			}()
		}
		wg.Wait()
	}
}
