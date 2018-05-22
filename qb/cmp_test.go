// Copyright (C) 2017 ScyllaDB
// Use of this source code is governed by a ALv2-style
// license that can be found in the LICENSE file.

package qb

import (
	"bytes"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestCmp(t *testing.T) {
	table := []struct {
		C Cmp
		S string
		N []string
	}{
		// Basic comparators
		{
			C: Eq("eq"),
			S: "eq=?",
			N: []string{"eq"},
		},
		{
			C: Lt("lt"),
			S: "lt<?",
			N: []string{"lt"},
		},
		{
			C: LtOrEq("lt"),
			S: "lt<=?",
			N: []string{"lt"},
		},
		{
			C: Gt("gt"),
			S: "gt>?",
			N: []string{"gt"},
		},
		{
			C: GtOrEq("gt"),
			S: "gt>=?",
			N: []string{"gt"},
		},
		{
			C: In("in"),
			S: "in IN ?",
			N: []string{"in"},
		},
		{
			C: Contains("cnt"),
			S: "cnt CONTAINS ?",
			N: []string{"cnt"},
		},

		// Custom bind names
		{
			C: EqNamed("eq", "name"),
			S: "eq=?",
			N: []string{"name"},
		},
		{
			C: LtNamed("lt", "name"),
			S: "lt<?",
			N: []string{"name"},
		},
		{
			C: LtOrEqNamed("lt", "name"),
			S: "lt<=?",
			N: []string{"name"},
		},
		{
			C: GtNamed("gt", "name"),
			S: "gt>?",
			N: []string{"name"},
		},
		{
			C: GtOrEqNamed("gt", "name"),
			S: "gt>=?",
			N: []string{"name"},
		},
		{
			C: InNamed("in", "name"),
			S: "in IN ?",
			N: []string{"name"},
		},
		{
			C: ContainsNamed("cnt", "name"),
			S: "cnt CONTAINS ?",
			N: []string{"name"},
		},

		// Literals
		{
			C: EqLit("eq", "litval"),
			S: "eq=litval",
		},
		{
			C: LtLit("lt", "litval"),
			S: "lt<litval",
		},
		{
			C: LtOrEqLit("lt", "litval"),
			S: "lt<=litval",
		},
		{
			C: GtLit("gt", "litval"),
			S: "gt>litval",
		},
		{
			C: GtOrEqLit("gt", "litval"),
			S: "gt>=litval",
		},
		{
			C: InLit("in", "litval"),
			S: "in IN litval",
		},
		{
			C: ContainsLit("cnt", "litval"),
			S: "cnt CONTAINS litval",
		},

		// Functions
		{
			C: EqFunc("eq", Fn("fn", "arg0", "arg1")),
			S: "eq=fn(?,?)",
			N: []string{"arg0", "arg1"},
		},
		{
			C: EqFunc("eq", MaxTimeuuid("arg0")),
			S: "eq=maxTimeuuid(?)",
			N: []string{"arg0"},
		},
		{
			C: EqFunc("eq", MinTimeuuid("arg0")),
			S: "eq=minTimeuuid(?)",
			N: []string{"arg0"},
		},
		{
			C: EqFunc("eq", Now()),
			S: "eq=now()",
		},
	}

	buf := bytes.Buffer{}
	for _, test := range table {
		buf.Reset()
		name := test.C.writeCql(&buf)
		if diff := cmp.Diff(test.S, buf.String()); diff != "" {
			t.Error(diff)
		}
		if diff := cmp.Diff(test.N, name); diff != "" {
			t.Error(diff)
		}
	}
}
