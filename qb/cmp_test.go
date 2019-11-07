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
			C: Ne("ne"),
			S: "ne!=?",
			N: []string{"ne"},
		},
		{
			C: NeTuple("ne", 3),
			S: "ne!=(?,?,?)",
			N: []string{"ne_0", "ne_1", "ne_2"},
		},
		{
			C: Lt("lt"),
			S: "lt<?",
			N: []string{"lt"},
		},
		{
			C: LtTuple("lt", 2),
			S: "lt<(?,?)",
			N: []string{"lt_0", "lt_1"},
		},
		{
			C: LtOrEq("lt"),
			S: "lt<=?",
			N: []string{"lt"},
		},
		{
			C: LtOrEqTuple("lt", 2),
			S: "lt<=(?,?)",
			N: []string{"lt_0", "lt_1"},
		},
		{
			C: Gt("gt"),
			S: "gt>?",
			N: []string{"gt"},
		},
		{
			C: GtTuple("gt", 2),
			S: "gt>(?,?)",
			N: []string{"gt_0", "gt_1"},
		},
		{
			C: GtOrEq("gt"),
			S: "gt>=?",
			N: []string{"gt"},
		},
		{
			C: GtOrEqTuple("gt", 2),
			S: "gt>=(?,?)",
			N: []string{"gt_0", "gt_1"},
		},
		{
			C: In("in"),
			S: "in IN ?",
			N: []string{"in"},
		},
		{
			C: InTuple("in", 2),
			S: "in IN (?,?)",
			N: []string{"in_0", "in_1"},
		},
		{
			C: Contains("cnt"),
			S: "cnt CONTAINS ?",
			N: []string{"cnt"},
		},
		{
			C: ContainsTuple("cnt", 2),
			S: "cnt CONTAINS (?,?)",
			N: []string{"cnt_0", "cnt_1"},
		},
		{
			C: ContainsKey("cntKey"),
			S: "cntKey CONTAINS KEY ?",
			N: []string{"cntKey"},
		},
		{
			C: ContainsKeyTuple("cntKey", 2),
			S: "cntKey CONTAINS KEY (?,?)",
			N: []string{"cntKey_0", "cntKey_1"},
		},
		{
			C: Like("like"),
			S: "like LIKE ?",
			N: []string{"like"},
		},
		{
			C: LikeTuple("like", 2),
			S: "like LIKE (?,?)",
			N: []string{"like_0", "like_1"},
		},

		// Custom bind names
		{
			C: EqNamed("eq", "name"),
			S: "eq=?",
			N: []string{"name"},
		},
		{
			C: NeNamed("ne", "name"),
			S: "ne!=?",
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
		{
			C: ContainsKeyNamed("cntKey", "name"),
			S: "cntKey CONTAINS KEY ?",
			N: []string{"name"},
		},

		// Literals
		{
			C: EqLit("eq", "litval"),
			S: "eq=litval",
		},
		{
			C: NeLit("ne", "litval"),
			S: "ne!=litval",
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
		{
			C: NeFunc("ne", Fn("fn", "arg0", "arg1", "arg2")),
			S: "ne!=fn(?,?,?)",
			N: []string{"arg0", "arg1", "arg2"},
		},
		{
			C: LtFunc("eq", Now()),
			S: "eq<now()",
		},
		{
			C: LtOrEqFunc("eq", MaxTimeuuid("arg0")),
			S: "eq<=maxTimeuuid(?)",
			N: []string{"arg0"},
		},
		{
			C: GtFunc("eq", Now()),
			S: "eq>now()",
		},
		{
			C: GtOrEqFunc("eq", MaxTimeuuid("arg0")),
			S: "eq>=maxTimeuuid(?)",
			N: []string{"arg0"},
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
