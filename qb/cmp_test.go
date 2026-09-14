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
			N: []string{"ne[0]", "ne[1]", "ne[2]"},
		},
		{
			C: Lt("lt"),
			S: "lt<?",
			N: []string{"lt"},
		},
		{
			C: LtTuple("lt", 2),
			S: "lt<(?,?)",
			N: []string{"lt[0]", "lt[1]"},
		},
		{
			C: LtOrEq("lt"),
			S: "lt<=?",
			N: []string{"lt"},
		},
		{
			C: LtOrEqTuple("lt", 2),
			S: "lt<=(?,?)",
			N: []string{"lt[0]", "lt[1]"},
		},
		{
			C: Gt("gt"),
			S: "gt>?",
			N: []string{"gt"},
		},
		{
			C: GtTuple("gt", 2),
			S: "gt>(?,?)",
			N: []string{"gt[0]", "gt[1]"},
		},
		{
			C: GtOrEq("gt"),
			S: "gt>=?",
			N: []string{"gt"},
		},
		{
			C: GtOrEqTuple("gt", 2),
			S: "gt>=(?,?)",
			N: []string{"gt[0]", "gt[1]"},
		},
		{
			C: In("in"),
			S: "in IN ?",
			N: []string{"in"},
		},
		{
			C: InTuple("in", 2),
			S: "in IN (?,?)",
			N: []string{"in[0]", "in[1]"},
		},
		{
			C: Contains("cnt"),
			S: "cnt CONTAINS ?",
			N: []string{"cnt"},
		},
		{
			C: ContainsTuple("cnt", 2),
			S: "cnt CONTAINS (?,?)",
			N: []string{"cnt[0]", "cnt[1]"},
		},
		{
			C: ContainsKey("cntKey"),
			S: "cntKey CONTAINS KEY ?",
			N: []string{"cntKey"},
		},
		{
			C: ContainsKeyTuple("cntKey", 2),
			S: "cntKey CONTAINS KEY (?,?)",
			N: []string{"cntKey[0]", "cntKey[1]"},
		},
		{
			C: Like("like"),
			S: "like LIKE ?",
			N: []string{"like"},
		},
		{
			C: LikeTuple("like", 2),
			S: "like LIKE (?,?)",
			N: []string{"like[0]", "like[1]"},
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
		{
			C: LikeTupleNamed("like", 2, "name"),
			S: "like LIKE (?,?)",
			N: []string{"name[0]", "name[1]"},
		},
		// Custom bind names on tuples
		{
			C: EqTupleNamed("eq", 2, "name"),
			S: "eq=(?,?)",
			N: []string{"name[0]", "name[1]"},
		},
		{
			C: NeTupleNamed("ne", 3, "name"),
			S: "ne!=(?,?,?)",
			N: []string{"name[0]", "name[1]", "name[2]"},
		},
		{
			C: LtTupleNamed("lt", 2, "name"),
			S: "lt<(?,?)",
			N: []string{"name[0]", "name[1]"},
		},
		{
			C: LtOrEqTupleNamed("lt", 2, "name"),
			S: "lt<=(?,?)",
			N: []string{"name[0]", "name[1]"},
		},
		{
			C: GtTupleNamed("gt", 2, "name"),
			S: "gt>(?,?)",
			N: []string{"name[0]", "name[1]"},
		},
		{
			C: GtOrEqTupleNamed("gt", 2, "name"),
			S: "gt>=(?,?)",
			N: []string{"name[0]", "name[1]"},
		},
		{
			C: InTupleNamed("in", 2, "name"),
			S: "in IN (?,?)",
			N: []string{"name[0]", "name[1]"},
		},
		{
			C: ContainsTupleNamed("cnt", 2, "name"),
			S: "cnt CONTAINS (?,?)",
			N: []string{"name[0]", "name[1]"},
		},
		{
			C: ContainsKeyTupleNamed("cntKey", 2, "name"),
			S: "cntKey CONTAINS KEY (?,?)",
			N: []string{"name[0]", "name[1]"},
		},

		// Literals
		{
			C: EqLit("eq", "litval"),
			S: "eq=litval",
		},
		{
			C: EqLitString("eq", "O'Brien"),
			S: "eq='O''Brien'",
		},
		{
			C: NeLit("ne", "litval"),
			S: "ne!=litval",
		},
		{
			C: NeLitString("ne", "O'Brien"),
			S: "ne!='O''Brien'",
		},
		{
			C: LtLit("lt", "litval"),
			S: "lt<litval",
		},
		{
			C: LtLitString("lt", "O'Brien"),
			S: "lt<'O''Brien'",
		},
		{
			C: LtOrEqLit("lt", "litval"),
			S: "lt<=litval",
		},
		{
			C: LtOrEqLitString("lt", "O'Brien"),
			S: "lt<='O''Brien'",
		},
		{
			C: GtLit("gt", "litval"),
			S: "gt>litval",
		},
		{
			C: GtLitString("gt", "O'Brien"),
			S: "gt>'O''Brien'",
		},
		{
			C: GtOrEqLit("gt", "litval"),
			S: "gt>=litval",
		},
		{
			C: GtOrEqLitString("gt", "O'Brien"),
			S: "gt>='O''Brien'",
		},
		{
			C: InLit("in", "litval"),
			S: "in IN litval",
		},
		{
			C: InLitString("in", "O'Brien"),
			S: "in IN ('O''Brien')",
		},
		{
			C: InLitStrings("in", "O'Brien", "RUNNING"),
			S: "in IN ('O''Brien','RUNNING')",
		},
		{
			C: ContainsLit("cnt", "litval"),
			S: "cnt CONTAINS litval",
		},
		{
			C: ContainsLitString("cnt", "O'Brien"),
			S: "cnt CONTAINS 'O''Brien'",
		},
		{
			C: ContainsKeyLit("cntKey", "litval"),
			S: "cntKey CONTAINS KEY litval",
		},
		{
			C: ContainsKeyLitString("cntKey", "O'Brien"),
			S: "cntKey CONTAINS KEY 'O''Brien'",
		},
		{
			C: LikeLit("like", "litval"),
			S: "like LIKE litval",
		},
		{
			C: LikeLitString("like", "O'B%"),
			S: "like LIKE 'O''B%'",
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

func TestInLitStringsRequiresLiteral(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("InLitStrings() should panic with no literals")
		}
	}()

	InLitStrings("in")
}
