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

		// Functions
		{
			C: Eq("eq").Func("fn", "arg0", "arg1"),
			S: "eq=fn(?,?)",
			N: []string{"arg0", "arg1"},
		},
		{
			C: Eq("eq").MaxTimeuuid("arg0"),
			S: "eq=maxTimeuuid(?)",
			N: []string{"arg0"},
		},
		{
			C: Eq("eq").MinTimeuuid("arg0"),
			S: "eq=minTimeuuid(?)",
			N: []string{"arg0"},
		},
		{
			C: Eq("eq").Now(),
			S: "eq=now()",
		},
		{
			C: Eq("eq").Token("arg0", "arg1"),
			S: "eq=token(?,?)",
			N: []string{"arg0", "arg1"},
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

func BenchmarkCmp(b *testing.B) {
	buf := bytes.Buffer{}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf.Reset()
		c := cmps{
			Eq("id"),
			Lt("user_uuid"),
			LtOrEq("firstname"),
			Gt("stars"),
		}
		c.writeCql(&buf)
	}
}
