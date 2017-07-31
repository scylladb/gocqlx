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
		N string
	}{
		{
			C: Eq("eq"),
			S: "eq=?",
			N: "eq",
		},
		{
			C: EqNamed("eq", "name"),
			S: "eq=?",
			N: "name",
		},
		{
			C: Lt("lt"),
			S: "lt<?",
			N: "lt",
		},
		{
			C: LtNamed("lt", "name"),
			S: "lt<?",
			N: "name",
		},
		{
			C: LtOrEq("lt"),
			S: "lt<=?",
			N: "lt",
		},
		{
			C: LtOrEqNamed("lt", "name"),
			S: "lt<=?",
			N: "name",
		},
		{
			C: Gt("gt"),
			S: "gt>?",
			N: "gt",
		},
		{
			C: GtNamed("gt", "name"),
			S: "gt>?",
			N: "name",
		},
		{
			C: GtOrEq("gt"),
			S: "gt>=?",
			N: "gt",
		},
		{
			C: GtOrEqNamed("gt", "name"),
			S: "gt>=?",
			N: "name",
		},
		{
			C: In("in"),
			S: "in IN ?",
			N: "in",
		},
		{
			C: InNamed("in", "name"),
			S: "in IN ?",
			N: "name",
		},
		{
			C: Contains("cnt"),
			S: "cnt CONTAINS ?",
			N: "cnt",
		},
		{
			C: ContainsNamed("cnt", "name"),
			S: "cnt CONTAINS ?",
			N: "name",
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
