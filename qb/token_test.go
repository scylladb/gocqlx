// Copyright (C) 2017 ScyllaDB
// Use of this source code is governed by a ALv2-style
// license that can be found in the LICENSE file.

package qb

import (
	"bytes"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestToken(t *testing.T) {
	table := []struct {
		C Cmp
		S string
		N []string
	}{
		// Basic comparators
		{
			C: Token("a", "b").Eq(),
			S: "token(a,b)=token(?,?)",
			N: []string{"a", "b"},
		},
		{
			C: Token("a", "b").Lt(),
			S: "token(a,b)<token(?,?)",
			N: []string{"a", "b"},
		},
		{
			C: Token("a", "b").LtOrEq(),
			S: "token(a,b)<=token(?,?)",
			N: []string{"a", "b"},
		},
		{
			C: Token("a", "b").Gt(),
			S: "token(a,b)>token(?,?)",
			N: []string{"a", "b"},
		},
		{
			C: Token("a", "b").GtOrEq(),
			S: "token(a,b)>=token(?,?)",
			N: []string{"a", "b"},
		},

		// Custom bind names
		{
			C: Token("a", "b").EqNamed("c", "d"),
			S: "token(a,b)=token(?,?)",
			N: []string{"c", "d"},
		},
		{
			C: Token("a", "b").LtNamed("c", "d"),
			S: "token(a,b)<token(?,?)",
			N: []string{"c", "d"},
		},
		{
			C: Token("a", "b").LtOrEqNamed("c", "d"),
			S: "token(a,b)<=token(?,?)",
			N: []string{"c", "d"},
		},
		{
			C: Token("a", "b").GtNamed("c", "d"),
			S: "token(a,b)>token(?,?)",
			N: []string{"c", "d"},
		},
		{
			C: Token("a", "b").GtOrEqNamed("c", "d"),
			S: "token(a,b)>=token(?,?)",
			N: []string{"c", "d"},
		},
		{
			C: Token("a", "b").EqValue(),
			S: "token(a,b)=?",
			N: []string{"token"},
		},
		{
			C: Token("a", "b").EqValueNamed("c"),
			S: "token(a,b)=?",
			N: []string{"c"},
		},
		{
			C: Token("a", "b").LtValue(),
			S: "token(a,b)<?",
			N: []string{"token"},
		},
		{
			C: Token("a", "b").LtValueNamed("c"),
			S: "token(a,b)<?",
			N: []string{"c"},
		},
		{
			C: Token("a", "b").LtOrEqValue(),
			S: "token(a,b)<=?",
			N: []string{"token"},
		},
		{
			C: Token("a", "b").LtOrEqValueNamed("c"),
			S: "token(a,b)<=?",
			N: []string{"c"},
		},
		{
			C: Token("a", "b").GtValue(),
			S: "token(a,b)>?",
			N: []string{"token"},
		},
		{
			C: Token("a", "b").GtValueNamed("c"),
			S: "token(a,b)>?",
			N: []string{"c"},
		},
		{
			C: Token("a", "b").GtOrEqValue(),
			S: "token(a,b)>=?",
			N: []string{"token"},
		},
		{
			C: Token("a", "b").GtOrEqValueNamed("c"),
			S: "token(a,b)>=?",
			N: []string{"c"},
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
