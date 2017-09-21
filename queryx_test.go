// Copyright (C) 2017 ScyllaDB
// Use of this source code is governed by a ALv2-style
// license that can be found in the LICENSE file.

// +build !integration

package gocqlx

import (
	"testing"

	"github.com/gocql/gocql"
	"github.com/google/go-cmp/cmp"
)

func TestCompileQuery(t *testing.T) {
	table := []struct {
		Q, R string
		V    []string
	}{
		// Basic test for named parameters, invalid char ',' terminating
		{
			Q: `INSERT INTO foo (a,b,c,d) VALUES (:name, :age, :first, :last)`,
			R: `INSERT INTO foo (a,b,c,d) VALUES (?, ?, ?, ?)`,
			V: []string{"name", "age", "first", "last"},
		},
		// This query tests a named parameter ending the string as well as numbers
		{
			Q: `SELECT * FROM a WHERE first_name=:name1 AND last_name=:name2`,
			R: `SELECT * FROM a WHERE first_name=? AND last_name=?`,
			V: []string{"name1", "name2"},
		},
		{
			Q: `SELECT "::foo" FROM a WHERE first_name=:name1 AND last_name=:name2`,
			R: `SELECT ":foo" FROM a WHERE first_name=? AND last_name=?`,
			V: []string{"name1", "name2"},
		},
		{
			Q: `SELECT 'a::b::c' || first_name, '::::ABC::_::' FROM person WHERE first_name=:first_name AND last_name=:last_name`,
			R: `SELECT 'a:b:c' || first_name, '::ABC:_:' FROM person WHERE first_name=? AND last_name=?`,
			V: []string{"first_name", "last_name"},
		},
		/* This unicode awareness test sadly fails, because of our byte-wise worldview.
		 * We could certainly iterate by Rune instead, though it's a great deal slower,
		 * it's probably the RightWay(tm)
		{
			Q: `INSERT INTO foo (a,b,c,d) VALUES (:あ, :b, :キコ, :名前)`,
			R: `INSERT INTO foo (a,b,c,d) VALUES (?, ?, ?, ?)`,
		},
		*/
	}

	for _, test := range table {
		qr, names, err := CompileNamedQuery([]byte(test.Q))
		if err != nil {
			t.Error(err)
		}
		if qr != test.R {
			t.Error("expected", test.R, "got", qr)
		}
		if diff := cmp.Diff(names, test.V); diff != "" {
			t.Error("names mismatch", diff)
		}
	}
}

func BenchmarkCompileNamedQuery(b *testing.B) {
	q := []byte("INSERT INTO cycling.cyclist_name (id, user_uuid, firstname, stars) VALUES (:id, :user_uuid, :firstname, :stars)")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		CompileNamedQuery(q)
	}
}

func TestBindStruct(t *testing.T) {
	v := &struct {
		Name  string
		Age   int
		First string
		Last  string
	}{
		Name:  "name",
		Age:   30,
		First: "first",
		Last:  "last",
	}

	t.Run("simple", func(t *testing.T) {
		names := []string{"name", "age", "first", "last"}
		args, err := bindStructArgs(names, v, nil, DefaultMapper)
		if err != nil {
			t.Fatal(err)
		}

		if diff := cmp.Diff(args, []interface{}{"name", 30, "first", "last"}); diff != "" {
			t.Error("args mismatch", diff)
		}
	})

	t.Run("error", func(t *testing.T) {
		names := []string{"name", "age", "first", "not_found"}
		_, err := bindStructArgs(names, v, nil, DefaultMapper)
		if err == nil {
			t.Fatal("unexpected error")
		}
	})

	t.Run("fallback", func(t *testing.T) {
		names := []string{"name", "age", "first", "not_found"}
		m := map[string]interface{}{
			"not_found": "last",
		}
		args, err := bindStructArgs(names, v, m, DefaultMapper)
		if err != nil {
			t.Fatal(err)
		}

		if diff := cmp.Diff(args, []interface{}{"name", 30, "first", "last"}); diff != "" {
			t.Error("args mismatch", diff)
		}
	})

	t.Run("fallback error", func(t *testing.T) {
		names := []string{"name", "age", "first", "not_found", "really_not_found"}
		m := map[string]interface{}{
			"not_found": "last",
		}
		_, err := bindStructArgs(names, v, m, DefaultMapper)
		if err == nil {
			t.Fatal("unexpected error")
		}
	})
}

func BenchmarkBindStruct(b *testing.B) {
	q := Query(&gocql.Query{}, []string{"name", "age", "first", "last"})
	type t struct {
		Name  string
		Age   int
		First string
		Last  string
	}
	am := t{"Jason Moiron", 30, "Jason", "Moiron"}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q.BindStruct(am)
	}
}

func TestBindMap(t *testing.T) {
	v := map[string]interface{}{
		"name":  "name",
		"age":   30,
		"first": "first",
		"last":  "last",
	}

	t.Run("simple", func(t *testing.T) {
		names := []string{"name", "age", "first", "last"}
		args, err := bindMapArgs(names, v)
		if err != nil {
			t.Fatal(err)
		}

		if diff := cmp.Diff(args, []interface{}{"name", 30, "first", "last"}); diff != "" {
			t.Error("args mismatch", diff)
		}
	})

	t.Run("error", func(t *testing.T) {
		names := []string{"name", "first", "not_found"}
		_, err := bindMapArgs(names, v)
		if err == nil {
			t.Fatal("unexpected error")
		}
	})
}

func BenchmarkBindMap(b *testing.B) {
	q := Queryx{
		Query: &gocql.Query{},
		Names: []string{"name", "age", "first", "last"},
	}
	am := map[string]interface{}{
		"name":  "Jason Moiron",
		"age":   30,
		"first": "Jason",
		"last":  "Moiron",
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q.BindMap(am)
	}
}
