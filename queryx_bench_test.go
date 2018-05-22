// Copyright (C) 2017 ScyllaDB
// Use of this source code is governed by a ALv2-style
// license that can be found in the LICENSE file.

package gocqlx_test

import (
	"testing"

	"github.com/gocql/gocql"
	"github.com/scylladb/gocqlx"
)

func BenchmarkCompileNamedQuery(b *testing.B) {
	q := []byte("INSERT INTO cycling.cyclist_name (id, user_uuid, firstname, stars) VALUES (:id, :user_uuid, :firstname, :stars)")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		gocqlx.CompileNamedQuery(q)
	}
}

func BenchmarkBindStruct(b *testing.B) {
	q := gocqlx.Query(&gocql.Query{}, []string{"name", "age", "first", "last"})
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

func BenchmarkBindMap(b *testing.B) {
	q := gocqlx.Queryx{
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
