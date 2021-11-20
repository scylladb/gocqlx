// Copyright (C) 2017 ScyllaDB
// Use of this source code is governed by a ALv2-style
// license that can be found in the LICENSE file.

//go:build all || integration
// +build all integration

package gocqlx_test

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/scylladb/gocqlx/v2"
	. "github.com/scylladb/gocqlx/v2/gocqlxtest"
	"github.com/scylladb/gocqlx/v2/qb"
)

type benchPerson struct {
	ID        int      `json:"id"`
	FirstName string   `json:"first_name"`
	LastName  string   `json:"last_name"`
	Email     []string `json:"email"`
	Gender    string   `json:"gender"`
	IPAddress string   `json:"ip_address"`
}

var benchPersonSchema = `
CREATE TABLE IF NOT EXISTS gocqlx_test.bench_person (
    id int,
    first_name text,
    last_name text,
    email list<text>,
    gender text,
    ip_address text,
    PRIMARY KEY(id)
)`

var benchPersonCols = []string{"id", "first_name", "last_name", "email", "gender", "ip_address"}

//
// Insert
//

// BenchmarkBaseGocqlInsert performs standard insert.
func BenchmarkBaseGocqlInsert(b *testing.B) {
	people := loadFixtures()
	session := CreateSession(b)
	defer session.Close()

	if err := session.ExecStmt(benchPersonSchema); err != nil {
		b.Fatal(err)
	}

	stmt, _ := qb.Insert("gocqlx_test.bench_person").Columns(benchPersonCols...).ToCql()
	q := session.Session.Query(stmt)
	defer q.Release()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p := people[i%len(people)]
		if err := q.Bind(p.ID, p.FirstName, p.LastName, p.Email, p.Gender, p.IPAddress).Exec(); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkGocqlInsert performs insert with struct binding.
func BenchmarkGocqlxInsert(b *testing.B) {
	people := loadFixtures()
	session := CreateSession(b)
	defer session.Close()

	if err := session.ExecStmt(benchPersonSchema); err != nil {
		b.Fatal(err)
	}

	stmt, names := qb.Insert("gocqlx_test.bench_person").Columns(benchPersonCols...).ToCql()
	q := session.Query(stmt, names)
	defer q.Release()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p := people[i%len(people)]
		if err := q.BindStruct(p).Exec(); err != nil {
			b.Fatal(err)
		}
	}
}

//
// Get
//

// BenchmarkBaseGocqlGet performs standard scan.
func BenchmarkBaseGocqlGet(b *testing.B) {
	people := loadFixtures()
	session := CreateSession(b)
	defer session.Close()

	initTable(b, session, people)

	stmt, _ := qb.Select("gocqlx_test.bench_person").Columns(benchPersonCols...).Where(qb.Eq("id")).Limit(1).ToCql()
	q := session.Session.Query(stmt)
	defer q.Release()

	var p benchPerson

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q.Bind(people[i%len(people)].ID)
		if err := q.Scan(&p.ID, &p.FirstName, &p.LastName, &p.Email, &p.Gender, &p.IPAddress); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkGocqlxGet performs get.
func BenchmarkGocqlxGet(b *testing.B) {
	people := loadFixtures()
	session := CreateSession(b)
	defer session.Close()

	initTable(b, session, people)

	stmt, names := qb.Select("gocqlx_test.bench_person").Columns(benchPersonCols...).Where(qb.Eq("id")).Limit(1).ToCql()
	q := session.Query(stmt, names)
	defer q.Release()

	var p benchPerson

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q.Bind(people[i%len(people)].ID)
		if err := q.Get(&p); err != nil {
			b.Fatal(err)
		}
	}
}

//
// Select
//

// BenchmarkBaseGocqlSelect performs standard loop scan with a slice of
// pointers.
func BenchmarkBaseGocqlSelect(b *testing.B) {
	people := loadFixtures()
	session := CreateSession(b)
	defer session.Close()

	initTable(b, session, people)

	stmt, _ := qb.Select("gocqlx_test.bench_person").Columns(benchPersonCols...).Limit(100).ToCql()
	q := session.Session.Query(stmt)
	defer q.Release()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		iter := q.Iter()
		v := make([]*benchPerson, 100)
		p := new(benchPerson)
		for iter.Scan(&p.ID, &p.FirstName, &p.LastName, &p.Email, &p.Gender, &p.IPAddress) {
			v = append(v, p)
			p = new(benchPerson)
		}
		if err := iter.Close(); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkGocqlSelect performs select to a slice pointers.
func BenchmarkGocqlxSelect(b *testing.B) {
	people := loadFixtures()
	session := CreateSession(b)
	defer session.Close()

	initTable(b, session, people)

	stmt, names := qb.Select("gocqlx_test.bench_person").Columns(benchPersonCols...).Limit(100).ToCql()
	q := session.Query(stmt, names)
	defer q.Release()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var v []*benchPerson
		if err := q.Select(&v); err != nil {
			b.Fatal(err)
		}
	}
}

func loadFixtures() []*benchPerson {
	f, err := os.Open("testdata/people.json")
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := f.Close(); err != nil {
			panic(err)
		}
	}()

	var v []*benchPerson
	if err := json.NewDecoder(f).Decode(&v); err != nil {
		panic(err)
	}

	return v
}

func initTable(b *testing.B, session gocqlx.Session, people []*benchPerson) {
	if err := session.ExecStmt(benchPersonSchema); err != nil {
		b.Fatal(err)
	}

	stmt, names := qb.Insert("gocqlx_test.bench_person").Columns(benchPersonCols...).ToCql()
	q := session.Query(stmt, names)

	for _, p := range people {
		if err := q.BindStruct(p).Exec(); err != nil {
			b.Fatal(err)
		}
	}
}
