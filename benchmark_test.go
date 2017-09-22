// Copyright (C) 2017 ScyllaDB
// Use of this source code is governed by a ALv2-style
// license that can be found in the LICENSE file.

// +build all integration

package gocqlx_test

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/gocql/gocql"
	"github.com/scylladb/gocqlx"
	"github.com/scylladb/gocqlx/qb"
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

//
// Insert
//

// BenchmarkE2EGocqlInsert performs standard insert.
func BenchmarkE2EGocqlInsert(b *testing.B) {
	people := loadFixtures()
	session := createSession(b)
	defer session.Close()

	if err := createTable(session, benchPersonSchema); err != nil {
		b.Fatal(err)
	}

	stmt, _ := qb.Insert("gocqlx_test.bench_person").Columns(benchPersonCols...).ToCql()
	q := session.Query(stmt)
	defer q.Release()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// prepare
		p := people[i%len(people)]
		if err := q.Bind(p.ID, p.FirstName, p.LastName, p.Email, p.Gender, p.IPAddress).Exec(); err != nil {
			b.Fatal(err)
		}
		// insert
		if err := q.Exec(); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkE2EGocqlInsert performs insert with struct binding.
func BenchmarkE2EGocqlxInsert(b *testing.B) {
	people := loadFixtures()
	session := createSession(b)
	defer session.Close()

	if err := createTable(session, benchPersonSchema); err != nil {
		b.Fatal(err)
	}

	stmt, names := qb.Insert("gocqlx_test.bench_person").Columns(benchPersonCols...).ToCql()
	q := gocqlx.Query(session.Query(stmt), names)
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

// BenchmarkE2EGocqlGet performs standard scan.
func BenchmarkE2EGocqlGet(b *testing.B) {
	people := loadFixtures()
	session := createSession(b)
	defer session.Close()

	initTable(b, session, people)

	stmt, _ := qb.Select("gocqlx_test.bench_person").Columns(benchPersonCols...).Where(qb.Eq("id")).Limit(1).ToCql()
	var p benchPerson

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// prepare
		q := session.Query(stmt)
		q.Bind(people[i%len(people)].ID)
		// scan
		if err := q.Scan(&p.ID, &p.FirstName, &p.LastName, &p.Email, &p.Gender, &p.IPAddress); err != nil {
			b.Fatal(err)
		}
		// release
		q.Release()
	}
}

// BenchmarkE2EGocqlxGet performs get.
func BenchmarkE2EGocqlxGet(b *testing.B) {
	people := loadFixtures()
	session := createSession(b)
	defer session.Close()

	initTable(b, session, people)

	stmt, _ := qb.Select("gocqlx_test.bench_person").Columns(benchPersonCols...).Where(qb.Eq("id")).Limit(1).ToCql()
	var p benchPerson

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// prepare
		q := session.Query(stmt)
		q.Bind(people[i%len(people)].ID)
		// get
		gocqlx.Get(&p, q)
	}
}

//
// Select
//

// BenchmarkE2EGocqlSelect performs standard loop scan.
func BenchmarkE2EGocqlSelect(b *testing.B) {
	people := loadFixtures()
	session := createSession(b)
	defer session.Close()

	initTable(b, session, people)

	stmt, _ := qb.Select("gocqlx_test.bench_person").Columns(benchPersonCols...).Limit(100).ToCql()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// prepare
		v := make([]*benchPerson, 100)
		q := session.Query(stmt)
		i := q.Iter()
		// loop scan
		p := new(benchPerson)
		for i.Scan(&p.ID, &p.FirstName, &p.LastName, &p.Email, &p.Gender, &p.IPAddress) {
			v = append(v, p)
			p = new(benchPerson)
		}
		if err := i.Close(); err != nil {
			b.Fatal(err)
		}
		// release
		q.Release()
	}
}

// BenchmarkE2EGocqlSelect performs select.
func BenchmarkE2EGocqlxSelect(b *testing.B) {
	people := loadFixtures()
	session := createSession(b)
	defer session.Close()

	initTable(b, session, people)

	stmt, _ := qb.Select("gocqlx_test.bench_person").Columns(benchPersonCols...).Limit(100).ToCql()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// prepare
		q := session.Query(stmt)
		var v []*benchPerson
		// select
		if err := gocqlx.Select(&v, q); err != nil {
			b.Fatal(err)
		}
	}
}

func initTable(b *testing.B, session *gocql.Session, people []*benchPerson) {
	if err := createTable(session, benchPersonSchema); err != nil {
		b.Fatal(err)
	}

	stmt, names := qb.Insert("gocqlx_test.bench_person").Columns(benchPersonCols...).ToCql()
	q := gocqlx.Query(session.Query(stmt), names)

	for _, p := range people {
		if err := q.BindStruct(p).Exec(); err != nil {
			b.Fatal(err)
		}
	}
}
