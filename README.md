# gocqlx [![GoDoc](http://img.shields.io/badge/go-documentation-blue.svg?style=flat-square)](http://godoc.org/github.com/scylladb/gocqlx) [![Go Report Card](https://goreportcard.com/badge/github.com/scylladb/gocqlx)](https://goreportcard.com/report/github.com/scylladb/gocqlx) [![Build Status](https://travis-ci.org/scylladb/gocqlx.svg?branch=master)](https://travis-ci.org/scylladb/gocqlx)

Package `gocqlx` is a Scylla / Cassandra productivity toolkit for `gocql`. It's 
similar to what `sqlx` is to `database/sql`.

It contains wrappers over `gocql` types that provide convenience methods which
are useful in the development of database driven applications. Under the
hood it uses `sqlx/reflectx` package so `sqlx` models will also work with `gocqlx`.

## Installation

    go get -u github.com/scylladb/gocqlx

## Features

* Flexible `SELECT`, `INSERT`, `UPDATE` `DELETE` and `BATCH` query building using a DSL
* Support for named parameters (:identifier) in queries
* Binding parameters form struct or map
* Scanning results into structs
* Fast!

Example, see [full example here](https://github.com/scylladb/gocqlx/blob/master/example_test.go)

```go
type Person struct {
	FirstName string  // no need to add `db:"first_name"` etc.
	LastName  string
	Email     []string
}

p := &Person{
	"Patricia",
	"Citizen",
	[]string{"patricia.citzen@gocqlx_test.com"},
}

// Insert
{
    stmt, names := qb.Insert("person").Columns("first_name", "last_name", "email").ToCql()
    q := gocqlx.Query(session.Query(stmt), names)

    if err := q.BindStruct(p).Exec(); err != nil {
        log.Fatal(err)
    }
}

// Batch
{
	i := qb.Insert("person").Columns("first_name", "last_name", "email")

	stmt, names := qb.Batch().
		Add("a.", i).
		Add("b.", i).
		ToCql()
	q := gocqlx.Query(session.Query(stmt), names)

	b := struct {
		A Person
		B Person
	}{
		A: Person{
			"Igy",
			"Citizen",
			[]string{"ian.citzen@gocqlx_test.com"},
		},
		B: Person{
			"Ian",
			"Citizen",
			[]string{"igy.citzen@gocqlx_test.com"},
		},
	}

	if err := q.BindStruct(&b).Exec(); err != nil {
		t.Fatal(err)
	}
}

// Get
{
	var p Person
	if err := gocqlx.Get(&p, session.Query("SELECT * FROM gocqlx_test.person WHERE first_name=?", "Patricia")); err != nil {
		t.Fatal("get:", err)
	}
	t.Log(p)  // {Patricia Citizen [patricia.citzen@gocqlx_test.com patricia1.citzen@gocqlx_test.com]}
}

// Select
{
	stmt, names := qb.Select("gocqlx_test.person").Where(qb.In("first_name")).ToCql()
	q := gocqlx.Query(session.Query(stmt), names)

	q.BindMap(qb.M{"first_name": []string{"Patricia", "Igy", "Ian"}})
	if err := q.Err(); err != nil {
		t.Fatal(err)
	}

	var people []Person
	if err := gocqlx.Select(&people, q.Query); err != nil {
		t.Fatal("select:", err)
	}
	t.Log(people)  // [{Ian Citizen [igy.citzen@gocqlx_test.com]} {Igy Citizen [ian.citzen@gocqlx_test.com]} {Patricia Citizen [patricia.citzen@gocqlx_test.com patricia1.citzen@gocqlx_test.com]}]
}
```

## Performance

Gocqlx is fast, this is a benchmark result comparing `gocqlx` to raw `gocql` 
on a local machine. For query binding (insert) `gocqlx` is faster then `gocql` 
thanks to smart caching, otherwise the performance is comparable.

```
BenchmarkE2EGocqlInsert-4         500000            258434 ns/op            2627 B/op         59 allocs/op
BenchmarkE2EGocqlxInsert-4       1000000            120257 ns/op            1555 B/op         34 allocs/op
BenchmarkE2EGocqlGet-4           1000000            131424 ns/op            1970 B/op         55 allocs/op
BenchmarkE2EGocqlxGet-4          1000000            131981 ns/op            2322 B/op         58 allocs/op
BenchmarkE2EGocqlSelect-4          30000           2588562 ns/op           34605 B/op        946 allocs/op
BenchmarkE2EGocqlxSelect-4         30000           2637187 ns/op           27718 B/op        951 allocs/op
```

See the [benchmark here](https://github.com/scylladb/gocqlx/blob/master/benchmark_test.go).
