# gocqlx [![GoDoc](http://img.shields.io/badge/go-documentation-blue.svg?style=flat-square)](http://godoc.org/github.com/scylladb/gocqlx) [![Go Report Card](https://goreportcard.com/badge/github.com/scylladb/gocqlx)](https://goreportcard.com/report/github.com/scylladb/gocqlx) [![Build Status](https://travis-ci.org/scylladb/gocqlx.svg?branch=master)](https://travis-ci.org/scylladb/gocqlx)

Package `gocqlx` is a Scylla / Cassandra productivity toolkit for `gocql`. It's 
similar to what `sqlx` is to `database/sql`.

It contains wrappers over `gocql` types that provide convenience methods which
are useful in the development of database driven applications.  Under the
hood it uses `sqlx/reflectx` package so `sqlx` models will also work with `gocqlx`.

## Installation

    go get github.com/scylladb/gocqlx

## Features

Fast, boilerplate free and flexible `SELECTS`, `INSERTS`, `UPDATES` and `DELETES`.

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

// Insert with TTL
{
    stmt, names := qb.Insert("person").Columns("first_name", "last_name", "email").TTL().ToCql()
    q := gocqlx.Query(session.Query(stmt), names)

    if err := q.BindStructMap(p, qb.M{"_ttl": qb.TTL(86400 * time.Second)}).Exec(); err != nil {
        log.Fatal(err)
    }
}

// Update
{
    p.Email = append(p.Email, "patricia1.citzen@gocqlx_test.com")

    stmt, names := qb.Update("person").Set("email").Where(qb.Eq("first_name"), qb.Eq("last_name")).ToCql()
    q := gocqlx.Query(session.Query(stmt), names)

    if err := q.BindStruct(p).Exec(); err != nil {
        log.Fatal(err)
    }
}

// Select
{
    stmt, names := qb.Select("person").Where(qb.In("first_name")).ToCql()
    q := gocqlx.Query(session.Query(stmt), names)

    q.BindMap(qb.M{"first_name": []string{"Patricia", "John"}})
    if err := q.Err(); err != nil {
        log.Fatal(err)
    }

    var people []Person
    if err := gocqlx.Select(&people, q.Query); err != nil {
        log.Fatal("select:", err)
    }
    log.Println(people)

    // [{Patricia Citizen [patricia.citzen@com patricia1.citzen@com]} {John Doe [johndoeDNE@gmail.net]}]
}
```

For more details see [example test](https://github.com/scylladb/gocqlx/blob/master/example_test.go).

## Performance

Gocqlx is fast, below is a benchmark result comparing `gocqlx` to raw `gocql` on
my machine, see the benchmark [here](https://github.com/scylladb/gocqlx/blob/master/benchmark_test.go).

For query binding gocqlx is faster as it does not require parameter rewriting 
while binding. For get and insert the performance is comparable.

```
BenchmarkE2EGocqlInsert-4         500000            258434 ns/op            2627 B/op         59 allocs/op
BenchmarkE2EGocqlxInsert-4       1000000            120257 ns/op            1555 B/op         34 allocs/op
BenchmarkE2EGocqlGet-4           1000000            131424 ns/op            1970 B/op         55 allocs/op
BenchmarkE2EGocqlxGet-4          1000000            131981 ns/op            2322 B/op         58 allocs/op
BenchmarkE2EGocqlSelect-4          30000           2588562 ns/op           34605 B/op        946 allocs/op
BenchmarkE2EGocqlxSelect-4         30000           2637187 ns/op           27718 B/op        951 allocs/op
```

Gocqlx comes with automatic snake case support for field names and does not 
require manual tagging. This is also fast, below is a comparison to 
`strings.ToLower` function (`sqlx` default).

```
BenchmarkSnakeCase-4            10000000               124 ns/op              32 B/op          2 allocs/op
BenchmarkToLower-4              100000000               57.9 ns/op             0 B/op          0 allocs/op
```

Building queries is fast and low on allocations too.

```
BenchmarkCmp-4                   3000000               464 ns/op             112 B/op          3 allocs/op
BenchmarkDeleteBuilder-4        10000000               214 ns/op             112 B/op          2 allocs/op
BenchmarkInsertBuilder-4        20000000               103 ns/op              64 B/op          1 allocs/op
BenchmarkSelectBuilder-4        10000000               214 ns/op             112 B/op          2 allocs/op
BenchmarkUpdateBuilder-4        10000000               212 ns/op             112 B/op          2 allocs/op
```

Enyoy!
