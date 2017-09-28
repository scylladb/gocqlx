# gocqlx [![GoDoc](http://img.shields.io/badge/go-documentation-blue.svg?style=flat-square)](http://godoc.org/github.com/scylladb/gocqlx) [![Go Report Card](https://goreportcard.com/badge/github.com/scylladb/gocqlx)](https://goreportcard.com/report/github.com/scylladb/gocqlx) [![Build Status](https://travis-ci.org/scylladb/gocqlx.svg?branch=master)](https://travis-ci.org/scylladb/gocqlx)

Package `gocqlx` is a productivity toolkit for ScyllaDB and Apache Cassandra®. 
It's an extension of `gocql`, similar to what `sqlx` is to `database/sql`.

It contains wrappers over `gocql` types that provide convenience methods which
are useful in the development of database driven applications. Under the
hood it uses `sqlx/reflectx` package so `sqlx` models will also work with `gocqlx`.

## Installation

    go get -u github.com/scylladb/gocqlx

## Features

* Builders for `SELECT`, `INSERT`, `UPDATE` `DELETE` and `BATCH` (supporting collections, counters and functions)
* Queries with named parameters (:identifier) support
* Binding parameters form struct or map
* Scanning results into structs and slices
* Automatic query releasing
* Schema migrations
* Fast!

## Example

```go
type Person struct {
    FirstName string  // no need to add `db:"first_name"` etc.
    LastName  string
    Email     []string
}

// Insert with query parameters bound from struct.
{
    p := &Person{
        "Patricia",
        "Citizen",
        []string{"patricia.citzen@gocqlx_test.com"},
    }

    stmt, names := qb.Insert("gocqlx_test.person").
        Columns("first_name", "last_name", "email").
        ToCql()

    q := gocqlx.Query(session.Query(stmt), names).BindStruct(p)

    if err := q.ExecRelease(); err != nil {
        t.Fatal(err)
    }
}

// Get the first result into a struct.
{
    stmt, names := qb.Select("gocqlx_test.person").
        Where(qb.Eq("first_name")).
        ToCql()

    q := gocqlx.Query(session.Query(stmt), names).BindMap(qb.M{
        "first_name": "Patricia",
    })

    var p Person
    if err := gocqlx.Get(&p, q.Query); err != nil {
        t.Fatal("get:", err)
    }

    t.Log(p)
    // {Patricia Citizen [patricia.citzen@gocqlx_test.com patricia1.citzen@gocqlx_test.com]}
}

// Select, load all the results into a slice.
{
    stmt, names := qb.Select("gocqlx_test.person").
        Where(qb.In("first_name")).
        ToCql()

    q := gocqlx.Query(session.Query(stmt), names).BindMap(qb.M{
        "first_name": []string{"Patricia", "Igy", "Ian"},
    })

    var people []Person
    if err := gocqlx.Select(&people, q.Query); err != nil {
        t.Fatal("select:", err)
    }

    t.Log(people)
    // [{Patricia Citizen [patricia.citzen@gocqlx_test.com patricia1.citzen@gocqlx_test.com]} {Igy Citizen [igy.citzen@gocqlx_test.com]} {Ian Citizen [ian.citzen@gocqlx_test.com]}]
}
```

See more examples in [example_test.go](https://github.com/scylladb/gocqlx/blob/master/example_test.go).

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

See the benchmark in [benchmark_test.go](https://github.com/scylladb/gocqlx/blob/master/benchmark_test.go).

## License

Copyright (C) 2017 ScyllaDB

This project is distributed under the Apache 2.0 license. See the [LICENSE](https://github.com/scylladb/gocqlx/blob/master/LICENSE) file for details.
It contains software from:

* [gocql project](https://github.com/gocql/gocql), licensed under the BSD license
* [sqlx project](https://github.com/jmoiron/sqlx), licensed under the MIT license

Apache®, Apache Cassandra®,  are either registered trademarks or trademarks of 
the Apache Software Foundation in the United States and/or other countries. 
No endorsement by The Apache Software Foundation is implied by the use of these marks.

GitHub star is always appreciated!