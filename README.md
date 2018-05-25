# GoCQLX [![GoDoc](http://img.shields.io/badge/go-documentation-blue.svg?style=flat-square)](http://godoc.org/github.com/scylladb/gocqlx) [![Go Report Card](https://goreportcard.com/badge/github.com/scylladb/gocqlx)](https://goreportcard.com/report/github.com/scylladb/gocqlx) [![Build Status](https://travis-ci.org/scylladb/gocqlx.svg?branch=master)](https://travis-ci.org/scylladb/gocqlx)

Package `gocqlx` is an idiomatic extension to `gocql` that provides usability features. With gocqlx you can bind the query parameters from maps and structs, use named query parameters (:identifier) and scan the query results into structs and slices. It comes with a fluent and flexible CQL query builder and a database migrations module.

## Installation

    go get -u github.com/scylladb/gocqlx

## Features

* Binding query parameters form struct or map
* Scanning results directly into struct or slice
* CQL query builder ([see more](https://github.com/scylladb/gocqlx/blob/master/qb))
* Database migrations ([see more](https://github.com/scylladb/gocqlx/blob/master/migrate))
* Fast!

## Example

```go
// Field names are converted to camel case by default, no need to add
// `db:"first_name"`, if you want to disable a filed add `db:"-"` tag.
type Person struct {
    FirstName string
    LastName  string
    Email     []string
}

// Bind query parameters from a struct.
{
    p := Person{
        "Patricia",
        "Citizen",
        []string{"patricia.citzen@gocqlx_test.com"},
    }

    stmt, names := qb.Insert("gocqlx_test.person").
        Columns("first_name", "last_name", "email").
        ToCql()

    err := gocqlx.Query(session.Query(stmt), names).BindStruct(&p).ExecRelease()
    if err != nil {
        t.Fatal(err)
    }
}

// Load the first result into a struct.
{
    stmt, names := qb.Select("gocqlx_test.person").
        Where(qb.Eq("first_name")).
        ToCql()

    var p Person

    q := gocqlx.Query(session.Query(stmt), names).BindMap(qb.M{
        "first_name": "Patricia",
    })
    if err := q.GetRelease(&p); err != nil {
        t.Fatal(err)
    }
}

// Load all the results into a slice.
{
    stmt, names := qb.Select("gocqlx_test.person").
        Where(qb.In("first_name")).
        ToCql()

    var people []Person

    q := gocqlx.Query(session.Query(stmt), names).BindMap(qb.M{
        "first_name": []string{"Patricia", "Igy", "Ian"},
    })
    if err := q.SelectRelease(&people); err != nil {
        t.Fatal(err)
    }
}

// Use named query parameters.
{
    p := &Person{
        "Jane",
        "Citizen",
        []string{"jane.citzen@gocqlx_test.com"},
    }

    stmt, names, err := gocqlx.CompileNamedQuery([]byte("INSERT INTO gocqlx_test.person (first_name, last_name, email) VALUES (:first_name, :last_name, :email)"))
    if err != nil {
        t.Fatal(err)
    }

    err = gocqlx.Query(session.Query(stmt), names).BindStruct(p).ExecRelease()
    if err != nil {
        t.Fatal(err)
    }
}
```

See more examples in [example_test.go](https://github.com/scylladb/gocqlx/blob/master/example_test.go).

## Performance

Gocqlx is fast, this is a benchmark result comparing `gocqlx` to raw `gocql` 
on a local machine, Intel(R) Core(TM) i7-7500U CPU @ 2.70GHz.

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

Apache®, Apache Cassandra® are either registered trademarks or trademarks of 
the Apache Software Foundation in the United States and/or other countries. 
No endorsement by The Apache Software Foundation is implied by the use of these marks.

GitHub star is always appreciated!
