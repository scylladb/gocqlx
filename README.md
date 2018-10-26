# GoCQLX [![GoDoc](http://img.shields.io/badge/go-documentation-blue.svg?style=flat-square)](http://godoc.org/github.com/scylladb/gocqlx) [![Go Report Card](https://goreportcard.com/badge/github.com/scylladb/gocqlx)](https://goreportcard.com/report/github.com/scylladb/gocqlx) [![Build Status](https://travis-ci.org/scylladb/gocqlx.svg?branch=master)](https://travis-ci.org/scylladb/gocqlx)

Package `gocqlx` is an idiomatic extension to `gocql` that provides usability features. With gocqlx you can bind the query parameters from maps and structs, use named query parameters (:identifier) and scan the query results into structs and slices. It comes with a fluent and flexible CQL query builder and a database migrations module.

## Installation

    go get -u github.com/scylladb/gocqlx

## Features

* Binding query parameters form struct or map
* Scanning results directly into struct or slice
* CQL query builder ([package qb](https://github.com/scylladb/gocqlx/blob/master/qb))
* Super simple CRUD operations based on table model ([package table](https://github.com/scylladb/gocqlx/blob/master/table))
* Database migrations ([package migrate](https://github.com/scylladb/gocqlx/blob/master/migrate))
* Fast!

## Example

```go
// Person represents a row in person table.
// Field names are converted to camel case by default, no need to add special tags.
// If you want to disable a field add `db:"-"` tag, it will not be persisted.
type Person struct {
    FirstName string
    LastName  string
    Email     []string
}

// Insert, bind data from struct.
{
    stmt, names := qb.Insert("gocqlx_test.person").Columns("first_name", "last_name", "email").ToCql()
    q := gocqlx.Query(session.Query(stmt), names).BindStruct(p)

    if err := q.ExecRelease(); err != nil {
        t.Fatal(err)
    }
}
// Get first result into a struct.
{
    var p Person
    stmt, names := qb.Select("gocqlx_test.person").Where(qb.Eq("first_name")).ToCql()
    q := gocqlx.Query(session.Query(stmt), names).BindMap(qb.M{
        "first_name": "Patricia",
    })
    if err := q.GetRelease(&p); err != nil {
        t.Fatal(err)
    }
}
// Load all the results into a slice.
{
    var people []Person
    stmt, names := qb.Select("gocqlx_test.person").Where(qb.In("first_name")).ToCql()
    q := gocqlx.Query(session.Query(stmt), names).BindMap(qb.M{
        "first_name": []string{"Patricia", "Igy", "Ian"},
    })
    if err := q.SelectRelease(&people); err != nil {
        t.Fatal(err)
    }
}

// metadata specifies table name and columns it must be in sync with schema.
var personMetadata = table.Metadata{
    Name:    "person",
    Columns: []string{"first_name", "last_name", "email"},
    PartKey: []string{"first_name"},
    SortKey: []string{"last_name"},
}

// personTable allows for simple CRUD operations based on personMetadata.
var personTable = table.New(personMetadata)

// Get by primary key.
{
    p := Person{
        "Patricia",
        "Citizen",
        nil, // no email
    }
    stmt, names := personTable.Get() // you can filter columns too
    q := gocqlx.Query(session.Query(stmt), names).BindStruct(p)
    if err := q.GetRelease(&p); err != nil {
        t.Fatal(err)
    }
}
```

See more examples in [example_test.go](https://github.com/scylladb/gocqlx/blob/master/example_test.go) and [table/example_test.go](https://github.com/scylladb/gocqlx/blob/master/table/example_test.go).

## Performance

Gocqlx is fast, this is a benchmark result comparing `gocqlx` to raw `gocql` 
on a local machine, Intel(R) Core(TM) i7-7500U CPU @ 2.70GHz.

```
BenchmarkE2EGocqlInsert            20000             86713 ns/op            2030 B/op         33 allocs/op
BenchmarkE2EGocqlxInsert           20000             87882 ns/op            2030 B/op         33 allocs/op
BenchmarkE2EGocqlGet               20000             94308 ns/op            1504 B/op         29 allocs/op
BenchmarkE2EGocqlxGet              20000             95722 ns/op            2128 B/op         33 allocs/op
BenchmarkE2EGocqlSelect             1000           1792469 ns/op           43595 B/op        921 allocs/op
BenchmarkE2EGocqlxSelect            1000           1839574 ns/op           36986 B/op        927 allocs/op
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
