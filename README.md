# 🚀 GocqlX [![GoDoc](https://pkg.go.dev/badge/github.com/scylladb/gocqlx/v3.svg)](https://pkg.go.dev/github.com/scylladb/gocqlx/v3) [![Go Report Card](https://goreportcard.com/badge/github.com/scylladb/gocqlx)](https://goreportcard.com/report/github.com/scylladb/gocqlx) [![Build Status](https://travis-ci.org/scylladb/gocqlx.svg?branch=master)](https://travis-ci.org/scylladb/gocqlx)

GocqlX makes working with Scylla easy and less error-prone.
It’s inspired by [Sqlx](https://github.com/jmoiron/sqlx), a tool for working with SQL databases, but it goes beyond what Sqlx provides.

## Compatibility

Versions of GocqlX prior to v3.0.0 are compatible with both [Apache Cassandra’s gocql](https://github.com/apache/cassandra-gocql-driver) and [ScyllaDB’s fork](https://github.com/scylladb/gocql).
However, starting with v3.0.0, GocqlX exclusively supports the scylladb/gocql driver.
If you are using GocqlX v3.0.0 or newer, you must ensure your `go.mod` includes a replace directive to point to ScyllaDB’s fork:

```go
// Use the latest version of scylladb/gocql; check for updates at https://github.com/scylladb/gocql/releases
replace github.com/gocql/gocql => github.com/scylladb/gocql v1.15.3
```

This is required because GocqlX relies on ScyllaDB-specific extensions and bug fixes introduced in the gocql fork. Attempting to use the standard gocql driver with GocqlX v3.0.0+ may lead to build or runtime issues.

## Features

* Binding query parameters from struct fields, map, or both
* Scanning query results into structs based on field names
* Convenient functions for common tasks such as loading a single row into a struct or all rows into a slice (list) of structs
* Making any struct a UDT without implementing marshalling functions
* GocqlX is fast. Its performance is comparable to raw driver. You can find some benchmarks [here](#performance).

Subpackages provide additional functionality:

* CQL query builder ([package qb](https://github.com/scylladb/gocqlx/blob/master/qb))
* CRUD operations based on table model ([package table](https://github.com/scylladb/gocqlx/blob/master/table))
* Database migrations ([package migrate](https://github.com/scylladb/gocqlx/blob/master/migrate))

## Installation GocqlX

Add GocqlX to your Go module:

```bash
go get github.com/scylladb/gocqlx/v3
```

## Installation schemagen

Unfortunately you can't install it via `go install`, since `go.mod` contains `replace` directive.
So, you have to check it out and install manually:
```bash
git clone git@github.com:scylladb/gocqlx.git
cd gocqlx/cmd/schemagen/
go install .
```

## Getting started

First, import the required packages:

```go
import (
	"fmt"
	"log"

	"github.com/gocql/gocql"
	"github.com/scylladb/gocqlx/v3"
	"github.com/scylladb/gocqlx/v3/qb"
	"github.com/scylladb/gocqlx/v3/table"
)
```

Wrap gocql Session:

```go
// Create gocql cluster.
cluster := gocql.NewCluster(hosts...)
// Wrap session on creation, gocqlx session embeds gocql.Session pointer.
session, err := gocqlx.WrapSession(cluster.CreateSession())
if err != nil {
	log.Fatal(err)
}
defer session.Close()
```

Specify table model:

```go
// metadata specifies table name and columns it must be in sync with schema.
var personMetadata = table.Metadata{
	Name:    "person",
	Columns: []string{"first_name", "last_name", "email"},
	PartKey: []string{"first_name"},
	SortKey: []string{"last_name"},
}

// personTable allows for simple CRUD operations based on personMetadata.
var personTable = table.New(personMetadata)

// Person represents a row in person table.
// Field names are converted to snake case by default, no need to add special tags.
// A field will not be persisted by adding the `db:"-"` tag or making it unexported.
type Person struct {
	FirstName string
	LastName  string
	Email     []string
	HairColor string `db:"-"`  // exported and skipped
	eyeColor  string           // unexported also skipped
}
```

Bind data from a struct and insert a row:

```go
p := Person{
	"Michał",
	"Matczuk",
	[]string{"michal@scylladb.com"},
	"red",    // not persisted
	"hazel"   // not persisted
}
q := session.Query(personTable.Insert()).BindStruct(p)
if err := q.ExecRelease(); err != nil {
	log.Fatal(err)
}
```

Load a single row to a struct:

```go
p := Person{
	"Michał",
	"Matczuk",
	nil, // no email
}
q := session.Query(personTable.Get()).BindStruct(p)
if err := q.GetRelease(&p); err != nil {
	log.Fatal(err)
}
fmt.Println(p)
// stdout: {Michał Matczuk [michal@scylladb.com]}
```

Load all rows in to a slice:

```go
var people []Person
q := session.Query(personTable.Select()).BindMap(qb.M{"first_name": "Michał"})
if err := q.SelectRelease(&people); err != nil {
	log.Fatal(err)
}
fmt.Println(people)
// stdout: [{Michał Matczuk [michal@scylladb.com]}]
```

## Generating table metadata with schemagen

Installation

```bash
go get -u "github.com/scylladb/gocqlx/v3/cmd/schemagen"
```

Usage:
```bash
schemagen [flags]

Flags:
  -cluster string
    	a comma-separated list of host:port tuples (default "127.0.0.1")
  -keyspace string
    	keyspace to inspect (required)
  -output string
    	the name of the folder to output to (default "models")
  -pkgname string
    	the name you wish to assign to your generated package (default "models") 
```

Example:

Running the following command for `examples` keyspace: 
```bash
schemagen -cluster="127.0.0.1:9042" -keyspace="examples" -output="models" -pkgname="models"
```

Generates `models/models.go` as follows:
```go
// Code generated by "gocqlx/cmd/schemagen"; DO NOT EDIT.

package models

import "github.com/scylladb/gocqlx/v3/table"

// Table models.
var (
	Playlists = table.New(table.Metadata{
		Name: "playlists",
		Columns: []string{
			"album",
			"artist",
			"id",
			"song_id",
			"title",
		},
		PartKey: []string{
			"id",
		},
		SortKey: []string{
			"title",
			"album",
			"artist",
		},
	})

	Songs = table.New(table.Metadata{
		Name: "songs",
		Columns: []string{
			"album",
			"artist",
			"data",
			"id",
			"tags",
			"title",
		},
		PartKey: []string{
			"id",
		},
		SortKey: []string{},
	})
)
```

## Examples

You can find lots of examples in [example_test.go](https://github.com/scylladb/gocqlx/blob/master/example_test.go).

Go and run them locally:

```bash
make run-scylla
make run-examples
```

## Training

The course [Using Scylla Drivers](https://university.scylladb.com/courses/using-scylla-drivers) in Scylla University explains how to use drivers in different languages to interact with a Scylla cluster. The lesson, [Golang and Scylla Part 3 - GoCQLX](https://university.scylladb.com/courses/using-scylla-drivers/lessons/golang-and-scylla-part-3-gocqlx/), goes over a sample application that, using GoCQLX, interacts with a three-node Scylla cluster. It connects to a Scylla cluster, displays the contents of a table, inserts and deletes data, and shows the contents of the table after each action. [Scylla University](https://university.scylladb.com/) includes other training material and online courses which will help you become a Scylla NoSQL database expert.

## Performance

GocqlX performance is comparable to the raw `gocql` driver.
Below benchmark results running on my laptop.

```
BenchmarkBaseGocqlInsert            2392            427491 ns/op            7804 B/op         39 allocs/op
BenchmarkGocqlxInsert               2479            435995 ns/op            7803 B/op         39 allocs/op
BenchmarkBaseGocqlGet               2853            452384 ns/op            7309 B/op         35 allocs/op
BenchmarkGocqlxGet                  2706            442645 ns/op            7646 B/op         38 allocs/op
BenchmarkBaseGocqlSelect             747           1664365 ns/op           49415 B/op        927 allocs/op
BenchmarkGocqlxSelect                667           1877859 ns/op           42521 B/op        932 allocs/op
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
