# gocqlx [![GoDoc](http://img.shields.io/badge/go-documentation-blue.svg?style=flat-square)](http://godoc.org/github.com/scylladb/gocqlx) [![Go Report Card](https://goreportcard.com/badge/github.com/scylladb/gocqlx)](https://goreportcard.com/report/github.com/scylladb/gocqlx) [![Build Status](http://img.shields.io/travis/scylladb/gocqlx.svg?style=flat-square)](https://travis-ci.org/scylladb/gocqlx)

Package `gocqlx` is a `gocql` extension, similar to what `sqlx` is to `database/sql`.

It contains wrappers over `gocql` types that provide convenience methods which
are useful in the development of database driven applications.  Under the
hood it uses `sqlx/reflectx` package so `sqlx` models will also work with `gocqlx`.

## Installation

    go get github.com/scylladb/gocqlx

## Features

Read all rows into a slice.

```go
var v []*Item
if err := gocqlx.Select(&v, session.Query(`SELECT * FROM items WHERE id = ?`, id)); err != nil {
    log.Fatal("select failed", err)
}
```

Read a single row into a struct.

```go
var v Item
if err := gocqlx.Get(&v, session.Query(`SELECT * FROM items WHERE id = ?`, id)); err != nil {
    log.Fatal("get failed", err)
}
```

Bind named query parameters from a struct or map.

```go
stmt, names, err := gocqlx.CompileNamedQuery([]byte("INSERT INTO items (id, name) VALUES (:id, :name)"))
if err != nil {
    t.Fatal("compile:", err)
}
q := gocqlx.Queryx{
    Query: session.Query(stmt),
    Names: names,
}
if err := q.BindStruct(&Item{"id", "name"}); err != nil {
    t.Fatal("bind:", err)
}
if err := q.Query.Exec(); err != nil {
    log.Fatal("get failed", err)
}
```

## Example

See [example test](https://github.com/scylladb/gocqlx/blob/master/example_test.go).
