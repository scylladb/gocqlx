# gocqlx [![GoDoc](http://img.shields.io/badge/go-documentation-blue.svg?style=flat-square)](http://godoc.org/github.com/scylladb/gocqlx) [![Go Report Card](https://goreportcard.com/badge/github.com/scylladb/gocqlx)](https://goreportcard.com/report/github.com/scylladb/gocqlx) [![Build Status](https://travis-ci.org/scylladb/gocqlx.svg?branch=master)](https://travis-ci.org/scylladb/gocqlx)

Package `gocqlx` is a Scylla / Cassandra productivity toolkit for `gocql`, it's 
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
	q := Query(qb.Insert("person").Columns("first_name", "last_name", "email").ToCql())
	if err := q.BindStruct(p); err != nil {
		t.Fatal("bind:", err)
	}
	mustExec(q.Query)
}

// Update
{
	p.Email = append(p.Email, "patricia1.citzen@gocqlx_test.com")

	q := Query(qb.Update("person").Set("email").Where(qb.Eq("first_name"), qb.Eq("last_name")).ToCql())
	if err := q.BindStruct(p); err != nil {
		t.Fatal("bind:", err)
	}
	mustExec(q.Query)
}

// Select
{
	q := Query(qb.Select("person").Where(qb.In("first_name")).ToCql())
	m := map[string]interface{}{
		"first_name": []string{"Patricia", "John"},
	}
	if err := q.BindMap(m); err != nil {
		t.Fatal("bind:", err)
	}

	var people []Person
	if err := gocqlx.Select(&people, q.Query); err != nil {
		t.Fatal(err)
	}
	t.Log(people)

	// [{Patricia Citizen [patricia.citzen@gocqlx_test.com patricia1.citzen@gocqlx_test.com]} {John Doe [johndoeDNE@gmail.net]}]
}
```

For more details see [example test](https://github.com/scylladb/gocqlx/blob/master/example_test.go).
