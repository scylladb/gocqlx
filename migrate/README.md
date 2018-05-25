# GoCQLX Migrations

Package `migrate` provides simple and flexible CQL migrations. 
Migrations can be read from a flat directory containing cql files.
There is no imposed naming schema, migration name is file name and the
migrations are processed in lexicographical order. Caller provides a
`gocql.Session`, the session must use a desired keyspace as migrate would try
to create migrations table.

## Features

* Each CQL statement will run once
* Go code migrations using callbacks 

## Example

```go
package main

import (
    "context"

    "github.com/scylladb/gocqlx/migrate"
)

const dir = "./cql" 

func main() {
    session := CreateSession()
    defer session.Close()

    ctx := context.Background()
    if err := migrate.Migrate(ctx, session, dir); err != nil {
        panic(err)
    }
}
```