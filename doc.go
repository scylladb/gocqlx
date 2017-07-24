// Package gocqlx is a gocql extension, similar to what sqlx is to database/sql.
//
// It provides a new type that seamlessly wraps gocql.Iter and provide
// convenience methods which are useful in the development of database driven
// applications.  None of the underlying gocql.Iter methods are changed.
// Instead all extended behavior is implemented through new methods defined on
// wrapper type.
//
// The wrapper type enables you to bind iterator row into a struct. Under the
// hood it uses sqlx/reflectx package, models / structs working whit sqlx will
// also work with gocqlx.
//
// Example, read all items for a given id
//
//     var v []*Item
//     if err := gocqlx.Select(session.Query(`SELECT * FROM items WHERE id = ?`, id), &v); err != nil {
//         log.Fatal("select failed", err)
//     }
package gocqlx
