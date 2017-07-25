// Package gocqlx is a gocql extension, similar to what sqlx is to database/sql.
//
// It contains wrappers over gocql types that provide convenience methods which
// are useful in the development of database driven applications.  Under the
// hood it uses sqlx/reflectx package so sqlx models will also work with gocqlx.
//
// Example, read all rows into a slice
//
//     var v []*Item
//     if err := gocqlx.Select(&v, session.Query(`SELECT * FROM items WHERE id = ?`, id)); err != nil {
//         log.Fatal("select failed", err)
//     }
//
// Example, read a single row into a struct
//
//     var v Item
//     if err := gocqlx.Get(&v, session.Query(`SELECT * FROM items WHERE id = ?`, id)); err != nil {
//         log.Fatal("get failed", err)
//     }
//
// Example, bind named query parameters from a struct or map
//
//     stmt, names, err := gocqlx.CompileNamedQuery([]byte("INSERT INTO items (id, name) VALUES (:id, :name)"))
//     if err != nil {
//         t.Fatal("compile:", err)
//     }
//     q := gocqlx.Queryx{
//         Query: session.Query(stmt),
//         Names: names,
//     }
//     if err := q.BindStruct(&Item{"id", "name"}); err != nil {
//         t.Fatal("bind:", err)
//     }
//     if err := q.Query.Exec(); err != nil {
//         log.Fatal("get failed", err)
//     }
//
package gocqlx
