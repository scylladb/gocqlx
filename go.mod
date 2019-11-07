module github.com/scylladb/gocqlx

go 1.13

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/gocql/gocql v0.0.0-00010101000000-000000000000
	github.com/golang/snappy v0.0.1 // indirect
	github.com/google/go-cmp v0.2.0
	github.com/scylladb/go-reflectx v1.0.1
	gopkg.in/inf.v0 v0.9.1
)

replace github.com/gocql/gocql => github.com/scylladb/gocql v1.3.1
