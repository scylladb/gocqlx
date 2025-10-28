module schemagentest

go 1.25.0

require (
	github.com/gocql/gocql v1.7.0
	github.com/google/go-cmp v0.7.0
	github.com/scylladb/gocqlx/v3 v3.0.4
)

require (
	github.com/google/uuid v1.6.0 // indirect
	github.com/klauspost/compress v1.18.1 // indirect
	github.com/scylladb/go-reflectx v1.0.1 // indirect
	gopkg.in/inf.v0 v0.9.1 // indirect
)

replace (
	github.com/gocql/gocql => github.com/scylladb/gocql v1.16.1
	github.com/scylladb/gocqlx/v3 => ../../..
)
