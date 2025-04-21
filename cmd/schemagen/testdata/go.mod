module schemagentest

go 1.17

require (
	github.com/gocql/gocql v1.7.0
	github.com/google/go-cmp v0.6.0
	github.com/scylladb/gocqlx/v3 v3.0.0
)

require (
	github.com/golang/snappy v1.0.0 // indirect
	github.com/hailocab/go-hostpool v0.0.0-20160125115350-e80d13ce29ed // indirect
	github.com/scylladb/go-reflectx v1.0.1 // indirect
	gopkg.in/inf.v0 v0.9.1 // indirect
)

replace (
	github.com/gocql/gocql => github.com/scylladb/gocql v1.14.4
	github.com/scylladb/gocqlx/v3 => ../../..
)
