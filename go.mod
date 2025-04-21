module github.com/scylladb/gocqlx/v3

go 1.17

require (
	github.com/gocql/gocql v1.7.0
	github.com/google/go-cmp v0.6.0
	github.com/psanford/memfs v0.0.0-20241019191636-4ef911798f9b
	github.com/scylladb/go-reflectx v1.0.1
	golang.org/x/sync v0.7.0
	gopkg.in/inf.v0 v0.9.1
)

require (
	github.com/golang/snappy v1.0.0 // indirect
	github.com/hailocab/go-hostpool v0.0.0-20160125115350-e80d13ce29ed // indirect
)

replace github.com/gocql/gocql => github.com/scylladb/gocql v1.14.4
