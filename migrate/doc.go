// Copyright (C) 2017 ScyllaDB
// Use of this source code is governed by a ALv2-style
// license that can be found in the LICENSE file.

// Package migrate provides simple and flexible ScyllaDB and Apache Cassandra®
// migrations. Migrations can be read from a flat directory containing cql files.
// There is no imposed naming schema, migration name is file name and the
// migrations are processed in lexicographical order. Caller provides a
// gocql.Session, the session must use a desired keyspace as migrate would try
// to create migrations table.
package migrate
