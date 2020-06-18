// Copyright (C) 2017 ScyllaDB
// Use of this source code is governed by a ALv2-style
// license that can be found in the LICENSE file.

package qb

// DELETE reference:
// https://cassandra.apache.org/doc/latest/cql/dml.html#delete

import (
	"bytes"
	"context"
	"time"

	"github.com/scylladb/gocqlx/v2"
)

// DeleteBuilder builds CQL DELETE statements.
type DeleteBuilder struct {
	table   string
	columns columns
	using   using
	where   where
	_if     _if
	exists  bool
}

// Delete returns a new DeleteBuilder with the given table name.
func Delete(table string) *DeleteBuilder {
	return &DeleteBuilder{
		table: table,
	}
}

// ToCql builds the query into a CQL string and named args.
func (b *DeleteBuilder) ToCql() (stmt string, names []string) {
	cql := bytes.Buffer{}

	cql.WriteString("DELETE ")
	if len(b.columns) > 0 {
		b.columns.writeCql(&cql)
		cql.WriteByte(' ')
	}
	cql.WriteString("FROM ")
	cql.WriteString(b.table)
	cql.WriteByte(' ')

	names = append(names, b.using.writeCql(&cql)...)
	names = append(names, b.where.writeCql(&cql)...)
	names = append(names, b._if.writeCql(&cql)...)

	if b.exists {
		cql.WriteString("IF EXISTS ")
	}

	stmt = cql.String()
	return
}

// Query returns query built on top of current DeleteBuilder state.
func (b *DeleteBuilder) Query(session gocqlx.Session) *gocqlx.Queryx {
	return session.Query(b.ToCql())
}

// QueryContext returns query wrapped with context built on top of current DeleteBuilder state.
func (b *DeleteBuilder) QueryContext(ctx context.Context, session gocqlx.Session) *gocqlx.Queryx {
	return b.Query(session).WithContext(ctx)
}

// From sets the table to be deleted from.
func (b *DeleteBuilder) From(table string) *DeleteBuilder {
	b.table = table
	return b
}

// Columns adds delete columns to the query.
func (b *DeleteBuilder) Columns(columns ...string) *DeleteBuilder {
	b.columns = append(b.columns, columns...)
	return b
}

// Timestamp adds USING TIMESTAMP clause to the query.
func (b *DeleteBuilder) Timestamp(t time.Time) *DeleteBuilder {
	b.using.Timestamp(t)
	return b
}

// TimestampNamed adds a USING TIMESTAMP clause to the query with a custom
// parameter name.
func (b *DeleteBuilder) TimestampNamed(name string) *DeleteBuilder {
	b.using.TimestampNamed(name)
	return b
}

// Where adds an expression to the WHERE clause of the query. Expressions are
// ANDed together in the generated CQL.
func (b *DeleteBuilder) Where(w ...Cmp) *DeleteBuilder {
	b.where = append(b.where, w...)
	return b
}

// If adds an expression to the IF clause of the query. Expressions are ANDed
// together in the generated CQL.
func (b *DeleteBuilder) If(w ...Cmp) *DeleteBuilder {
	b._if = append(b._if, w...)
	return b
}

// Existing sets a IF EXISTS clause on the query.
func (b *DeleteBuilder) Existing() *DeleteBuilder {
	b.exists = true
	return b
}
