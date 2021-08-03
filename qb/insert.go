// Copyright (C) 2017 ScyllaDB
// Use of this source code is governed by a ALv2-style
// license that can be found in the LICENSE file.

package qb

// INSERT reference:
// https://cassandra.apache.org/doc/latest/cql/dml.html#insert

import (
	"bytes"
	"context"
	"time"

	"github.com/scylladb/gocqlx/v2"
)

// initializer specifies an value for a column in an insert operation.
type initializer struct {
	column string
	value  value
}

// InsertBuilder builds CQL INSERT statements.
type InsertBuilder struct {
	table   string
	columns []initializer
	unique  bool
	using   using
	json    bool
}

// Insert returns a new InsertBuilder with the given table name.
func Insert(table string) *InsertBuilder {
	return &InsertBuilder{
		table: table,
	}
}

// ToCql builds the query into a CQL string and named args.
func (b *InsertBuilder) ToCql() (stmt string, names []string) {
	cql := bytes.Buffer{}

	cql.WriteString("INSERT ")

	cql.WriteString("INTO ")
	cql.WriteString(b.table)
	cql.WriteByte(' ')

	if b.json {
		// Ignore everything else since it goes into the Json
		cql.WriteString("JSON ?")
		stmt = cql.String()
		return
	}

	cql.WriteByte('(')
	for i, c := range b.columns {
		cql.WriteString(c.column)
		if i < len(b.columns)-1 {
			cql.WriteByte(',')
		}
	}
	cql.WriteString(") ")

	cql.WriteString("VALUES (")
	for i, c := range b.columns {
		names = append(names, c.value.writeCql(&cql)...)
		if i < len(b.columns)-1 {
			cql.WriteByte(',')
		}
	}
	cql.WriteString(") ")

	if b.unique {
		cql.WriteString("IF NOT EXISTS ")
	}
	names = append(names, b.using.writeCql(&cql)...)

	stmt = cql.String()
	return
}

// Query returns query built on top of current InsertBuilder state.
func (b *InsertBuilder) Query(session gocqlx.Session) *gocqlx.Queryx {
	return session.Query(b.ToCql())
}

// QueryContext returns query wrapped with context built on top of current InsertBuilder state.
func (b *InsertBuilder) QueryContext(ctx context.Context, session gocqlx.Session) *gocqlx.Queryx {
	return b.Query(session).WithContext(ctx)
}

// Into sets the INTO clause of the query.
func (b *InsertBuilder) Into(table string) *InsertBuilder {
	b.table = table
	return b
}

// Json sets the Json clause of the query.
func (b *InsertBuilder) Json() *InsertBuilder {
	b.json = true
	return b
}

// Columns adds insert columns to the query.
func (b *InsertBuilder) Columns(columns ...string) *InsertBuilder {
	for _, c := range columns {
		b.columns = append(b.columns, initializer{
			column: c,
			value:  param(c),
		})
	}
	return b
}

// NamedColumn adds an insert column with a custom parameter name.
func (b *InsertBuilder) NamedColumn(column, name string) *InsertBuilder {
	b.columns = append(b.columns, initializer{
		column: column,
		value:  param(name),
	})
	return b
}

// LitColumn adds an insert column with a literal value to the query.
func (b *InsertBuilder) LitColumn(column, literal string) *InsertBuilder {
	b.columns = append(b.columns, initializer{
		column: column,
		value:  lit(literal),
	})
	return b
}

// FuncColumn adds an insert column initialized by evaluating a CQL function.
func (b *InsertBuilder) FuncColumn(column string, fn *Func) *InsertBuilder {
	b.columns = append(b.columns, initializer{
		column: column,
		value:  fn,
	})
	return b
}

// TupleColumn adds an insert column for a tuple value to the query.
func (b *InsertBuilder) TupleColumn(column string, count int) *InsertBuilder {
	b.columns = append(b.columns, initializer{
		column: column,
		value: tupleParam{
			param: param(column),
			count: count,
		},
	})
	return b
}

// Unique sets a IF NOT EXISTS clause on the query.
func (b *InsertBuilder) Unique() *InsertBuilder {
	b.unique = true
	return b
}

// TTL adds USING TTL clause to the query.
func (b *InsertBuilder) TTL(d time.Duration) *InsertBuilder {
	b.using.TTL(d)
	return b
}

// TTLNamed adds USING TTL clause to the query with a custom parameter name.
func (b *InsertBuilder) TTLNamed(name string) *InsertBuilder {
	b.using.TTLNamed(name)
	return b
}

// Timestamp adds USING TIMESTAMP clause to the query.
func (b *InsertBuilder) Timestamp(t time.Time) *InsertBuilder {
	b.using.Timestamp(t)
	return b
}

// TimestampNamed adds a USING TIMESTAMP clause to the query with a custom
// parameter name.
func (b *InsertBuilder) TimestampNamed(name string) *InsertBuilder {
	b.using.TimestampNamed(name)
	return b
}

// Timeout adds USING TIMEOUT clause to the query.
func (b *InsertBuilder) Timeout(d time.Duration) *InsertBuilder {
	b.using.Timeout(d)
	return b
}

// TimeoutNamed adds a USING TIMEOUT clause to the query with a custom
// parameter name.
func (b *InsertBuilder) TimeoutNamed(name string) *InsertBuilder {
	b.using.TimeoutNamed(name)
	return b
}
