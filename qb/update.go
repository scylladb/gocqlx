// Copyright (C) 2017 ScyllaDB
// Use of this source code is governed by a ALv2-style
// license that can be found in the LICENSE file.

package qb

// UPDATE reference:
// https://cassandra.apache.org/doc/latest/cql/dml.html#update

import (
	"bytes"
	"context"
	"time"

	"github.com/scylladb/gocqlx/v2"
)

// assignment specifies an assignment in a set operation.
type assignment struct {
	column      string
	value       value
	valuePrefix string // Tbe value prefix to use for add/remove operations.
}

func (a assignment) writeCql(cql *bytes.Buffer) (names []string) {
	cql.WriteString(a.column)
	cql.WriteByte('=')
	cql.WriteString(a.valuePrefix)
	return a.value.writeCql(cql)
}

// UpdateBuilder builds CQL UPDATE statements.
type UpdateBuilder struct {
	table       string
	using       using
	assignments []assignment
	where       where
	_if         _if
	exists      bool
}

// Update returns a new UpdateBuilder with the given table name.
func Update(table string) *UpdateBuilder {
	return &UpdateBuilder{
		table: table,
	}
}

// ToCql builds the query into a CQL string and named args.
func (b *UpdateBuilder) ToCql() (stmt string, names []string) {
	cql := bytes.Buffer{}

	cql.WriteString("UPDATE ")
	cql.WriteString(b.table)
	cql.WriteByte(' ')

	names = append(names, b.using.writeCql(&cql)...)

	cql.WriteString("SET ")
	for i, a := range b.assignments {
		names = append(names, a.writeCql(&cql)...)
		if i < len(b.assignments)-1 {
			cql.WriteByte(',')
		}
	}
	cql.WriteByte(' ')

	names = append(names, b.where.writeCql(&cql)...)
	names = append(names, b._if.writeCql(&cql)...)

	if b.exists {
		cql.WriteString("IF EXISTS ")
	}

	stmt = cql.String()
	return
}

// Query returns query built on top of current UpdateBuilder state.
func (b *UpdateBuilder) Query(session gocqlx.Session) *gocqlx.Queryx {
	return session.Query(b.ToCql())
}

// QueryContext returns query wrapped with context built on top of current UpdateBuilder state.
func (b *UpdateBuilder) QueryContext(ctx context.Context, session gocqlx.Session) *gocqlx.Queryx {
	return b.Query(session).WithContext(ctx)
}

// Table sets the table to be updated.
func (b *UpdateBuilder) Table(table string) *UpdateBuilder {
	b.table = table
	return b
}

// TTL adds USING TTL clause to the query.
func (b *UpdateBuilder) TTL(d time.Duration) *UpdateBuilder {
	b.using.TTL(d)
	return b
}

// TTLNamed adds USING TTL clause to the query with a custom parameter name.
func (b *UpdateBuilder) TTLNamed(name string) *UpdateBuilder {
	b.using.TTLNamed(name)
	return b
}

// Timestamp adds USING TIMESTAMP clause to the query.
func (b *UpdateBuilder) Timestamp(t time.Time) *UpdateBuilder {
	b.using.Timestamp(t)
	return b
}

// TimestampNamed adds a USING TIMESTAMP clause to the query with a custom
// parameter name.
func (b *UpdateBuilder) TimestampNamed(name string) *UpdateBuilder {
	b.using.TimestampNamed(name)
	return b
}

// Timeout adds USING TIMEOUT clause to the query.
func (b *UpdateBuilder) Timeout(d time.Duration) *UpdateBuilder {
	b.using.Timeout(d)
	return b
}

// TimeoutNamed adds a USING TIMEOUT clause to the query with a custom
// parameter name.
func (b *UpdateBuilder) TimeoutNamed(name string) *UpdateBuilder {
	b.using.TimeoutNamed(name)
	return b
}

// Set adds SET clauses to the query.
// To set a tuple column use SetTuple instead.
func (b *UpdateBuilder) Set(columns ...string) *UpdateBuilder {
	for _, c := range columns {
		b.assignments = append(b.assignments, assignment{
			column: c,
			value:  param(c),
		})
	}

	return b
}

// SetNamed adds SET column=? clause to the query with a custom parameter name.
func (b *UpdateBuilder) SetNamed(column, name string) *UpdateBuilder {
	b.assignments = append(
		b.assignments, assignment{column: column, value: param(name)})
	return b
}

// SetLit adds SET column=literal clause to the query.
func (b *UpdateBuilder) SetLit(column, literal string) *UpdateBuilder {
	b.assignments = append(
		b.assignments, assignment{column: column, value: lit(literal)})
	return b
}

// SetFunc adds SET column=someFunc(?...) clause to the query.
func (b *UpdateBuilder) SetFunc(column string, fn *Func) *UpdateBuilder {
	b.assignments = append(b.assignments, assignment{column: column, value: fn})
	return b
}

// SetTuple adds a SET clause for a tuple to the query.
func (b *UpdateBuilder) SetTuple(column string, count int) *UpdateBuilder {
	b.assignments = append(b.assignments, assignment{
		column: column,
		value: tupleParam{
			param: param(column),
			count: count,
		},
	})
	return b
}

// Add adds SET column=column+? clauses to the query.
func (b *UpdateBuilder) Add(column string) *UpdateBuilder {
	return b.addValue(column, param(column))
}

// AddNamed adds SET column=column+? clauses to the query with a custom
// parameter name.
func (b *UpdateBuilder) AddNamed(column, name string) *UpdateBuilder {
	return b.addValue(column, param(name))
}

// AddLit adds SET column=column+literal clauses to the query.
func (b *UpdateBuilder) AddLit(column, literal string) *UpdateBuilder {
	return b.addValue(column, lit(literal))
}

// AddFunc adds SET column=column+someFunc(?...) clauses to the query.
func (b *UpdateBuilder) AddFunc(column string, fn *Func) *UpdateBuilder {
	return b.addValue(column, fn)
}

func (b *UpdateBuilder) addValue(column string, value value) *UpdateBuilder {
	b.assignments = append(b.assignments, assignment{
		column:      column,
		value:       value,
		valuePrefix: column + "+",
	})
	return b
}

// Remove adds SET column=column-? clauses to the query.
func (b *UpdateBuilder) Remove(column string) *UpdateBuilder {
	return b.removeValue(column, param(column))
}

// RemoveNamed adds SET column=column-? clauses to the query with a custom
// parameter name.
func (b *UpdateBuilder) RemoveNamed(column, name string) *UpdateBuilder {
	return b.removeValue(column, param(name))
}

// RemoveLit adds SET column=column-literal clauses to the query.
func (b *UpdateBuilder) RemoveLit(column, literal string) *UpdateBuilder {
	return b.removeValue(column, lit(literal))
}

// RemoveFunc adds SET column=column-someFunc(?...) clauses to the query.
func (b *UpdateBuilder) RemoveFunc(column string, fn *Func) *UpdateBuilder {
	return b.removeValue(column, fn)
}

func (b *UpdateBuilder) removeValue(column string, value value) *UpdateBuilder {
	b.assignments = append(b.assignments, assignment{
		column:      column,
		value:       value,
		valuePrefix: column + "-",
	})
	return b
}

// Where adds an expression to the WHERE clause of the query. Expressions are
// ANDed together in the generated CQL.
func (b *UpdateBuilder) Where(w ...Cmp) *UpdateBuilder {
	if len(b.where) == 0 {
		b.where = w
	} else {
		b.where = append(b.where, w...)
	}
	return b
}

// If adds an expression to the IF clause of the query. Expressions are ANDed
// together in the generated CQL.
func (b *UpdateBuilder) If(w ...Cmp) *UpdateBuilder {
	if len(b._if) == 0 {
		b._if = w
	} else {
		b._if = append(b._if, w...)
	}
	return b
}

// Existing sets a IF EXISTS clause on the query.
func (b *UpdateBuilder) Existing() *UpdateBuilder {
	b.exists = true
	return b
}
