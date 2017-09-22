// Copyright (C) 2017 ScyllaDB
// Use of this source code is governed by a ALv2-style
// license that can be found in the LICENSE file.

package qb

// UPDATE reference:
// https://cassandra.apache.org/doc/latest/cql/dml.html#update

import (
	"bytes"
	"fmt"
)

// assignment specifies an assignment in a set operation.
type assignment struct {
	column string
	name   string
	expr   bool
	fn     *Func
}

func (a assignment) writeCql(cql *bytes.Buffer) (names []string) {
	cql.WriteString(a.column)
	switch {
	case a.expr:
		names = append(names, a.name)
	case a.fn != nil:
		cql.WriteByte('=')
		names = append(names, a.fn.writeCql(cql)...)
	default:
		cql.WriteByte('=')
		cql.WriteByte('?')
		names = append(names, a.name)
	}
	return
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

// Table sets the table to be updated.
func (b *UpdateBuilder) Table(table string) *UpdateBuilder {
	b.table = table
	return b
}

// Timestamp sets a USING TIMESTAMP clause on the query.
func (b *UpdateBuilder) Timestamp() *UpdateBuilder {
	b.using.timestamp = true
	return b
}

// TTL sets a USING TTL clause on the query.
func (b *UpdateBuilder) TTL() *UpdateBuilder {
	b.using.ttl = true
	return b
}

// Set adds SET clauses to the query.
func (b *UpdateBuilder) Set(columns ...string) *UpdateBuilder {
	for _, c := range columns {
		b.assignments = append(b.assignments, assignment{
			column: c,
			name:   c,
		})
	}

	return b
}

// SetFunc adds SET column=someFunc(?...) clause to the query.
func (b *UpdateBuilder) SetFunc(column string, fn *Func) *UpdateBuilder {
	b.assignments = append(b.assignments, assignment{column: column, fn: fn})
	return b
}

// Add adds SET column=column+? clauses to the query.
func (b *UpdateBuilder) Add(column string) *UpdateBuilder {
	return b.AddNamed(column, column)
}

// AddNamed adds SET column=column+? clauses to the query with a custom
// parameter name.
func (b *UpdateBuilder) AddNamed(column, name string) *UpdateBuilder {
	b.assignments = append(b.assignments, assignment{
		column: fmt.Sprint(column, "=", column, "+?"),
		name:   name,
		expr:   true,
	})
	return b
}

// Remove adds SET column=column-? clauses to the query.
func (b *UpdateBuilder) Remove(column string) *UpdateBuilder {
	return b.RemoveNamed(column, column)
}

// RemoveNamed adds SET column=column-? clauses to the query with a custom
// parameter name.
func (b *UpdateBuilder) RemoveNamed(column, name string) *UpdateBuilder {
	b.assignments = append(b.assignments, assignment{
		column: fmt.Sprint(column, "=", column, "-?"),
		name:   name,
		expr:   true,
	})
	return b
}

// Where adds an expression to the WHERE clause of the query. Expressions are
// ANDed together in the generated CQL.
func (b *UpdateBuilder) Where(w ...Cmp) *UpdateBuilder {
	b.where = append(b.where, w...)
	return b
}

// If adds an expression to the IF clause of the query. Expressions are ANDed
// together in the generated CQL.
func (b *UpdateBuilder) If(w ...Cmp) *UpdateBuilder {
	b._if = append(b._if, w...)
	return b
}

// Existing sets a IF EXISTS clause on the query.
func (b *UpdateBuilder) Existing() *UpdateBuilder {
	b.exists = true
	return b
}
