// Copyright (C) 2017 ScyllaDB
// Use of this source code is governed by a ALv2-style
// license that can be found in the LICENSE file.

package qb

// SELECT reference:
// https://cassandra.apache.org/doc/latest/cql/dml.html#select

import (
	"bytes"
	"fmt"
)

// Order specifies sorting order.
type Order bool

const (
	// ASC is ascending order
	ASC Order = true
	// DESC is descending order
	DESC = false
)

func (o Order) String() string {
	if o {
		return "ASC"
	}
	return "DESC"
}

// SelectBuilder builds CQL SELECT statements.
type SelectBuilder struct {
	table             string
	columns           columns
	distinct          columns
	where             where
	groupBy           columns
	orderBy           columns
	limit             uint
	limitPerPartition uint
	allowFiltering    bool
}

// Select returns a new SelectBuilder with the given table name.
func Select(table string) *SelectBuilder {
	return &SelectBuilder{
		table: table,
	}
}

// ToCql builds the query into a CQL string and named args.
func (b *SelectBuilder) ToCql() (stmt string, names []string) {
	cql := bytes.Buffer{}

	cql.WriteString("SELECT ")
	switch {
	case len(b.distinct) > 0:
		cql.WriteString("DISTINCT ")
		b.distinct.writeCql(&cql)
	case len(b.groupBy) > 0:
		b.groupBy.writeCql(&cql)
		if len(b.columns) != 0 {
			cql.WriteByte(',')
			b.columns.writeCql(&cql)
		}
	case len(b.columns) == 0:
		cql.WriteByte('*')
	default:
		b.columns.writeCql(&cql)
	}
	cql.WriteString(" FROM ")
	cql.WriteString(b.table)
	cql.WriteByte(' ')

	names = b.where.writeCql(&cql)

	if len(b.groupBy) > 0 {
		cql.WriteString("GROUP BY ")
		b.groupBy.writeCql(&cql)
		cql.WriteByte(' ')
	}

	if len(b.orderBy) > 0 {
		cql.WriteString("ORDER BY ")
		b.orderBy.writeCql(&cql)
		cql.WriteByte(' ')
	}

	if b.limit != 0 {
		cql.WriteString("LIMIT ")
		cql.WriteString(fmt.Sprint(b.limit))
		cql.WriteByte(' ')
	}

	if b.limitPerPartition != 0 {
		cql.WriteString("PER PARTITION LIMIT ")
		cql.WriteString(fmt.Sprint(b.limitPerPartition))
		cql.WriteByte(' ')
	}

	if b.allowFiltering {
		cql.WriteString("ALLOW FILTERING ")
	}

	stmt = cql.String()
	return
}

// From sets the table to be selected from.
func (b *SelectBuilder) From(table string) *SelectBuilder {
	b.table = table
	return b
}

// Columns adds result columns to the query.
func (b *SelectBuilder) Columns(columns ...string) *SelectBuilder {
	b.columns = append(b.columns, columns...)
	return b
}

// As is a helper for adding a column AS name result column to the query.
func As(column, name string) string {
	return column + " AS " + name
}

// Distinct sets DISTINCT clause on the query.
func (b *SelectBuilder) Distinct(columns ...string) *SelectBuilder {
	b.distinct = append(b.distinct, columns...)
	return b
}

// Where adds an expression to the WHERE clause of the query. Expressions are
// ANDed together in the generated CQL.
func (b *SelectBuilder) Where(w ...Cmp) *SelectBuilder {
	b.where = append(b.where, w...)
	return b
}

// GroupBy sets GROUP BY clause on the query. Columns must be a primary key,
// this will automatically add the the columns as first selectors.
func (b *SelectBuilder) GroupBy(columns ...string) *SelectBuilder {
	b.groupBy = append(b.groupBy, columns...)
	return b
}

// OrderBy sets ORDER BY clause on the query.
func (b *SelectBuilder) OrderBy(column string, o Order) *SelectBuilder {
	b.orderBy = append(b.orderBy, column+" "+o.String())
	return b
}

// Limit sets a LIMIT clause on the query.
func (b *SelectBuilder) Limit(limit uint) *SelectBuilder {
	b.limit = limit
	return b
}

// LimitPerPartition sets a PER PARTITION LIMIT clause on the query.
func (b *SelectBuilder) LimitPerPartition(limit uint) *SelectBuilder {
	b.limitPerPartition = limit
	return b
}

// AllowFiltering sets a ALLOW FILTERING clause on the query.
func (b *SelectBuilder) AllowFiltering() *SelectBuilder {
	b.allowFiltering = true
	return b
}

// Count produces 'count(column)'.
func (b *SelectBuilder) Count(column string) *SelectBuilder {
	b.fn("count", column)
	return b
}

// CountAll produces 'count(*)'.
func (b *SelectBuilder) CountAll() *SelectBuilder {
	b.Count("*")
	return b
}

// Min produces 'min(column)' aggregation function.
func (b *SelectBuilder) Min(column string) *SelectBuilder {
	b.fn("min", column)
	return b
}

// Max produces 'max(column)' aggregation function.
func (b *SelectBuilder) Max(column string) *SelectBuilder {
	b.fn("max", column)
	return b
}

// Avg produces 'avg(column)' aggregation function.
func (b *SelectBuilder) Avg(column string) *SelectBuilder {
	b.fn("avg", column)
	return b
}

// Sum produces 'sum(column)' aggregation function.
func (b *SelectBuilder) Sum(column string) *SelectBuilder {
	b.fn("sum", column)
	return b
}

func (b *SelectBuilder) fn(name, column string) *SelectBuilder {
	b.Columns(name + "(" + column + ")")
	return b
}
