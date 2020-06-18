// Copyright (C) 2017 ScyllaDB
// Use of this source code is governed by a ALv2-style
// license that can be found in the LICENSE file.

package qb

// SELECT reference:
// https://cassandra.apache.org/doc/latest/cql/dml.html#select

import (
	"bytes"
	"context"
	"fmt"

	"github.com/scylladb/gocqlx/v2"
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
	bypassCache       bool
	json              bool
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

	if b.json {
		cql.WriteString("JSON ")
	}

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

	if b.bypassCache {
		cql.WriteString("BYPASS CACHE ")
	}

	stmt = cql.String()
	return
}

// Query returns query built on top of current SelectBuilder state.
func (b *SelectBuilder) Query(session gocqlx.Session) *gocqlx.Queryx {
	return session.Query(b.ToCql())
}

// QueryContext returns query wrapped with context built on top of current SelectBuilder state.
func (b *SelectBuilder) QueryContext(ctx context.Context, session gocqlx.Session) *gocqlx.Queryx {
	return b.Query(session).WithContext(ctx)
}

// From sets the table to be selected from.
func (b *SelectBuilder) From(table string) *SelectBuilder {
	b.table = table
	return b
}

// Json sets the clause of the query.
func (b *SelectBuilder) Json() *SelectBuilder {
	b.json = true
	return b
}

// Columns adds result columns to the query.
func (b *SelectBuilder) Columns(columns ...string) *SelectBuilder {
	if len(b.columns) == 0 {
		b.columns = columns
	} else {
		b.columns = append(b.columns, columns...)
	}
	return b
}

// As is a helper for adding a column AS name result column to the query.
func As(column, name string) string {
	return column + " AS " + name
}

// Distinct sets DISTINCT clause on the query.
func (b *SelectBuilder) Distinct(columns ...string) *SelectBuilder {
	if len(b.where) == 0 {
		b.distinct = columns
	} else {
		b.distinct = append(b.distinct, columns...)
	}
	return b
}

// Where adds an expression to the WHERE clause of the query. Expressions are
// ANDed together in the generated CQL.
func (b *SelectBuilder) Where(w ...Cmp) *SelectBuilder {
	if len(b.where) == 0 {
		b.where = w
	} else {
		b.where = append(b.where, w...)
	}
	return b
}

// GroupBy sets GROUP BY clause on the query. Columns must be a primary key,
// this will automatically add the the columns as first selectors.
func (b *SelectBuilder) GroupBy(columns ...string) *SelectBuilder {
	if len(b.groupBy) == 0 {
		b.groupBy = columns
	} else {
		b.groupBy = append(b.groupBy, columns...)
	}
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

// BypassCache sets a BYPASS CACHE clause on the query.
//
// BYPASS CACHE is a feature specific to ScyllaDB.
// See https://docs.scylladb.com/getting-started/dml/#bypass-cache
func (b *SelectBuilder) BypassCache() *SelectBuilder {
	b.bypassCache = true
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

func (b *SelectBuilder) fn(name, column string) {
	b.Columns(name + "(" + column + ")")
}
