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

// SelectBuilder builds CQL SELECT statements.
type SelectBuilder struct {
	table             string
	columns           columns
	distinct          columns
	where             where
	groupBy           columns
	orderBy           string
	order             Order
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
		cql.WriteByte(',')
		b.columns.writeCql(&cql)
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

	if b.orderBy != "" {
		cql.WriteString("ORDER BY ")
		cql.WriteString(b.orderBy)
		if b.order {
			cql.WriteString(" ASC ")
		} else {
			cql.WriteString(" DESC ")
		}
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
	b.orderBy, b.order = column, o
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
