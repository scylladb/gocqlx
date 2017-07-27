package qb

// SELECT reference:
// http://docs.datastax.com/en/dse/5.1/cql/cql/cql_reference/cql_commands/cqlSelect.html

import (
	"bytes"
	"errors"
	"fmt"
)

type Order bool

const (
	ASC  Order = true
	DESC       = false
)

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

func (b *SelectBuilder) ToCql() (stmt string, names []string, err error) {
	if b.table == "" {
		err = errors.New("select statements must specify a table")
		return
	}
	if b.distinct != "" && len(b.columns) > 0 {
		err = fmt.Errorf("select statements must specify either a column list or DISTINCT partition_key")
		return
	}

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

func (b *SelectBuilder) Columns(columns ...string) *SelectBuilder {
	b.columns = append(b.columns, columns...)
	return b
}

func (b *SelectBuilder) Distinct(columns... string) *SelectBuilder {
	b.distinct = append(b.distinct, columns...)
	return b
}

func (b *SelectBuilder) Where(e ...expr) *SelectBuilder {
	b.where = append(b.where, e...)
	return b
}

// GroupBy sets GROUP BY clause on the query. Columns must be a primary key,
// this will automatically add the the columns as first selectors.
func (b *SelectBuilder) GroupBy(columns... string) *SelectBuilder {
	b.groupBy = append(b.groupBy, columns...)
	return b
}

func (b *SelectBuilder) OrderBy(column string, o Order) *SelectBuilder {
	b.orderBy, b.order = column, o
	return b
}

func (b *SelectBuilder) Limit(limit uint) *SelectBuilder {
	b.limit = limit
	return b
}

func (b *SelectBuilder) LimitPerPartition(limit uint) *SelectBuilder {
	b.limitPerPartition = limit
	return b
}

func (b *SelectBuilder) AllowFiltering() *SelectBuilder {
	b.allowFiltering = true
	return b
}
