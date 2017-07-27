package qb

// DELETE reference:
// http://docs.datastax.com/en/dse/5.1/cql/cql/cql_reference/cql_commands/cqlDelete.html

import (
	"bytes"
	"errors"
	"fmt"
	"time"
)

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

func (b *DeleteBuilder) ToCql() (stmt string, names []string, err error) {
	if b.table == "" {
		err = errors.New("delete statements must specify a table")
		return
	}
	if len(b.where) == 0 {
		err = fmt.Errorf("delete statements must have at least one WHERE clause")
		return
	}

	cql := bytes.Buffer{}

	cql.WriteString("DELETE ")
	if len(b.columns) > 0 {
		b.columns.writeCql(&cql)
		cql.WriteByte(' ')
	}
	cql.WriteString("FROM ")
	cql.WriteString(b.table)
	cql.WriteByte(' ')

	b.using.writeCql(&cql)

	names = append(names, b.where.writeCql(&cql)...)
	names = append(names, b._if.writeCql(&cql)...)

	if b.exists {
		cql.WriteString("IF EXISTS ")
	}

	stmt = cql.String()
	return
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

func (b *DeleteBuilder) Timestamp(t time.Time) *DeleteBuilder {
	b.using.timestamp = t
	return b
}

func (b *DeleteBuilder) Where(w ...Cmp) *DeleteBuilder {
	b.where = append(b.where, w...)
	return b
}

func (b *DeleteBuilder) If(w ...Cmp) *DeleteBuilder {
	b._if = append(b._if, w...)
	return b
}

func (b *DeleteBuilder) Existing() *DeleteBuilder {
	b.exists = true
	return b
}
