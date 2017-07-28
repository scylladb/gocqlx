package qb

// INSERT reference:
// http://docs.datastax.com/en/dse/5.1/cql/cql/cql_reference/cql_commands/cqlInsert.html

import (
	"bytes"
	"time"
)

type InsertBuilder struct {
	table   string
	columns columns
	unique  bool
	using   using
}

// Insert returns a new InsertBuilder with the given table name.
func Insert(table string) *InsertBuilder {
	return &InsertBuilder{
		table: table,
	}
}

func (b *InsertBuilder) ToCql() (stmt string, names []string) {
	cql := bytes.Buffer{}

	cql.WriteString("INSERT ")

	cql.WriteString("INTO ")
	cql.WriteString(b.table)
	cql.WriteByte(' ')

	cql.WriteByte('(')
	b.columns.writeCql(&cql)
	cql.WriteString(") ")

	cql.WriteString("VALUES (")
	placeholders(&cql, len(b.columns))
	cql.WriteString(") ")

	b.using.writeCql(&cql)

	if b.unique {
		cql.WriteString("IF NOT EXISTS ")
	}

	stmt, names = cql.String(), b.columns
	return
}

func (b *InsertBuilder) Into(table string) *InsertBuilder {
	b.table = table
	return b
}

func (b *InsertBuilder) Columns(columns ...string) *InsertBuilder {
	b.columns = append(b.columns, columns...)
	return b
}

func (b *InsertBuilder) Unique() *InsertBuilder {
	b.unique = true
	return b
}

func (b *InsertBuilder) Timestamp(t time.Time) *InsertBuilder {
	b.using.timestamp = t
	return b
}

func (b *InsertBuilder) TTL(d time.Duration) *InsertBuilder {
	b.using.ttl = d
	return b
}
