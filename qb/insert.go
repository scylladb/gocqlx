package qb

// INSERT reference:
// https://cassandra.apache.org/doc/latest/cql/dml.html#insert

import (
	"bytes"
	"time"
)

// InsertBuilder builds CQL INSERT statements.
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

// ToCql builds the query into a CQL string and named args.
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

// Into sets the INTO clause of the query.
func (b *InsertBuilder) Into(table string) *InsertBuilder {
	b.table = table
	return b
}

// Columns adds insert columns to the query.
func (b *InsertBuilder) Columns(columns ...string) *InsertBuilder {
	b.columns = append(b.columns, columns...)
	return b
}

// Unique sets a IF NOT EXISTS clause on the query.
func (b *InsertBuilder) Unique() *InsertBuilder {
	b.unique = true
	return b
}

// Timestamp sets a USING TIMESTAMP clause on the query.
func (b *InsertBuilder) Timestamp(t time.Time) *InsertBuilder {
	b.using.timestamp = t
	return b
}

// TTL sets a USING TTL clause on the query.
func (b *InsertBuilder) TTL(d time.Duration) *InsertBuilder {
	b.using.ttl = d
	return b
}
