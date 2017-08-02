package qb

// UPDATE reference:
// https://cassandra.apache.org/doc/latest/cql/dml.html#update

import (
	"bytes"
)

// UpdateBuilder builds CQL UPDATE statements.
type UpdateBuilder struct {
	table   string
	using   using
	columns columns
	where   where
	_if     _if
	exists  bool
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
	for i, c := range b.columns {
		cql.WriteString(c)
		cql.WriteString("=?")
		if i < len(b.columns)-1 {
			cql.WriteByte(',')
		}
	}
	names = append(names, b.columns...)
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
	b.columns = append(b.columns, columns...)
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
