package qb

import (
	"bytes"
	"errors"
	"fmt"
)

// UPDATE reference:
// http://docs.datastax.com/en/dse/5.1/cql/cql/cql_reference/cql_commands/cqlUpdate.html

import (
	"time"
)

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

func (b *UpdateBuilder) ToCql() (stmt string, names []string, err error) {
	if b.table == "" {
		err = errors.New("update statements must specify a table")
		return
	}
	if len(b.columns) == 0 {
		err = fmt.Errorf("update statements must have at least one SET clause")
		return
	}
	if len(b.where) == 0 {
		err = fmt.Errorf("update statements must have at least one WHERE clause")
		return
	}

	cql := bytes.Buffer{}

	cql.WriteString("UPDATE ")
	cql.WriteString(b.table)
	cql.WriteByte(' ')

	b.using.writeCql(&cql)

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

func (b *UpdateBuilder) Timestamp(t time.Time) *UpdateBuilder {
	b.using.timestamp = t
	return b
}

func (b *UpdateBuilder) TTL(d time.Duration) *UpdateBuilder {
	b.using.ttl = d
	return b
}

func (b *UpdateBuilder) Set(columns ...string) *UpdateBuilder {
	b.columns = append(b.columns, columns...)
	return b
}

func (b *UpdateBuilder) Where(e ...expr) *UpdateBuilder {
	b.where = append(b.where, e...)
	return b
}

func (b *UpdateBuilder) If(e ...expr) *UpdateBuilder {
	b._if = append(b._if, e...)
	return b
}

func (b *UpdateBuilder) Existing() *UpdateBuilder {
	b.exists = true
	return b
}
