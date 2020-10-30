// Copyright (C) 2017 ScyllaDB
// Use of this source code is governed by a ALv2-style
// license that can be found in the LICENSE file.

package table

import (
	"context"

	"github.com/scylladb/gocqlx/v2"
	"github.com/scylladb/gocqlx/v2/qb"
)

// Metadata represents table schema.
type Metadata struct {
	Name    string
	Columns []string
	PartKey []string
	SortKey []string
}

type cql struct {
	stmt  string
	names []string
}

// Table allows for simple CRUD operations, it's backed by query builders from
// gocqlx/qb package.
type Table struct {
	metadata      Metadata
	primaryKeyCmp []qb.Cmp
	partKeyCmp    []qb.Cmp

	get    cql
	sel    cql
	insert cql
}

// New creates new Table based on table schema read from Metadata.
func New(m Metadata) *Table { // nolint: gocritic
	t := &Table{
		metadata: m,
	}

	// prepare primary and partition key comparators
	t.primaryKeyCmp = make([]qb.Cmp, 0, len(m.PartKey)+len(m.SortKey))
	for _, k := range m.PartKey {
		t.primaryKeyCmp = append(t.primaryKeyCmp, qb.Eq(k))
	}
	for _, k := range m.SortKey {
		t.primaryKeyCmp = append(t.primaryKeyCmp, qb.Eq(k))
	}
	t.partKeyCmp = make([]qb.Cmp, len(m.PartKey))
	copy(t.partKeyCmp, t.primaryKeyCmp[:len(t.metadata.PartKey)])

	// prepare get stmt
	t.get.stmt, t.get.names = qb.Select(m.Name).Where(t.primaryKeyCmp...).ToCql()
	// prepare select stmt
	t.sel.stmt, t.sel.names = qb.Select(m.Name).Where(t.partKeyCmp...).ToCql()
	// prepare insert stmt
	t.insert.stmt, t.insert.names = qb.Insert(m.Name).Columns(m.Columns...).ToCql()

	return t
}

// Metadata returns copy of table metadata.
func (t *Table) Metadata() Metadata {
	return t.metadata
}

// PrimaryKeyCmp returns copy of table's primaryKeyCmp.
func (t *Table) PrimaryKeyCmp() []qb.Cmp {
	primaryKeyCmp := make([]qb.Cmp, len(t.primaryKeyCmp))
	copy(primaryKeyCmp, t.primaryKeyCmp)
	return primaryKeyCmp
}

// Name returns table name.
func (t *Table) Name() string {
	return t.metadata.Name
}

// Get returns select by primary key statement.
func (t *Table) Get(columns ...string) (stmt string, names []string) {
	if len(columns) == 0 {
		return t.get.stmt, t.get.names
	}

	return qb.Select(t.metadata.Name).
		Columns(columns...).
		Where(t.primaryKeyCmp...).
		ToCql()
}

// GetQuery returns query which gets by partition key.
func (t *Table) GetQuery(session gocqlx.Session, columns ...string) *gocqlx.Queryx {
	return session.Query(t.Get(columns...))
}

// GetQueryContext returns query wrapped with context which gets by partition key.
func (t *Table) GetQueryContext(ctx context.Context, session gocqlx.Session, columns ...string) *gocqlx.Queryx {
	return t.GetQuery(session, columns...).WithContext(ctx)
}

// Select returns select by partition key statement.
func (t *Table) Select(columns ...string) (stmt string, names []string) {
	if len(columns) == 0 {
		return t.sel.stmt, t.sel.names
	}

	return qb.Select(t.metadata.Name).
		Columns(columns...).
		Where(t.primaryKeyCmp[0:len(t.metadata.PartKey)]...).
		ToCql()
}

// SelectQuery returns query which selects by partition key statement.
func (t *Table) SelectQuery(session gocqlx.Session, columns ...string) *gocqlx.Queryx {
	return session.Query(t.Select(columns...))
}

// SelectQueryContext returns query wrapped with context which selects by partition key statement.
func (t *Table) SelectQueryContext(ctx context.Context, session gocqlx.Session, columns ...string) *gocqlx.Queryx {
	return t.SelectQuery(session, columns...).WithContext(ctx)
}

// SelectBuilder returns a builder initialised to select by partition key
// statement.
func (t *Table) SelectBuilder(columns ...string) *qb.SelectBuilder {
	return qb.Select(t.metadata.Name).Columns(columns...).Where(t.partKeyCmp...)
}

// SelectAll returns select * statement.
func (t *Table) SelectAll() (stmt string, names []string) {
	return qb.Select(t.metadata.Name).ToCql()
}

// Insert returns insert all columns statement.
func (t *Table) Insert() (stmt string, names []string) {
	return t.insert.stmt, t.insert.names
}

// InsertQuery returns query which inserts all columns.
func (t *Table) InsertQuery(session gocqlx.Session) *gocqlx.Queryx {
	return session.Query(t.Insert())
}

// InsertQueryContext returns query wrapped with context which inserts all columns.
func (t *Table) InsertQueryContext(ctx context.Context, session gocqlx.Session) *gocqlx.Queryx {
	return t.InsertQuery(session).WithContext(ctx)
}

// InsertBuilder returns a builder initialised with all columns.
func (t *Table) InsertBuilder() *qb.InsertBuilder {
	return qb.Insert(t.metadata.Name).Columns(t.metadata.Columns...)
}

// Update returns update by primary key statement.
func (t *Table) Update(columns ...string) (stmt string, names []string) {
	return t.UpdateBuilder(columns...).ToCql()
}

// UpdateQuery returns query which updates by primary key.
func (t *Table) UpdateQuery(session gocqlx.Session, columns ...string) *gocqlx.Queryx {
	return session.Query(t.Update(columns...))
}

// UpdateQueryContext returns query wrapped with context which updates by primary key.
func (t *Table) UpdateQueryContext(ctx context.Context, session gocqlx.Session, columns ...string) *gocqlx.Queryx {
	return t.UpdateQuery(session, columns...).WithContext(ctx)
}

// UpdateBuilder returns a builder initialised to update by primary key statement.
func (t *Table) UpdateBuilder(columns ...string) *qb.UpdateBuilder {
	return qb.Update(t.metadata.Name).Set(columns...).Where(t.primaryKeyCmp...)
}

// Delete returns delete by primary key statement.
func (t *Table) Delete(columns ...string) (stmt string, names []string) {
	return t.DeleteBuilder(columns...).ToCql()
}

// DeleteQuery returns query which delete by primary key.
func (t *Table) DeleteQuery(session gocqlx.Session, columns ...string) *gocqlx.Queryx {
	return session.Query(t.Delete(columns...))
}

// DeleteQueryContext returns query wrapped with context which delete by primary key.
func (t *Table) DeleteQueryContext(ctx context.Context, session gocqlx.Session, columns ...string) *gocqlx.Queryx {
	return t.DeleteQuery(session, columns...).WithContext(ctx)
}

// DeleteBuilder returns a builder initialised to delete by primary key statement.
func (t *Table) DeleteBuilder(columns ...string) *qb.DeleteBuilder {
	return qb.Delete(t.metadata.Name).Columns(columns...).Where(t.primaryKeyCmp...)
}
