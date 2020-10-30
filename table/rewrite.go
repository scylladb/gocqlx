// Copyright (C) 2017 ScyllaDB
// Use of this source code is governed by a ALv2-style
// license that can be found in the LICENSE file.

package table

import (
	"github.com/scylladb/gocqlx/v2"
)

// RewriteRows performs a sequential rewrite of all rows in a table.
func RewriteRows(session gocqlx.Session, t *Table, options ...func(q *gocqlx.Queryx)) error {
	insert := t.InsertQuery(session)
	defer insert.Release()

	// Apply query options
	for _, o := range options {
		o(insert)
	}

	// Iterate over all rows and reinsert them
	iter := session.Query(t.SelectAll()).Iter()
	m := make(map[string]interface{})
	for iter.MapScan(m) {
		if err := insert.BindMap(m).Exec(); err != nil {
			return err
		}
		m = make(map[string]interface{})
	}
	return iter.Close()
}
