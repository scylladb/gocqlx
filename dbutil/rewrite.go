// Copyright (C) 2017 ScyllaDB
// Use of this source code is governed by a ALv2-style
// license that can be found in the LICENSE file.

package dbutil

import (
	"github.com/scylladb/gocqlx/v2"
	"github.com/scylladb/gocqlx/v2/table"
)

// RewriteTable rewrites src table to dst table.
// Rows can be transformed using the transform function.
// If row map is empty after transformation the row is skipped.
// Additional options can be passed to modify the insert query.
func RewriteTable(session gocqlx.Session, dst, src *table.Table, transform func(map[string]interface{}), options ...func(q *gocqlx.Queryx)) error {
	insert := dst.InsertQuery(session)
	defer insert.Release()

	// Apply query options
	for _, o := range options {
		o(insert)
	}

	// Iterate over all rows and reinsert them to dst table
	iter := session.Query(src.SelectAll()).Iter()
	m := make(map[string]interface{})
	for iter.MapScan(m) {
		if transform != nil {
			transform(m)
		}
		if len(m) == 0 {
			continue // map is empty - no need to clean
		}
		if err := insert.BindMap(m).Exec(); err != nil {
			return err
		}
		m = map[string]interface{}{}
	}
	return iter.Close()
}
