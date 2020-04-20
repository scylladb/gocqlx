// Copyright (C) 2017 ScyllaDB
// Use of this source code is governed by a ALv2-style
// license that can be found in the LICENSE file.

package gocqlx

import (
	"github.com/scylladb/go-reflectx"
)

// DefaultMapper uses `db` tag and automatically converts struct field names to
// snake case. It can be set to whatever you want, but it is encouraged to be
// set before gocqlx is used as name-to-field mappings are cached after first
// use on a type.
//
// A custom mapper can always be set per Sessionm, Query and Iter.
var DefaultMapper = reflectx.NewMapperFunc("db", reflectx.CamelToSnakeASCII)
