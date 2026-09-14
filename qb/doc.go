// Copyright (C) 2017 ScyllaDB
// Use of this source code is governed by a ALv2-style
// license that can be found in the LICENSE file.

// Package qb provides CQL query builders. The builders create CQL statement
// and a list of named parameters that can later be bound using gocqlx.
//
// Table names passed to statement builders may be keyspace-qualified. Unquoted
// identifiers that require CQL quoting, such as mixed-case names or reserved
// keywords, are quoted automatically while already quoted identifiers are
// preserved. This intentionally changes generated CQL for mixed-case unquoted
// names: Foobar is rendered as "Foobar" instead of Foobar, preserving case
// rather than relying on CQL's lowercase folding. Pass lowercase names when
// targeting lowercase tables.
package qb
