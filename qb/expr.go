// Copyright (C) 2017 ScyllaDB
// Use of this source code is governed by a ALv2-style
// license that can be found in the LICENSE file.

package qb

import (
	"bytes"
)

type columns []string

func (cols columns) writeCql(cql *bytes.Buffer) {
	for i, c := range cols {
		cql.WriteString(c)
		if i < len(cols)-1 {
			cql.WriteByte(',')
		}
	}
}

type using struct {
	timestamp bool
	ttl       bool
}

func (u using) writeCql(cql *bytes.Buffer) (names []string) {
	if u.timestamp {
		cql.WriteString("USING TIMESTAMP ? ")
		names = append(names, "_ts")
	}

	if u.ttl {
		if u.timestamp {
			cql.WriteString("AND TTL ? ")
		} else {
			cql.WriteString("USING TTL ? ")
		}
		names = append(names, "_ttl")
	}

	return
}

type where cmps

func (w where) writeCql(cql *bytes.Buffer) (names []string) {
	if len(w) == 0 {
		return
	}

	cql.WriteString("WHERE ")
	return cmps(w).writeCql(cql)
}

type _if cmps

func (w _if) writeCql(cql *bytes.Buffer) (names []string) {
	if len(w) == 0 {
		return
	}

	cql.WriteString("IF ")
	return cmps(w).writeCql(cql)
}
