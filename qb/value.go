// Copyright (C) 2017 ScyllaDB
// Use of this source code is governed by a ALv2-style
// license that can be found in the LICENSE file.

package qb

import "bytes"

// value is a CQL value expression for use in an initializer, assignment,
// or comparison.
type value interface {
	// writeCql writes the bytes for this value to the buffer and returns the
	// list of names of parameters which need substitution.
	writeCql(cql *bytes.Buffer) (names []string)
}

// param is a named CQL '?' parameter.
type param string

func (p param) writeCql(cql *bytes.Buffer) (names []string) {
	cql.WriteByte('?')
	return []string{string(p)}
}

// param is a named CQL tuple '?' parameter.
type tupleParam struct {
	param param
	count int
}

func (t tupleParam) writeCql(cql *bytes.Buffer) (names []string) {
	cql.WriteByte('(')
	for i := 0; i < t.count-1; i++ {
		cql.WriteByte('?')
		cql.WriteByte(',')
	}
	cql.WriteByte('?')
	cql.WriteByte(')')
	return []string{string(t.param)}
}

// lit is a literal CQL value.
type lit string

func (l lit) writeCql(cql *bytes.Buffer) (names []string) {
	cql.WriteString(string(l))
	return nil
}
