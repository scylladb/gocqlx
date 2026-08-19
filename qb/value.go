// Copyright (C) 2017 ScyllaDB
// Use of this source code is governed by a ALv2-style
// license that can be found in the LICENSE file.

package qb

import (
	"bytes"
	"strconv"
	"strings"
)

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

// tupleParam is a named CQL tuple '?' parameter.
type tupleParam struct {
	param param
	count int
}

func (t tupleParam) writeCql(cql *bytes.Buffer) (names []string) {
	baseName := string(t.param)
	cql.WriteByte('(')
	for i := 0; i < t.count-1; i++ {
		cql.WriteByte('?')
		cql.WriteByte(',')
		names = append(names, baseName+"["+strconv.Itoa(i)+"]")
	}
	cql.WriteByte('?')
	cql.WriteByte(')')
	names = append(names, baseName+"["+strconv.Itoa(t.count-1)+"]")

	return
}

// lit is a literal CQL value.
type lit string

func (l lit) writeCql(cql *bytes.Buffer) (names []string) {
	cql.WriteString(string(l))
	return nil
}

// stringLit is a quoted CQL string literal.
type stringLit string

func (l stringLit) writeCql(cql *bytes.Buffer) (names []string) {
	writeCqlString(cql, string(l))
	return nil
}

// stringListLit is a parenthesized list of quoted CQL string literals.
type stringListLit []string

func (l stringListLit) writeCql(cql *bytes.Buffer) (names []string) {
	cql.WriteByte('(')
	for i, s := range l {
		writeCqlString(cql, s)
		if i < len(l)-1 {
			cql.WriteByte(',')
		}
	}
	cql.WriteByte(')')
	return nil
}

func writeCqlString(cql *bytes.Buffer, s string) {
	cql.WriteByte('\'')
	cql.WriteString(strings.ReplaceAll(s, "'", "''"))
	cql.WriteByte('\'')
}
