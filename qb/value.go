package qb

import "bytes"

// value is a CQL value expression for use in an initializer, assignment, or comparison.
type value interface {
	// clone produces a clone of this value. Writes to the original and the clone are independent.
	// It also marks a type as implementing the value interface.
	clone() value
	// writeCql writes the bytes for this value to the buffer and returns the list of names of
	// parameters which need substitution.
	writeCql(cql *bytes.Buffer) (names []string)
}

// param is a named CQL '?' parameter.
type param string

func (p param) clone() value {
	return p
}

func (p param) writeCql(cql *bytes.Buffer) (names []string) {
	cql.WriteByte('?')
	return []string{string(p)}
}

// lit is a literal CQL value.
type lit string

func (l lit) clone() value {
	return l
}

func (l lit) writeCql(cql *bytes.Buffer) (names []string) {
	cql.WriteString(string(l))
	return []string{}
}
