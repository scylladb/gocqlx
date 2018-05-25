// Copyright (C) 2017 ScyllaDB
// Use of this source code is governed by a ALv2-style
// license that can be found in the LICENSE file.

package qb

import (
	"bytes"
)

// op specifies Cmd operation type.
type op byte

const (
	eq op = iota
	lt
	leq
	gt
	geq
	in
	cnt
)

// Cmp if a filtering comparator that is used in WHERE and IF clauses.
type Cmp struct {
	op     op
	column string
	value  value
}

func (c Cmp) writeCql(cql *bytes.Buffer) (names []string) {
	cql.WriteString(c.column)
	switch c.op {
	case eq:
		cql.WriteByte('=')
	case lt:
		cql.WriteByte('<')
	case leq:
		cql.WriteByte('<')
		cql.WriteByte('=')
	case gt:
		cql.WriteByte('>')
	case geq:
		cql.WriteByte('>')
		cql.WriteByte('=')
	case in:
		cql.WriteString(" IN ")
	case cnt:
		cql.WriteString(" CONTAINS ")
	}
	return c.value.writeCql(cql)
}

// Eq produces column=?.
func Eq(column string) Cmp {
	return Cmp{
		op:     eq,
		column: column,
		value:  param(column),
	}
}

// EqNamed produces column=? with a custom parameter name.
func EqNamed(column, name string) Cmp {
	return Cmp{
		op:     eq,
		column: column,
		value:  param(name),
	}
}

// EqLit produces column=literal and does not add a parameter to the query.
func EqLit(column, literal string) Cmp {
	return Cmp{
		op:     eq,
		column: column,
		value:  lit(literal),
	}
}

// EqFunc produces column=someFunc(?...).
func EqFunc(column string, fn *Func) Cmp {
	return Cmp{
		op:     eq,
		column: column,
		value:  fn,
	}
}

// Lt produces column<?.
func Lt(column string) Cmp {
	return Cmp{
		op:     lt,
		column: column,
		value:  param(column),
	}
}

// LtNamed produces column<? with a custom parameter name.
func LtNamed(column, name string) Cmp {
	return Cmp{
		op:     lt,
		column: column,
		value:  param(name),
	}
}

// LtLit produces column<literal and does not add a parameter to the query.
func LtLit(column, literal string) Cmp {
	return Cmp{
		op:     lt,
		column: column,
		value:  lit(literal),
	}
}

// LtFunc produces column<someFunc(?...).
func LtFunc(column string, fn *Func) Cmp {
	return Cmp{
		op:     lt,
		column: column,
		value:  fn,
	}
}

// LtOrEq produces column<=?.
func LtOrEq(column string) Cmp {
	return Cmp{
		op:     leq,
		column: column,
		value:  param(column),
	}
}

// LtOrEqNamed produces column<=? with a custom parameter name.
func LtOrEqNamed(column, name string) Cmp {
	return Cmp{
		op:     leq,
		column: column,
		value:  param(name),
	}
}

// LtOrEqLit produces column<=literal and does not add a parameter to the query.
func LtOrEqLit(column, literal string) Cmp {
	return Cmp{
		op:     leq,
		column: column,
		value:  lit(literal),
	}
}

// LtOrEqFunc produces column<=someFunc(?...).
func LtOrEqFunc(column string, fn *Func) Cmp {
	return Cmp{
		op:     leq,
		column: column,
		value:  fn,
	}
}

// Gt produces column>?.
func Gt(column string) Cmp {
	return Cmp{
		op:     gt,
		column: column,
		value:  param(column),
	}
}

// GtNamed produces column>? with a custom parameter name.
func GtNamed(column, name string) Cmp {
	return Cmp{
		op:     gt,
		column: column,
		value:  param(name),
	}
}

// GtLit produces column>literal and does not add a parameter to the query.
func GtLit(column, literal string) Cmp {
	return Cmp{
		op:     gt,
		column: column,
		value:  lit(literal),
	}
}

// GtFunc produces column>someFunc(?...).
func GtFunc(column string, fn *Func) Cmp {
	return Cmp{
		op:     gt,
		column: column,
		value:  fn,
	}
}

// GtOrEq produces column>=?.
func GtOrEq(column string) Cmp {
	return Cmp{
		op:     geq,
		column: column,
		value:  param(column),
	}
}

// GtOrEqNamed produces column>=? with a custom parameter name.
func GtOrEqNamed(column, name string) Cmp {
	return Cmp{
		op:     geq,
		column: column,
		value:  param(name),
	}
}

// GtOrEqLit produces column>=literal and does not add a parameter to the query.
func GtOrEqLit(column, literal string) Cmp {
	return Cmp{
		op:     geq,
		column: column,
		value:  lit(literal),
	}
}

// GtOrEqFunc produces column>=someFunc(?...).
func GtOrEqFunc(column string, fn *Func) Cmp {
	return Cmp{
		op:     geq,
		column: column,
		value:  fn,
	}
}

// In produces column IN ?.
func In(column string) Cmp {
	return Cmp{
		op:     in,
		column: column,
		value:  param(column),
	}
}

// InNamed produces column IN ? with a custom parameter name.
func InNamed(column, name string) Cmp {
	return Cmp{
		op:     in,
		column: column,
		value:  param(name),
	}
}

// InLit produces column IN literal and does not add a parameter to the query.
func InLit(column, literal string) Cmp {
	return Cmp{
		op:     in,
		column: column,
		value:  lit(literal),
	}
}

// Contains produces column CONTAINS ?.
func Contains(column string) Cmp {
	return Cmp{
		op:     cnt,
		column: column,
		value:  param(column),
	}
}

// ContainsNamed produces column CONTAINS ? with a custom parameter name.
func ContainsNamed(column, name string) Cmp {
	return Cmp{
		op:     cnt,
		column: column,
		value:  param(name),
	}
}

// ContainsLit produces column CONTAINS literal and does not add a parameter to the query.
func ContainsLit(column, literal string) Cmp {
	return Cmp{
		op:     cnt,
		column: column,
		value:  lit(literal),
	}
}

type cmps []Cmp

func (cs cmps) writeCql(cql *bytes.Buffer) (names []string) {
	for i, c := range cs {
		names = append(names, c.writeCql(cql)...)
		if i < len(cs)-1 {
			cql.WriteString(" AND ")
		}
	}
	cql.WriteByte(' ')
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
