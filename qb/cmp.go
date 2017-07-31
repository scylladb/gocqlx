package qb

import "bytes"

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
	name   string
}

func (cmp Cmp) writeCql(cql *bytes.Buffer) string {
	cql.WriteString(cmp.column)
	switch cmp.op {
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
	cql.WriteByte('?')

	return cmp.name
}

// Eq produces column=?.
func Eq(column string) Cmp {
	return Cmp{
		op:     eq,
		column: column,
		name:   column,
	}
}

// EqNamed produces column=? with a custom parameter name.
func EqNamed(column, name string) Cmp {
	return Cmp{
		op:     eq,
		column: column,
		name:   name,
	}
}

// Lt produces column<?.
func Lt(column string) Cmp {
	return Cmp{
		op:     lt,
		column: column,
		name:   column,
	}
}

// LtNamed produces column<? with a custom parameter name.
func LtNamed(column, name string) Cmp {
	return Cmp{
		op:     lt,
		column: column,
		name:   name,
	}
}

// LtOrEq produces column<=?.
func LtOrEq(column string) Cmp {
	return Cmp{
		op:     leq,
		column: column,
		name:   column,
	}
}

// LtOrEqNamed produces column<=? with a custom parameter name.
func LtOrEqNamed(column, name string) Cmp {
	return Cmp{
		op:     leq,
		column: column,
		name:   name,
	}
}

// Gt produces column>?.
func Gt(column string) Cmp {
	return Cmp{
		op:     gt,
		column: column,
		name:   column,
	}
}

// GtNamed produces column>? with a custom parameter name.
func GtNamed(column, name string) Cmp {
	return Cmp{
		op:     gt,
		column: column,
		name:   name,
	}
}

// GtOrEq produces column>=?.
func GtOrEq(column string) Cmp {
	return Cmp{
		op:     geq,
		column: column,
		name:   column,
	}
}

// GtOrEqNamed produces column>=? with a custom parameter name.
func GtOrEqNamed(column, name string) Cmp {
	return Cmp{
		op:     geq,
		column: column,
		name:   name,
	}
}

// In produces column IN ?.
func In(column string) Cmp {
	return Cmp{
		op:     in,
		column: column,
		name:   column,
	}
}

// InNamed produces column IN ? with a custom parameter name.
func InNamed(column, name string) Cmp {
	return Cmp{
		op:     in,
		column: column,
		name:   name,
	}
}

// Contains produces column CONTAINS ?.
func Contains(column string) Cmp {
	return Cmp{
		op:     cnt,
		column: column,
		name:   column,
	}
}

// ContainsNamed produces column CONTAINS ? with a custom parameter name.
func ContainsNamed(column, name string) Cmp {
	return Cmp{
		op:     cnt,
		column: column,
		name:   name,
	}
}

type cmps []Cmp

func (cs cmps) writeCql(cql *bytes.Buffer) (names []string) {
	for i, c := range cs {
		names = append(names, c.writeCql(cql))
		if i < len(cs)-1 {
			cql.WriteString(" AND ")
		}
	}
	cql.WriteByte(' ')
	return
}
