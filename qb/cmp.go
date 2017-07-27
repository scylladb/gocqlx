package qb

import "bytes"

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

func Eq(column string) Cmp {
	return Cmp{
		op:     eq,
		column: column,
		name:   column,
	}
}

func EqNamed(column, name string) Cmp {
	return Cmp{
		op:     eq,
		column: column,
		name:   name,
	}
}

func Lt(column string) Cmp {
	return Cmp{
		op:     lt,
		column: column,
		name:   column,
	}
}

func LtNamed(column, name string) Cmp {
	return Cmp{
		op:     lt,
		column: column,
		name:   name,
	}
}

func LtOrEq(column string) Cmp {
	return Cmp{
		op:     leq,
		column: column,
		name:   column,
	}
}

func LtOrEqNamed(column, name string) Cmp {
	return Cmp{
		op:     leq,
		column: column,
		name:   name,
	}
}

func Gt(column string) Cmp {
	return Cmp{
		op:     gt,
		column: column,
		name:   column,
	}
}

func GtNamed(column, name string) Cmp {
	return Cmp{
		op:     gt,
		column: column,
		name:   name,
	}
}

func GtOrEq(column string) Cmp {
	return Cmp{
		op:     geq,
		column: column,
		name:   column,
	}
}

func GtOrEqNamed(column, name string) Cmp {
	return Cmp{
		op:     geq,
		column: column,
		name:   name,
	}
}

func In(column string) Cmp {
	return Cmp{
		op:     in,
		column: column,
		name:   column,
	}
}

func InNamed(column, name string) Cmp {
	return Cmp{
		op:     in,
		column: column,
		name:   name,
	}
}

func Contains(column string) Cmp {
	return Cmp{
		op:     cnt,
		column: column,
		name:   column,
	}
}

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
