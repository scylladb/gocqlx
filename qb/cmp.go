package qb

// Functions reference:
// http://cassandra.apache.org/doc/latest/cql/functions.html

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
	fn     string
	names  []string
}

// Func wraps comparator value with a custom function, fn is a function name,
// names are function arguments' bind names. For instance function:
//
//    CREATE FUNCTION somefunction(somearg int, anotherarg text)
//
// can be used like this:
//
//    stmt, names := qb.Select("table").
//        Where(qb.Eq("t").Func("somefunction", "somearg", "anotherarg")).
//        ToCql()
//
//    q := gocqlx.Query(session.Query(stmt), names).BindMap(qb.M{
//        "somearg": 1,
//        "anotherarg": "text",
//    })
func (c Cmp) Func(fn string, names ...string) Cmp {
	c.fn = fn
	c.names = names
	return c
}

// MinTimeuuid sets minTimeuuid(?) compare value.
func (c Cmp) MinTimeuuid(name string) Cmp {
	return c.Func("minTimeuuid", name)
}

// MaxTimeuuid sets maxTimeuuid(?) compare value.
func (c Cmp) MaxTimeuuid(name string) Cmp {
	return c.Func("maxTimeuuid", name)
}

// Now sets now() compare value.
func (c Cmp) Now() Cmp {
	return c.Func("now")
}

// Token sets Token(?,?...) compare value.
func (c Cmp) Token(names ...string) Cmp {
	return c.Func("token", names...)
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

	if c.fn == "" {
		cql.WriteByte('?')
		if c.names == nil {
			names = append(names, c.column)
		} else {
			names = append(names, c.names...)
		}
	} else {
		cql.WriteString(c.fn)
		cql.WriteByte('(')
		placeholders(cql, len(c.names))
		cql.WriteByte(')')
		names = append(names, c.names...)
	}

	return
}

// Eq produces column=?.
func Eq(column string) Cmp {
	return Cmp{
		op:     eq,
		column: column,
	}
}

// EqNamed produces column=? with a custom parameter name.
func EqNamed(column, name string) Cmp {
	return Cmp{
		op:     eq,
		column: column,
		names:  []string{name},
	}
}

// Lt produces column<?.
func Lt(column string) Cmp {
	return Cmp{
		op:     lt,
		column: column,
	}
}

// LtNamed produces column<? with a custom parameter name.
func LtNamed(column, name string) Cmp {
	return Cmp{
		op:     lt,
		column: column,
		names:  []string{name},
	}
}

// LtOrEq produces column<=?.
func LtOrEq(column string) Cmp {
	return Cmp{
		op:     leq,
		column: column,
	}
}

// LtOrEqNamed produces column<=? with a custom parameter name.
func LtOrEqNamed(column, name string) Cmp {
	return Cmp{
		op:     leq,
		column: column,
		names:  []string{name},
	}
}

// Gt produces column>?.
func Gt(column string) Cmp {
	return Cmp{
		op:     gt,
		column: column,
	}
}

// GtNamed produces column>? with a custom parameter name.
func GtNamed(column, name string) Cmp {
	return Cmp{
		op:     gt,
		column: column,
		names:  []string{name},
	}
}

// GtOrEq produces column>=?.
func GtOrEq(column string) Cmp {
	return Cmp{
		op:     geq,
		column: column,
	}
}

// GtOrEqNamed produces column>=? with a custom parameter name.
func GtOrEqNamed(column, name string) Cmp {
	return Cmp{
		op:     geq,
		column: column,
		names:  []string{name},
	}
}

// In produces column IN ?.
func In(column string) Cmp {
	return Cmp{
		op:     in,
		column: column,
	}
}

// InNamed produces column IN ? with a custom parameter name.
func InNamed(column, name string) Cmp {
	return Cmp{
		op:     in,
		column: column,
		names:  []string{name},
	}
}

// Contains produces column CONTAINS ?.
func Contains(column string) Cmp {
	return Cmp{
		op:     cnt,
		column: column,
	}
}

// ContainsNamed produces column CONTAINS ? with a custom parameter name.
func ContainsNamed(column, name string) Cmp {
	return Cmp{
		op:     cnt,
		column: column,
		names:  []string{name},
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
