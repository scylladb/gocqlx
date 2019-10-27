// Copyright (C) 2017 ScyllaDB
// Use of this source code is governed by a ALv2-style
// license that can be found in the LICENSE file.

package qb

import (
	"fmt"
	"strings"
)

// TokenBuilder helps implement pagination using token function.
type TokenBuilder []string

// Token creates a new TokenBuilder.
func Token(columns ...string) TokenBuilder {
	return columns
}

// Eq produces token(column)=token(?).
func (t TokenBuilder) Eq() Cmp {
	return t.cmp(eq, nil)
}

// EqValue produces token(column)=?
func (t TokenBuilder) EqValue() Cmp {
	return t.valueCmp(eq, "")
}

// EqNamed produces token(column)=token(?) with a custom parameter name.
func (t TokenBuilder) EqNamed(names ...string) Cmp {
	return t.cmp(eq, names)
}

// EqValueNamed produces token(column)=? with a custom parameter name.
func (t TokenBuilder) EqValueNamed(name string) Cmp {
	return t.valueCmp(eq, name)
}

// Lt produces token(column)<token(?).
func (t TokenBuilder) Lt() Cmp {
	return t.cmp(lt, nil)
}

// LtValue produces token(column)<?.
func (t TokenBuilder) LtValue() Cmp {
	return t.valueCmp(lt, "")
}

// LtNamed produces token(column)<token(?) with a custom parameter name.
func (t TokenBuilder) LtNamed(names ...string) Cmp {
	return t.cmp(lt, names)
}

// LtValueNamed produces token(column)<? with a custom parameter name.
func (t TokenBuilder) LtValueNamed(name string) Cmp {
	return t.valueCmp(lt, name)
}

// LtOrEq produces token(column)<=token(?).
func (t TokenBuilder) LtOrEq() Cmp {
	return t.cmp(leq, nil)
}

// LtOrEqValue produces token(column)<=?.
func (t TokenBuilder) LtOrEqValue() Cmp {
	return t.valueCmp(leq, "")
}

// LtOrEqNamed produces token(column)<=token(?) with a custom parameter name.
func (t TokenBuilder) LtOrEqNamed(names ...string) Cmp {
	return t.cmp(leq, names)
}

// LtOrEqValueNamed produces token(column)<=? with a custom parameter name.
func (t TokenBuilder) LtOrEqValueNamed(name string) Cmp {
	return t.valueCmp(leq, name)
}

// Gt produces token(column)>token(?).
func (t TokenBuilder) Gt() Cmp {
	return t.cmp(gt, nil)
}

// GtValue produces token(column)>?.
func (t TokenBuilder) GtValue() Cmp {
	return t.valueCmp(gt, "")
}

// GtNamed produces token(column)>token(?) with a custom parameter name.
func (t TokenBuilder) GtNamed(names ...string) Cmp {
	return t.cmp(gt, names)
}

// GtValueNamed produces token(column)>? with a custom parameter name.
func (t TokenBuilder) GtValueNamed(name string) Cmp {
	return t.valueCmp(gt, name)
}

// GtOrEq produces token(column)>=token(?).
func (t TokenBuilder) GtOrEq() Cmp {
	return t.cmp(geq, nil)
}

// GtOrEqValue produces token(column)>=?.
func (t TokenBuilder) GtOrEqValue() Cmp {
	return t.valueCmp(geq, "")
}

// GtOrEqNamed produces token(column)>=token(?) with a custom parameter name.
func (t TokenBuilder) GtOrEqNamed(names ...string) Cmp {
	return t.cmp(geq, names)
}

// GtOrEqValueNamed produces token(column)>=? with a custom parameter name.
func (t TokenBuilder) GtOrEqValueNamed(name string) Cmp {
	return t.valueCmp(geq, name)
}

func (t TokenBuilder) cmp(op op, names []string) Cmp {
	s := names
	if s == nil {
		s = t
	}
	return Cmp{
		op:     op,
		column: fmt.Sprint("token(", strings.Join(t, ","), ")"),
		value:  Fn("token", s...),
	}
}

func (t TokenBuilder) valueCmp(op op, name string) Cmp {
	if name == "" {
		name = "token"
	}
	return Cmp{
		op:     op,
		column: fmt.Sprint("token(", strings.Join(t, ","), ")"),
		value:  param(name),
	}
}
