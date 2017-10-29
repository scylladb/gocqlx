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
	return TokenBuilder(columns)
}

// Eq produces token(column)=token(?).
func (t TokenBuilder) Eq() Cmp {
	return t.cmp(eq, nil)
}

// EqNamed produces token(column)=token(?) with a custom parameter name.
func (t TokenBuilder) EqNamed(names ...string) Cmp {
	return t.cmp(eq, names)
}

// Lt produces token(column)<token(?).
func (t TokenBuilder) Lt() Cmp {
	return t.cmp(lt, nil)
}

// LtNamed produces token(column)<token(?) with a custom parameter name.
func (t TokenBuilder) LtNamed(names ...string) Cmp {
	return t.cmp(lt, names)
}

// LtOrEq produces token(column)<=token(?).
func (t TokenBuilder) LtOrEq() Cmp {
	return t.cmp(leq, nil)
}

// LtOrEqNamed produces token(column)<=token(?) with a custom parameter name.
func (t TokenBuilder) LtOrEqNamed(names ...string) Cmp {
	return t.cmp(leq, names)
}

// Gt produces token(column)>token(?).
func (t TokenBuilder) Gt() Cmp {
	return t.cmp(gt, nil)
}

// GtNamed produces token(column)>token(?) with a custom parameter name.
func (t TokenBuilder) GtNamed(names ...string) Cmp {
	return t.cmp(gt, names)
}

// GtOrEq produces token(column)>=token(?).
func (t TokenBuilder) GtOrEq() Cmp {
	return t.cmp(geq, nil)
}

// GtOrEqNamed produces token(column)>=token(?) with a custom parameter name.
func (t TokenBuilder) GtOrEqNamed(names ...string) Cmp {
	return t.cmp(geq, names)
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
