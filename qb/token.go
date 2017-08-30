package qb

import (
	"fmt"
	"strings"
)

// Token creates a new TokenBuilder.
func Token(columns ...string) TokenBuilder {
	return TokenBuilder(columns)
}

// TokenBuilder helps implement pagination using token function.
type TokenBuilder []string

// Eq produces token(column)=token(?).
func (t TokenBuilder) Eq() Cmp {
	return Cmp{
		op:     eq,
		fn:     "token",
		column: fmt.Sprint("token(", strings.Join(t, ","), ")"),
		names:  t,
	}
}

// EqNamed produces token(column)=token(?) with a custom parameter name.
func (t TokenBuilder) EqNamed(names ...string) Cmp {
	return Cmp{
		op:     eq,
		fn:     "token",
		column: fmt.Sprint("token(", strings.Join(t, ","), ")"),
		names:  names,
	}
}

// Lt produces token(column)<token(?).
func (t TokenBuilder) Lt() Cmp {
	return Cmp{
		op:     lt,
		fn:     "token",
		column: fmt.Sprint("token(", strings.Join(t, ","), ")"),
		names:  t,
	}
}

// LtNamed produces token(column)<token(?) with a custom parameter name.
func (t TokenBuilder) LtNamed(names ...string) Cmp {
	return Cmp{
		op:     lt,
		fn:     "token",
		column: fmt.Sprint("token(", strings.Join(t, ","), ")"),
		names:  names,
	}
}

// LtOrEq produces token(column)<=token(?).
func (t TokenBuilder) LtOrEq() Cmp {
	return Cmp{
		op:     leq,
		fn:     "token",
		column: fmt.Sprint("token(", strings.Join(t, ","), ")"),
		names:  t,
	}
}

// LtOrEqNamed produces token(column)<=token(?) with a custom parameter name.
func (t TokenBuilder) LtOrEqNamed(names ...string) Cmp {
	return Cmp{
		op:     leq,
		fn:     "token",
		column: fmt.Sprint("token(", strings.Join(t, ","), ")"),
		names:  names,
	}
}

// Gt produces token(column)>token(?).
func (t TokenBuilder) Gt() Cmp {
	return Cmp{
		op:     gt,
		fn:     "token",
		column: fmt.Sprint("token(", strings.Join(t, ","), ")"),
		names:  t,
	}
}

// GtNamed produces token(column)>token(?) with a custom parameter name.
func (t TokenBuilder) GtNamed(names ...string) Cmp {
	return Cmp{
		op:     gt,
		fn:     "token",
		column: fmt.Sprint("token(", strings.Join(t, ","), ")"),
		names:  names,
	}
}

// GtOrEq produces token(column)>=token(?).
func (t TokenBuilder) GtOrEq() Cmp {
	return Cmp{
		op:     geq,
		fn:     "token",
		column: fmt.Sprint("token(", strings.Join(t, ","), ")"),
		names:  t,
	}
}

// GtOrEqNamed produces token(column)>=token(?) with a custom parameter name.
func (t TokenBuilder) GtOrEqNamed(names ...string) Cmp {
	return Cmp{
		op:     geq,
		fn:     "token",
		column: fmt.Sprint("token(", strings.Join(t, ","), ")"),
		names:  names,
	}
}
