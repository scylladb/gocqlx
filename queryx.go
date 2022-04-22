// Copyright (C) 2017 ScyllaDB
// Use of this source code is governed by a ALv2-style
// license that can be found in the LICENSE file.

package gocqlx

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"
	"strconv"

	"github.com/gocql/gocql"
	"github.com/scylladb/go-reflectx"
)

// CompileNamedQueryString translates query with named parameters in a form
// ':<identifier>' to query with '?' placeholders and a list of parameter names.
// If you need to use ':' in a query, i.e. with maps or UDTs use '::' instead.
func CompileNamedQueryString(qs string) (stmt string, names []string, err error) {
	return CompileNamedQuery([]byte(qs))
}

// CompileNamedQuery translates query with named parameters in a form
// ':<identifier>' to query with '?' placeholders and a list of parameter names.
// If you need to use ':' in a query, i.e. with maps or UDTs use '::' instead.
func CompileNamedQuery(qs []byte) (stmt string, names []string, err error) {
	// guess number of names
	n := bytes.Count(qs, []byte(":"))
	if n == 0 {
		return "", nil, errors.New("expected a named query")
	}
	names = make([]string, 0, n)
	rebound := make([]byte, 0, len(qs))

	inName := false
	last := len(qs) - 1
	name := make([]byte, 0, 10)

	for i, b := range qs {
		// a ':' while we're in a name is an error
		switch {
		case b == ':':
			// if this is the second ':' in a '::' escape sequence, append a ':'
			if inName && i > 0 && qs[i-1] == ':' {
				rebound = append(rebound, ':')
				inName = false
				continue
			} else if inName {
				err = errors.New("unexpected `:` while reading named param at " + strconv.Itoa(i))
				return stmt, names, err
			}
			inName = true
			name = []byte{}
			// if we're in a name, and this is an allowed character, continue
		case inName && (allowedBindRune(b) || b == '_' || b == '.') && i != last:
			// append the byte to the name if we are in a name and not on the last byte
			name = append(name, b)
			// if we're in a name and it's not an allowed character, the name is done
		case inName:
			inName = false
			// if this is the final byte of the string and it is part of the name, then
			// make sure to add it to the name
			if i == last && allowedBindRune(b) {
				name = append(name, b)
			}
			// add the string representation to the names list
			names = append(names, string(name))
			// add a proper bindvar for the bindType
			rebound = append(rebound, '?')
			// add this byte to string unless it was not part of the name
			if i != last {
				rebound = append(rebound, b)
			} else if !allowedBindRune(b) {
				rebound = append(rebound, b)
			}
		default:
			// this is a normal byte and should just go onto the rebound query
			rebound = append(rebound, b)
		}
	}

	return string(rebound), names, err
}

func allowedBindRune(b byte) bool {
	return (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') || (b >= '0' && b <= '9')
}

// Queryx is a wrapper around gocql.Query which adds struct binding capabilities.
type Queryx struct {
	*gocql.Query
	Names  []string
	Mapper *reflectx.Mapper

	tr  Transformer
	err error
}

// Query creates a new Queryx from gocql.Query using a default mapper.
//
// Deprecated: Use gocqlx.Session.Query API instead.
func Query(q *gocql.Query, names []string) *Queryx {
	return &Queryx{
		Query:  q,
		Names:  names,
		Mapper: DefaultMapper,
		tr:     DefaultBindTransformer,
	}
}

// WithBindTransformer sets the query bind transformer.
// The transformer is called right before binding a value to a named parameter.
func (q *Queryx) WithBindTransformer(tr Transformer) *Queryx {
	q.tr = tr
	return q
}

// BindStruct binds query named parameters to values from arg using mapper. If
// value cannot be found error is reported.
func (q *Queryx) BindStruct(arg interface{}) *Queryx {
	arglist, err := q.bindStructArgs(arg, nil)
	if err != nil {
		q.err = fmt.Errorf("bind error: %s", err)
	} else {
		q.err = nil
		q.Bind(arglist...)
	}

	return q
}

// BindStructMap binds query named parameters to values from arg0 and arg1
// using a mapper. If value cannot be found in arg0 it's looked up in arg1
// before reporting an error.
func (q *Queryx) BindStructMap(arg0 interface{}, arg1 map[string]interface{}) *Queryx {
	arglist, err := q.bindStructArgs(arg0, arg1)
	if err != nil {
		q.err = fmt.Errorf("bind error: %s", err)
	} else {
		q.err = nil
		q.Bind(arglist...)
	}

	return q
}

func (q *Queryx) bindStructArgs(arg0 interface{}, arg1 map[string]interface{}) ([]interface{}, error) {
	arglist := make([]interface{}, 0, len(q.Names))

	// grab the indirected value of arg
	v := reflect.ValueOf(arg0)
	for v = reflect.ValueOf(arg0); v.Kind() == reflect.Ptr; {
		v = v.Elem()
	}

	err := q.Mapper.TraversalsByNameFunc(v.Type(), q.Names, func(i int, t []int) error {
		if len(t) != 0 {
			val := reflectx.FieldByIndexesReadOnly(v, t) // nolint:scopelint
			arglist = append(arglist, val.Interface())
		} else {
			val, ok := arg1[q.Names[i]]
			if !ok {
				return fmt.Errorf("could not find name %q in %#v and %#v", q.Names[i], arg0, arg1)
			}
			arglist = append(arglist, val)
		}

		if q.tr != nil {
			arglist[i] = q.tr(q.Names[i], arglist[i])
		}

		return nil
	})

	return arglist, err
}

// BindMap binds query named parameters using map.
func (q *Queryx) BindMap(arg map[string]interface{}) *Queryx {
	arglist, err := q.bindMapArgs(arg)
	if err != nil {
		q.err = fmt.Errorf("bind error: %s", err)
	} else {
		q.err = nil
		q.Bind(arglist...)
	}

	return q
}

func (q *Queryx) bindMapArgs(arg map[string]interface{}) ([]interface{}, error) {
	arglist := make([]interface{}, 0, len(q.Names))

	for _, name := range q.Names {
		val, ok := arg[name]
		if !ok {
			return arglist, fmt.Errorf("could not find name %q in %#v", name, arg)
		}

		if q.tr != nil {
			val = q.tr(name, val)
		}
		arglist = append(arglist, val)
	}
	return arglist, nil
}

// Bind sets query arguments of query. This can also be used to rebind new query arguments
// to an existing query instance.
func (q *Queryx) Bind(v ...interface{}) *Queryx {
	q.Query.Bind(udtWrapSlice(q.Mapper, DefaultUnsafe, v)...)
	return q
}

// Err returns any binding errors.
func (q *Queryx) Err() error {
	return q.err
}

// Exec executes the query without returning any rows.
func (q *Queryx) Exec() error {
	if q.err != nil {
		return q.err
	}
	return q.Query.Exec()
}

// ExecRelease calls Exec and releases the query, a released query cannot be
// reused.
func (q *Queryx) ExecRelease() error {
	defer q.Release()
	return q.Exec()
}

// ExecCAS executes the Lightweight Transaction query, returns whether query was applied.
// See: https://docs.scylladb.com/using-scylla/lwt/ for more details.
//
// When using Cassandra it may be necessary to use NoSkipMetadata in order to obtain an
// accurate "applied" value. See the documentation of NoSkipMetaData method on this page
// for more details.
func (q *Queryx) ExecCAS() (applied bool, err error) {
	q.NoSkipMetadata()
	iter := q.Iter().StructOnly()
	if err := iter.Get(&struct{}{}); err != nil {
		return false, err
	}
	return iter.applied, iter.Close()
}

// ExecCASRelease calls ExecCAS and releases the query, a released query cannot be
// reused.
func (q *Queryx) ExecCASRelease() (bool, error) {
	defer q.Release()
	return q.ExecCAS()
}

// Get scans first row into a destination and closes the iterator.
//
// If the destination type is a struct pointer, then Iter.StructScan will be
// used.
// If the destination is some other type, then the row must only have one column
// which can scan into that type.
// This includes types that implement gocql.Unmarshaler and gocql.UDTUnmarshaler.
//
// If you'd like to treat a type that implements gocql.Unmarshaler or
// gocql.UDTUnmarshaler as an ordinary struct you should call
// Iter().StructOnly().Get(dest) instead.
//
// If no rows were selected, ErrNotFound is returned.
func (q *Queryx) Get(dest interface{}) error {
	if q.err != nil {
		return q.err
	}
	return q.Iter().Get(dest)
}

// GetRelease calls Get and releases the query, a released query cannot be
// reused.
func (q *Queryx) GetRelease(dest interface{}) error {
	defer q.Release()
	return q.Get(dest)
}

// GetCAS executes a lightweight transaction.
// If the transaction fails because the existing values did not match,
// the previous values will be stored in dest object.
// See: https://docs.scylladb.com/using-scylla/lwt/ for more details.
func (q *Queryx) GetCAS(dest interface{}) (applied bool, err error) {
	if q.err != nil {
		return false, q.err
	}

	q.NoSkipMetadata()
	iter := q.Iter()
	if err := iter.Get(dest); err != nil {
		return false, err
	}

	return iter.applied, iter.Close()
}

// GetCASRelease calls GetCAS and releases the query, a released query cannot be
// reused.
func (q *Queryx) GetCASRelease(dest interface{}) (bool, error) {
	defer q.Release()
	return q.GetCAS(dest)
}

// Select scans all rows into a destination, which must be a pointer to slice
// of any type, and closes the iterator.
//
// If the destination slice type is a struct, then Iter.StructScan will be used
// on each row.
// If the destination is some other type, then each row must only have one
// column which can scan into that type.
// This includes types that implement gocql.Unmarshaler and gocql.UDTUnmarshaler.
//
// If you'd like to treat a type that implements gocql.Unmarshaler or
// gocql.UDTUnmarshaler as an ordinary struct you should call
// Iter().StructOnly().Select(dest) instead.
//
// If no rows were selected, ErrNotFound is NOT returned.
func (q *Queryx) Select(dest interface{}) error {
	if q.err != nil {
		return q.err
	}
	return q.Iter().Select(dest)
}

// SelectRelease calls Select and releases the query, a released query cannot be
// reused.
func (q *Queryx) SelectRelease(dest interface{}) error {
	defer q.Release()
	return q.Select(dest)
}

// Iter returns Iterx instance for the query. It should be used when data is too
// big to be loaded with Select in order to do row by row iteration.
// See Iterx StructScan function.
func (q *Queryx) Iter() *Iterx {
	return &Iterx{
		Iter:   q.Query.Iter(),
		Mapper: q.Mapper,
		unsafe: DefaultUnsafe,
	}
}
