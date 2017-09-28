// Copyright (C) 2017 ScyllaDB
// Use of this source code is governed by a ALv2-style
// license that can be found in the LICENSE file.

package gocqlx

import (
	"errors"
	"fmt"
	"reflect"

	"github.com/gocql/gocql"
	"github.com/jmoiron/sqlx/reflectx"
)

// Get is a convenience function for creating iterator and calling Get.
func Get(dest interface{}, q *gocql.Query) error {
	return Iter(q).Get(dest)
}

// Select is a convenience function for creating iterator and calling Select.
func Select(dest interface{}, q *gocql.Query) error {
	return Iter(q).Select(dest)
}

// Iterx is a wrapper around gocql.Iter which adds struct scanning capabilities.
type Iterx struct {
	*gocql.Iter
	query *gocql.Query
	err   error

	unsafe bool
	Mapper *reflectx.Mapper
	// these fields cache memory use for a rows during iteration w/ structScan
	started bool
	fields  [][]int
	values  []interface{}
}

// Iter creates a new Iterx from gocql.Query using a default mapper.
func Iter(q *gocql.Query) *Iterx {
	return &Iterx{
		Iter:   q.Iter(),
		query:  q,
		Mapper: DefaultMapper,
	}
}

// Unsafe forces the iterator to ignore missing fields. By default when scanning
// a struct if result row has a column that cannot be mapped to any destination
// field an error is reported. With unsafe such columns are ignored.
func (iter *Iterx) Unsafe() *Iterx {
	iter.unsafe = true
	return iter
}

// Get scans first row into a destination and closes the iterator. If the
// destination type is a Struct, then StructScan will be used. If the
// destination is some other type, then the row must only have one column which
// can scan into that type. If no rows were selected, ErrNotFound is returned.
func (iter *Iterx) Get(dest interface{}) error {
	if iter.query == nil {
		return errors.New("using released query")
	}

	iter.scanAny(dest, false)
	iter.Close()
	iter.ReleaseQuery()

	return iter.checkErrAndNotFound()
}

func (iter *Iterx) scanAny(dest interface{}, structOnly bool) bool {
	value := reflect.ValueOf(dest)
	if value.Kind() != reflect.Ptr {
		iter.err = errors.New("must pass a pointer, not a value, to StructScan destination")
		return false
	}
	if value.IsNil() {
		iter.err = errors.New("nil pointer passed to StructScan destination")
		return false
	}

	// no results or query error
	if iter.Iter.NumRows() == 0 {
		return false
	}

	base := reflectx.Deref(value.Type())
	scannable := isScannable(base)

	if structOnly && scannable {
		iter.err = structOnlyError(base)
		return false
	}

	if scannable && len(iter.Columns()) > 1 {
		iter.err = fmt.Errorf("scannable dest type %s with >1 columns (%d) in result", base.Kind(), len(iter.Columns()))
		return false
	}

	if scannable {
		return iter.Scan(dest)
	}

	return iter.StructScan(dest)
}

// Select scans all rows into a destination, which must be a slice of any type
// and closes the iterator. If the destination slice type is a Struct, then
// StructScan will be used on each row. If the destination is some other type,
// then each row must only have one column which can scan into that type.
// If no rows were selected, ErrNotFound is returned.
func (iter *Iterx) Select(dest interface{}) error {
	if iter.query == nil {
		return errors.New("using released query")
	}

	iter.scanAll(dest, false)
	iter.Close()
	iter.ReleaseQuery()

	return iter.err
}

func (iter *Iterx) scanAll(dest interface{}, structOnly bool) bool {
	value := reflect.ValueOf(dest)

	// json.Unmarshal returns errors for these
	if value.Kind() != reflect.Ptr {
		iter.err = errors.New("must pass a pointer, not a value, to StructScan destination")
		return false
	}
	if value.IsNil() {
		iter.err = errors.New("nil pointer passed to StructScan destination")
		return false
	}

	// no results or query error
	if iter.Iter.NumRows() == 0 {
		return false
	}

	slice, err := baseType(value.Type(), reflect.Slice)
	if err != nil {
		iter.err = err
		return false
	}

	isPtr := slice.Elem().Kind() == reflect.Ptr
	base := reflectx.Deref(slice.Elem())
	scannable := isScannable(base)

	if structOnly && scannable {
		iter.err = structOnlyError(base)
		return false
	}

	// if it's a base type make sure it only has 1 column;  if not return an error
	if scannable && len(iter.Columns()) > 1 {
		iter.err = fmt.Errorf("non-struct dest type %s with >1 columns (%d)", base.Kind(), len(iter.Columns()))
		return false
	}

	var (
		alloc bool
		v     reflect.Value
		vp    reflect.Value
		ok    bool
	)
	for {
		// create a new struct type (which returns PtrTo) and indirect it
		vp = reflect.New(base)

		// scan into the struct field pointers
		if !scannable {
			ok = iter.StructScan(vp.Interface())
		} else {
			ok = iter.Scan(vp.Interface())
		}
		if !ok {
			break
		}

		// allocate memory for the page data
		if !alloc {
			v = reflect.MakeSlice(slice, 0, iter.Iter.NumRows())
			alloc = true
		}

		if isPtr {
			v = reflect.Append(v, vp)
		} else {
			v = reflect.Append(v, reflect.Indirect(vp))
		}
	}

	// update dest if allocated slice
	if alloc {
		reflect.Indirect(value).Set(v)
	}

	return true
}

// StructScan is like gocql.Iter.Scan, but scans a single row into a single
// Struct. Use this and iterate manually when the memory load of Select() might
// be prohibitive. StructScan caches the reflect work of matching up column
// positions to fields to avoid that overhead per scan, which means it is not
// safe to run StructScan on the same Iterx instance with different struct
// types.
func (iter *Iterx) StructScan(dest interface{}) bool {
	if iter.query == nil {
		iter.err = errors.New("using released query")
		return false
	}

	v := reflect.ValueOf(dest)
	if v.Kind() != reflect.Ptr {
		iter.err = errors.New("must pass a pointer, not a value, to StructScan destination")
		return false
	}

	// no results or query error
	if iter.Iter.NumRows() == 0 {
		return false
	}

	if !iter.started {
		columns := columnNames(iter.Iter.Columns())
		m := iter.Mapper

		iter.fields = m.TraversalsByName(v.Type(), columns)
		// if we are not unsafe and are missing fields, return an error
		if !iter.unsafe {
			if f, err := missingFields(iter.fields); err != nil {
				iter.err = fmt.Errorf("missing destination name %q in %T", columns[f], dest)
				return false
			}
		}
		iter.values = make([]interface{}, len(columns))
		iter.started = true
	}

	err := fieldsByTraversal(v, iter.fields, iter.values, true)
	if err != nil {
		iter.err = err
		return false
	}
	// scan into the struct field pointers and append to our results
	return iter.Iter.Scan(iter.values...)
}

func columnNames(ci []gocql.ColumnInfo) []string {
	r := make([]string, len(ci))
	for i, column := range ci {
		r[i] = column.Name
	}
	return r
}

// Close closes the iterator and returns any errors that happened during
// the query or the iteration.
func (iter *Iterx) Close() error {
	err := iter.Iter.Close()
	if iter.err == nil {
		iter.err = err
	}
	return iter.err
}

// ReleaseQuery releases underling query back into a pool of queries. Note that
// the iterator needs to be closed first.
func (iter *Iterx) ReleaseQuery() {
	if iter.query != nil {
		iter.query.Release()
		iter.query = nil
	}
}

// checkErrAndNotFound handle error and NotFound in one method.
func (iter *Iterx) checkErrAndNotFound() error {
	if iter.err != nil {
		return iter.err
	} else if iter.Iter.NumRows() == 0 {
		return gocql.ErrNotFound
	}
	return nil
}
