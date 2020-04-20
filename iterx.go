// Copyright (C) 2017 ScyllaDB
// Use of this source code is governed by a ALv2-style
// license that can be found in the LICENSE file.

package gocqlx

import (
	"errors"
	"fmt"
	"reflect"

	"github.com/gocql/gocql"
	"github.com/scylladb/go-reflectx"
)

// DefaultUnsafe enables the behavior of forcing the iterator to ignore
// missing fields for all queries. See Unsafe below for more information.
var DefaultUnsafe bool

// Iterx is a wrapper around gocql.Iter which adds struct scanning capabilities.
type Iterx struct {
	*gocql.Iter
	Mapper *reflectx.Mapper

	unsafe     bool
	structOnly bool
	applied    bool
	err        error

	// Cache memory for a rows during iteration in structScan.
	fields [][]int
	values []interface{}
}

// Unsafe forces the iterator to ignore missing fields. By default when scanning
// a struct if result row has a column that cannot be mapped to any destination
// field an error is reported. With unsafe such columns are ignored.
func (iter *Iterx) Unsafe() *Iterx {
	iter.unsafe = true
	return iter
}

// StructOnly forces the iterator to treat a single-argument struct as
// non-scannable. This is is useful if you need to scan a row into a struct
// that also implements gocql.UDTUnmarshaler or in rare cases gocql.Unmarshaler.
func (iter *Iterx) StructOnly() *Iterx {
	iter.structOnly = true
	return iter
}

// Get scans first row into a destination and closes the iterator.
//
// If the destination type is a struct pointer, then StructScan will be
// used.
// If the destination is some other type, then the row must only have one column
// which can scan into that type.
// This includes types that implement gocql.Unmarshaler and gocql.UDTUnmarshaler.
//
// If you'd like to treat a type that implements gocql.Unmarshaler or
// gocql.UDTUnmarshaler as an ordinary struct you should call
// StructOnly().Get(dest) instead.
//
// If no rows were selected, ErrNotFound is returned.
func (iter *Iterx) Get(dest interface{}) error {
	iter.scanAny(dest)
	iter.Close()

	return iter.checkErrAndNotFound()
}

func (iter *Iterx) scanAny(dest interface{}) bool {
	value := reflect.ValueOf(dest)

	if value.Kind() != reflect.Ptr {
		iter.err = fmt.Errorf("expected a pointer but got %T", dest)
		return false
	}
	if value.IsNil() {
		iter.err = errors.New("expected a pointer but got nil")
		return false
	}

	base := reflectx.Deref(value.Type())
	scannable := iter.isScannable(base)

	if iter.structOnly && scannable {
		if base.Kind() == reflect.Struct {
			scannable = false
		} else {
			iter.err = structOnlyError(base)
			return false
		}
	}

	if scannable && len(iter.Columns()) > 1 {
		iter.err = fmt.Errorf("expected 1 column in result while scanning scannable type %s but got %d", base.Kind(), len(iter.Columns()))
		return false
	}

	if scannable {
		return iter.scan(value)
	}

	return iter.structScan(value)
}

// Select scans all rows into a destination, which must be a pointer to slice
// of any type, and closes the iterator.
//
// If the destination slice type is a struct, then StructScan will be used
// on each row.
// If the destination is some other type, then each row must only have one
// column which can scan into that type.
// This includes types that implement gocql.Unmarshaler and gocql.UDTUnmarshaler.
//
// If you'd like to treat a type that implements gocql.Unmarshaler or
// gocql.UDTUnmarshaler as an ordinary struct you should call
// StructOnly().Select(dest) instead.
//
// If no rows were selected, ErrNotFound is NOT returned.
func (iter *Iterx) Select(dest interface{}) error {
	iter.scanAll(dest)
	iter.Close()

	return iter.err
}

func (iter *Iterx) scanAll(dest interface{}) bool {
	value := reflect.ValueOf(dest)

	// json.Unmarshal returns errors for these
	if value.Kind() != reflect.Ptr {
		iter.err = fmt.Errorf("expected a pointer but got %T", dest)
		return false
	}
	if value.IsNil() {
		iter.err = errors.New("expected a pointer but got nil")
		return false
	}

	slice, err := baseType(value.Type(), reflect.Slice)
	if err != nil {
		iter.err = err
		return false
	}

	isPtr := slice.Elem().Kind() == reflect.Ptr
	base := reflectx.Deref(slice.Elem())
	scannable := iter.isScannable(base)

	if iter.structOnly && scannable {
		if base.Kind() == reflect.Struct {
			scannable = false
		} else {
			iter.err = structOnlyError(base)
			return false
		}
	}

	// if it's a base type make sure it only has 1 column;  if not return an error
	if scannable && len(iter.Columns()) > 1 {
		iter.err = fmt.Errorf("expected 1 column in result while scanning scannable type %s but got %d", base.Kind(), len(iter.Columns()))
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
			ok = iter.structScan(vp)
		} else {
			ok = iter.scan(vp)
		}
		if !ok {
			break
		}

		// allocate memory for the page data
		if !alloc {
			v = reflect.MakeSlice(slice, 0, iter.NumRows())
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

// isScannable takes the reflect.Type and the actual dest value and returns
// whether or not it's Scannable. t is scannable if:
//   * ptr to t implements gocql.Unmarshaler, gocql.UDTUnmarshaler or UDT
//   * it is not a struct
//   * it has no exported fields
func (iter *Iterx) isScannable(t reflect.Type) bool {
	ptr := reflect.PtrTo(t)
	switch {
	case ptr.Implements(unmarshallerInterface):
		return true
	case ptr.Implements(udtUnmarshallerInterface):
		return true
	case ptr.Implements(autoUDTInterface):
		return true
	case t.Kind() != reflect.Struct:
		return true
	default:
		return len(iter.Mapper.TypeMap(t).Index) == 0
	}
}

func (iter *Iterx) scan(value reflect.Value) bool {
	if value.Kind() != reflect.Ptr {
		panic("value must be a pointer")
	}
	return iter.Iter.Scan(udtWrapValue(value, iter.Mapper, iter.unsafe))
}

// StructScan is like gocql.Iter.Scan, but scans a single row into a single
// struct. Use this and iterate manually when the memory load of Select() might
// be prohibitive. StructScan caches the reflect work of matching up column
// positions to fields to avoid that overhead per scan, which means it is not
// safe to run StructScan on the same Iterx instance with different struct
// types.
func (iter *Iterx) StructScan(dest interface{}) bool {
	value := reflect.ValueOf(dest)

	if value.Kind() != reflect.Ptr {
		iter.err = fmt.Errorf("expected a pointer but got %T", dest)
		return false
	}
	if value.IsNil() {
		iter.err = errors.New("expected a pointer but got nil")
		return false
	}

	return iter.structScan(value)
}

const appliedColumn = "[applied]"

func (iter *Iterx) structScan(value reflect.Value) bool {
	if value.Kind() != reflect.Ptr {
		panic("value must be a pointer")
	}

	if iter.fields == nil {
		columns := columnNames(iter.Iter.Columns())
		cas := len(columns) > 0 && columns[0] == appliedColumn

		iter.fields = iter.Mapper.TraversalsByName(value.Type(), columns)
		// if we are not unsafe and it's not CAS query and are missing fields, return an error
		if !iter.unsafe && !cas {
			if f, err := missingFields(iter.fields); err != nil {
				iter.err = fmt.Errorf("missing destination name %q in %s", columns[f], reflect.Indirect(value).Type())
				return false
			}
		}
		iter.values = make([]interface{}, len(columns))
		if cas {
			iter.values[0] = &iter.applied
		}
	}

	if err := iter.fieldsByTraversal(value, iter.fields, iter.values); err != nil {
		iter.err = err
		return false
	}

	// scan into the struct field pointers and append to our results
	return iter.Iter.Scan(iter.values...)
}

// fieldsByName fills a values interface with fields from the passed value based
// on the traversals in int.
// We write this instead of using FieldsByName to save allocations and map
// lookups when iterating over many rows.
// Empty traversals will get an interface pointer.
func (iter *Iterx) fieldsByTraversal(value reflect.Value, traversals [][]int, values []interface{}) error {
	value = reflect.Indirect(value)
	if value.Kind() != reflect.Struct {
		return fmt.Errorf("expected a struct but got %s", value.Type())
	}

	for i, traversal := range traversals {
		if len(traversal) == 0 {
			continue
		}
		f := reflectx.FieldByIndexes(value, traversal).Addr()
		values[i] = udtWrapValue(f, iter.Mapper, iter.unsafe)
	}

	return nil
}

func columnNames(ci []gocql.ColumnInfo) []string {
	r := make([]string, len(ci))
	for i, column := range ci {
		r[i] = column.Name
	}
	return r
}

// Scan consumes the next row of the iterator and copies the columns of the
// current row into the values pointed at by dest. Use nil as a dest value
// to skip the corresponding column. Scan might send additional queries
// to the database to retrieve the next set of rows if paging was enabled.
//
// Scan returns true if the row was successfully unmarshaled or false if the
// end of the result set was reached or if an error occurred. Close should
// be called afterwards to retrieve any potential errors.
func (iter *Iterx) Scan(dest ...interface{}) bool {
	return iter.Iter.Scan(udtWrapSlice(iter.Mapper, iter.unsafe, dest)...)
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

// checkErrAndNotFound handle error and NotFound in one method.
func (iter *Iterx) checkErrAndNotFound() error {
	if iter.err != nil {
		return iter.err
	} else if iter.Iter.NumRows() == 0 {
		return gocql.ErrNotFound
	}
	return nil
}
