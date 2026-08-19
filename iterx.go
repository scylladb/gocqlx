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

// DefaultStrict disables the behavior of forcing queries and iterators to ignore
// missing fields for all queries. See Strict below for more information.
var DefaultStrict bool

// Iterx is a wrapper around gocql.Iter which adds struct scanning capabilities.
type Iterx struct {
	err error
	*gocql.Iter
	Mapper *reflectx.Mapper

	// Cache memory for rows during iteration in structScan.
	scanPlan   *structScanPlan
	rowScanner gocql.Scanner
	strict     bool
	structOnly bool
	applied    bool
}

// Strict forces the iterator to disable ignoring missing fields. In Strict mode
// when scanning a struct if result row has a column that cannot be mapped to any
// destination field an error is reported. By default such columns are ignored.
func (iter *Iterx) Strict() *Iterx {
	iter.strict = true
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
	_ = iter.Close()

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

	if tuple, ok := iter.topLevelTuple(base); ok {
		return iter.scanTuple(value, tuple)
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
	_ = iter.Close()

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

	tuple, tupleContainer := iter.topLevelTuple(base)

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
		switch {
		case tupleContainer:
			ok = iter.scanTuple(vp, tuple)
		case !scannable:
			ok = iter.structScan(vp)
		default:
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
//   - ptr to t implements gocql.Unmarshaler, gocql.UDTUnmarshaler or UDT
//   - it is not a struct
//   - it has no exported fields
func (iter *Iterx) isScannable(t reflect.Type) bool {
	ptr := reflect.PointerTo(t)
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
	return iter.Iter.Scan(udtWrapValue(value, iter.Mapper, iter.strict))
}

func (iter *Iterx) topLevelTuple(t reflect.Type) (gocql.TupleTypeInfo, bool) {
	if !isTupleScanContainerType(t) {
		return gocql.TupleTypeInfo{}, false
	}

	columns := iter.Columns()
	if len(columns) != 1 {
		return gocql.TupleTypeInfo{}, false
	}

	tuple, ok := columns[0].TypeInfo.(gocql.TupleTypeInfo)
	return tuple, ok
}

func (iter *Iterx) scanTuple(value reflect.Value, tuple gocql.TupleTypeInfo) bool {
	if !iter.nextRow() {
		return false
	}

	values := make([]interface{}, len(tuple.Elems))
	for i := range tuple.Elems {
		elem, err := tupleElementAddr(value, i, len(tuple.Elems))
		if err != nil {
			iter.err = err
			return false
		}
		if elem.Elem().Kind() == reflect.Interface {
			values[i] = tupleInterfaceScanner{value: elem.Elem()}
			continue
		}
		values[i] = udtWrapValue(elem, iter.Mapper, iter.strict)
	}

	if err := iter.rowScanner.Scan(values...); err != nil {
		iter.err = err
		return false
	}
	return true
}

func (iter *Iterx) nextRow() bool {
	if iter.rowScanner == nil {
		iter.rowScanner = iter.Iter.Scanner()
	}
	return iter.rowScanner.Next()
}

// StructScan is like gocql.Iter.Scan, but scans a single row into a single
// struct. Use this and iterate manually when the memory load of Select() might
// be prohibitive. StructScan caches the reflect work of matching up column
// positions to fields to avoid that overhead per scan, which means it is not
// safe to run StructScan on the same Iterx instance with different struct
// types.
//
// Tuple columns can scan into non-byte array, slice, or tuple-shaped struct
// fields with the same mapped name. Tuple-shaped structs honor mapper-visible
// fields, including embedded fields and db:"-" exclusions. Element fields named
// with db tags, such as `db:"coordinates[0]"`, remain supported. When both forms
// are present, the field mapped to the tuple column takes precedence.
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

type structScanPlan struct {
	columns      []string
	fields       [][]int
	tupleIndexes []int // Element index for tuple containers; -1 for direct destinations.
	tupleCounts  []int // Tuple arity; zero for non-tuple columns.
	values       []interface{}
	deferValues  bool // Destinations must be prepared only after a row is available.
}

type tupleInterfaceScanner struct {
	value reflect.Value
}

type tupleDiscardScanner struct{}

func (tupleDiscardScanner) UnmarshalCQL(gocql.TypeInfo, []byte) error {
	return nil
}

func (s tupleInterfaceScanner) UnmarshalCQL(info gocql.TypeInfo, data []byte) error {
	if !s.value.CanSet() {
		return fmt.Errorf("cannot set tuple interface element %s", s.value.Type())
	}
	if data == nil {
		s.value.Set(reflect.Zero(s.value.Type()))
		return nil
	}

	value, err := info.NewWithError()
	if err != nil {
		return err
	}
	if err := gocql.Unmarshal(info, data, value); err != nil {
		return err
	}

	elem := reflect.ValueOf(value).Elem()
	if !elem.Type().AssignableTo(s.value.Type()) {
		return fmt.Errorf("cannot unmarshal %s into %s", info, s.value.Type())
	}
	s.value.Set(elem)
	return nil
}

func (iter *Iterx) structScan(value reflect.Value) bool {
	if value.Kind() != reflect.Ptr {
		panic("value must be a pointer")
	}

	if iter.scanPlan == nil {
		plan, err := iter.structScanPlan(value.Type(), iter.Columns())
		if err != nil {
			iter.err = err
			return false
		}
		cas := len(plan.columns) > 0 && plan.columns[0] == appliedColumn

		// if we are strict and it's not CAS query and are missing fields, return an error
		if iter.strict && !cas {
			if f, err := missingFields(plan.fields); err != nil {
				iter.err = fmt.Errorf("missing destination name %q in %s", plan.columns[f], reflect.Indirect(value).Type())
				return false
			}
		}
		if cas {
			plan.values[0] = &iter.applied
		}
		iter.scanPlan = &plan
	}

	if iter.scanPlan.deferValues && !iter.nextRow() {
		return false
	}

	if err := iter.fieldsByTraversal(value, iter.scanPlan); err != nil {
		iter.err = err
		return false
	}

	// scan into the struct field pointers and append to our results
	if iter.scanPlan.deferValues {
		if err := iter.rowScanner.Scan(iter.scanPlan.values...); err != nil {
			iter.err = err
			return false
		}
		return true
	}
	return iter.Iter.Scan(iter.scanPlan.values...)
}

func (iter *Iterx) structScanPlan(t reflect.Type, columnInfo []gocql.ColumnInfo) (structScanPlan, error) {
	columns := make([]string, len(columnInfo))
	ordinary := true
	for i, column := range columnInfo {
		columns[i] = column.Name
		if _, ok := column.TypeInfo.(gocql.TupleTypeInfo); ok {
			ordinary = false
		}
	}
	if ordinary {
		return structScanPlan{
			columns: columns,
			fields:  iter.Mapper.TraversalsByName(t, columns),
			values:  make([]interface{}, len(columns)),
		}, nil
	}

	var plan structScanPlan
	appendDestination := func(name string, traversal []int, tupleIndex, tupleCount int, discard interface{}) {
		plan.columns = append(plan.columns, name)
		if traversal == nil {
			traversal = []int{}
		}
		plan.fields = append(plan.fields, traversal)
		plan.tupleIndexes = append(plan.tupleIndexes, tupleIndex)
		plan.tupleCounts = append(plan.tupleCounts, tupleCount)
		plan.values = append(plan.values, discard)
	}

	for _, column := range columnInfo {
		tuple, ok := column.TypeInfo.(gocql.TupleTypeInfo)
		if !ok {
			appendDestination(column.Name, traversalByName(iter.Mapper, t, column.Name), -1, 0, nil)
			continue
		}

		traversal := traversalByName(iter.Mapper, t, column.Name)
		if len(traversal) != 0 {
			fieldType := fieldTypeByTraversal(t, traversal)
			switch {
			case isTupleScanContainerType(fieldType):
				plan.deferValues = true
				baseType := derefTupleType(fieldType)
				if baseType.Kind() == reflect.Array && baseType.Len() != len(tuple.Elems) {
					return structScanPlan{}, fmt.Errorf(
						"cannot scan tuple column %q into %s: array length %d does not match tuple element count %d",
						column.Name, fieldType, baseType.Len(), len(tuple.Elems),
					)
				}
				for i := range tuple.Elems {
					appendDestination(gocql.TupleColumnName(column.Name, i), traversal, i, len(tuple.Elems), nil)
				}
				continue
			case isTupleStructType(fieldType, unmarshallerInterface):
				elemTraversals := tupleStructTraversals(iter.Mapper, fieldType)
				if len(elemTraversals) != len(tuple.Elems) {
					return structScanPlan{}, fmt.Errorf(
						"cannot scan tuple column %q into %s: struct has %d mapped fields, expected %d tuple elements",
						column.Name, fieldType, len(elemTraversals), len(tuple.Elems),
					)
				}
				for i, fieldTraversal := range elemTraversals {
					elemTraversal := append(append([]int(nil), traversal...), fieldTraversal...)
					appendDestination(gocql.TupleColumnName(column.Name, i), elemTraversal, -1, len(tuple.Elems), nil)
				}
				continue
			}
			return structScanPlan{}, fmt.Errorf(
				"cannot scan tuple column %q into %v; expected a non-byte array, slice, or struct with %d mapped fields",
				column.Name, fieldType, len(tuple.Elems),
			)
		}

		for i := range tuple.Elems {
			name := gocql.TupleColumnName(column.Name, i)
			traversal := traversalByName(iter.Mapper, t, name)
			var discard interface{}
			if len(traversal) == 0 {
				discard = tupleDiscardScanner{}
			}
			appendDestination(name, traversal, -1, len(tuple.Elems), discard)
		}
	}

	return plan, nil
}

// fieldsByTraversal fills plan.values with addressable destinations from the
// passed value based on the traversals in plan.fields.
// We write this instead of using FieldsByName to save allocations and map
// lookups when iterating over many rows.
// Columns with an empty traversal keep the destination assigned during planning.
func (iter *Iterx) fieldsByTraversal(value reflect.Value, plan *structScanPlan) error {
	value = reflect.Indirect(value)
	if value.Kind() != reflect.Struct {
		return fmt.Errorf("expected a struct but got %s", value.Type())
	}

	for i, traversal := range plan.fields {
		if len(traversal) == 0 {
			continue
		}
		f := reflectx.FieldByIndexes(value, traversal)
		if plan.tupleIndexes[i] >= 0 {
			elem, err := tupleElementAddr(f, plan.tupleIndexes[i], plan.tupleCounts[i])
			if err != nil {
				return err
			}
			if elem.Elem().Kind() == reflect.Interface {
				plan.values[i] = tupleInterfaceScanner{value: elem.Elem()}
				continue
			}
			plan.values[i] = udtWrapValue(elem, iter.Mapper, iter.strict)
			continue
		}
		if plan.tupleCounts[i] > 0 && f.Kind() == reflect.Interface {
			plan.values[i] = tupleInterfaceScanner{value: f}
			continue
		}
		f = f.Addr()
		plan.values[i] = udtWrapValue(f, iter.Mapper, iter.strict)
	}

	return nil
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
	return iter.Iter.Scan(udtWrapSlice(iter.Mapper, iter.strict, dest)...)
}

// Close closes the iterator and returns any errors that happened during
// the query or the iteration.
func (iter *Iterx) Close() error {
	var err error
	if iter.rowScanner != nil {
		scanner := iter.rowScanner
		iter.rowScanner = nil
		err = scanner.Err()
	} else {
		err = iter.Iter.Close()
	}
	if iter.err == nil {
		iter.err = err
	}
	return iter.err
}

// checkErrAndNotFound handle error and NotFound in one method.
func (iter *Iterx) checkErrAndNotFound() error {
	if iter.err != nil {
		return iter.err
	} else if iter.NumRows() == 0 {
		return gocql.ErrNotFound
	}
	return nil
}
