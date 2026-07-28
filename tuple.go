// Copyright (C) 2017 ScyllaDB
// Use of this source code is governed by a ALv2-style
// license that can be found in the LICENSE file.

package gocqlx

import (
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"

	"github.com/scylladb/go-reflectx"
)

func parseTupleElementName(name string) (base string, index int, ok bool) {
	if !strings.HasSuffix(name, "]") {
		return "", 0, false
	}

	open := strings.LastIndexByte(name, '[')
	if open <= 0 {
		return "", 0, false
	}

	index, err := strconv.Atoi(name[open+1 : len(name)-1])
	// The inferred tuple count is index + 1, so the largest int cannot be
	// represented as a valid tuple element index.
	if err != nil || index < 0 || index == math.MaxInt {
		return "", 0, false
	}

	return name[:open], index, true
}

func tupleCountsByName(names []string) map[string]int {
	var counts map[string]int
	for _, name := range names {
		base, index, ok := parseTupleElementName(name)
		if !ok {
			continue
		}
		if counts == nil {
			counts = make(map[string]int)
		}
		count := index + 1
		if count > counts[base] {
			counts[base] = count
		}
	}
	return counts
}

func traversalByName(mapper *reflectx.Mapper, t reflect.Type, name string) []int {
	traversals := mapper.TraversalsByName(t, []string{name})
	if len(traversals) == 0 || len(traversals[0]) == 0 {
		return nil
	}
	return traversals[0]
}

func fieldTypeByTraversal(t reflect.Type, traversal []int) reflect.Type {
	t = reflectx.Deref(t)
	for _, index := range traversal {
		if t.Kind() != reflect.Struct || index >= t.NumField() {
			return nil
		}
		t = t.Field(index).Type
		t = reflectx.Deref(t)
	}
	return t
}

func isTupleContainerType(t reflect.Type) bool {
	if t == nil {
		return false
	}
	t = reflectx.Deref(t)
	return t.Kind() == reflect.Array || t.Kind() == reflect.Slice
}

func tupleElementByName(mapper *reflectx.Mapper, value reflect.Value, name string, counts map[string]int) (reflect.Value, bool, error) {
	base, index, ok := parseTupleElementName(name)
	if !ok {
		return reflect.Value{}, false, nil
	}

	traversal := traversalByName(mapper, value.Type(), base)
	if len(traversal) == 0 {
		return reflect.Value{}, false, nil
	}

	field, err := fieldByTraversalReadOnly(value, traversal)
	if err != nil {
		return reflect.Value{}, false, fmt.Errorf("could not bind tuple element %q: %w", name, err)
	}
	if !isTupleContainerType(field.Type()) {
		return reflect.Value{}, false, fmt.Errorf("could not bind tuple element %q: expected array or slice in %q but got %s", name, base, field.Type())
	}

	elem, err := tupleElementValue(field, index, counts[base])
	if err != nil {
		return reflect.Value{}, false, fmt.Errorf("could not bind tuple element %q: %w", name, err)
	}
	return elem, true, nil
}

func tupleElementValue(value reflect.Value, index, count int) (reflect.Value, error) {
	value, err := derefTupleValue(value, false)
	if err != nil {
		return reflect.Value{}, err
	}

	switch value.Kind() {
	case reflect.Array, reflect.Slice:
		if value.Kind() == reflect.Slice && value.IsNil() {
			return reflect.Value{}, fmt.Errorf("nil slice does not match tuple element count %d", count)
		}
		if value.Len() != count {
			return reflect.Value{}, fmt.Errorf("%s length %d does not match tuple element count %d", value.Kind(), value.Len(), count)
		}
		if index < 0 || index >= value.Len() {
			return reflect.Value{}, fmt.Errorf("tuple element index %d out of range for %s of length %d", index, value.Kind(), value.Len())
		}
		return value.Index(index), nil
	default:
		return reflect.Value{}, fmt.Errorf("expected array or slice but got %s", value.Kind())
	}
}

func tupleElementAddr(value reflect.Value, index, count int) (reflect.Value, error) {
	value, err := derefTupleValue(value, true)
	if err != nil {
		return reflect.Value{}, err
	}

	switch value.Kind() {
	case reflect.Array:
		if value.Len() != count {
			return reflect.Value{}, fmt.Errorf("array length %d does not match tuple element count %d", value.Len(), count)
		}
	case reflect.Slice:
		if value.Len() != count {
			if !value.CanSet() {
				return reflect.Value{}, fmt.Errorf("cannot resize unsettable slice %s", value.Type())
			}
			next := reflect.MakeSlice(value.Type(), count, count)
			reflect.Copy(next, value)
			value.Set(next)
		}
	default:
		return reflect.Value{}, fmt.Errorf("expected array or slice but got %s", value.Kind())
	}

	if index < 0 || index >= value.Len() {
		return reflect.Value{}, fmt.Errorf("tuple element index %d out of range for %s of length %d", index, value.Kind(), value.Len())
	}
	elem := value.Index(index)
	if !elem.CanAddr() {
		return reflect.Value{}, fmt.Errorf("cannot address tuple element %d in %s", index, value.Type())
	}
	return elem.Addr(), nil
}

func fieldByTraversalReadOnly(value reflect.Value, traversal []int) (reflect.Value, error) {
	for _, index := range traversal {
		value = reflect.Indirect(value)
		if !value.IsValid() {
			return reflect.Value{}, fmt.Errorf("nil pointer in field traversal")
		}
		if value.Kind() != reflect.Struct || index < 0 || index >= value.NumField() {
			return reflect.Value{}, fmt.Errorf("invalid field traversal")
		}
		value = value.Field(index)
	}
	return value, nil
}

func derefTupleValue(value reflect.Value, allocate bool) (reflect.Value, error) {
	if !value.IsValid() {
		return reflect.Value{}, fmt.Errorf("expected array or slice but got nil")
	}

	for value.Kind() == reflect.Ptr {
		if value.IsNil() {
			if !allocate {
				return reflect.Value{}, fmt.Errorf("nil %s", value.Type())
			}
			if !value.CanSet() {
				return reflect.Value{}, fmt.Errorf("cannot allocate unsettable %s", value.Type())
			}
			value.Set(reflect.New(value.Type().Elem()))
		}
		value = value.Elem()
	}

	return value, nil
}

func tupleElementFromMap(m map[string]interface{}, name string, counts map[string]int) (elem interface{}, ok bool, err error) {
	base, index, ok := parseTupleElementName(name)
	if !ok {
		return nil, false, nil
	}

	value, ok := m[base]
	if !ok {
		return nil, false, nil
	}

	rv := reflect.ValueOf(value)
	if !rv.IsValid() || !isTupleContainerType(rv.Type()) {
		return nil, false, fmt.Errorf("could not bind tuple element %q: expected array or slice in %q but got %T", name, base, value)
	}

	element, err := tupleElementValue(rv, index, counts[base])
	if err != nil {
		return nil, false, fmt.Errorf("could not bind tuple element %q: %w", name, err)
	}
	return element.Interface(), true, nil
}
