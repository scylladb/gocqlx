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

// structOnlyError returns an error appropriate for type when a non-scannable
// struct is expected but something else is given.
func structOnlyError(t reflect.Type) error {
	if isStruct := t.Kind() == reflect.Struct; !isStruct {
		return fmt.Errorf("expected a struct but got %s", t.Kind())
	}

	if isUnmarshaller := reflect.PtrTo(t).Implements(unmarshallerInterface); isUnmarshaller {
		return fmt.Errorf("expected a struct but the provided struct type %s implements gocql.Unmarshaler", t.Name())
	}

	if isUDTUnmarshaller := reflect.PtrTo(t).Implements(udtUnmarshallerInterface); isUDTUnmarshaller {
		return fmt.Errorf("expected a struct but the provided struct type %s implements gocql.UDTUnmarshaler", t.Name())
	}

	return fmt.Errorf("expected a struct, but struct %s has no exported fields", t.Name())
}

// reflect helpers

var (
	unmarshallerInterface    = reflect.TypeOf((*gocql.Unmarshaler)(nil)).Elem()
	udtUnmarshallerInterface = reflect.TypeOf((*gocql.UDTUnmarshaler)(nil)).Elem()
)

func baseType(t reflect.Type, expected reflect.Kind) (reflect.Type, error) {
	t = reflectx.Deref(t)
	if t.Kind() != expected {
		return nil, fmt.Errorf("expected %s but got %s", expected, t.Kind())
	}
	return t, nil
}

// fieldsByName fills a values interface with fields from the passed value based
// on the traversals in int. If ptrs is true, return addresses instead of values.
// We write this instead of using FieldsByName to save allocations and map lookups
// when iterating over many rows. Empty traversals will get an interface pointer.
// Because of the necessity of requesting ptrs or values, it's considered a bit too
// specialized for inclusion in reflectx itself.
func fieldsByTraversal(v reflect.Value, traversals [][]int, values []interface{}, ptrs bool) error {
	v = reflect.Indirect(v)
	if v.Kind() != reflect.Struct {
		return errors.New("argument not a struct")
	}

	for i, traversal := range traversals {
		if len(traversal) == 0 {
			continue
		}
		f := reflectx.FieldByIndexes(v, traversal)
		if ptrs {
			values[i] = f.Addr().Interface()
		} else {
			values[i] = f.Interface()
		}
	}
	return nil
}

func missingFields(transversals [][]int) (field int, err error) {
	for i, t := range transversals {
		if len(t) == 0 {
			return i, errors.New("missing field")
		}
	}
	return 0, nil
}
