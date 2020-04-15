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

	if isAutoUDT := reflect.PtrTo(t).Implements(autoUDTInterface); isAutoUDT {
		return fmt.Errorf("expected a struct but the provided struct type %s implements gocqlx.UDT", t.Name())
	}

	return fmt.Errorf("expected a struct, but struct %s has no exported fields", t.Name())
}

// reflect helpers

var (
	unmarshallerInterface    = reflect.TypeOf((*gocql.Unmarshaler)(nil)).Elem()
	udtUnmarshallerInterface = reflect.TypeOf((*gocql.UDTUnmarshaler)(nil)).Elem()
	autoUDTInterface         = reflect.TypeOf((*UDT)(nil)).Elem()
)

func baseType(t reflect.Type, expected reflect.Kind) (reflect.Type, error) {
	t = reflectx.Deref(t)
	if t.Kind() != expected {
		return nil, fmt.Errorf("expected %s but got %s", expected, t.Kind())
	}
	return t, nil
}

func missingFields(transversals [][]int) (field int, err error) {
	for i, t := range transversals {
		if len(t) == 0 {
			return i, errors.New("missing field")
		}
	}
	return 0, nil
}
