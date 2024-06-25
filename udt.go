// Copyright (C) 2017 ScyllaDB
// Use of this source code is governed by a ALv2-style
// license that can be found in the LICENSE file.

package gocqlx

import (
	"fmt"
	"reflect"

	"github.com/gocql/gocql"
	"github.com/scylladb/go-reflectx"
)

// UDT is a marker interface that needs to be embedded in a struct if you want
// to marshal or unmarshal it as a User Defined Type.
type UDT interface {
	udt()
}

var (
	_ gocql.UDTMarshaler   = udt{}
	_ gocql.UDTUnmarshaler = udt{}
)

type udt struct {
	field  map[string]reflect.Value
	value  reflect.Value
	strict bool
}

func makeUDT(value reflect.Value, mapper *reflectx.Mapper, strict bool) udt {
	return udt{
		value:  value,
		field:  mapper.FieldMap(value),
		strict: strict,
	}
}

func (u udt) MarshalUDT(name string, info gocql.TypeInfo) ([]byte, error) {
	value, ok := u.field[name]
	if ok {
		return gocql.Marshal(info, value.Interface())
	}
	if !u.strict {
		return nil, nil
	}
	return nil, fmt.Errorf("missing name %q in %s", name, u.value.Type())
}

func (u udt) UnmarshalUDT(name string, info gocql.TypeInfo, data []byte) error {
	value, ok := u.field[name]
	if ok {
		return gocql.Unmarshal(info, data, value.Addr().Interface())
	}
	if !u.strict {
		return nil
	}
	return fmt.Errorf("missing name %q in %s", name, u.value.Type())
}

// udtWrapValue adds UDT wrapper if needed.
func udtWrapValue(value reflect.Value, mapper *reflectx.Mapper, strict bool) interface{} {
	if value.Type().Implements(autoUDTInterface) {
		return makeUDT(value, mapper, strict)
	}
	return value.Interface()
}

// udtWrapSlice adds UDT wrapper if needed.
func udtWrapSlice(mapper *reflectx.Mapper, strict bool, v []interface{}) []interface{} {
	for i := range v {
		if _, ok := v[i].(UDT); ok {
			v[i] = makeUDT(reflect.ValueOf(v[i]), mapper, strict)
		}
	}
	return v
}
