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
	value  reflect.Value
	field  map[string]reflect.Value
	unsafe bool
}

func makeUDT(value reflect.Value, mapper *reflectx.Mapper, unsafe bool) udt {
	return udt{
		value:  value,
		field:  mapper.FieldMap(value),
		unsafe: unsafe,
	}
}

func (u udt) MarshalUDT(name string, info gocql.TypeInfo) ([]byte, error) {
	value, ok := u.field[name]
	if !ok {
		return nil, fmt.Errorf("missing name %q in %s", name, u.value.Type())
	}

	return gocql.Marshal(info, value.Interface())
}

func (u udt) UnmarshalUDT(name string, info gocql.TypeInfo, data []byte) error {
	value, ok := u.field[name]
	if !ok && !u.unsafe {
		return fmt.Errorf("missing name %q in %s", name, u.value.Type())
	}

	return gocql.Unmarshal(info, data, value.Addr().Interface())
}

// udtWrapValue adds UDT wrapper if needed.
func udtWrapValue(value reflect.Value, mapper *reflectx.Mapper, unsafe bool) interface{} {
	if value.Type().Implements(autoUDTInterface) {
		return makeUDT(value, mapper, unsafe)
	}
	return value.Interface()
}

// udtWrapSlice adds UDT wrapper if needed.
func udtWrapSlice(mapper *reflectx.Mapper, unsafe bool, v []interface{}) []interface{} {
	for i := range v {
		v[i] = udtWrap(mapper, unsafe, v[i])
	}
	return v
}

func udtWrap(mapper *reflectx.Mapper, unsafe bool, v interface{}) interface{} {
	t := reflect.TypeOf(v)
	switch t.Kind() {
	case reflect.Array:
	case reflect.Slice:
		v = udtWrapSliceArray(mapper, unsafe, v)
	case reflect.Map:
		v = udtWrapSliceMap(mapper, unsafe, v)
	default:
		if _, ok := v.(UDT); ok {
			v = makeUDT(reflect.ValueOf(v), mapper, unsafe)
		}
	}

	return v
}

func udtWrapSliceMap(mapper *reflectx.Mapper, unsafe bool, v interface{}) interface{} {
	val := reflect.ValueOf(v)
	if val.Kind() != reflect.Map {
		return v
	}

	keys := val.MapKeys() //MapKeys zaman aliyor!!!

	if val.Len() == 0 {
		return v
	} else if val.Type().Key().Kind() != reflect.String {
		//Unsupported map key type
		return v
	} else if k := val.MapIndex(keys[0]).Kind(); k != reflect.Array && k != reflect.Slice && k != reflect.Map {
		if _, ok := val.MapIndex(keys[0]).Interface().(UDT); !ok { //Eger ki bir deger udt degilse slice olusturmaya gerek yok.
			return v
		}
	}

	m := make(map[string]interface{})
	for _, key := range keys {
		m[key.String()] = udtWrap(mapper, unsafe, val.MapIndex(key).Interface())
	}

	return m
}

func udtWrapSliceArray(mapper *reflectx.Mapper, unsafe bool, v interface{}) interface{} {
	val := reflect.ValueOf(v)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}

	if val.Len() == 0 {
		return v
	} else if k := val.Index(0).Kind(); k != reflect.Array && k != reflect.Slice && k != reflect.Map {
		if _, ok := val.Index(0).Interface().(UDT); !ok { //Eger ki bir deger udt degilse slice olusturmaya gerek yok.
			return v
		}
	}

	slice := make([]interface{}, val.Len())

	for i := 0; i < val.Len(); i++ {
		slice[i] = udtWrap(mapper, unsafe, val.Index(i).Interface())
	}

	return slice
}
