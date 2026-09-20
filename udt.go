// Copyright (C) 2017 ScyllaDB
// Use of this source code is governed by a ALv2-style
// license that can be found in the LICENSE file.

package gocqlx

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"reflect"
	"runtime"
	"sync"

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
	_ gocql.Marshaler      = udtCodec{}
	_ gocql.Unmarshaler    = udtCodec{}

	marshalerInterface    = reflect.TypeOf((*gocql.Marshaler)(nil)).Elem()
	udtMarshalerInterface = reflect.TypeOf((*gocql.UDTMarshaler)(nil)).Elem()
	udtTypeCache          sync.Map
)

type udt struct {
	fields *reflectx.StructMap
	mapper *reflectx.Mapper
	value  reflect.Value
	strict bool
}

func makeUDT(value reflect.Value, mapper *reflectx.Mapper, strict bool) udt {
	return udt{
		value:  value,
		fields: mapper.TypeMap(reflect.Indirect(value).Type()),
		mapper: mapper,
		strict: strict,
	}
}

func (u udt) MarshalUDT(name string, info gocql.TypeInfo) ([]byte, error) {
	value, ok := u.fieldByName(name, true)
	if ok {
		return marshalUDTValue(info, value, u.mapper, u.strict)
	}
	if !u.strict {
		return nil, nil
	}
	return nil, fmt.Errorf("missing name %q in %s", name, u.value.Type())
}

func (u udt) UnmarshalUDT(name string, info gocql.TypeInfo, data []byte) error {
	value, ok := u.fieldByName(name, false)
	if ok {
		return unmarshalUDTValue(info, data, value.Addr(), u.mapper, u.strict)
	}
	if !u.strict {
		return nil
	}
	return fmt.Errorf("missing name %q in %s", name, u.value.Type())
}

func (u udt) fieldByName(name string, readOnly bool) (reflect.Value, bool) {
	value := reflect.Indirect(u.value)
	field, ok := u.fields.Names[name]
	if !ok {
		return reflect.Value{}, false
	}
	if !readOnly {
		return reflectx.FieldByIndexes(value, field.Index), true
	}

	// FieldByIndexesReadOnly panics on a nil intermediate pointer. Keep this
	// traversal open-coded so marshaling can return the field's zero value.
	for _, index := range field.Index {
		value = reflect.Indirect(value)
		if !value.IsValid() {
			return reflect.Zero(field.Field.Type), true
		}
		value = value.Field(index)
	}
	return value, true
}

// udtCodec is an internal compatibility layer for mapper-aware UDTs in
// collections. TODO: replace its custom collection framing when
// https://github.com/scylladb/gocql/issues/985 provides an upstream hook.
type udtCodec struct {
	mapper *reflectx.Mapper
	value  reflect.Value
	strict bool
}

func (u udtCodec) MarshalCQL(info gocql.TypeInfo) ([]byte, error) {
	return marshalUDTValue(info, u.value, u.mapper, u.strict)
}

func (u udtCodec) UnmarshalCQL(info gocql.TypeInfo, data []byte) error {
	return unmarshalUDTValue(info, data, u.value, u.mapper, u.strict)
}

// udtWrapValue adds UDT wrapper if needed.
func udtWrapValue(value reflect.Value, mapper *reflectx.Mapper, strict bool) interface{} {
	if !value.IsValid() {
		return nil
	}
	value = indirectInterface(value)
	if isNil(value) {
		return value.Interface()
	}
	if value.Type().Implements(autoUDTInterface) {
		return udtCodec{value: value, mapper: mapper, strict: strict}
	}
	if typeContainsUDT(value.Type()) {
		return udtCodec{value: value, mapper: mapper, strict: strict}
	}
	return value.Interface()
}

// udtWrapSlice adds UDT wrapper if needed.
func udtWrapSlice(mapper *reflectx.Mapper, strict bool, v []interface{}) []interface{} {
	out := make([]interface{}, len(v))
	for i := range v {
		out[i] = udtWrapValue(reflect.ValueOf(v[i]), mapper, strict)
	}
	return out
}

func marshalUDTValue(info gocql.TypeInfo, value reflect.Value, mapper *reflectx.Mapper, strict bool) ([]byte, error) {
	value = indirectInterface(value)
	if !value.IsValid() || value.Kind() == reflect.Ptr && value.IsNil() {
		return nil, nil
	}
	autoUDT := value.Type().Implements(autoUDTInterface)
	if value.CanInterface() {
		v := value.Interface()
		if _, ok := v.(gocql.Marshaler); ok && (!autoUDT || !hasPromotedCodec(value.Type(), marshalerInterface)) {
			return gocql.Marshal(info, v)
		}
		if info.Type() == gocql.TypeUDT {
			if _, ok := v.(gocql.UDTMarshaler); ok && (!autoUDT || !hasPromotedCodec(value.Type(), udtMarshalerInterface)) {
				return gocql.Marshal(info, v)
			}
		}
	}
	if value.Kind() == reflect.Interface && value.IsNil() {
		return nil, nil
	}
	if autoUDT {
		return gocql.Marshal(info, makeUDT(value, mapper, strict))
	}
	if value.Kind() == reflect.Ptr {
		return marshalUDTValue(info, value.Elem(), mapper, strict)
	}

	switch info.Type() {
	case gocql.TypeList, gocql.TypeSet:
		return marshalUDTCollection(info, value, mapper, strict)
	case gocql.TypeMap:
		return marshalUDTMap(info, value, mapper, strict)
	default:
		// TODO: add mapper-aware tuple and vector recursion in #395.
		return gocql.Marshal(info, value.Interface())
	}
}

func marshalUDTMap(info gocql.TypeInfo, value reflect.Value, mapper *reflectx.Mapper, strict bool) ([]byte, error) {
	mapInfo, ok := info.(gocql.CollectionType)
	if !ok || value.Kind() != reflect.Map {
		return gocql.Marshal(info, value.Interface())
	}
	if value.IsNil() {
		return nil, nil
	}

	buf := &bytes.Buffer{}
	if err := writeCollectionSize(buf, value.Len()); err != nil {
		return nil, err
	}
	for _, key := range value.MapKeys() {
		data, err := marshalUDTValue(mapInfo.Key, key, mapper, strict)
		if err != nil {
			return nil, err
		}
		if err := writeCollectionBytes(buf, data); err != nil {
			return nil, err
		}

		data, err = marshalUDTValue(mapInfo.Elem, value.MapIndex(key), mapper, strict)
		if err != nil {
			return nil, err
		}
		if err := writeCollectionBytes(buf, data); err != nil {
			return nil, err
		}
	}
	return buf.Bytes(), nil
}

func marshalUDTCollection(info gocql.TypeInfo, value reflect.Value, mapper *reflectx.Mapper, strict bool) ([]byte, error) {
	collectionInfo, ok := info.(gocql.CollectionType)
	if !ok {
		return gocql.Marshal(info, value.Interface())
	}

	var items []reflect.Value
	switch value.Kind() {
	case reflect.Slice:
		if value.IsNil() {
			return nil, nil
		}
		items = make([]reflect.Value, value.Len())
		for i := range items {
			items[i] = value.Index(i)
		}
	case reflect.Array:
		items = make([]reflect.Value, value.Len())
		for i := range items {
			items[i] = value.Index(i)
		}
	case reflect.Map:
		if value.Type().Elem().Kind() != reflect.Struct || value.Type().Elem().NumField() != 0 {
			return gocql.Marshal(info, value.Interface())
		}
		items = value.MapKeys()
	default:
		return gocql.Marshal(info, value.Interface())
	}

	buf := &bytes.Buffer{}
	if err := writeCollectionSize(buf, len(items)); err != nil {
		return nil, err
	}
	for _, item := range items {
		data, err := marshalUDTValue(collectionInfo.Elem, item, mapper, strict)
		if err != nil {
			return nil, err
		}
		if err := writeCollectionBytes(buf, data); err != nil {
			return nil, err
		}
	}
	return buf.Bytes(), nil
}

func unmarshalUDTValue(info gocql.TypeInfo, data []byte, value reflect.Value, mapper *reflectx.Mapper, strict bool) error {
	value = indirectInterface(value)
	if !value.IsValid() {
		return fmt.Errorf("cannot unmarshal %s into an invalid value", info)
	}
	autoUDT := value.Type().Implements(autoUDTInterface)
	if value.CanInterface() {
		v := value.Interface()
		if _, ok := v.(gocql.Unmarshaler); ok && (!autoUDT || !hasPromotedCodec(value.Type(), unmarshallerInterface)) {
			return gocql.Unmarshal(info, data, v)
		}
		if info.Type() == gocql.TypeUDT {
			if _, ok := v.(gocql.UDTUnmarshaler); ok && (!autoUDT || !hasPromotedCodec(value.Type(), udtUnmarshallerInterface)) {
				return gocql.Unmarshal(info, data, v)
			}
		}
	}
	if autoUDT {
		// TODO: remove this automatic-UDT workaround after
		// https://github.com/scylladb/gocql/issues/986 is fixed upstream.
		if data == nil {
			return zeroUDTValue(value)
		}
		return gocql.Unmarshal(info, data, makeUDT(value, mapper, strict))
	}
	if value.Kind() == reflect.Ptr {
		if value.IsNil() {
			if data == nil {
				return nil
			}
			value.Set(reflect.New(value.Type().Elem()))
		}
		if value.Elem().Kind() == reflect.Ptr {
			if data == nil {
				value.Elem().Set(reflect.Zero(value.Type().Elem()))
				return nil
			}
			if value.Elem().IsNil() {
				value.Elem().Set(reflect.New(value.Type().Elem().Elem()))
			}
			return unmarshalUDTValue(info, data, value.Elem(), mapper, strict)
		}
		value = value.Elem()
	}

	switch info.Type() {
	case gocql.TypeList, gocql.TypeSet:
		if !value.CanSet() {
			return fmt.Errorf("cannot unmarshal %s into non-pointer %s", info, value.Type())
		}
		return unmarshalUDTCollection(info, data, value, mapper, strict)
	case gocql.TypeMap:
		if !value.CanSet() {
			return fmt.Errorf("cannot unmarshal %s into non-pointer %s", info, value.Type())
		}
		return unmarshalUDTMap(info, data, value, mapper, strict)
	default:
		if value.CanAddr() {
			return gocql.Unmarshal(info, data, value.Addr().Interface())
		}
		return gocql.Unmarshal(info, data, value.Interface())
	}
}

func zeroUDTValue(value reflect.Value) error {
	if value.Kind() == reflect.Ptr {
		if value.IsNil() {
			return nil
		}
		value = value.Elem()
	}
	if !value.CanSet() {
		return fmt.Errorf("cannot zero %s", value.Type())
	}
	value.Set(reflect.Zero(value.Type()))
	return nil
}

func unmarshalUDTCollection(info gocql.TypeInfo, data []byte, value reflect.Value, mapper *reflectx.Mapper, strict bool) error {
	collectionInfo, ok := info.(gocql.CollectionType)
	if !ok {
		return unmarshalUDTFallback(info, data, value)
	}

	kind := value.Kind()
	isMapSet := kind == reflect.Map && value.Type().Elem().Kind() == reflect.Struct && value.Type().Elem().NumField() == 0
	if kind != reflect.Slice && kind != reflect.Array && !isMapSet {
		return unmarshalUDTFallback(info, data, value)
	}
	if data == nil {
		if kind == reflect.Array {
			return unmarshalUDTFallback(info, data, value)
		}
		value.Set(reflect.Zero(value.Type()))
		return nil
	}

	n, rest, err := readCollectionSize(data)
	if err != nil {
		return err
	}
	if n < 0 {
		return fmt.Errorf("negative collection size %d", n)
	}

	switch {
	case kind == reflect.Array:
		if value.Len() != n {
			return fmt.Errorf("cannot unmarshal collection of length %d into %s", n, value.Type())
		}
	case kind == reflect.Slice:
		value.Set(reflect.MakeSlice(value.Type(), n, n))
	case isMapSet:
		value.Set(reflect.MakeMapWithSize(value.Type(), n))
	}

	for i := 0; i < n; i++ {
		itemData, next, err := readCollectionBytes(rest)
		if err != nil {
			return err
		}
		rest = next

		if isMapSet {
			key := reflect.New(value.Type().Key())
			if err := unmarshalUDTValue(collectionInfo.Elem, itemData, key, mapper, strict); err != nil {
				return err
			}
			value.SetMapIndex(key.Elem(), reflect.Zero(value.Type().Elem()))
			continue
		}

		if err := unmarshalUDTValue(collectionInfo.Elem, itemData, value.Index(i).Addr(), mapper, strict); err != nil {
			return err
		}
	}
	return nil
}

func unmarshalUDTFallback(info gocql.TypeInfo, data []byte, value reflect.Value) error {
	if value.CanAddr() {
		return gocql.Unmarshal(info, data, value.Addr().Interface())
	}
	return gocql.Unmarshal(info, data, value.Interface())
}

func unmarshalUDTMap(info gocql.TypeInfo, data []byte, value reflect.Value, mapper *reflectx.Mapper, strict bool) error {
	mapInfo, ok := info.(gocql.CollectionType)
	if !ok || value.Kind() != reflect.Map {
		return unmarshalUDTFallback(info, data, value)
	}
	if data == nil {
		value.Set(reflect.Zero(value.Type()))
		return nil
	}

	n, rest, err := readCollectionSize(data)
	if err != nil {
		return err
	}
	if n < 0 {
		return fmt.Errorf("negative map size %d", n)
	}
	value.Set(reflect.MakeMapWithSize(value.Type(), n))
	for i := 0; i < n; i++ {
		keyData, next, err := readCollectionBytes(rest)
		if err != nil {
			return err
		}
		rest = next
		key := reflect.New(value.Type().Key())
		if err := unmarshalUDTValue(mapInfo.Key, keyData, key, mapper, strict); err != nil {
			return err
		}

		valueData, next, err := readCollectionBytes(rest)
		if err != nil {
			return err
		}
		rest = next
		item := reflect.New(value.Type().Elem())
		if err := unmarshalUDTValue(mapInfo.Elem, valueData, item, mapper, strict); err != nil {
			return err
		}
		value.SetMapIndex(key.Elem(), item.Elem())
	}
	return nil
}

func writeCollectionBytes(buf *bytes.Buffer, data []byte) error {
	if data == nil {
		return writeCollectionSize(buf, -1)
	}
	if err := writeCollectionSize(buf, len(data)); err != nil {
		return err
	}
	_, err := buf.Write(data)
	return err
}

func writeCollectionSize(buf *bytes.Buffer, size int) error {
	if size > math.MaxInt32 {
		return fmt.Errorf("collection too large")
	}
	var data [4]byte
	binary.BigEndian.PutUint32(data[:], uint32(int32(size)))
	_, err := buf.Write(data[:])
	return err
}

func readCollectionBytes(data []byte) (value, rest []byte, err error) {
	size, rest, err := readCollectionSize(data)
	if err != nil {
		return nil, nil, err
	}
	if size < 0 {
		return nil, rest, nil
	}
	if len(rest) < size {
		return nil, nil, fmt.Errorf("unexpected end of collection data")
	}
	return rest[:size], rest[size:], nil
}

func readCollectionSize(data []byte) (size int, rest []byte, err error) {
	if len(data) < 4 {
		return 0, nil, fmt.Errorf("unexpected end of collection data")
	}
	return int(int32(binary.BigEndian.Uint32(data[:4]))), data[4:], nil
}

func containsUDT(t reflect.Type, seen map[reflect.Type]bool) bool {
	if t.Implements(autoUDTInterface) {
		return true
	}
	if seen[t] {
		return false
	}
	seen[t] = true
	switch t.Kind() {
	case reflect.Ptr, reflect.Slice, reflect.Array:
		return containsUDT(t.Elem(), seen)
	case reflect.Map:
		return containsUDT(t.Key(), seen) || containsUDT(t.Elem(), seen)
	case reflect.Interface:
		// The dynamic value may be an automatic UDT. Use the mapper-aware
		// collection codec because the interface's method set cannot tell us.
		return true
	}
	// Struct fields are intentionally not traversed. Mapper-aware UDT behavior
	// requires the struct itself to implement the UDT marker contract.
	return false
}

func typeContainsUDT(t reflect.Type) bool {
	if cached, ok := udtTypeCache.Load(t); ok {
		return cached.(bool)
	}
	result := containsUDT(t, make(map[reflect.Type]bool))
	udtTypeCache.Store(t, result)
	return result
}

// hasPromotedCodec reports whether the codec implemented by t is promoted from
// an anonymous field. Automatic UDT mapping takes precedence in that case so
// an embedded field's codec cannot accidentally serialize the whole outer UDT.
func hasPromotedCodec(t, codec reflect.Type) bool {
	includePointerMethods := t.Kind() == reflect.Ptr
	methodName := codec.Method(0).Name
	method, ok := t.MethodByName(methodName)
	if !ok || !isAutogeneratedMethod(method) {
		return false
	}
	if includePointerMethods {
		t = t.Elem()
		// A value-receiver method also has an autogenerated wrapper in the
		// pointer method set. Check the value method before treating that
		// wrapper as promotion.
		if method, ok := t.MethodByName(methodName); ok && !isAutogeneratedMethod(method) {
			return false
		}
	}
	if t.Kind() != reflect.Struct {
		return false
	}

	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		if !field.Anonymous {
			continue
		}
		if field.Type.Implements(codec) {
			return true
		}
		if includePointerMethods && field.Type.Kind() != reflect.Ptr && reflect.PointerTo(field.Type).Implements(codec) {
			return true
		}
	}
	return false
}

func isAutogeneratedMethod(method reflect.Method) bool {
	fn := runtime.FuncForPC(method.Func.Pointer())
	if fn == nil {
		return false
	}
	file, _ := fn.FileLine(fn.Entry())
	return file == "<autogenerated>"
}

func indirectInterface(value reflect.Value) reflect.Value {
	for value.IsValid() && value.Kind() == reflect.Interface && !value.IsNil() {
		value = value.Elem()
	}
	return value
}

func isNil(value reflect.Value) bool {
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Ptr, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}
