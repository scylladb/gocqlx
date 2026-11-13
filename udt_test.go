// Copyright (C) 2017 ScyllaDB
// Use of this source code is governed by a ALv2-style
// license that can be found in the LICENSE file.

package gocqlx

import (
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/google/go-cmp/cmp"
)

type collectionUDT struct {
	UDT
	UserID string `db:"user_id"`
}

type collectionUDTKey struct {
	UDT
	Code string `db:"code"`
}

type customMarshalerUDT struct {
	UDT
}

type customUDTCodec struct {
	UDT
}

type promotedScalarCodec struct{}

type promotedScalarCodecUDT struct {
	UDT
	promotedScalarCodec
	Value string `db:"value"`
}

type promotedScalarPointerCodecUDT struct {
	UDT
	*promotedScalarCodec
	Value string `db:"value"`
}

type promotedUDTCodec struct{}

type promotedUDTCodecUDT struct {
	UDT
	promotedUDTCodec
	Value string `db:"value"`
}

type explicitScalarCodecUDT struct {
	UDT
	promotedScalarCodec
	Value string `db:"value"`
}

type nilMarshalerUDTMap map[string]collectionUDT

type nilUDTMarshalerMap map[string]collectionUDT

var (
	errCustomMarshalerUDT  = errors.New("custom UDT marshaler called")
	errCustomUDTCodec      = errors.New("custom UDT codec called")
	errExplicitScalarCodec = errors.New("explicit scalar codec called")
)

func (customMarshalerUDT) MarshalCQL(gocql.TypeInfo) ([]byte, error) {
	return nil, errCustomMarshalerUDT
}

func (nilMarshalerUDTMap) MarshalCQL(gocql.TypeInfo) ([]byte, error) {
	return nil, errCustomMarshalerUDT
}

func (nilUDTMarshalerMap) MarshalUDT(string, gocql.TypeInfo) ([]byte, error) {
	return nil, errCustomUDTCodec
}

func (customUDTCodec) MarshalUDT(string, gocql.TypeInfo) ([]byte, error) {
	return nil, errCustomUDTCodec
}

func (*customUDTCodec) UnmarshalUDT(string, gocql.TypeInfo, []byte) error {
	return errCustomUDTCodec
}

func (promotedScalarCodec) MarshalCQL(gocql.TypeInfo) ([]byte, error) {
	return nil, errCustomMarshalerUDT
}

func (*promotedScalarCodec) UnmarshalCQL(gocql.TypeInfo, []byte) error {
	return errCustomMarshalerUDT
}

func (promotedUDTCodec) MarshalUDT(string, gocql.TypeInfo) ([]byte, error) {
	return nil, errCustomUDTCodec
}

func (*promotedUDTCodec) UnmarshalUDT(string, gocql.TypeInfo, []byte) error {
	return errCustomUDTCodec
}

func (explicitScalarCodecUDT) MarshalCQL(gocql.TypeInfo) ([]byte, error) {
	return nil, errExplicitScalarCodec
}

func (*explicitScalarCodecUDT) UnmarshalCQL(gocql.TypeInfo, []byte) error {
	return errExplicitScalarCodec
}

func TestUDTInMaps(t *testing.T) {
	textType := gocql.NewNativeType(4, gocql.TypeText)
	valueType := gocql.NewUDTType(4, "collection_udt", "",
		gocql.UDTField{Name: "user_id", Type: textType},
	)
	keyType := gocql.NewUDTType(4, "collection_udt_key", "",
		gocql.UDTField{Name: "code", Type: textType},
	)

	tests := []struct {
		name  string
		info  gocql.TypeInfo
		value interface{}
		new   func() interface{}
	}{
		{
			name: "map values",
			info: gocql.NewCollectionType(gocql.NewNativeType(4, gocql.TypeMap), textType, valueType),
			value: map[string]collectionUDT{
				"one": {UserID: "user-1"},
			},
			new: func() interface{} { return new(map[string]collectionUDT) },
		},
		{
			name: "map pointer values",
			info: gocql.NewCollectionType(gocql.NewNativeType(4, gocql.TypeMap), textType, valueType),
			value: map[string]*collectionUDT{
				"one": {UserID: "user-1"},
			},
			new: func() interface{} { return new(map[string]*collectionUDT) },
		},
		{
			name:  "empty map",
			info:  gocql.NewCollectionType(gocql.NewNativeType(4, gocql.TypeMap), textType, valueType),
			value: map[string]collectionUDT{},
			new:   func() interface{} { return new(map[string]collectionUDT) },
		},
		{
			name:  "nil map",
			info:  gocql.NewCollectionType(gocql.NewNativeType(4, gocql.TypeMap), textType, valueType),
			value: map[string]collectionUDT(nil),
			new:   func() interface{} { return new(map[string]collectionUDT) },
		},
		{
			name: "null map value",
			info: gocql.NewCollectionType(gocql.NewNativeType(4, gocql.TypeMap), textType, valueType),
			value: map[string]*collectionUDT{
				"one": nil,
			},
			new: func() interface{} { return new(map[string]*collectionUDT) },
		},
		{
			name: "map keys",
			info: gocql.NewCollectionType(gocql.NewNativeType(4, gocql.TypeMap), keyType, valueType),
			value: map[collectionUDTKey]collectionUDT{
				{Code: "one"}: {UserID: "user-1"},
			},
			new: func() interface{} { return new(map[collectionUDTKey]collectionUDT) },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			wrapped := udtWrapValue(reflect.ValueOf(tt.value), DefaultMapper, false)
			data, err := gocql.Marshal(tt.info, wrapped)
			if err != nil {
				t.Fatal(err)
			}

			got := tt.new()
			wrapped = udtWrapValue(reflect.ValueOf(got), DefaultMapper, false)
			if err := gocql.Unmarshal(tt.info, data, wrapped); err != nil {
				t.Fatal(err)
			}

			if diff := cmp.Diff(tt.value, reflect.ValueOf(got).Elem().Interface(), cmp.AllowUnexported(collectionUDT{}, collectionUDTKey{})); diff != "" {
				t.Fatal(diff)
			}
		})
	}
}

func TestBatchWrapsUDTMaps(t *testing.T) {
	query := Query(&gocql.Query{}, []string{"value"})
	batch := &Batch{Batch: &gocql.Batch{}}
	value := map[string]collectionUDT{"one": {UserID: "user-1"}}

	if err := batch.Bind(query, value); err != nil {
		t.Fatal(err)
	}
	if _, ok := batch.Entries[0].Args[0].(udtCodec); !ok {
		t.Fatalf("batch argument was not wrapped: %T", batch.Entries[0].Args[0])
	}
}

func TestBatchBindDoesNotMutateArgs(t *testing.T) {
	query := Query(&gocql.Query{}, []string{"value"})
	batch := &Batch{Batch: &gocql.Batch{}}
	value := map[string]collectionUDT{"one": {UserID: "user-1"}}
	args := []interface{}{value}

	if err := batch.Bind(query, args...); err != nil {
		t.Fatal(err)
	}
	if _, ok := args[0].(map[string]collectionUDT); !ok {
		t.Fatalf("caller argument was replaced: %T", args[0])
	}
	if _, ok := batch.Entries[0].Args[0].(udtCodec); !ok {
		t.Fatalf("batch argument was not wrapped: %T", batch.Entries[0].Args[0])
	}
}

func TestUDTWrapValueLeavesConcreteNonUDTValuesUnwrapped(t *testing.T) {
	tests := []interface{}{
		map[string]string{"one": "value"},
		[]int{1},
		time.Unix(1, 0),
	}

	for _, value := range tests {
		wrapped := udtWrapValue(reflect.ValueOf(value), DefaultMapper, false)
		if _, ok := wrapped.(udtCodec); ok {
			t.Fatalf("non-UDT value was wrapped: %T", value)
		}
	}
}

func TestUDTCollectionFramingMatchesGocql(t *testing.T) {
	textType := gocql.NewNativeType(4, gocql.TypeText)
	intType := gocql.NewNativeType(4, gocql.TypeInt)

	tests := []struct {
		name     string
		info     gocql.TypeInfo
		upstream interface{}
		wrapped  interface{}
	}{
		{
			name:     "map",
			info:     gocql.NewCollectionType(gocql.NewNativeType(4, gocql.TypeMap), textType, textType),
			upstream: map[string]string{"one": "value"},
			wrapped:  map[string]interface{}{"one": "value"},
		},
		{
			name:     "list",
			info:     gocql.NewCollectionType(gocql.NewNativeType(4, gocql.TypeList), nil, intType),
			upstream: []int{1},
			wrapped:  []interface{}{1},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			want, err := gocql.Marshal(tt.info, tt.upstream)
			if err != nil {
				t.Fatal(err)
			}
			got, err := gocql.Marshal(tt.info, udtWrapValue(reflect.ValueOf(tt.wrapped), DefaultMapper, false))
			if err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(want, got); diff != "" {
				t.Fatal(diff)
			}
		})
	}
}

func TestUDTCollectionTypedNilMatchesGocql(t *testing.T) {
	textType := gocql.NewNativeType(4, gocql.TypeText)
	setType := gocql.NewCollectionType(gocql.NewNativeType(4, gocql.TypeSet), nil, textType)
	mapType := gocql.NewCollectionType(gocql.NewNativeType(4, gocql.TypeMap), textType, textType)

	tests := []struct {
		name  string
		info  gocql.TypeInfo
		value []interface{}
	}{
		{
			name:  "wrong map type",
			info:  gocql.NewCollectionType(gocql.NewNativeType(4, gocql.TypeList), nil, textType),
			value: []interface{}{map[string]string(nil)},
		},
		{
			name:  "wrong slice type",
			info:  gocql.NewCollectionType(gocql.NewNativeType(4, gocql.TypeList), nil, textType),
			value: []interface{}{[]string(nil)},
		},
		{
			name:  "nil map set is empty",
			info:  gocql.NewCollectionType(gocql.NewNativeType(4, gocql.TypeList), nil, setType),
			value: []interface{}{map[string]struct{}(nil)},
		},
		{
			name:  "nil map is null",
			info:  gocql.NewCollectionType(gocql.NewNativeType(4, gocql.TypeList), nil, mapType),
			value: []interface{}{map[string]string(nil)},
		},
		{
			name: "nil slice is null",
			info: gocql.NewCollectionType(gocql.NewNativeType(4, gocql.TypeList), nil,
				gocql.NewCollectionType(gocql.NewNativeType(4, gocql.TypeList), nil, textType)),
			value: []interface{}{[]string(nil)},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			want, wantErr := gocql.Marshal(tt.info, tt.value)
			got, gotErr := gocql.Marshal(tt.info, udtWrapValue(reflect.ValueOf(tt.value), DefaultMapper, false))
			if (wantErr == nil) != (gotErr == nil) {
				t.Fatalf("error mismatch: gocql=%v wrapped=%v", wantErr, gotErr)
			}
			if diff := cmp.Diff(want, got); diff != "" {
				t.Fatal(diff)
			}
		})
	}
}

func TestUDTInterfaceMapValuesMarshal(t *testing.T) {
	textType := gocql.NewNativeType(4, gocql.TypeText)
	valueType := gocql.NewUDTType(4, "collection_udt", "",
		gocql.UDTField{Name: "user_id", Type: textType},
	)
	mapType := gocql.NewCollectionType(gocql.NewNativeType(4, gocql.TypeMap), textType, valueType)
	value := map[string]UDT{"one": collectionUDT{UserID: "user-1"}}

	data, err := gocql.Marshal(mapType, udtWrapValue(reflect.ValueOf(value), DefaultMapper, false))
	if err != nil {
		t.Fatal(err)
	}

	var got map[string]collectionUDT
	if err := gocql.Unmarshal(mapType, data, udtWrapValue(reflect.ValueOf(&got), DefaultMapper, false)); err != nil {
		t.Fatal(err)
	}
	want := map[string]collectionUDT{"one": {UserID: "user-1"}}
	if diff := cmp.Diff(want, got, cmp.AllowUnexported(collectionUDT{})); diff != "" {
		t.Fatal(diff)
	}
}

func TestUDTEmptyInterfaceCollectionValuesMarshal(t *testing.T) {
	textType := gocql.NewNativeType(4, gocql.TypeText)
	valueType := gocql.NewUDTType(4, "collection_udt", "",
		gocql.UDTField{Name: "user_id", Type: textType},
	)

	tests := []struct {
		name  string
		info  gocql.TypeInfo
		value interface{}
		want  interface{}
		new   func() interface{}
	}{
		{
			name: "map values",
			info: gocql.NewCollectionType(gocql.NewNativeType(4, gocql.TypeMap), textType, valueType),
			value: map[string]interface{}{
				"one": collectionUDT{UserID: "user-1"},
			},
			want: map[string]collectionUDT{
				"one": {UserID: "user-1"},
			},
			new: func() interface{} { return new(map[string]collectionUDT) },
		},
		{
			name: "slice values",
			info: gocql.NewCollectionType(gocql.NewNativeType(4, gocql.TypeList), nil, valueType),
			value: []interface{}{
				collectionUDT{UserID: "user-1"},
			},
			want: []collectionUDT{
				{UserID: "user-1"},
			},
			new: func() interface{} { return new([]collectionUDT) },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			wrapped := udtWrapValue(reflect.ValueOf(tt.value), DefaultMapper, false)
			if _, ok := wrapped.(udtCodec); !ok {
				t.Fatalf("collection was not wrapped: %T", wrapped)
			}
			data, err := gocql.Marshal(tt.info, wrapped)
			if err != nil {
				t.Fatal(err)
			}

			got := tt.new()
			if err := gocql.Unmarshal(tt.info, data, udtWrapValue(reflect.ValueOf(got), DefaultMapper, false)); err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(tt.want, reflect.ValueOf(got).Elem().Interface(), cmp.AllowUnexported(collectionUDT{})); diff != "" {
				t.Fatal(diff)
			}
		})
	}
}

func TestUDTMapValuesWithNilFieldsMarshal(t *testing.T) {
	type nullableUDT struct {
		UDT
		Optional *string           `db:"optional"`
		Labels   map[string]string `db:"labels"`
	}

	textType := gocql.NewNativeType(4, gocql.TypeText)
	labelsType := gocql.NewCollectionType(gocql.NewNativeType(4, gocql.TypeMap), textType, textType)
	valueType := gocql.NewUDTType(4, "nullable_udt", "",
		gocql.UDTField{Name: "optional", Type: textType},
		gocql.UDTField{Name: "labels", Type: labelsType},
	)
	mapType := gocql.NewCollectionType(gocql.NewNativeType(4, gocql.TypeMap), textType, valueType)
	value := map[string]nullableUDT{"one": {}}

	data, err := gocql.Marshal(mapType, udtWrapValue(reflect.ValueOf(value), DefaultMapper, false))
	if err != nil {
		t.Fatal(err)
	}

	var got map[string]nullableUDT
	if err := gocql.Unmarshal(mapType, data, udtWrapValue(reflect.ValueOf(&got), DefaultMapper, false)); err != nil {
		t.Fatal(err)
	}
	if got["one"].Optional != nil || got["one"].Labels != nil {
		t.Fatalf("nil fields were not preserved: %#v", got["one"])
	}
}

func TestUDTSliceMarshalPreservesNilFields(t *testing.T) {
	type nullableUDT struct {
		UDT
		Optional *string           `db:"optional"`
		Labels   map[string]string `db:"labels"`
	}

	textType := gocql.NewNativeType(4, gocql.TypeText)
	labelsType := gocql.NewCollectionType(gocql.NewNativeType(4, gocql.TypeMap), textType, textType)
	valueType := gocql.NewUDTType(4, "nullable_udt", "",
		gocql.UDTField{Name: "optional", Type: textType},
		gocql.UDTField{Name: "labels", Type: labelsType},
	)
	listType := gocql.NewCollectionType(gocql.NewNativeType(4, gocql.TypeList), nil, valueType)
	value := []nullableUDT{{}}

	if _, err := gocql.Marshal(listType, udtWrapValue(reflect.ValueOf(value), DefaultMapper, false)); err != nil {
		t.Fatal(err)
	}
	if value[0].Optional != nil || value[0].Labels != nil {
		t.Fatalf("marshal mutated input: %#v", value[0])
	}
}

func TestUDTMapStrictMode(t *testing.T) {
	type incompleteUDT struct {
		UDT
	}

	textType := gocql.NewNativeType(4, gocql.TypeText)
	valueType := gocql.NewUDTType(4, "collection_udt", "",
		gocql.UDTField{Name: "user_id", Type: textType},
	)
	mapType := gocql.NewCollectionType(gocql.NewNativeType(4, gocql.TypeMap), textType, valueType)
	value := map[string]incompleteUDT{"one": {}}

	_, err := gocql.Marshal(mapType, udtWrapValue(reflect.ValueOf(value), DefaultMapper, true))
	if err == nil {
		t.Fatal("expected missing UDT field error")
	}

	data, err := gocql.Marshal(mapType, udtWrapValue(reflect.ValueOf(map[string]collectionUDT{
		"one": {UserID: "user-1"},
	}), DefaultMapper, false))
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]incompleteUDT
	if err := gocql.Unmarshal(mapType, data, udtWrapValue(reflect.ValueOf(&got), DefaultMapper, true)); err == nil {
		t.Fatal("expected missing UDT field error while unmarshaling")
	}
}

func TestUDTWithMapField(t *testing.T) {
	type outerUDT struct {
		UDT
		ByName map[string]collectionUDT `db:"by_name"`
	}

	textType := gocql.NewNativeType(4, gocql.TypeText)
	valueType := gocql.NewUDTType(4, "collection_udt", "",
		gocql.UDTField{Name: "user_id", Type: textType},
	)
	mapType := gocql.NewCollectionType(gocql.NewNativeType(4, gocql.TypeMap), textType, valueType)
	outerType := gocql.NewUDTType(4, "outer_udt", "",
		gocql.UDTField{Name: "by_name", Type: mapType},
	)

	want := outerUDT{ByName: map[string]collectionUDT{
		"one": {UserID: "user-1"},
	}}
	data, err := gocql.Marshal(outerType, makeUDT(reflect.ValueOf(want), DefaultMapper, false))
	if err != nil {
		t.Fatal(err)
	}

	var got outerUDT
	if err := gocql.Unmarshal(outerType, data, makeUDT(reflect.ValueOf(&got), DefaultMapper, false)); err != nil {
		t.Fatal(err)
	}
	if diff := cmp.Diff(want, got, cmp.AllowUnexported(collectionUDT{})); diff != "" {
		t.Fatal(diff)
	}
}

func TestUDTInNestedCollections(t *testing.T) {
	textType := gocql.NewNativeType(4, gocql.TypeText)
	valueType := gocql.NewUDTType(4, "collection_udt", "",
		gocql.UDTField{Name: "user_id", Type: textType},
	)
	keyType := gocql.NewUDTType(4, "collection_udt_key", "",
		gocql.UDTField{Name: "code", Type: textType},
	)
	mapType := gocql.NewCollectionType(gocql.NewNativeType(4, gocql.TypeMap), textType, valueType)
	listType := gocql.NewCollectionType(gocql.NewNativeType(4, gocql.TypeList), nil, valueType)

	tests := []struct {
		name  string
		info  gocql.TypeInfo
		value interface{}
		new   func() interface{}
	}{
		{
			name: "map list values",
			info: gocql.NewCollectionType(gocql.NewNativeType(4, gocql.TypeMap), textType, listType),
			value: map[string][]collectionUDT{
				"one": {{UserID: "user-1"}},
			},
			new: func() interface{} { return new(map[string][]collectionUDT) },
		},
		{
			name: "map array values",
			info: gocql.NewCollectionType(gocql.NewNativeType(4, gocql.TypeMap), textType, listType),
			value: map[string][1]collectionUDT{
				"one": {{UserID: "user-1"}},
			},
			new: func() interface{} { return new(map[string][1]collectionUDT) },
		},
		{
			name: "list map values",
			info: gocql.NewCollectionType(gocql.NewNativeType(4, gocql.TypeList), nil, mapType),
			value: []map[string]collectionUDT{
				{"one": {UserID: "user-1"}},
			},
			new: func() interface{} { return new([]map[string]collectionUDT) },
		},
		{
			name: "set values",
			info: gocql.NewCollectionType(gocql.NewNativeType(4, gocql.TypeSet), nil, valueType),
			value: []collectionUDT{
				{UserID: "user-1"},
			},
			new: func() interface{} { return new([]collectionUDT) },
		},
		{
			name: "set map values",
			info: gocql.NewCollectionType(gocql.NewNativeType(4, gocql.TypeSet), nil, keyType),
			value: map[collectionUDTKey]struct{}{
				{Code: "one"}: {},
			},
			new: func() interface{} { return new(map[collectionUDTKey]struct{}) },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, err := gocql.Marshal(tt.info, udtWrapValue(reflect.ValueOf(tt.value), DefaultMapper, false))
			if err != nil {
				t.Fatal(err)
			}

			got := tt.new()
			if err := gocql.Unmarshal(tt.info, data, udtWrapValue(reflect.ValueOf(got), DefaultMapper, false)); err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(tt.value, reflect.ValueOf(got).Elem().Interface(), cmp.AllowUnexported(collectionUDT{}, collectionUDTKey{})); diff != "" {
				t.Fatal(diff)
			}
		})
	}
}

func TestUDTNullNestedValueClearsDestination(t *testing.T) {
	type outerUDT struct {
		UDT
		Nested collectionUDT `db:"nested"`
	}

	textType := gocql.NewNativeType(4, gocql.TypeText)
	nestedType := gocql.NewUDTType(4, "collection_udt", "",
		gocql.UDTField{Name: "user_id", Type: textType},
	)
	outerType := gocql.NewUDTType(4, "outer_udt", "",
		gocql.UDTField{Name: "nested", Type: nestedType},
	)

	got := outerUDT{Nested: collectionUDT{UserID: "stale"}}
	nullNestedUDT := []byte{0xff, 0xff, 0xff, 0xff}
	if err := gocql.Unmarshal(outerType, nullNestedUDT, makeUDT(reflect.ValueOf(&got), DefaultMapper, false)); err != nil {
		t.Fatal(err)
	}
	if got.Nested.UserID != "" {
		t.Fatalf("null nested UDT retained %q", got.Nested.UserID)
	}
}

func TestUDTNullTopLevelValueClearsDestination(t *testing.T) {
	textType := gocql.NewNativeType(4, gocql.TypeText)
	valueType := gocql.NewUDTType(4, "collection_udt", "",
		gocql.UDTField{Name: "user_id", Type: textType},
	)

	got := collectionUDT{UserID: "stale"}
	if err := gocql.Unmarshal(valueType, nil, udtWrapValue(reflect.ValueOf(&got), DefaultMapper, false)); err != nil {
		t.Fatal(err)
	}
	if got.UserID != "" {
		t.Fatalf("null UDT retained %q", got.UserID)
	}
}

func TestUDTWrapValueTypedNil(t *testing.T) {
	var value *collectionUDT
	wrapped := udtWrapValue(reflect.ValueOf(value), DefaultMapper, false)
	if got, ok := wrapped.(*collectionUDT); !ok || got != nil {
		t.Fatalf("typed nil changed: %#v", wrapped)
	}
}

func TestUDTCustomMarshalerPrecedence(t *testing.T) {
	textType := gocql.NewNativeType(4, gocql.TypeText)
	valueType := gocql.NewUDTType(4, "custom_udt", "",
		gocql.UDTField{Name: "value", Type: textType},
	)
	mapType := gocql.NewCollectionType(gocql.NewNativeType(4, gocql.TypeMap), textType, valueType)
	value := map[string]customMarshalerUDT{"one": {}}

	_, err := gocql.Marshal(mapType, udtWrapValue(reflect.ValueOf(value), DefaultMapper, false))
	if !errors.Is(err, errCustomMarshalerUDT) {
		t.Fatalf("custom marshaler was bypassed: %v", err)
	}
}

func TestUDTNilCollectionCustomCodecPrecedence(t *testing.T) {
	type marshalerOuterUDT struct {
		UDT
		Value nilMarshalerUDTMap `db:"value"`
	}
	type udtMarshalerOuterUDT struct {
		UDT
		Value nilUDTMarshalerMap `db:"value"`
	}

	textType := gocql.NewNativeType(4, gocql.TypeText)
	valueType := gocql.NewUDTType(4, "custom_udt", "",
		gocql.UDTField{Name: "value", Type: textType},
	)
	mapType := gocql.NewCollectionType(gocql.NewNativeType(4, gocql.TypeMap), textType, valueType)
	mapOuterType := gocql.NewUDTType(4, "outer_udt", "",
		gocql.UDTField{Name: "value", Type: mapType},
	)
	udtOuterType := gocql.NewUDTType(4, "outer_udt", "",
		gocql.UDTField{Name: "value", Type: valueType},
	)

	tests := []struct {
		name  string
		info  gocql.TypeInfo
		value interface{}
		want  error
	}{
		{
			name:  "Marshaler",
			info:  mapOuterType,
			value: marshalerOuterUDT{},
			want:  errCustomMarshalerUDT,
		},
		{
			name:  "UDTMarshaler",
			info:  udtOuterType,
			value: udtMarshalerOuterUDT{},
			want:  errCustomUDTCodec,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := gocql.Marshal(tt.info, makeUDT(reflect.ValueOf(tt.value), DefaultMapper, false))
			if !errors.Is(err, tt.want) {
				t.Fatalf("custom codec was bypassed: %v", err)
			}
		})
	}
}

func TestUDTNilPointerCustomCodecIsNull(t *testing.T) {
	valueType := gocql.NewUDTType(4, "custom_udt", "")
	data, err := marshalUDTValue(valueType, reflect.ValueOf((*customMarshalerUDT)(nil)), DefaultMapper, false)
	if err != nil {
		t.Fatalf("nil pointer invoked custom codec: %v", err)
	}
	if data != nil {
		t.Fatalf("nil pointer marshaled as %v", data)
	}
}

func TestUDTCustomCodecPrecedence(t *testing.T) {
	textType := gocql.NewNativeType(4, gocql.TypeText)
	valueType := gocql.NewUDTType(4, "custom_udt", "",
		gocql.UDTField{Name: "user_id", Type: textType},
	)
	mapType := gocql.NewCollectionType(gocql.NewNativeType(4, gocql.TypeMap), textType, valueType)

	value := map[string]customUDTCodec{"one": {}}
	_, err := gocql.Marshal(mapType, udtWrapValue(reflect.ValueOf(value), DefaultMapper, false))
	if !errors.Is(err, errCustomUDTCodec) {
		t.Fatalf("custom UDT marshaler was bypassed: %v", err)
	}

	data, err := gocql.Marshal(mapType, udtWrapValue(reflect.ValueOf(map[string]collectionUDT{
		"one": {UserID: "user-1"},
	}), DefaultMapper, false))
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]customUDTCodec
	err = gocql.Unmarshal(mapType, data, udtWrapValue(reflect.ValueOf(&got), DefaultMapper, false))
	if !errors.Is(err, errCustomUDTCodec) {
		t.Fatalf("custom UDT unmarshaler was bypassed: %v", err)
	}
}

func TestUDTPromotedCodecDoesNotOverrideAutomaticMapping(t *testing.T) {
	textType := gocql.NewNativeType(4, gocql.TypeText)
	valueType := gocql.NewUDTType(4, "promoted_codec_udt", "",
		gocql.UDTField{Name: "value", Type: textType},
	)

	tests := []struct {
		name  string
		value interface{}
		new   func() interface{}
	}{
		{
			name:  "scalar codec",
			value: promotedScalarCodecUDT{Value: "expected"},
			new:   func() interface{} { return new(promotedScalarCodecUDT) },
		},
		{
			name: "scalar pointer codec",
			value: promotedScalarPointerCodecUDT{
				promotedScalarCodec: new(promotedScalarCodec),
				Value:               "expected",
			},
			new: func() interface{} { return new(promotedScalarPointerCodecUDT) },
		},
		{
			name:  "UDT codec",
			value: promotedUDTCodecUDT{Value: "expected"},
			new:   func() interface{} { return new(promotedUDTCodecUDT) },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, err := gocql.Marshal(valueType, udtWrapValue(reflect.ValueOf(tt.value), DefaultMapper, false))
			if err != nil {
				t.Fatal(err)
			}

			got := tt.new()
			if err := gocql.Unmarshal(valueType, data, udtWrapValue(reflect.ValueOf(got), DefaultMapper, false)); err != nil {
				t.Fatal(err)
			}
			wantValue := reflect.ValueOf(tt.value).FieldByName("Value").String()
			gotValue := reflect.ValueOf(got).Elem().FieldByName("Value").String()
			if gotValue != wantValue {
				t.Fatalf("got %q, expected %q", gotValue, wantValue)
			}
		})
	}
}

func TestUDTExplicitCodecOverridesPromotedCodec(t *testing.T) {
	textType := gocql.NewNativeType(4, gocql.TypeText)
	valueType := gocql.NewUDTType(4, "explicit_codec_udt", "",
		gocql.UDTField{Name: "value", Type: textType},
	)

	tests := []struct {
		name  string
		info  gocql.TypeInfo
		value interface{}
		plain interface{}
		new   func() interface{}
	}{
		{
			name:  "list element",
			info:  gocql.NewCollectionType(gocql.NewNativeType(4, gocql.TypeList), nil, valueType),
			value: []explicitScalarCodecUDT{{promotedScalarCodec: promotedScalarCodec{}, Value: "expected"}},
			plain: []promotedScalarCodecUDT{{Value: "expected"}},
			new:   func() interface{} { return new([]explicitScalarCodecUDT) },
		},
		{
			name:  "map value",
			info:  gocql.NewCollectionType(gocql.NewNativeType(4, gocql.TypeMap), textType, valueType),
			value: map[string]explicitScalarCodecUDT{"one": {promotedScalarCodec: promotedScalarCodec{}, Value: "expected"}},
			plain: map[string]promotedScalarCodecUDT{"one": {Value: "expected"}},
			new:   func() interface{} { return new(map[string]explicitScalarCodecUDT) },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := gocql.Marshal(tt.info, udtWrapValue(reflect.ValueOf(tt.value), DefaultMapper, false))
			if !errors.Is(err, errExplicitScalarCodec) {
				t.Fatalf("explicit marshaler was bypassed: %v", err)
			}

			data, err := gocql.Marshal(tt.info, udtWrapValue(reflect.ValueOf(tt.plain), DefaultMapper, false))
			if err != nil {
				t.Fatal(err)
			}
			err = gocql.Unmarshal(tt.info, data, udtWrapValue(reflect.ValueOf(tt.new()), DefaultMapper, false))
			if !errors.Is(err, errExplicitScalarCodec) {
				t.Fatalf("explicit unmarshaler was bypassed: %v", err)
			}
		})
	}
}

func TestUDTCollectionUnmarshalRejectsNonPointer(t *testing.T) {
	textType := gocql.NewNativeType(4, gocql.TypeText)
	valueType := gocql.NewUDTType(4, "collection_udt", "",
		gocql.UDTField{Name: "user_id", Type: textType},
	)
	mapType := gocql.NewCollectionType(gocql.NewNativeType(4, gocql.TypeMap), textType, valueType)
	listType := gocql.NewCollectionType(gocql.NewNativeType(4, gocql.TypeList), nil, valueType)

	tests := []struct {
		name  string
		info  gocql.TypeInfo
		value interface{}
		dest  interface{}
	}{
		{
			name:  "map",
			info:  mapType,
			value: map[string]collectionUDT{"one": {UserID: "user-1"}},
			dest:  map[string]collectionUDT{},
		},
		{
			name:  "slice",
			info:  listType,
			value: []collectionUDT{{UserID: "user-1"}},
			dest:  []collectionUDT{},
		},
		{
			name:  "array",
			info:  listType,
			value: [1]collectionUDT{{UserID: "user-1"}},
			dest:  [1]collectionUDT{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, err := gocql.Marshal(tt.info, udtWrapValue(reflect.ValueOf(tt.value), DefaultMapper, false))
			if err != nil {
				t.Fatal(err)
			}

			err = gocql.Unmarshal(tt.info, data, udtWrapValue(reflect.ValueOf(tt.dest), DefaultMapper, false))
			if err == nil {
				t.Fatal("expected non-pointer destination error")
			}
		})
	}
}

func BenchmarkUDTWrapValueScalar(b *testing.B) {
	value := reflect.ValueOf(new(int))
	_ = udtWrapValue(value, DefaultMapper, false)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = udtWrapValue(value, DefaultMapper, false)
	}
}

func BenchmarkContainsUDTScalarUncached(b *testing.B) {
	typeOfValue := reflect.TypeOf(new(int))
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = containsUDT(typeOfValue, make(map[reflect.Type]bool))
	}
}
