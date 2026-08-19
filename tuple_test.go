// Copyright (C) 2017 ScyllaDB
// Use of this source code is governed by a ALv2-style
// license that can be found in the LICENSE file.

package gocqlx

import (
	"math"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/gocql/gocql"
	"github.com/google/go-cmp/cmp"
)

type customTupleMarshaler []int

func (customTupleMarshaler) MarshalCQL(gocql.TypeInfo) ([]byte, error) {
	return nil, nil
}

type customTupleUnmarshaler []int

func (*customTupleUnmarshaler) UnmarshalCQL(gocql.TypeInfo, []byte) error {
	return nil
}

func TestParseTupleElementName(t *testing.T) {
	tests := []struct {
		name      string
		wantBase  string
		wantIndex int
		wantOK    bool
	}{
		{name: "c[0]", wantBase: "c", wantOK: true},
		{name: "c[1]", wantBase: "c", wantIndex: 1, wantOK: true},
		{name: "c[+1]"},
		{name: "c[01]"},
		{name: "c[]"},
		{name: "c[-1]"},
		{name: "[0]"},
		{name: "c[" + strconv.Itoa(math.MaxInt) + "]"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			base, index, ok := parseTupleElementName(test.name)
			if base != test.wantBase || index != test.wantIndex || ok != test.wantOK {
				t.Fatalf("parseTupleElementName(%q) = (%q, %d, %t), want (%q, %d, %t)", test.name, base, index, ok, test.wantBase, test.wantIndex, test.wantOK)
			}
		})
	}
}

func TestTupleBindElements(t *testing.T) {
	tests := []struct {
		name  string
		stmt  string
		names []string
		want  []tupleBindElement
	}{
		{
			name:  "insert tuple",
			stmt:  "INSERT INTO tbl (k,c) VALUES (?,(?,?)) ",
			names: []string{"k", "c[0]", "c[1]"},
			want: []tupleBindElement{
				{},
				{base: "c", count: 2},
				{base: "c", index: 1, count: 2},
			},
		},
		{
			name:  "collection element",
			stmt:  "UPDATE tbl SET items[0]=? ",
			names: []string{"items[0]"},
			want:  []tupleBindElement{{}},
		},
		{
			name:  "single element tuple",
			stmt:  "INSERT INTO tbl (c) VALUES ((?)) ",
			names: []string{"c[0]"},
			want:  []tupleBindElement{{base: "c", count: 1}},
		},
		{
			name:  "scalar insert",
			stmt:  "INSERT INTO tbl (c) VALUES (?) ",
			names: []string{"c[0]"},
			want:  []tupleBindElement{{}},
		},
		{
			name:  "function arguments",
			stmt:  "INSERT INTO tbl (c) VALUES (fn(?,?)) ",
			names: []string{"c[0]", "c[1]"},
			want:  []tupleBindElement{{}, {}},
		},
		{
			name:  "tuple and collection element",
			stmt:  "UPDATE tbl SET c=(?,?),items[0]=? ",
			names: []string{"c[0]", "c[1]", "items[0]"},
			want: []tupleBindElement{
				{base: "c", count: 2},
				{base: "c", index: 1, count: 2},
				{},
			},
		},
		{
			name:  "dollar quoted literal before tuple",
			stmt:  "INSERT INTO tbl (literal,c) VALUES ($$?$$,(?,?)) ",
			names: []string{"c[0]", "c[1]"},
			want: []tupleBindElement{
				{base: "c", count: 2},
				{base: "c", index: 1, count: 2},
			},
		},
		{
			name:  "slash comment before tuple",
			stmt:  "UPDATE tbl SET c=// ? is not a marker\n(?,?) ",
			names: []string{"c[0]", "c[1]"},
			want: []tupleBindElement{
				{base: "c", count: 2},
				{base: "c", index: 1, count: 2},
			},
		},
		{
			name:  "block comment between operator and tuple",
			stmt:  "UPDATE tbl SET c=/* tuple follows */(?,?) ",
			names: []string{"c[0]", "c[1]"},
			want: []tupleBindElement{
				{base: "c", count: 2},
				{base: "c", index: 1, count: 2},
			},
		},
		{
			name:  "tuple first in collection literal",
			stmt:  "UPDATE tbl SET values=[(?,?)] ",
			names: []string{"value[0]", "value[1]"},
			want: []tupleBindElement{
				{base: "value", count: 2},
				{base: "value", index: 1, count: 2},
			},
		},
		{
			name:  "tuple in UDT literal",
			stmt:  "UPDATE tbl SET value={point:(?,?)} ",
			names: []string{"point[0]", "point[1]"},
			want: []tupleBindElement{
				{base: "point", count: 2},
				{base: "point", index: 1, count: 2},
			},
		},
		{
			name:  "non-canonical name",
			stmt:  "UPDATE tbl SET c=(?,?) ",
			names: []string{"c[0]", "c[+1]"},
			want:  []tupleBindElement{{}, {}},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := tupleBindElements(test.stmt, test.names)
			if !reflect.DeepEqual(got, test.want) {
				t.Fatalf("tuple bind metadata = %#v, want %#v", got, test.want)
			}
		})
	}
}

func TestTupleContainerTypes(t *testing.T) {
	if isTupleBindContainerType(reflect.TypeOf(customTupleMarshaler{})) {
		t.Fatal("custom marshaler must not be decomposed")
	}
	if isTupleScanContainerType(reflect.TypeOf(customTupleUnmarshaler{})) {
		t.Fatal("custom unmarshaler must not be decomposed")
	}
	if isTupleBindContainerType(reflect.TypeOf([]byte{})) || isTupleScanContainerType(reflect.TypeOf([2]byte{})) {
		t.Fatal("byte sequences must not be tuple containers")
	}
	if !isTupleBindContainerType(reflect.TypeOf([][]byte{})) {
		t.Fatal("a slice of blobs must be a tuple container")
	}
}

func TestIterxTupleStructScanPlan(t *testing.T) {
	columns := []gocql.ColumnInfo{
		{
			Name:     "k",
			TypeInfo: gocql.NewNativeType(0, gocql.TypeInt),
		},
		{
			Name: "c",
			TypeInfo: gocql.NewTupleType(
				gocql.NewNativeType(0, gocql.TypeTuple),
				gocql.NewNativeType(0, gocql.TypeInt),
				gocql.NewNativeType(0, gocql.TypeInt),
			),
		},
	}

	t.Run("array", func(t *testing.T) {
		type row struct {
			K int
			C [2]int
		}

		iter := &Iterx{Mapper: DefaultMapper}
		plan, err := iter.structScanPlan(reflect.TypeOf(row{}), columns)
		if err != nil {
			t.Fatal(err)
		}

		if diff := cmp.Diff(plan.columns, []string{"k", "c[0]", "c[1]"}); diff != "" {
			t.Error("names mismatch", diff)
		}
		if diff := cmp.Diff(plan.tupleIndexes, []int{-1, 0, 1}); diff != "" {
			t.Error("tuple indexes mismatch", diff)
		}
		if diff := cmp.Diff(plan.tupleCounts, []int{0, 2, 2}); diff != "" {
			t.Error("tuple counts mismatch", diff)
		}

		var r row
		if err := iter.fieldsByTraversal(reflect.ValueOf(&r), &plan); err != nil {
			t.Fatal(err)
		}

		reflect.ValueOf(plan.values[1]).Elem().SetInt(12)
		reflect.ValueOf(plan.values[2]).Elem().SetInt(34)
		if diff := cmp.Diff(r.C, [2]int{12, 34}); diff != "" {
			t.Error("tuple value mismatch", diff)
		}
	})

	t.Run("slice", func(t *testing.T) {
		type row struct {
			K int
			C []int
		}

		iter := &Iterx{Mapper: DefaultMapper}
		plan, err := iter.structScanPlan(reflect.TypeOf(row{}), columns)
		if err != nil {
			t.Fatal(err)
		}

		var r row
		if err := iter.fieldsByTraversal(reflect.ValueOf(&r), &plan); err != nil {
			t.Fatal(err)
		}

		reflect.ValueOf(plan.values[1]).Elem().SetInt(56)
		reflect.ValueOf(plan.values[2]).Elem().SetInt(78)
		if diff := cmp.Diff(r.C, []int{56, 78}); diff != "" {
			t.Error("tuple value mismatch", diff)
		}
	})

	t.Run("interface slice", func(t *testing.T) {
		columns := []gocql.ColumnInfo{
			{
				Name: "c",
				TypeInfo: gocql.NewTupleType(
					gocql.NewNativeType(0, gocql.TypeTuple),
					gocql.NewNativeType(0, gocql.TypeInt),
					gocql.NewNativeType(0, gocql.TypeText),
				),
			},
		}
		type row struct {
			C []interface{}
		}

		iter := &Iterx{Mapper: DefaultMapper}
		plan, err := iter.structScanPlan(reflect.TypeOf(row{}), columns)
		if err != nil {
			t.Fatal(err)
		}

		var r row
		if err := iter.fieldsByTraversal(reflect.ValueOf(&r), &plan); err != nil {
			t.Fatal(err)
		}

		tuple := columns[0].TypeInfo.(gocql.TupleTypeInfo)
		intData := []byte{0, 0, 0, 42}
		textData := []byte("answer")
		if err := gocql.Unmarshal(tuple.Elems[0], intData, plan.values[0]); err != nil {
			t.Fatal(err)
		}
		if err := gocql.Unmarshal(tuple.Elems[1], textData, plan.values[1]); err != nil {
			t.Fatal(err)
		}

		if diff := cmp.Diff(r.C, []interface{}{42, "answer"}); diff != "" {
			t.Error("tuple value mismatch", diff)
		}
	})

	t.Run("element fields", func(t *testing.T) {
		type row struct {
			K  int
			C0 int `db:"c[0]"`
			C1 int `db:"c[1]"`
		}

		iter := &Iterx{Mapper: DefaultMapper}
		plan, err := iter.structScanPlan(reflect.TypeOf(row{}), columns)
		if err != nil {
			t.Fatal(err)
		}

		var r row
		if err := iter.fieldsByTraversal(reflect.ValueOf(&r), &plan); err != nil {
			t.Fatal(err)
		}

		reflect.ValueOf(plan.values[1]).Elem().SetInt(90)
		reflect.ValueOf(plan.values[2]).Elem().SetInt(12)
		if diff := cmp.Diff(row{C0: 90, C1: 12}, r); diff != "" {
			t.Error("tuple value mismatch", diff)
		}
	})

	t.Run("interface element fields", func(t *testing.T) {
		type row struct {
			K  int
			C0 interface{} `db:"c[0]"`
			C1 interface{} `db:"c[1]"`
		}

		iter := &Iterx{Mapper: DefaultMapper}
		plan, err := iter.structScanPlan(reflect.TypeOf(row{}), columns)
		if err != nil {
			t.Fatal(err)
		}
		if diff := cmp.Diff(plan.tupleCounts, []int{0, 2, 2}); diff != "" {
			t.Error("tuple counts mismatch", diff)
		}

		var r row
		if err := iter.fieldsByTraversal(reflect.ValueOf(&r), &plan); err != nil {
			t.Fatal(err)
		}

		tuple := columns[1].TypeInfo.(gocql.TupleTypeInfo)
		if err := gocql.Unmarshal(tuple.Elems[0], []byte{0, 0, 0, 34}, plan.values[1]); err != nil {
			t.Fatal(err)
		}
		if err := gocql.Unmarshal(tuple.Elems[1], []byte{0, 0, 0, 56}, plan.values[2]); err != nil {
			t.Fatal(err)
		}

		if diff := cmp.Diff(row{C0: 34, C1: 56}, r); diff != "" {
			t.Error("tuple value mismatch", diff)
		}
	})

	t.Run("struct interface fields", func(t *testing.T) {
		type tupleValue struct {
			First  interface{}
			Second interface{}
		}
		type row struct {
			C tupleValue
		}

		iter := &Iterx{Mapper: DefaultMapper}
		plan, err := iter.structScanPlan(reflect.TypeOf(row{}), columns)
		if err != nil {
			t.Fatal(err)
		}

		var r row
		if err := iter.fieldsByTraversal(reflect.ValueOf(&r), &plan); err != nil {
			t.Fatal(err)
		}

		tuple := columns[1].TypeInfo.(gocql.TupleTypeInfo)
		if err := gocql.Unmarshal(tuple.Elems[0], []byte{0, 0, 0, 34}, plan.values[1]); err != nil {
			t.Fatal(err)
		}
		if err := gocql.Unmarshal(tuple.Elems[1], []byte{0, 0, 0, 56}, plan.values[2]); err != nil {
			t.Fatal(err)
		}

		if diff := cmp.Diff(row{C: tupleValue{First: 34, Second: 56}}, r); diff != "" {
			t.Error("tuple value mismatch", diff)
		}
	})

	t.Run("struct", func(t *testing.T) {
		type tupleValue struct {
			Field1 int
			Field2 int
		}
		type row struct {
			K int
			C tupleValue
		}

		iter := &Iterx{Mapper: DefaultMapper}
		plan, err := iter.structScanPlan(reflect.TypeOf(row{}), columns)
		if err != nil {
			t.Fatal(err)
		}

		var r row
		if err := iter.fieldsByTraversal(reflect.ValueOf(&r), &plan); err != nil {
			t.Fatal(err)
		}

		reflect.ValueOf(plan.values[1]).Elem().SetInt(23)
		reflect.ValueOf(plan.values[2]).Elem().SetInt(45)
		if diff := cmp.Diff(tupleValue{Field1: 23, Field2: 45}, r.C); diff != "" {
			t.Error("tuple value mismatch", diff)
		}
	})

	t.Run("struct mapper fields", func(t *testing.T) {
		type embedded struct {
			First int
		}
		type tupleValue struct {
			embedded
			Ignored string `db:"-"`
			Second  int
		}
		type row struct {
			C tupleValue
		}

		iter := &Iterx{Mapper: DefaultMapper}
		plan, err := iter.structScanPlan(reflect.TypeOf(row{}), columns)
		if err != nil {
			t.Fatal(err)
		}

		var r row
		if err := iter.fieldsByTraversal(reflect.ValueOf(&r), &plan); err != nil {
			t.Fatal(err)
		}
		reflect.ValueOf(plan.values[1]).Elem().SetInt(23)
		reflect.ValueOf(plan.values[2]).Elem().SetInt(45)
		if r.C.First != 23 || r.C.Second != 45 || r.C.Ignored != "" {
			t.Fatalf("tuple value mismatch: %#v", r.C)
		}
	})

	t.Run("container takes precedence over element fields", func(t *testing.T) {
		type row struct {
			K  int
			C  [2]int
			C0 int `db:"c[0]"`
			C1 int `db:"c[1]"`
		}

		iter := &Iterx{Mapper: DefaultMapper}
		plan, err := iter.structScanPlan(reflect.TypeOf(row{}), columns)
		if err != nil {
			t.Fatal(err)
		}

		var r row
		if err := iter.fieldsByTraversal(reflect.ValueOf(&r), &plan); err != nil {
			t.Fatal(err)
		}

		reflect.ValueOf(plan.values[1]).Elem().SetInt(67)
		reflect.ValueOf(plan.values[2]).Elem().SetInt(89)
		if diff := cmp.Diff(row{C: [2]int{67, 89}}, r); diff != "" {
			t.Error("tuple value mismatch", diff)
		}
	})

	t.Run("byte slice", func(t *testing.T) {
		byteColumns := []gocql.ColumnInfo{
			{
				Name: "c",
				TypeInfo: gocql.NewTupleType(
					gocql.NewNativeType(0, gocql.TypeTuple),
					gocql.NewNativeType(0, gocql.TypeTinyInt),
					gocql.NewNativeType(0, gocql.TypeTinyInt),
				),
			},
		}
		type row struct {
			C []byte
		}

		iter := &Iterx{Mapper: DefaultMapper}
		_, err := iter.structScanPlan(reflect.TypeOf(row{}), byteColumns)
		if err == nil || !strings.Contains(err.Error(), "expected a non-byte array") {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("unsupported same-name field takes precedence", func(t *testing.T) {
		type row struct {
			C  int
			C0 int `db:"c[0]"`
			C1 int `db:"c[1]"`
		}

		iter := &Iterx{Mapper: DefaultMapper}
		_, err := iter.structScanPlan(reflect.TypeOf(row{}), columns)
		if err == nil || !strings.Contains(err.Error(), `cannot scan tuple column "c" into int`) {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("tuple struct rejects unexported field", func(t *testing.T) {
		type tupleValue struct {
			Exported int
			private  int
		}
		type row struct {
			C tupleValue
		}

		iter := &Iterx{Mapper: DefaultMapper}
		_, err := iter.structScanPlan(reflect.TypeOf(row{C: tupleValue{private: 1}}), columns)
		if err == nil || !strings.Contains(err.Error(), `struct has 1 mapped fields, expected 2 tuple elements`) {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("missing tuple elements keep scan alignment", func(t *testing.T) {
		type row struct {
			K int
		}

		iter := &Iterx{Mapper: DefaultMapper}
		plan, err := iter.structScanPlan(reflect.TypeOf(row{}), columns)
		if err != nil {
			t.Fatal(err)
		}

		if len(plan.fields[1]) != 0 || len(plan.fields[2]) != 0 {
			t.Fatal("unexpected tuple element traversal")
		}
		if plan.values[1] == nil || plan.values[2] == nil {
			t.Fatal("missing tuple elements must have discard destinations")
		}
	})

	t.Run("missing custom tuple elements keep scan alignment", func(t *testing.T) {
		customColumns := []gocql.ColumnInfo{
			{
				Name: "c",
				TypeInfo: gocql.NewTupleType(
					gocql.NewNativeType(0, gocql.TypeTuple),
					gocql.NewCustomType(0, gocql.TypeCustom, "example.CustomType"),
				),
			},
		}
		type row struct {
			K int
		}

		iter := &Iterx{Mapper: DefaultMapper}
		plan, err := iter.structScanPlan(reflect.TypeOf(row{}), customColumns)
		if err != nil {
			t.Fatal(err)
		}

		if len(plan.fields[0]) != 0 || plan.values[0] == nil {
			t.Fatal("missing custom tuple element must have a discard destination")
		}
		custom := customColumns[0].TypeInfo.(gocql.TupleTypeInfo).Elems[0]
		if err := gocql.Unmarshal(custom, []byte("ignored"), plan.values[0]); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("array arity mismatch", func(t *testing.T) {
		type row struct {
			K int
			C [3]int
		}

		iter := &Iterx{Mapper: DefaultMapper}
		_, err := iter.structScanPlan(reflect.TypeOf(row{}), columns)
		if err == nil || !strings.Contains(err.Error(), `array length 3 does not match tuple element count 2`) {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("slice allocation per row", func(t *testing.T) {
		type row struct {
			C []int
		}

		iter := &Iterx{Mapper: DefaultMapper}
		plan, err := iter.structScanPlan(reflect.TypeOf(row{}), columns[1:])
		if err != nil {
			t.Fatal(err)
		}

		var current row
		if err := iter.fieldsByTraversal(reflect.ValueOf(&current), &plan); err != nil {
			t.Fatal(err)
		}
		reflect.ValueOf(plan.values[0]).Elem().SetInt(12)
		reflect.ValueOf(plan.values[1]).Elem().SetInt(34)
		first := current

		if err := iter.fieldsByTraversal(reflect.ValueOf(&current), &plan); err != nil {
			t.Fatal(err)
		}
		reflect.ValueOf(plan.values[0]).Elem().SetInt(56)
		reflect.ValueOf(plan.values[1]).Elem().SetInt(78)

		if diff := cmp.Diff([]int{12, 34}, first.C); diff != "" {
			t.Error("retained row was overwritten", diff)
		}
		if diff := cmp.Diff([]int{56, 78}, current.C); diff != "" {
			t.Error("current row mismatch", diff)
		}
	})
}
