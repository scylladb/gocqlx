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

func TestParseTupleElementName(t *testing.T) {
	tests := []struct {
		name      string
		wantBase  string
		wantIndex int
		wantOK    bool
	}{
		{name: "c[+1]", wantBase: "c", wantIndex: 1, wantOK: true},
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
		plan, err := iter.structScanPlan(reflect.TypeOf(row{}), byteColumns)
		if err != nil {
			t.Fatal(err)
		}

		var r row
		if err := iter.fieldsByTraversal(reflect.ValueOf(&r), &plan); err != nil {
			t.Fatal(err)
		}

		tuple := byteColumns[0].TypeInfo.(gocql.TupleTypeInfo)
		if err := gocql.Unmarshal(tuple.Elems[0], []byte{12}, plan.values[0]); err != nil {
			t.Fatal(err)
		}
		if err := gocql.Unmarshal(tuple.Elems[1], []byte{34}, plan.values[1]); err != nil {
			t.Fatal(err)
		}
		if diff := cmp.Diff([]byte{12, 34}, r.C); diff != "" {
			t.Error("tuple value mismatch", diff)
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
		if err == nil || !strings.Contains(err.Error(), `cannot scan tuple column "c" into unexported field "private"`) {
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
		plan, err := iter.structScanPlan(reflect.TypeOf(row{}), columns)
		if err != nil {
			t.Fatal(err)
		}

		err = iter.fieldsByTraversal(reflect.ValueOf(&row{}), &plan)
		if err == nil {
			t.Fatal("unexpected nil error")
		}
	})
}
