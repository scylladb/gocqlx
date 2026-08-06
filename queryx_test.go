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

func TestCompileQuery(t *testing.T) {
	table := []struct {
		Q, R string
		V    []string
	}{
		// Basic test for named parameters, invalid char ',' terminating
		{
			Q: `INSERT INTO foo (a,b,c,d) VALUES (:name, :age, :first, :last)`,
			R: `INSERT INTO foo (a,b,c,d) VALUES (?, ?, ?, ?)`,
			V: []string{"name", "age", "first", "last"},
		},
		// This query tests a named parameter ending the string as well as numbers
		{
			Q: `SELECT * FROM a WHERE first_name=:name1 AND last_name=:name2`,
			R: `SELECT * FROM a WHERE first_name=? AND last_name=?`,
			V: []string{"name1", "name2"},
		},
		{
			Q: `SELECT "::foo" FROM a WHERE first_name=:name1 AND last_name=:name2`,
			R: `SELECT ":foo" FROM a WHERE first_name=? AND last_name=?`,
			V: []string{"name1", "name2"},
		},
		{
			Q: `SELECT 'a::b::c' || first_name, '::::ABC::_::' FROM person WHERE first_name=:first_name AND last_name=:last_name`,
			R: `SELECT 'a:b:c' || first_name, '::ABC:_:' FROM person WHERE first_name=? AND last_name=?`,
			V: []string{"first_name", "last_name"},
		},
		/* This unicode awareness test sadly fails, because of our byte-wise worldview.
		 * We could certainly iterate by Rune instead, though it's a great deal slower,
		 * it's probably the RightWay(tm)
		{
			Q: `INSERT INTO foo (a,b,c,d) VALUES (:あ, :b, :キコ, :名前)`,
			R: `INSERT INTO foo (a,b,c,d) VALUES (?, ?, ?, ?)`,
		},
		*/
	}

	for _, test := range table {
		qr, names, err := CompileNamedQuery([]byte(test.Q))
		if err != nil {
			t.Error(err)
		}
		if qr != test.R {
			t.Error("expected", test.R, "got", qr)
		}
		if diff := cmp.Diff(names, test.V); diff != "" {
			t.Error("names mismatch", diff)
		}
	}
}

func TestQueryxBindStruct(t *testing.T) {
	v := &struct {
		Name  string
		Age   int
		First string
		Last  string
	}{
		Name:  "name",
		Age:   30,
		First: "first",
		Last:  "last",
	}

	t.Run("simple", func(t *testing.T) {
		names := []string{"name", "age", "first", "last"}
		args, err := Query(nil, names).bindStructArgs(v, nil)
		if err != nil {
			t.Fatal(err)
		}

		if diff := cmp.Diff(args, []interface{}{"name", 30, "first", "last"}); diff != "" {
			t.Error("args mismatch", diff)
		}
	})

	t.Run("with transformer", func(t *testing.T) {
		tr := func(name string, val interface{}) interface{} {
			if name == "age" {
				return 42
			}
			return val
		}

		names := []string{"name", "age", "first", "last"}
		args, err := Query(nil, names).WithBindTransformer(tr).bindStructArgs(v, nil)
		if err != nil {
			t.Fatal(err)
		}

		if diff := cmp.Diff(args, []interface{}{"name", 42, "first", "last"}); diff != "" {
			t.Error("args mismatch", diff)
		}
	})

	t.Run("error", func(t *testing.T) {
		names := []string{"name", "age", "first", "not_found"}
		_, err := Query(nil, names).bindStructArgs(v, nil)
		if err == nil {
			t.Fatal("unexpected error")
		}
	})

	t.Run("fallback", func(t *testing.T) {
		names := []string{"name", "age", "first", "not_found"}
		m := map[string]interface{}{
			"not_found": "last",
		}
		args, err := Query(nil, names).bindStructArgs(v, m)
		if err != nil {
			t.Fatal(err)
		}

		if diff := cmp.Diff(args, []interface{}{"name", 30, "first", "last"}); diff != "" {
			t.Error("args mismatch", diff)
		}
	})

	t.Run("fallback with transformer", func(t *testing.T) {
		tr := func(name string, val interface{}) interface{} {
			if name == "not_found" {
				return "map_found"
			}
			return val
		}

		names := []string{"name", "age", "first", "not_found"}
		m := map[string]interface{}{
			"not_found": "last",
		}
		args, err := Query(nil, names).WithBindTransformer(tr).bindStructArgs(v, m)
		if err != nil {
			t.Fatal(err)
		}

		if diff := cmp.Diff(args, []interface{}{"name", 30, "first", "map_found"}); diff != "" {
			t.Error("args mismatch", diff)
		}
	})

	t.Run("fallback error", func(t *testing.T) {
		names := []string{"name", "age", "first", "not_found", "really_not_found"}
		m := map[string]interface{}{
			"not_found": "last",
		}
		_, err := Query(nil, names).bindStructArgs(v, m)
		if err == nil {
			t.Fatal("unexpected error")
		}
	})

	t.Run("tuple array", func(t *testing.T) {
		v := &struct {
			Coordinates [2]int
		}{
			Coordinates: [2]int{12, 34},
		}
		names := []string{"coordinates[0]", "coordinates[1]"}
		args, err := Query(nil, names).bindStructArgs(v, nil)
		if err != nil {
			t.Fatal(err)
		}

		if diff := cmp.Diff(args, []interface{}{12, 34}); diff != "" {
			t.Error("args mismatch", diff)
		}
	})

	t.Run("tuple slice", func(t *testing.T) {
		v := &struct {
			Coordinates []int
		}{
			Coordinates: []int{56, 78},
		}
		names := []string{"coordinates[0]", "coordinates[1]"}
		args, err := Query(nil, names).bindStructArgs(v, nil)
		if err != nil {
			t.Fatal(err)
		}

		if diff := cmp.Diff(args, []interface{}{56, 78}); diff != "" {
			t.Error("args mismatch", diff)
		}
	})

	t.Run("tuple array arity mismatch", func(t *testing.T) {
		v := &struct {
			Coordinates [3]int
		}{
			Coordinates: [3]int{12, 34, 56},
		}
		names := []string{"coordinates[0]", "coordinates[1]"}
		_, err := Query(nil, names).bindStructArgs(v, nil)
		if err == nil {
			t.Fatal("unexpected nil error")
		}
		if !strings.Contains(err.Error(), "array length 3 does not match tuple element count 2") {
			t.Fatal(err)
		}
	})

	t.Run("tuple slice arity mismatch", func(t *testing.T) {
		v := &struct {
			Coordinates []int
		}{
			Coordinates: []int{56, 78, 90},
		}
		names := []string{"coordinates[0]", "coordinates[1]"}
		_, err := Query(nil, names).bindStructArgs(v, nil)
		if err == nil {
			t.Fatal("unexpected nil error")
		}
		if !strings.Contains(err.Error(), "slice length 3 does not match tuple element count 2") {
			t.Fatal(err)
		}
	})

	t.Run("tuple byte array", func(t *testing.T) {
		v := &struct {
			Coordinates [2]byte
		}{
			Coordinates: [2]byte{12, 34},
		}
		names := []string{"coordinates[0]", "coordinates[1]"}
		args, err := Query(nil, names).bindStructArgs(v, nil)
		if err != nil {
			t.Fatal(err)
		}
		if diff := cmp.Diff(args, []interface{}{byte(12), byte(34)}); diff != "" {
			t.Error("args mismatch", diff)
		}
	})

	t.Run("unsupported tuple field does not fall back to map", func(t *testing.T) {
		v := &struct {
			Coordinates int
		}{
			Coordinates: 12,
		}
		names := []string{"coordinates[0]", "coordinates[1]"}
		_, err := Query(nil, names).bindStructArgs(v, map[string]interface{}{
			"coordinates": []int{34, 56},
		})
		if err == nil || !strings.Contains(err.Error(), `expected array or slice in "coordinates" but got int`) {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("tuple index overflow", func(t *testing.T) {
		v := &struct {
			Coordinates []int
		}{
			Coordinates: []int{},
		}
		name := "coordinates[" + strconv.Itoa(math.MaxInt) + "]"
		_, err := Query(nil, []string{name}).bindStructArgs(v, nil)
		if err == nil || !strings.Contains(err.Error(), name) {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("tuple container behind nil embedded pointer", func(t *testing.T) {
		type tupleFields struct {
			Coordinates []int
		}
		v := &struct {
			*tupleFields
		}{}
		_, err := Query(nil, []string{"coordinates[0]"}).bindStructArgs(v, nil)
		if err == nil || !strings.Contains(err.Error(), "nil pointer in field traversal") {
			t.Fatalf("unexpected error: %v", err)
		}
	})
}

func TestQueryxBindMap(t *testing.T) {
	v := map[string]interface{}{
		"name":  "name",
		"age":   30,
		"first": "first",
		"last":  "last",
	}

	t.Run("simple", func(t *testing.T) {
		names := []string{"name", "age", "first", "last"}
		args, err := Query(nil, names).bindMapArgs(v)
		if err != nil {
			t.Fatal(err)
		}

		if diff := cmp.Diff(args, []interface{}{"name", 30, "first", "last"}); diff != "" {
			t.Error("args mismatch", diff)
		}
	})

	t.Run("with transformer", func(t *testing.T) {
		tr := func(name string, val interface{}) interface{} {
			if name == "age" {
				return 42
			}
			return val
		}

		names := []string{"name", "age", "first", "last"}
		args, err := Query(nil, names).WithBindTransformer(tr).bindMapArgs(v)
		if err != nil {
			t.Fatal(err)
		}

		if diff := cmp.Diff(args, []interface{}{"name", 42, "first", "last"}); diff != "" {
			t.Error("args mismatch", diff)
		}
	})

	t.Run("error", func(t *testing.T) {
		names := []string{"name", "first", "not_found"}
		_, err := Query(nil, names).bindMapArgs(v)
		if err == nil {
			t.Fatal("unexpected error")
		}
	})

	t.Run("tuple array", func(t *testing.T) {
		names := []string{"coordinates[0]", "coordinates[1]"}
		args, err := Query(nil, names).bindMapArgs(map[string]interface{}{
			"coordinates": [2]int{12, 34},
		})
		if err != nil {
			t.Fatal(err)
		}

		if diff := cmp.Diff(args, []interface{}{12, 34}); diff != "" {
			t.Error("args mismatch", diff)
		}
	})

	t.Run("tuple slice", func(t *testing.T) {
		names := []string{"coordinates[0]", "coordinates[1]"}
		args, err := Query(nil, names).bindMapArgs(map[string]interface{}{
			"coordinates": []int{56, 78},
		})
		if err != nil {
			t.Fatal(err)
		}

		if diff := cmp.Diff(args, []interface{}{56, 78}); diff != "" {
			t.Error("args mismatch", diff)
		}
	})

	t.Run("tuple array arity mismatch", func(t *testing.T) {
		names := []string{"coordinates[0]", "coordinates[1]"}
		_, err := Query(nil, names).bindMapArgs(map[string]interface{}{
			"coordinates": [3]int{12, 34, 56},
		})
		if err == nil {
			t.Fatal("unexpected nil error")
		}
		if !strings.Contains(err.Error(), "array length 3 does not match tuple element count 2") {
			t.Fatal(err)
		}
	})

	t.Run("tuple slice arity mismatch", func(t *testing.T) {
		names := []string{"coordinates[0]", "coordinates[1]"}
		_, err := Query(nil, names).bindMapArgs(map[string]interface{}{
			"coordinates": []int{56, 78, 90},
		})
		if err == nil {
			t.Fatal("unexpected nil error")
		}
		if !strings.Contains(err.Error(), "slice length 3 does not match tuple element count 2") {
			t.Fatal(err)
		}
	})

	t.Run("tuple nil slice", func(t *testing.T) {
		names := []string{"coordinates[0]", "coordinates[1]"}
		_, err := Query(nil, names).bindMapArgs(map[string]interface{}{
			"coordinates": []int(nil),
		})
		if err == nil {
			t.Fatal("unexpected nil error")
		}
		if !strings.Contains(err.Error(), "nil slice does not match tuple element count 2") {
			t.Fatal(err)
		}
	})

	t.Run("tuple byte slice", func(t *testing.T) {
		names := []string{"coordinates[0]", "coordinates[1]"}
		args, err := Query(nil, names).bindMapArgs(map[string]interface{}{
			"coordinates": []byte("ab"),
		})
		if err != nil {
			t.Fatal(err)
		}
		if diff := cmp.Diff(args, []interface{}{byte('a'), byte('b')}); diff != "" {
			t.Error("args mismatch", diff)
		}
	})

	t.Run("tuple blob slice", func(t *testing.T) {
		names := []string{"coordinates[0]", "coordinates[1]"}
		args, err := Query(nil, names).bindMapArgs(map[string]interface{}{
			"coordinates": [][]byte{{1, 2}, {3, 4}},
		})
		if err != nil {
			t.Fatal(err)
		}
		if diff := cmp.Diff(args, []interface{}{[]byte{1, 2}, []byte{3, 4}}); diff != "" {
			t.Error("args mismatch", diff)
		}
	})

	t.Run("tuple index overflow", func(t *testing.T) {
		name := "coordinates[" + strconv.Itoa(math.MaxInt) + "]"
		_, err := Query(nil, []string{name}).bindMapArgs(map[string]interface{}{
			"coordinates": []int{},
		})
		if err == nil || !strings.Contains(err.Error(), name) {
			t.Fatalf("unexpected error: %v", err)
		}
	})
}

func TestQueryxAllWrapped(t *testing.T) {
	var (
		gocqlQueryPtr = reflect.TypeOf((*gocql.Query)(nil))
		queryxPtr     = reflect.TypeOf((*Queryx)(nil))
	)

	for i := 0; i < gocqlQueryPtr.NumMethod(); i++ {
		m, ok := queryxPtr.MethodByName(gocqlQueryPtr.Method(i).Name)
		if !ok {
			t.Fatalf("Queryx missing method %s", gocqlQueryPtr.Method(i).Name)
		}

		for j := 0; j < m.Type.NumOut(); j++ {
			if m.Type.Out(j) == gocqlQueryPtr {
				t.Errorf("Queryx method %s not wrapped", m.Name)
			}
		}
	}
}
