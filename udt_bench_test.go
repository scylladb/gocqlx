package gocqlx

import (
	"reflect"
	"testing"

	"github.com/scylladb/go-reflectx"
)

func BenchmarkUDT_SupportedArrayMap(b *testing.B) {
	mapper := DefaultMapper
	firstPeople := udt_test_people{
		ID:            31,
		Name:          "TEST_MAN",
		NormalAddress: udt_test_address{Address: "NORMAL_ADDRESS", Number: 9022345, TEST: true},
		Address:       []udt_test_address{{Address: "MY_HOME_Z_MY_HOME", Number: 1, TEST: true}, {Address: "MY_HOME_Y_MY_HOME", Number: 2, TEST: true}, {Address: "MY_HOME_X_MY_HOME", Number: 3, TEST: false}},
		ArrayAddress: [][]udt_test_address{
			{
				{Address: "MY_HOME_Z_1_1_MY_HOME", Number: 2, TEST: true}, {Address: "MY_HOME_Y_1_1_MY_HOME", Number: 5, TEST: true}, {Address: "MY_HOME_X_1_1_MY_HOME", Number: 8, TEST: true},
				{Address: "MY_HOME_Z_1_2_MY_HOME", Number: 3, TEST: false}, {Address: "MY_HOME_Y_1_2_MY_HOME", Number: 6, TEST: true}, {Address: "MY_HOME_X_1_2_MY_HOME", Number: 9, TEST: true},
				{Address: "MY_HOME_Z_1_3_MY_HOME", Number: 4, TEST: true}, {Address: "MY_HOME_Y_1_3_MY_HOME", Number: 7, TEST: true}, {Address: "MY_HOME_X_1_3_MY_HOME", Number: 10, TEST: false},
			},
			{
				{Address: "MY_HOME_Z_2_1_MY_HOME", Number: 2, TEST: true}, {Address: "MY_HOME_Y_2_1_MY_HOME", Number: 5, TEST: true}, {Address: "MY_HOME_X_2_1_MY_HOME", Number: 8, TEST: false},
				{Address: "MY_HOME_Z_2_2_MY_HOME", Number: 3, TEST: false}, {Address: "MY_HOME_Y_2_2_MY_HOME", Number: 6, TEST: true}, {Address: "MY_HOME_X_2_2_MY_HOME", Number: 9, TEST: true},
				{Address: "MY_HOME_Z_2_3_MY_HOME", Number: 4, TEST: true}, {Address: "MY_HOME_Y_2_3_MY_HOME", Number: 7, TEST: false}, {Address: "MY_HOME_X_2_3_MY_HOME", Number: 10, TEST: true},
			},
		},
		MapAddress: map[string]udt_test_address{"MYFIRSTHOME": {Address: "MY_XASD_HOME_Z_MY_HOME", Number: 2000, TEST: true}, "MYSECONDHOME": {Address: "MY_SADS_HOME_Y_MY_HOME", Number: 10000, TEST: false}},
		MapArrayAddress: map[string][]udt_test_address{
			"MYADDRESSADDRESS":   {{Address: "MY_MAP_ARRAY_HOME_Z_MY_HOME", Number: 2001, TEST: true}, {Address: "MY_MAP_ARRAY_HOME_Y_MY_HOME", Number: 2002, TEST: true}, {Address: "MY_MAP_ARRAY_HOME_X_MY_HOME", Number: 2003, TEST: false}},
			"MY2ADDRESS2ADDRESS": {{Address: "MY_MAP_ARRAY_HOME_Z_MY_HOME", Number: 2001, TEST: true}, {Address: "MY_MAP_ARRAY_HOME_Y_MY_HOME", Number: 2002, TEST: true}, {Address: "MY_MAP_ARRAY_HOME_X_MY_HOME", Number: 2003, TEST: false}},
		},
	}

	names := []string{"id", "name", "address", "array_address", "map_address", "map_array_address", "n_address"}
	args, err := Query(nil, names).bindStructArgs(firstPeople, nil)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		udtWrapSlice(mapper, true, args)
	}
}

func BenchmarkUDT_UnsupportedArrayMap(b *testing.B) {
	mapper := DefaultMapper
	firstPeople := udt_test_people{
		ID:            31,
		Name:          "TEST_MAN",
		NormalAddress: udt_test_address{Address: "NORMAL_ADDRESS", Number: 9022345, TEST: true},
		Address:       []udt_test_address{{Address: "MY_HOME_Z_MY_HOME", Number: 1, TEST: true}, {Address: "MY_HOME_Y_MY_HOME", Number: 2, TEST: true}, {Address: "MY_HOME_X_MY_HOME", Number: 3, TEST: false}},
		ArrayAddress: [][]udt_test_address{
			{
				{Address: "MY_HOME_Z_1_1_MY_HOME", Number: 2, TEST: true}, {Address: "MY_HOME_Y_1_1_MY_HOME", Number: 5, TEST: true}, {Address: "MY_HOME_X_1_1_MY_HOME", Number: 8, TEST: true},
				{Address: "MY_HOME_Z_1_2_MY_HOME", Number: 3, TEST: false}, {Address: "MY_HOME_Y_1_2_MY_HOME", Number: 6, TEST: true}, {Address: "MY_HOME_X_1_2_MY_HOME", Number: 9, TEST: true},
				{Address: "MY_HOME_Z_1_3_MY_HOME", Number: 4, TEST: true}, {Address: "MY_HOME_Y_1_3_MY_HOME", Number: 7, TEST: true}, {Address: "MY_HOME_X_1_3_MY_HOME", Number: 10, TEST: false},
			},
			{
				{Address: "MY_HOME_Z_2_1_MY_HOME", Number: 2, TEST: true}, {Address: "MY_HOME_Y_2_1_MY_HOME", Number: 5, TEST: true}, {Address: "MY_HOME_X_2_1_MY_HOME", Number: 8, TEST: false},
				{Address: "MY_HOME_Z_2_2_MY_HOME", Number: 3, TEST: false}, {Address: "MY_HOME_Y_2_2_MY_HOME", Number: 6, TEST: true}, {Address: "MY_HOME_X_2_2_MY_HOME", Number: 9, TEST: true},
				{Address: "MY_HOME_Z_2_3_MY_HOME", Number: 4, TEST: true}, {Address: "MY_HOME_Y_2_3_MY_HOME", Number: 7, TEST: false}, {Address: "MY_HOME_X_2_3_MY_HOME", Number: 10, TEST: true},
			},
		},
		MapAddress: map[string]udt_test_address{"MYFIRSTHOME": {Address: "MY_XASD_HOME_Z_MY_HOME", Number: 2000, TEST: true}, "MYSECONDHOME": {Address: "MY_SADS_HOME_Y_MY_HOME", Number: 10000, TEST: false}},
		MapArrayAddress: map[string][]udt_test_address{
			"MYADDRESSADDRESS":   {{Address: "MY_MAP_ARRAY_HOME_Z_MY_HOME", Number: 2001, TEST: true}, {Address: "MY_MAP_ARRAY_HOME_Y_MY_HOME", Number: 2002, TEST: true}, {Address: "MY_MAP_ARRAY_HOME_X_MY_HOME", Number: 2003, TEST: false}},
			"MY2ADDRESS2ADDRESS": {{Address: "MY_MAP_ARRAY_HOME_Z_MY_HOME", Number: 2001, TEST: true}, {Address: "MY_MAP_ARRAY_HOME_Y_MY_HOME", Number: 2002, TEST: true}, {Address: "MY_MAP_ARRAY_HOME_X_MY_HOME", Number: 2003, TEST: false}},
		},
	}

	names := []string{"id", "name", "address", "array_address", "map_address", "map_array_address", "n_address"}
	args, err := Query(nil, names).bindStructArgs(firstPeople, nil)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		oldUdtWrapSlice(mapper, true, args)
	}
}

func BenchmarkUDT_SupportedArrayMapOneArray(b *testing.B) {
	mapper := DefaultMapper
	firstPeople := udt_test_people{
		ID:            31,
		Name:          "TEST_MAN",
		NormalAddress: udt_test_address{Address: "NORMAL_ADDRESS", Number: 9022345, TEST: true},
		Address:       []udt_test_address{{Address: "MY_HOME_Z_MY_HOME", Number: 1, TEST: true}, {Address: "MY_HOME_Y_MY_HOME", Number: 2, TEST: true}, {Address: "MY_HOME_X_MY_HOME", Number: 3, TEST: false}},
	}

	names := []string{"id", "name", "address", "array_address", "map_address", "map_array_address", "n_address"}
	args, err := Query(nil, names).bindStructArgs(firstPeople, nil)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		udtWrapSlice(mapper, true, args)
	}
}

func BenchmarkUDT_UnsupportedArrayMapOneArray(b *testing.B) {
	mapper := DefaultMapper
	firstPeople := udt_test_people{
		ID:            31,
		Name:          "TEST_MAN",
		NormalAddress: udt_test_address{Address: "NORMAL_ADDRESS", Number: 9022345, TEST: true},
		Address:       []udt_test_address{{Address: "MY_HOME_Z_MY_HOME", Number: 1, TEST: true}, {Address: "MY_HOME_Y_MY_HOME", Number: 2, TEST: true}, {Address: "MY_HOME_X_MY_HOME", Number: 3, TEST: false}},
	}

	names := []string{"id", "name", "address", "array_address", "map_address", "map_array_address", "n_address"}
	args, err := Query(nil, names).bindStructArgs(firstPeople, nil)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		oldUdtWrapSlice(mapper, true, args)
	}
}

func oldUdtWrapSlice(mapper *reflectx.Mapper, unsafe bool, v []interface{}) []interface{} {
	for i := range v {
		if _, ok := v[i].(UDT); ok {
			v[i] = makeUDT(reflect.ValueOf(v[i]), mapper, unsafe)
		}
	}
	return v
}
