package gocqlx

import (
	"reflect"
	"testing"

	"github.com/scylladb/go-reflectx"
)

func BenchmarkUDT_SupportedArrayMap_NewDesign(b *testing.B) {
	mapper := DefaultMapper
	firstPeople := udt_test_people{
		ID:            31,
		Name:          "TEST_MAN",
		NormalAddress: udt_test_address{Address: "NORMAL_ADDRESS", Number: 9022345, TEST: true},
		Address:       UDTList{udt_test_address{Address: "MY_HOME_Z_MY_HOME", Number: 1, TEST: true}, udt_test_address{Address: "MY_HOME_Y_MY_HOME", Number: 2, TEST: true}, udt_test_address{Address: "MY_HOME_X_MY_HOME", Number: 3, TEST: false}},
		MapAddress:    UDTMap{"MYFIRSTHOME": udt_test_address{Address: "MY_XASD_HOME_Z_MY_HOME", Number: 2000, TEST: true}, "MYSECONDHOME": udt_test_address{Address: "MY_SADS_HOME_Y_MY_HOME", Number: 10000, TEST: false}},
	}

	names := []string{"id", "name", "address" /*, "array_address"*/, "map_address" /*, "map_array_address", "n_address"*/}
	args, err := Query(nil, names).bindStructArgs(firstPeople, nil)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		udtWrapSlice(mapper, true, args)
	}
}

func BenchmarkUDT_UnsupportedArrayMap_NewDesign(b *testing.B) {
	mapper := DefaultMapper
	firstPeople := udt_test_people{
		ID:            31,
		Name:          "TEST_MAN",
		NormalAddress: udt_test_address{Address: "NORMAL_ADDRESS", Number: 9022345, TEST: true},
		Address:       UDTList{udt_test_address{Address: "MY_HOME_Z_MY_HOME", Number: 1, TEST: true}, udt_test_address{Address: "MY_HOME_Y_MY_HOME", Number: 2, TEST: true}, udt_test_address{Address: "MY_HOME_X_MY_HOME", Number: 3, TEST: false}},
		MapAddress:    UDTMap{"MYFIRSTHOME": udt_test_address{Address: "MY_XASD_HOME_Z_MY_HOME", Number: 2000, TEST: true}, "MYSECONDHOME": udt_test_address{Address: "MY_SADS_HOME_Y_MY_HOME", Number: 10000, TEST: false}},
	}

	names := []string{"id", "name", "address" /*, "array_address"*/, "map_address" /*, "map_array_address", "n_address"*/}
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
