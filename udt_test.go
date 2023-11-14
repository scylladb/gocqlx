package gocqlx

import (
	"encoding/json"
	"fmt"
	"testing"
)

type udt_test_address struct {
	UDT

	Address string
	Number  int
	TEST    bool
}

type udt_test_people struct {
	ID              int                           `db:"id"`
	Name            string                        `db:"name"`
	NormalAddress   udt_test_address              `db:"n_address"`
	Address         []udt_test_address            `db:"address"`
	ArrayAddress    [][]udt_test_address          `db:"array_address"`
	MapAddress      map[string]udt_test_address   `db:"map_address"`
	MapArrayAddress map[string][]udt_test_address `db:"map_array_address"`
}

func TestUDT_Array_Map(t *testing.T) {
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
		t.Fatal(err)
	}

	//interfaceConvert(firstPeople.MapAddress)

	args = udtWrapSlice(mapper, true, args)

	fmt.Println(args) //DEBUG

	if data, err := json.Marshal(args); err != nil {
		t.Fatal(err)
	} else {
		fmt.Println(string(data))
	}

	//TODO: Check ....
}
