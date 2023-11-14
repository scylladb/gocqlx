package gocqlx

import (
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
	ID            int              `db:"id"`
	Name          string           `db:"name"`
	NormalAddress udt_test_address `db:"n_address"`
	Address       UDTList          `db:"address"`
	//ArrayAddress    [][]udt_test_address          `db:"array_address"`
	MapAddress UDTMap `db:"map_address"`
	//MapArrayAddress map[string][]udt_test_address `db:"map_array_address"`
}

func TestNewDesign(t *testing.T) {
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
		t.Fatal(err)
	}

	//interfaceConvert(firstPeople.MapAddress)

	args = udtWrapSlice(mapper, true, args)

	fmt.Println(args) //DEBUG

	//if data, err := json.Marshal(args); err != nil {
	//	t.Fatal(err)
	//} else {
	//	fmt.Println(string(data))
	//}

	if _, ok := args[2].([]udt); !ok {
		t.Fatal("not []udt")
	}

	if _, ok := args[3].(map[string]udt); !ok {
		t.Fatal("not map[string]udt")
	}
}
