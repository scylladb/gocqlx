// Copyright (C) 2017 ScyllaDB
// Use of this source code is governed by a ALv2-style
// license that can be found in the LICENSE file.

// +build all integration

package gocqlx_test

import (
	"math/big"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/scylladb/gocqlx"

	"gopkg.in/inf.v0"
)

type FullName struct {
	FirstName string
	LastName  string
}

func (n FullName) MarshalCQL(info gocql.TypeInfo) ([]byte, error) {
	return []byte(n.FirstName + " " + n.LastName), nil
}

func (n *FullName) UnmarshalCQL(info gocql.TypeInfo, data []byte) error {
	t := strings.SplitN(string(data), " ", 2)
	n.FirstName, n.LastName = t[0], t[1]
	return nil
}

func TestStruct(t *testing.T) {
	session := createSession(t)
	defer session.Close()
	if err := createTable(session, `CREATE TABLE gocqlx_test.struct_table (
			testuuid       timeuuid PRIMARY KEY,
			testtimestamp  timestamp,
			testvarchar    varchar,
			testbigint     bigint,
			testblob       blob,
			testbool       boolean,
			testfloat      float,
			testdouble     double,
			testint        int,
			testdecimal    decimal,
			testlist       list<text>,
			testset        set<int>,
			testmap        map<varchar, varchar>,
			testvarint     varint,
			testinet       inet,
			testcustom     text

		)`); err != nil {
		t.Fatal("create table:", err)
	}

	type StructTable struct {
		Testuuid      gocql.UUID
		Testvarchar   string
		Testbigint    int64
		Testtimestamp time.Time
		Testblob      []byte
		Testbool      bool
		Testfloat     float32
		Testdouble    float64
		Testint       int
		Testdecimal   *inf.Dec
		Testlist      []string
		Testset       []int
		Testmap       map[string]string
		Testvarint    *big.Int
		Testinet      string
		Testcustom    FullName
	}

	bigInt := new(big.Int)
	if _, ok := bigInt.SetString("830169365738487321165427203929228", 10); !ok {
		t.Fatal("failed setting bigint by string")
	}

	m := StructTable{
		Testuuid:      gocql.TimeUUID(),
		Testvarchar:   "Test VarChar",
		Testbigint:    time.Now().Unix(),
		Testtimestamp: time.Now().Truncate(time.Millisecond).UTC(),
		Testblob:      []byte("test blob"),
		Testbool:      true,
		Testfloat:     float32(4.564),
		Testdouble:    float64(4.815162342),
		Testint:       2343,
		Testdecimal:   inf.NewDec(100, 0),
		Testlist:      []string{"quux", "foo", "bar", "baz", "quux"},
		Testset:       []int{1, 2, 3, 4, 5, 6, 7, 8, 9},
		Testmap:       map[string]string{"field1": "val1", "field2": "val2", "field3": "val3"},
		Testvarint:    bigInt,
		Testinet:      "213.212.2.19",
		Testcustom:    FullName{FirstName: "John", LastName: "Doe"},
	}

	if err := session.Query(`INSERT INTO struct_table (testuuid, testtimestamp, testvarchar, testbigint, testblob, testbool, testfloat,testdouble, testint, testdecimal, testlist, testset, testmap, testvarint, testinet, testcustom) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		m.Testuuid,
		m.Testtimestamp,
		m.Testvarchar,
		m.Testbigint,
		m.Testblob,
		m.Testbool,
		m.Testfloat,
		m.Testdouble,
		m.Testint,
		m.Testdecimal,
		m.Testlist,
		m.Testset,
		m.Testmap,
		m.Testvarint,
		m.Testinet,
		m.Testcustom).Exec(); err != nil {
		t.Fatal("insert:", err)
	}

	t.Run("get", func(t *testing.T) {
		var v StructTable
		if err := gocqlx.Get(&v, session.Query(`SELECT * FROM struct_table`)); err != nil {
			t.Fatal("get failed", err)
		}

		if !reflect.DeepEqual(m, v) {
			t.Fatal("not equals")
		}
	})

	t.Run("select", func(t *testing.T) {
		var v []StructTable
		if err := gocqlx.Select(&v, session.Query(`SELECT * FROM struct_table`)); err != nil {
			t.Fatal("select failed", err)
		}

		if len(v) != 1 {
			t.Fatal("select unexpecrted number of rows", len(v))
		}

		if !reflect.DeepEqual(m, v[0]) {
			t.Fatal("not equals")
		}
	})

	t.Run("select ptr", func(t *testing.T) {
		var v []*StructTable
		if err := gocqlx.Select(&v, session.Query(`SELECT * FROM struct_table`)); err != nil {
			t.Fatal("select failed", err)
		}

		if len(v) != 1 {
			t.Fatal("select unexpecrted number of rows", len(v))
		}

		if !reflect.DeepEqual(&m, v[0]) {
			t.Fatal("not equals")
		}
	})
}

func TestScannable(t *testing.T) {
	session := createSession(t)
	defer session.Close()
	if err := createTable(session, `CREATE TABLE gocqlx_test.scannable_table (testfullname text PRIMARY KEY)`); err != nil {
		t.Fatal("create table:", err)
	}
	m := FullName{"John", "Doe"}

	if err := session.Query(`INSERT INTO scannable_table (testfullname) values (?)`, m).Exec(); err != nil {
		t.Fatal("insert:", err)
	}

	t.Run("get", func(t *testing.T) {
		var v FullName
		if err := gocqlx.Get(&v, session.Query(`SELECT testfullname FROM scannable_table`)); err != nil {
			t.Fatal("get failed", err)
		}

		if !reflect.DeepEqual(m, v) {
			t.Fatal("not equals")
		}
	})

	t.Run("select", func(t *testing.T) {
		var v []FullName
		if err := gocqlx.Select(&v, session.Query(`SELECT testfullname FROM scannable_table`)); err != nil {
			t.Fatal("get failed", err)
		}

		if len(v) != 1 {
			t.Fatal("select unexpecrted number of rows", len(v))
		}

		if !reflect.DeepEqual(m, v[0]) {
			t.Fatal("not equals")
		}
	})

	t.Run("select ptr", func(t *testing.T) {
		var v []*FullName
		if err := gocqlx.Select(&v, session.Query(`SELECT testfullname FROM scannable_table`)); err != nil {
			t.Fatal("get failed", err)
		}

		if len(v) != 1 {
			t.Fatal("select unexpecrted number of rows", len(v))
		}

		if !reflect.DeepEqual(&m, v[0]) {
			t.Fatal("not equals")
		}
	})
}

func TestUnsafe(t *testing.T) {
	session := createSession(t)
	defer session.Close()
	if err := createTable(session, `CREATE TABLE gocqlx_test.unsafe_table (testtext text PRIMARY KEY, testtextunbound text)`); err != nil {
		t.Fatal("create table:", err)
	}
	if err := session.Query(`INSERT INTO unsafe_table (testtext, testtextunbound) values (?, ?)`, "test", "test").Exec(); err != nil {
		t.Fatal("insert:", err)
	}

	type UnsafeTable struct {
		Testtext string
	}

	t.Run("safe get", func(t *testing.T) {
		var v UnsafeTable
		i := gocqlx.Iter(session.Query(`SELECT * FROM unsafe_table`))
		if err := i.Get(&v); err == nil || err.Error() != "missing destination name \"testtextunbound\" in *gocqlx_test.UnsafeTable" {
			t.Fatal("expected ErrNotFound", "got", err)
		}
	})

	t.Run("safe select", func(t *testing.T) {
		var v []UnsafeTable
		i := gocqlx.Iter(session.Query(`SELECT * FROM unsafe_table`))
		if err := i.Select(&v); err == nil || err.Error() != "missing destination name \"testtextunbound\" in *gocqlx_test.UnsafeTable" {
			t.Fatal("expected ErrNotFound", "got", err)
		}
		if cap(v) > 0 {
			t.Fatal("side effect alloc")
		}
	})

	t.Run("unsafe get", func(t *testing.T) {
		var v UnsafeTable
		i := gocqlx.Iter(session.Query(`SELECT * FROM unsafe_table`))
		if err := i.Unsafe().Get(&v); err != nil {
			t.Fatal(err)
		}
		if v.Testtext != "test" {
			t.Fatal("get failed")
		}
	})

	t.Run("unsafe select", func(t *testing.T) {
		var v []UnsafeTable
		i := gocqlx.Iter(session.Query(`SELECT * FROM unsafe_table`))
		if err := i.Unsafe().Select(&v); err != nil {
			t.Fatal(err)
		}
		if len(v) != 1 {
			t.Fatal("select failed")
		}
		if v[0].Testtext != "test" {
			t.Fatal("select failed")
		}
	})
}

func TestNotFound(t *testing.T) {
	session := createSession(t)
	defer session.Close()
	if err := createTable(session, `CREATE TABLE gocqlx_test.not_found_table (testtext text PRIMARY KEY)`); err != nil {
		t.Fatal("create table:", err)
	}

	type NotFoundTable struct {
		Testtext string
	}

	t.Run("get cql error", func(t *testing.T) {
		var v NotFoundTable
		i := gocqlx.Iter(session.Query(`SELECT * FROM not_found_table WRONG`))

		err := i.Get(&v)
		if err == nil || !strings.Contains(err.Error(), "WRONG") {
			t.Fatal(err)
		}
	})

	t.Run("get", func(t *testing.T) {
		var v NotFoundTable
		i := gocqlx.Iter(session.Query(`SELECT * FROM not_found_table`))
		if err := i.Get(&v); err != gocql.ErrNotFound {
			t.Fatal("expected ErrNotFound", "got", err)
		}
	})

	t.Run("select cql error", func(t *testing.T) {
		var v []NotFoundTable
		i := gocqlx.Iter(session.Query(`SELECT * FROM not_found_table WRONG`))

		err := i.Select(&v)
		if err == nil || !strings.Contains(err.Error(), "WRONG") {
			t.Fatal(err)
		}
	})

	t.Run("select", func(t *testing.T) {
		var v []NotFoundTable
		i := gocqlx.Iter(session.Query(`SELECT * FROM not_found_table`))
		if err := i.Select(&v); err != gocql.ErrNotFound {
			t.Fatal("expected ErrNotFound", "got", err)
		}
		if cap(v) > 0 {
			t.Fatal("side effect alloc")
		}
	})
}
