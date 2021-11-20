// Copyright (C) 2017 ScyllaDB
// Use of this source code is governed by a ALv2-style
// license that can be found in the LICENSE file.

//go:build all || integration
// +build all integration

package gocqlx_test

import (
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/scylladb/gocqlx/v2"
	. "github.com/scylladb/gocqlx/v2/gocqlxtest"
	"github.com/scylladb/gocqlx/v2/qb"
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

type FullNameUDT struct {
	gocqlx.UDT
	FullName
}

type FullNamePtrUDT struct {
	gocqlx.UDT
	*FullName
}

func TestIterxStruct(t *testing.T) {
	session := CreateSession(t)
	defer session.Close()

	if err := session.ExecStmt(`CREATE TYPE gocqlx_test.FullName (first_Name text, last_name text)`); err != nil {
		t.Fatal("create type:", err)
	}

	if err := session.ExecStmt(`CREATE TABLE gocqlx_test.struct_table (
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
			testcustom     text,
			testudt        gocqlx_test.FullName,
			testptrudt     gocqlx_test.FullName
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
		Testudt       FullNameUDT
		Testptrudt    FullNamePtrUDT
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
		Testudt:       FullNameUDT{FullName: FullName{FirstName: "John", LastName: "Doe"}},
		Testptrudt:    FullNamePtrUDT{FullName: &FullName{FirstName: "John", LastName: "Doe"}},
	}

	const insertStmt = `INSERT INTO struct_table (testuuid, testtimestamp, testvarchar, testbigint, testblob, testbool, testfloat,testdouble, testint, testdecimal, testlist, testset, testmap, testvarint, testinet, testcustom, testudt, testptrudt) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`

	if err := session.Query(insertStmt, nil).Bind(
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
		m.Testcustom,
		m.Testudt,
		m.Testptrudt).ExecRelease(); err != nil {
		t.Fatal("insert:", err)
	}

	diffOpts := cmpopts.IgnoreUnexported(big.Int{}, inf.Dec{})

	const stmt = `SELECT * FROM struct_table`

	t.Run("get", func(t *testing.T) {
		var v StructTable
		if err := session.Query(stmt, nil).Get(&v); err != nil {
			t.Fatal("Get() failed:", err)
		}
		if diff := cmp.Diff(m, v, diffOpts); diff != "" {
			t.Fatalf("Get()=%+v expected %+v, diff: %s", v, m, diff)
		}
	})

	t.Run("select", func(t *testing.T) {
		var v []StructTable
		if err := session.Query(stmt, nil).Select(&v); err != nil {
			t.Fatal("Select() failed:", err)
		}
		if len(v) != 1 {
			t.Fatalf("Select()=%+v expected 1 row got %d", v, len(v))
		}
		if diff := cmp.Diff(m, v[0], diffOpts); diff != "" {
			t.Fatalf("Select()[0]=%+v expected %+v, diff: %s", v[0], m, diff)
		}
	})

	t.Run("select ptr", func(t *testing.T) {
		var v []*StructTable
		if err := session.Query(stmt, nil).Select(&v); err != nil {
			t.Fatal("Select() failed:", err)
		}
		if len(v) != 1 {
			t.Fatalf("Select()=%+v expected 1 row got %d", v, len(v))
		}
		if diff := cmp.Diff(&m, v[0], diffOpts); diff != "" {
			t.Fatalf("Select()[0]=%+v expected %+v, diff: %s", v[0], &m, diff)
		}
	})

	t.Run("struct scan", func(t *testing.T) {
		var (
			v StructTable
			n int
		)

		iter := session.Query(stmt, nil).Iter()
		for iter.StructScan(&v) {
			n++
		}
		if err := iter.Close(); err != nil {
			t.Fatal("StructScan() failed:", err)
		}
		if n != 1 {
			t.Fatalf("StructScan() expected 1 row got %d", n)
		}
		if diff := cmp.Diff(m, v, diffOpts); diff != "" {
			t.Fatalf("StructScan()=%+v expected %+v, diff: %s", v, m, diff)
		}
	})
}

func TestIterxScannable(t *testing.T) {
	session := CreateSession(t)
	defer session.Close()

	if err := session.ExecStmt(`CREATE TABLE gocqlx_test.scannable_table (testfullname text PRIMARY KEY)`); err != nil {
		t.Fatal("create table:", err)
	}

	m := FullName{"John", "Doe"}

	if err := session.Query(`INSERT INTO scannable_table (testfullname) values (?)`, nil).Bind(m).Exec(); err != nil {
		t.Fatal("insert:", err)
	}

	const stmt = `SELECT testfullname FROM scannable_table`

	t.Run("get", func(t *testing.T) {
		var v FullName
		if err := session.Query(stmt, nil).Get(&v); err != nil {
			t.Fatal("Get() failed:", err)
		}
		if diff := cmp.Diff(m, v); diff != "" {
			t.Fatalf("Get()=%+v expected %+v, diff: %s", v, m, diff)
		}
	})

	t.Run("select", func(t *testing.T) {
		var v []FullName
		if err := session.Query(stmt, nil).Select(&v); err != nil {
			t.Fatal("Select() failed:", err)
		}
		if len(v) != 1 {
			t.Fatalf("Select()=%+v expected 1 row got %d", v, len(v))
		}
		if diff := cmp.Diff(m, v[0]); diff != "" {
			t.Fatalf("Select()[0]=%+v expected %+v, diff: %s", v[0], m, diff)
		}
	})

	t.Run("select ptr", func(t *testing.T) {
		var v []*FullName
		if err := session.Query(stmt, nil).Select(&v); err != nil {
			t.Fatal("Select() failed:", err)
		}
		if len(v) != 1 {
			t.Fatalf("Select()=%+v expected 1 row got %d", v, len(v))
		}
		if diff := cmp.Diff(&m, v[0]); diff != "" {
			t.Fatalf("Select()[0]=%+v expected %+v, diff: %s", v[0], &m, diff)
		}
	})
}

func TestIterxStructOnly(t *testing.T) {
	session := CreateSession(t)
	defer session.Close()

	if err := session.ExecStmt(`CREATE TABLE gocqlx_test.struct_only_table (first_name text, last_name text, PRIMARY KEY (first_name, last_name))`); err != nil {
		t.Fatal("create table:", err)
	}

	m := FullName{"John", "Doe"}

	if err := session.Query(`INSERT INTO struct_only_table (first_name, last_name) values (?, ?)`, nil).Bind(m.FirstName, m.LastName).Exec(); err != nil {
		t.Fatal("insert:", err)
	}

	const stmt = `SELECT first_name, last_name FROM struct_only_table`

	t.Run("get", func(t *testing.T) {
		var v FullName
		if err := session.Query(stmt, nil).Iter().StructOnly().Get(&v); err != nil {
			t.Fatal("Get() failed:", err)
		}
		if diff := cmp.Diff(m, v); diff != "" {
			t.Fatalf("Get()=%+v expected %+v, diff: %s", v, m, diff)
		}
	})

	t.Run("select", func(t *testing.T) {
		var v []FullName
		if err := session.Query(stmt, nil).Iter().StructOnly().Select(&v); err != nil {
			t.Fatal("Select() failed:", err)
		}
		if len(v) != 1 {
			t.Fatalf("Select()=%+v expected 1 row got %d", v, len(v))
		}
		if diff := cmp.Diff(m, v[0]); diff != "" {
			t.Fatalf("Select()[0]=%+v expected %+v, diff: %s", v[0], m, diff)
		}
	})

	t.Run("select ptr", func(t *testing.T) {
		var v []*FullName
		if err := session.Query(stmt, nil).Iter().StructOnly().Select(&v); err != nil {
			t.Fatal("Select() failed:", err)
		}
		if len(v) != 1 {
			t.Fatalf("Select()=%+v expected 1 row got %d", v, len(v))
		}
		if diff := cmp.Diff(&m, v[0]); diff != "" {
			t.Fatalf("Select()[0]=%+v expected %+v, diff: %s", v[0], &m, diff)
		}
	})

	const golden = "expected 1 column in result"

	t.Run("get error", func(t *testing.T) {
		var v FullName
		err := session.Query(stmt, nil).Get(&v)
		if err == nil || !strings.HasPrefix(err.Error(), golden) {
			t.Fatalf("Get() error=%q expected %s", err, golden)
		}
	})

	t.Run("select error", func(t *testing.T) {
		var v []FullName
		err := session.Query(stmt, nil).Select(&v)
		if err == nil || !strings.HasPrefix(err.Error(), golden) {
			t.Fatalf("Select() error=%q expected %s", err, golden)
		}
	})
}

func TestIterxStructOnlyUDT(t *testing.T) {
	session := CreateSession(t)
	defer session.Close()

	if err := session.ExecStmt(`CREATE TABLE gocqlx_test.struct_only_udt_table (first_name text, last_name text, PRIMARY KEY (first_name, last_name))`); err != nil {
		t.Fatal("create table:", err)
	}

	m := FullNameUDT{
		FullName: FullName{
			FirstName: "John",
			LastName:  "Doe",
		},
	}

	if err := session.Query(`INSERT INTO struct_only_udt_table (first_name, last_name) values (?, ?)`, nil).Bind(m.FirstName, m.LastName).Exec(); err != nil {
		t.Fatal("insert:", err)
	}

	const stmt = `SELECT first_name, last_name FROM struct_only_udt_table`

	t.Run("get", func(t *testing.T) {
		var v FullNameUDT
		if err := session.Query(stmt, nil).Iter().StructOnly().Get(&v); err != nil {
			t.Fatal("Get() failed:", err)
		}
		if diff := cmp.Diff(m, v); diff != "" {
			t.Fatalf("Get()=%+v expected %+v, diff: %s", v, m, diff)
		}
	})

	t.Run("select", func(t *testing.T) {
		var v []FullNameUDT
		if err := session.Query(stmt, nil).Iter().StructOnly().Select(&v); err != nil {
			t.Fatal("Select() failed:", err)
		}
		if len(v) != 1 {
			t.Fatalf("Select()=%+v expected 1 row got %d", v, len(v))
		}
		if diff := cmp.Diff(m, v[0]); diff != "" {
			t.Fatalf("Select()[0]=%+v expected %+v, diff: %s", v[0], m, diff)
		}
	})

	t.Run("select ptr", func(t *testing.T) {
		var v []*FullNameUDT
		if err := session.Query(stmt, nil).Iter().StructOnly().Select(&v); err != nil {
			t.Fatal("Select() failed:", err)
		}
		if len(v) != 1 {
			t.Fatalf("Select()=%+v expected 1 row got %d", v, len(v))
		}
		if diff := cmp.Diff(&m, v[0]); diff != "" {
			t.Fatalf("Select()[0]=%+v expected %+v, diff: %s", v[0], &m, diff)
		}
	})

	const golden = "expected 1 column in result"

	t.Run("get error", func(t *testing.T) {
		var v FullNameUDT
		err := session.Query(stmt, nil).Get(&v)
		if err == nil || !strings.HasPrefix(err.Error(), golden) {
			t.Fatalf("Get() error=%q expected %s", err, golden)
		}
	})

	t.Run("select error", func(t *testing.T) {
		var v []FullNameUDT
		err := session.Query(stmt, nil).Select(&v)
		if err == nil || !strings.HasPrefix(err.Error(), golden) {
			t.Fatalf("Select() error=%q expected %s", err, golden)
		}
	})
}

func TestIterxUnsafe(t *testing.T) {
	session := CreateSession(t)
	defer session.Close()

	if err := session.ExecStmt(`CREATE TABLE gocqlx_test.unsafe_table (testtext text PRIMARY KEY, testtextunbound text)`); err != nil {
		t.Fatal("create table:", err)
	}
	if err := session.Query(`INSERT INTO unsafe_table (testtext, testtextunbound) values (?, ?)`, nil).Bind("test", "test").Exec(); err != nil {
		t.Fatal("insert:", err)
	}

	type UnsafeTable struct {
		Testtext string
	}

	m := UnsafeTable{
		Testtext: "test",
	}

	const (
		stmt   = `SELECT * FROM unsafe_table`
		golden = "missing destination name \"testtextunbound\" in gocqlx_test.UnsafeTable"
	)

	t.Run("get", func(t *testing.T) {
		var v UnsafeTable
		err := session.Query(stmt, nil).Get(&v)
		if err == nil || !strings.HasPrefix(err.Error(), golden) {
			t.Fatalf("Get() error=%q expected %s", err, golden)
		}
	})

	t.Run("select", func(t *testing.T) {
		var v []UnsafeTable
		err := session.Query(stmt, nil).Select(&v)
		if err == nil || !strings.HasPrefix(err.Error(), golden) {
			t.Fatalf("Select() error=%q expected %s", err, golden)
		}
		if cap(v) > 0 {
			t.Fatalf("Select() effect alloc cap=%d expected 0", cap(v))
		}
	})

	t.Run("get unsafe", func(t *testing.T) {
		var v UnsafeTable
		err := session.Query(stmt, nil).Iter().Unsafe().Get(&v)
		if err != nil {
			t.Fatal("Get() failed:", err)
		}
		if diff := cmp.Diff(m, v); diff != "" {
			t.Fatalf("Get()=%+v expected %+v, diff: %s", v, m, diff)
		}
	})

	t.Run("select unsafe", func(t *testing.T) {
		var v []UnsafeTable
		err := session.Query(stmt, nil).Iter().Unsafe().Select(&v)
		if err != nil {
			t.Fatal("Select() failed:", err)
		}
		if len(v) != 1 {
			t.Fatalf("Select()=%+v expected 1 row got %d", v, len(v))
		}
		if diff := cmp.Diff(m, v[0]); diff != "" {
			t.Fatalf("Select()[0]=%+v expected %+v, diff: %s", v[0], m, diff)
		}
	})

	t.Run("select default unsafe", func(t *testing.T) {
		gocqlx.DefaultUnsafe = true
		defer func() {
			gocqlx.DefaultUnsafe = false
		}()
		var v []UnsafeTable
		err := session.Query(stmt, nil).Iter().Select(&v)
		if err != nil {
			t.Fatal("Select() failed:", err)
		}
		if len(v) != 1 {
			t.Fatalf("Select()=%+v expected 1 row got %d", v, len(v))
		}
		if diff := cmp.Diff(m, v[0]); diff != "" {
			t.Fatalf("Select()[0]=%+v expected %+v, diff: %s", v[0], m, diff)
		}
	})
}

func TestIterxNotFound(t *testing.T) {
	session := CreateSession(t)
	defer session.Close()

	if err := session.ExecStmt(`CREATE TABLE gocqlx_test.not_found_table (testtext text PRIMARY KEY)`); err != nil {
		t.Fatal("create table:", err)
	}

	type NotFoundTable struct {
		Testtext string
	}

	t.Run("get cql error", func(t *testing.T) {
		var v NotFoundTable
		err := session.Query(`SELECT * FROM not_found_table WRONG`, nil).RetryPolicy(nil).Get(&v)
		if err == nil || !strings.Contains(err.Error(), "WRONG") {
			t.Fatalf("Get() error=%q", err)
		}
	})

	t.Run("get", func(t *testing.T) {
		var v NotFoundTable
		err := session.Query(`SELECT * FROM not_found_table`, nil).Get(&v)
		if err != gocql.ErrNotFound {
			t.Fatalf("Get() error=%q expected %s", err, gocql.ErrNotFound)
		}
	})

	t.Run("select cql error", func(t *testing.T) {
		var v []NotFoundTable
		err := session.Query(`SELECT * FROM not_found_table WRONG`, nil).RetryPolicy(nil).Select(&v)
		if err == nil || !strings.Contains(err.Error(), "WRONG") {
			t.Fatalf("Get() error=%q", err)
		}
	})

	t.Run("select", func(t *testing.T) {
		var v []NotFoundTable
		err := session.Query(`SELECT * FROM not_found_table`, nil).Select(&v)
		if err != nil {
			t.Fatalf("Select() error=%q expected %s", err, gocql.ErrNotFound)
		}
		if cap(v) > 0 {
			t.Fatalf("Select() effect alloc cap=%d expected 0", cap(v))
		}
	})
}

func TestIterxErrorOnNil(t *testing.T) {
	session := CreateSession(t)
	defer session.Close()

	if err := session.ExecStmt(`CREATE TABLE gocqlx_test.nil_table (testtext text PRIMARY KEY)`); err != nil {
		t.Fatal("create table:", err)
	}

	const (
		stmt   = "SELECT * FROM not_found_table WRONG"
		golden = "expected a pointer but got <nil>"
	)

	t.Run("get", func(t *testing.T) {
		err := session.Query(stmt, nil).Get(nil)
		if err == nil || err.Error() != golden {
			t.Fatalf("Get() error=%q expected %q", err, golden)
		}
	})
	t.Run("select", func(t *testing.T) {
		err := session.Query(stmt, nil).Select(nil)
		if err == nil || err.Error() != golden {
			t.Fatalf("Select() error=%q expected %q", err, golden)
		}
	})
	t.Run("struct scan", func(t *testing.T) {
		iter := session.Query(stmt, nil).Iter()
		iter.StructScan(nil)
		err := iter.Close()
		if err == nil || err.Error() != golden {
			t.Fatalf("StructScan() error=%q expected %q", err, golden)
		}
	})
}

func TestIterxPaging(t *testing.T) {
	session := CreateSession(t)
	defer session.Close()

	if err := session.ExecStmt(`CREATE TABLE gocqlx_test.paging_table (id int PRIMARY KEY, val int)`); err != nil {
		t.Fatal("create table:", err)
	}
	if err := session.ExecStmt(`CREATE INDEX id_val_index ON gocqlx_test.paging_table (val)`); err != nil {
		t.Fatal("create index:", err)
	}

	q := session.Query(qb.Insert("gocqlx_test.paging_table").Columns("id", "val").ToCql())
	for i := 0; i < 5000; i++ {
		if err := q.Bind(i, i).Exec(); err != nil {
			t.Fatal(err)
		}
	}

	type Paging struct {
		ID  int
		Val int
	}

	stmt, names := qb.Select("gocqlx_test.paging_table").
		Where(qb.Lt("val")).
		AllowFiltering().
		Columns("id", "val").ToCql()
	iter := session.Query(stmt, names).Bind(100).PageSize(10).Iter()
	defer iter.Close()

	var cnt int
	for {
		p := &Paging{}
		if !iter.StructScan(p) {
			break
		}
		cnt++
	}
	if cnt != 100 {
		t.Fatal("expected 100", "got", cnt)
	}
}

func TestIterxCAS(t *testing.T) {
	session := CreateSession(t)
	defer session.Close()

	const (
		id         = 0
		baseSalary = 1000
		minSalary  = 2000
	)

	john := struct {
		ID     int
		Salary int
	}{ID: id, Salary: baseSalary}

	if err := session.ExecStmt(`CREATE TABLE gocqlx_test.cas_table (id int PRIMARY KEY, salary int)`); err != nil {
		t.Fatal("create table:", err)
	}

	insert := session.Query(qb.Insert("cas_table").Columns("id", "salary").Unique().ToCql())

	applied, err := insert.BindStruct(john).ExecCAS()
	if err != nil {
		t.Fatal("ExecCAS() failed:", err)
	}
	if !applied {
		t.Error("ExecCAS() expected first insert success")
	}

	applied, err = insert.BindStruct(john).ExecCAS()
	if err != nil {
		t.Fatal("ExecCAS() failed:", err)
	}
	if applied {
		t.Error("ExecCAS() Expected second insert to not be applied")
	}

	update := session.Query(qb.Update("cas_table").
		SetNamed("salary", "min_salary").
		Where(qb.Eq("id")).
		If(qb.LtNamed("salary", "min_salary")).
		ToCql(),
	)

	applied, err = update.BindStructMap(john, qb.M{"min_salary": minSalary}).GetCAS(&john)
	if err != nil {
		t.Fatal("GetCAS() failed:", err)
	}
	if !applied {
		t.Error("GetCAS() expected update to be applied")
	}
	if john.Salary != baseSalary {
		t.Error("GetCAS()=%=v expected to have pre-image", john)
	}

	applied, err = update.BindStructMap(john, qb.M{"min_salary": minSalary * 2}).GetCAS(&john)
	if err != nil {
		t.Fatal(err)
	}
	if !applied {
		t.Error("GetCAS() expected update to be applied")
	}
	if john.Salary != minSalary {
		t.Error("GetCAS()=%=v expected to have pre-image", john)
	}
}
