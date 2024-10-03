package multiEntities

import (
	"github.com/scylladb/gocqlx/v2/entitybatch/sample"
	"os"
	"testing"
)

var (
	dropTable1         = `drop table if exists table1;`
	dropTable2         = `drop table if exists table2;`

	crateTable1 = `
CREATE TABLE IF NOT EXISTS table1 (
    id text,
    name text,
    PRIMARY KEY (id)
);
`
	crateTable2 = `
CREATE TABLE IF NOT EXISTS table2 (
    id text,
    name text,
    PRIMARY KEY (id)
);
`
)

func TestMain(m *testing.M) {
	// load config files, keys, etc
	if err := PrepareTestEnv(); err != nil {
		panic(err.Error())
	}

	exit_val := m.Run()

	// truncate tested tables, etc
	TeardownTestEnv()

	os.Exit(exit_val)
}

func PrepareTestEnv() error {
	if err := sample.Setup(); err != nil {
		return err
	}

	if err := dropTables(); err != nil {
		return err
	}

	if err := createTables(); err != nil {
		return err
	}

	return nil
}

func dropTables() error {
	if err := sample.Execute_Statement(dropTable1); err != nil {
		return err
	}

	if err := sample.Execute_Statement(dropTable2); err != nil {
		return err
	}

	return nil
}

func createTables() error {
	if err := sample.Execute_Statement(crateTable1); err != nil {
		return err
	}

	if err := sample.Execute_Statement(crateTable2); err != nil {
		return err
	}

	return nil
}

func TeardownTestEnv() {
	dropTables()
}

func Test_CUD(t *testing.T) {
	table1 := &T_Table1{"1", "name1"}
	table2 := &T_Table2{"2", "name2"}
	if err := DataAccess.Create(table1, table2); err != nil {
		t.Fatal(err.Error())
	}

	table1.Name = "new name1"
	table2.Name = "new name2"

	if err := DataAccess.Update(table1, table2); err != nil {
		t.Fatal(err.Error())
	}

	{
		// check update table1
		tmp, err := SelectAll(0)
		if err != nil {
			t.Fatal(err.Error())
		}
		table1rows := tmp.(*[]T_Table1)
		if table1rows == nil || len(*table1rows) != 1 {
			t.Fatal("No table1 rows returned.")
		}
		if (*table1rows)[0].Name != "new name1" {
			t.Errorf("Expected new table1.Name is new Name1, but got %s", (*table1rows)[0].Name)
		}

		// check update table2
		tmp, err = SelectAll(1)
		if err != nil {
			t.Fatal(err.Error())
		}
		table2rows := tmp.(*[]T_Table2)
		if table2rows == nil || len(*table2rows) != 1 {
			t.Fatal("No table1 rows returned.")
		}
		if (*table2rows)[0].Name != "new name2" {
			t.Errorf("Expected new table2.Name is new Name2, but got %s", (*table2rows)[0].Name)
		}
	}

	if err := DataAccess.Delete(table1, table2); err != nil {
		t.Fatal(err.Error())
	}

	{
		// check delete table1
		tmp, err := SelectAll(0)
		if err != nil {
			t.Fatal(err.Error())
		}
		table1rows := tmp.(*[]T_Table1)
		if table1rows != nil && len(*table1rows) != 0 {
			t.Fatal("table1 is not deleted.")
		}

		// check update table2
		tmp, err = SelectAll(1)
		if err != nil {
			t.Fatal(err.Error())
		}
		table2rows := tmp.(*[]T_Table2)
		if table2rows != nil && len(*table2rows) != 0  {
			t.Fatal("table2 is not deleted.")
		}
	}
}
