package db

import (
	"github.com/scylladb/gocqlx/v2/table"
	"reflect"
	"strconv"
	"testing"
)

// Base table is people(primary key: id)
// Search table is people_by_age(primary key: age, id)
type people struct {
	Id string
	FirstName string
	LastName string
	Age int
}

const (
	base_table = "people"
	search_table = "people_by_age"
)

var (
	singleEntityBatch = &SingleEntityBatch{}
)

func init() {
	var metadatas []table.Metadata
	metadatas = append(metadatas, table.Metadata{
		Name:    base_table,
		Columns: []string{"id", "first_name", "last_name", "age"},
		PartKey: []string{"id"},
		SortKey: []string{"first_name", "age"},
	})
	metadatas = append(metadatas, table.Metadata{
		Name:    search_table,
		Columns: []string{"id", "first_name", "last_name", "age"},
		PartKey: []string{"age"},
		SortKey: []string{"id", "first_name"},
	})
	singleEntityBatch.SetupTables(metadatas)
}

func TestGenerateFindByPartKeyOperationName(t *testing.T) {
	type args struct {
		tableName string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "base_table",
			args: args{
				tableName: base_table,
			},
			want: FINDBYPARTKEY + base_table,
		},
		{
			name: "search_table",
			args: args{
				tableName: search_table,
			},
			want: FINDBYPARTKEY + search_table,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := GenerateFindByPartKeyOperationName(tt.args.tableName); got != tt.want {
				t.Errorf("GenerateFindByPartKeyOperationName() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGenerateFindByPrimaryKeysOperationName(t *testing.T) {
	type args struct {
		tableName   string
		numSortCols int
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "base_table_0",
			args: args{
				tableName:   base_table,
				numSortCols: 0,
			},
			want: FINDBYPARTKEYSANDSORTKEYS + base_table + "_" + strconv.Itoa(0),
		},
		{
			name: "base_table_1",
			args: args{
				tableName:   base_table,
				numSortCols: 1,
			},
			want: FINDBYPARTKEYSANDSORTKEYS + base_table + "_" + strconv.Itoa(1),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := GenerateFindByPartKeysAndSortKeysOperationName(tt.args.tableName, tt.args.numSortCols); got != tt.want {
				t.Errorf("GenerateFindByPartKeysAndSortKeysOperationName() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSingleEntityBatch_GenerateDeleteAndCreateOperation(t *testing.T) {
	expected := Operation{
		Stmt:  "BEGIN BATCH DELETE FROM people USING TIMESTAMP 1592734743192106 WHERE id=? AND first_name=? AND age=? ; DELETE FROM people_by_age USING TIMESTAMP 1592734743192115 WHERE age=? AND id=? AND first_name=? ; INSERT INTO people (id,first_name,last_name,age) VALUES (?,?,?,?) USING TIMESTAMP 1592734743192119 ; INSERT INTO people_by_age (id,first_name,last_name,age) VALUES (?,?,?,?) USING TIMESTAMP 1592734743192125 ; APPLY BATCH ",
		Names: []string{"people_old.id", "people_old.first_name", "people_old.age", "people_by_age_old.age", "people_by_age_old.id", "people_by_age_old.first_name", "people_new.id", "people_new.first_name", "people_new.last_name", "people_new.age", "people_by_age_new.id", "people_by_age_new.first_name", "people_by_age_new.last_name", "people_by_age_new.age"},
	}
	got := singleEntityBatch.GenerateDeleteAndCreateOperation()

	// TODO: Pass the two time stamps as parameters into make SingleEntityBatch.GenerateDeleteAndCreateOperation testable.
	// The stmt contains the latest time stamp, so it cannot be tested for now.
	// Stmt looks like:
	// BEGIN BATCH DELETE FROM people USING TIMESTAMP 1592735095355312 WHERE id=? AND first_name=? AND age=? ; DELETE FROM people_by_age USING TIMESTAMP 1592735095355321 WHERE age=? AND id=? AND first_name=? ; INSERT INTO people (id,first_name,last_name,age) VALUES (?,?,?,?) USING TIMESTAMP 1592735095355325 ; INSERT INTO people_by_age (id,first_name,last_name,age) VALUES (?,?,?,?) USING TIMESTAMP 1592735095355331 ; APPLY BATCH
	//if got.Stmt != expected.Stmt {
	//	t.Errorf("\n expected Stmt: %s, \n  but got Stmt: %s", expected.Stmt, got.Stmt)
	//}

	if !reflect.DeepEqual(got.Names, expected.Names) {
		t.Errorf("expected Names: %s, got Names: %s", expected.Names, got.Names)
	}
}

func TestSingleEntityBatch_PreBuildOperations(t *testing.T) {
	expected := make(map[string]Operation)

	expected["Create"] = Operation{
		Stmt:  "BEGIN BATCH INSERT INTO people (id,first_name,last_name,age) VALUES (?,?,?,?) ; INSERT INTO people_by_age (id,first_name,last_name,age) VALUES (?,?,?,?) ; APPLY BATCH ",
		Names: []string{"people.id", "people.first_name", "people.last_name", "people.age", "people_by_age.id", "people_by_age.first_name", "people_by_age.last_name", "people_by_age.age"},
	}

	expected["Read"] = Operation{
		Stmt:  "SELECT id,first_name,last_name,age FROM people WHERE id=? AND first_name=? AND age=? ",
		Names: []string{"id", "first_name", "age"},
	}

	expected["Update"] = Operation{
		Stmt:  "BEGIN BATCH UPDATE people SET last_name=? WHERE id=? AND first_name=? AND age=? ; UPDATE people_by_age SET last_name=? WHERE age=? AND id=? AND first_name=? ; APPLY BATCH ",
		Names: []string{"people.last_name", "people.id", "people.first_name", "people.age", "people_by_age.last_name", "people_by_age.age", "people_by_age.id", "people_by_age.first_name"},
	}

	expected["Delete"] = Operation{
		Stmt:  "BEGIN BATCH DELETE FROM people WHERE id=? AND first_name=? AND age=? ; DELETE FROM people_by_age WHERE age=? AND id=? AND first_name=? ; APPLY BATCH ",
		Names: []string{"people.id", "people.first_name", "people.age", "people_by_age.age", "people_by_age.id", "people_by_age.first_name"},
	}

	expected["FindByPartKeyAndSortKey_people_0"] = Operation{
		Stmt:  "SELECT id,first_name,last_name,age FROM people WHERE id=? ",
		Names: []string{"id"},
	}

	expected["FindByPartKeyAndSortKey_people_1"] = Operation{
		Stmt:  "SELECT id,first_name,last_name,age FROM people WHERE id=? AND first_name=? ",
		Names: []string{"id", "first_name"},
	}

	expected["FindByPartKeyAndSortKey_people_by_age_0"] = Operation{
		Stmt:  "SELECT id,first_name,last_name,age FROM people_by_age WHERE age=? ",
		Names: []string{"age"},
	}

	expected["FindByPartKeyAndSortKey_people_by_age_1"] = Operation{
		Stmt:  "SELECT id,first_name,last_name,age FROM people_by_age WHERE age=? AND id=? ",
		Names: []string{"age", "id"},
	}

	expected["FindByPartKey_people"] = Operation{
		Stmt:  "SELECT id,first_name,last_name,age FROM people WHERE id=? ",
		Names: []string{"id"},
	}

	expected["FindByPartKey_people_by_age"] = Operation{
		Stmt:  "SELECT id,first_name,last_name,age FROM people_by_age WHERE age=? ",
		Names: []string{"age"},
	}
	singleEntityBatch.PreBuildOperations()

	for k, v := range singleEntityBatch.PrebuiltOperations {
		if !reflect.DeepEqual(expected[k].Names, v.Names) {
			t.Errorf("\n expected: %+v, \n but got: %+v", expected[k].Names, v.Names)
		}

		if expected[k].Stmt != v.Stmt {
			t.Errorf("\n expected: %+v \n  but got: %+v", expected[k].Stmt, v.Stmt)
		}
	}
}

func Test_getFiledsForUpdateBatch(t *testing.T) {
	type args struct {
		t *table.Table
	}
	tests := []struct {
		name string
		args args
		want []string
	}{
		{
			name: "base_table",
			args: args{
				t: singleEntityBatch.tables[0],
			},
			want: []string{"last_name"},
		},
		{
			name: "search_table",
			args: args{
				t: singleEntityBatch.tables[1],
			},
			want: []string{"last_name"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := getFiledsForUpdateBatch(tt.args.t); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("getFiledsForUpdateBatch() = %v, want %v", got, tt.want)
			}
		})
	}
}