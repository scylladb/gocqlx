package singleEntity

import (
	"encoding/json"
	"github.com/scylladb/gocqlx/v2/entitybatch/sample"
	"os"
	"strings"
	"testing"
)

var (
	dropTable = `drop table if exists single_entity;`
	crateTable = `
CREATE TABLE single_entity (
    id text,
    name text,
    alias text,
    age text,
    hobby text,
    PRIMARY KEY ((id), name, alias)
);
`

	dropTableByName = `drop table if exists single_entity_by_name;`
	crateTableByName = `
CREATE TABLE single_entity_by_name (
    id text,
    name text,
    alias text,
    age text,
    hobby text,
    PRIMARY KEY ((name), id, alias)
);
`
	dropTableByAge = `drop table if exists single_entity_by_age;`
	crateTableByAge = `
CREATE TABLE single_entity_by_age (
    id text,
    name text,
    alias text,
    age text,
    hobby text,
    PRIMARY KEY ((age), id, alias)
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
	if err := sample.Execute_Statement(dropTable); err != nil {
		return err
	}
	if err := sample.Execute_Statement(dropTableByName); err != nil {
		return err
	}
	if err := sample.Execute_Statement(dropTableByAge); err != nil {
		return err
	}

	return nil
}

func createTables() error {
	if err := sample.Execute_Statement(crateTable); err != nil {
		return err
	}
	if err := sample.Execute_Statement(crateTableByName); err != nil {
		return err
	}
	if err := sample.Execute_Statement(crateTableByAge); err != nil {
		return err
	}

	return nil
}

func TeardownTestEnv() {
	dropTables()
}

func Test_CRUD(t *testing.T) {
	singleEntityCreate := T_SingleEntity{
		Id:   "id1",
		Name:	"name1",
		Alias: "alias1",
		Age:  "age1",
		Hobby: "hobby1",
	}
	if err := DataAccess.Create(&singleEntityCreate); err != nil {
		t.Fatal(err.Error())
	}

	singleEntityUpdate := singleEntityCreate
	singleEntityUpdate.Hobby += "new"
	err := DataAccess.Update(&singleEntityUpdate)
	if err != nil {
		t.Fatal(err.Error())
	}
	singleEntity_update_json, _ := json.Marshal(singleEntityUpdate)

	tmp := T_SingleEntity{
		Id:   "id1",
		Name: "name1",
		Alias: "alias1",
		Age:  "",
	}
	singleEntity_read, err := DataAccess.Read(&tmp)
	if err != nil {
		t.Fatal(err.Error())
	}
	singleEntity_read_json, _ := json.Marshal(singleEntity_read)

	if string(singleEntity_update_json) != string(singleEntity_read_json) {
		t.Fatal("singleEntity_read_json: ", string(singleEntity_read_json), "\n", "singleEntity_update_json: ", string(singleEntity_update_json))
	}

	if err := DataAccess.Delete(singleEntity_read); err != nil {
		t.Fatal(err.Error())
	}

	tmp = T_SingleEntity{
		Id:   "id1",
		Name: "name1",
		Alias: "alias1",
		Age:  "",
	}
	singleEntity_read_after_delete, err := DataAccess.Read(&tmp)
	if err != nil {
		if !strings.Contains(err.Error(), "not found") {
			t.Fatal(err.Error())
		}
	}
	if singleEntity_read_after_delete != nil {
		t.Fatal()
	}
}

/* The incorrect Update usage: update a field which is a partition key of a search/sub table.
For example, age is the partition key of single_entity_by_age, if you use Update to update it, the result is:
The existing single_entity row doesn't change. There is a new row created in single_entity_by_age table.
This is because, the filed age is filtered out when constructing the statement by the function below:
func (da *dbAccessBatch) getFiledsForUpdateBatch(t *table.Table) []string
The statement looks like below:

BEGIN BATCH
UPDATE single_entity SET hobby=? WHERE id=? AND name=? AND alias=? ;
UPDATE single_entity_by_name SET hobby=? WHERE name=? AND id=? AND alias=? ;
UPDATE single_entity_by_age SET hobby=? WHERE age=? AND id=? AND alias=? ;
APPLY BATCH

after the data is bound, it's
BEGIN BATCH
UPDATE single_entity SET hobby='hobby1' WHERE id='id1' AND name='name1' AND alias='alias1' ;
UPDATE single_entity_by_name SET hobby='hobby1' WHERE id='id1' AND name='name1' AND alias='alias1';
UPDATE single_entity_by_age SET hobby='hobby1' WHERE age='newage1' AND id='id1' AND alias='alias1';
APPLY BATCH

So the first two cqls don't change anything, the 3rd cql (UPDATE single_entity_by_age SET ... WHERE age='newage1') creates a new row.
*/
func Test_IncorrectUpdate(t *testing.T) {
	singleEntityCreate := T_SingleEntity{
		Id:   "id1",
		Name:	"name1",
		Alias: "alias1",
		Age:  "age1",
		Hobby: "hobby1",
	}
	if err := DataAccess.Create(&singleEntityCreate); err != nil {
		t.Fatal(err.Error())
	}

	singleEntityUpdate := singleEntityCreate
	singleEntityUpdate.Age += "new"
	err := DataAccess.Update(&singleEntityUpdate)
	if err != nil {
		t.Fatal(err.Error())
	}

	tmp := T_SingleEntity{
		Id:   "id1",
		Name: "name1",
		Alias: "alias1",
		Age:  "",
	}
	singleEntity_read, err := DataAccess.Read(&tmp)
	if err != nil {
		t.Fatal(err.Error())
	}

	if singleEntity_read.(*T_SingleEntity).Age != singleEntityCreate.Age {
		t.Errorf("The Update should not be able to change the search table's partition key!")
	}

	/* TODO: Add check for the unexpectedly created row.
	This incorrect usage of Update creates another ne row in single_entity_by_age
	 age     | id  | alias  | hobby  | name
	---------+-----+--------+--------+-------
	    age1 | id1 | alias1 | hobby1 | name1
	 age1new | id1 | alias1 | hobby1 |  null
	*/
}

func Test_FindByPartKey(t *testing.T) {
	singleEntityCreate := T_SingleEntity{
		Id:   "id1",
		Name: "name1",
		Alias: "alias1",
		Age:  "age1",
	}
	if err := DataAccess.Create(&singleEntityCreate); err != nil {
		t.Fatal(err.Error())
	}

	singleEntityCreate_read_json, _ := json.Marshal(singleEntityCreate)

	searchInput := T_SingleEntity{
		Name:   "name1",
	}
	singleEntity_read, err := DataAccess.FindByPartKey(SINGLE_ENTITY_BY_NAME, &searchInput)
	if err != nil {
		t.Fatal(err.Error())
	}
	singleEntity_read_json, _ := json.Marshal((*(singleEntity_read.(*[]T_SingleEntity)))[0])

	if string(singleEntityCreate_read_json) != string(singleEntity_read_json) {
		t.Fatal("singleEntity_read_json: ", string(singleEntity_read_json), "\n", "singleEntityCreate_read_json: ", string(singleEntityCreate_read_json))
	}
}

func Test_FindByPartKeyAndSortKey(t *testing.T) {
	singleEntityCreate := T_SingleEntity{
		Id:   "id1",
		Name: "name1",
		Alias: "alias1",
		Age:  "age1",
	}
	if err := DataAccess.Create(&singleEntityCreate); err != nil {
		t.Fatal(err.Error())
	}

	singleEntityCreate_read_json, _ := json.Marshal(singleEntityCreate)

	searchInput := T_SingleEntity{
		Name:   "name1",
		Id: "id1",
	}
	// We have 2 sort columns in all. As a demo, we use only one of them (the first one)
	singleEntity_read, err := DataAccess.FindByPartKeyAndSortKey(SINGLE_ENTITY_BY_NAME, 1, &searchInput)
	if err != nil {
		t.Fatal(err.Error())
	}
	singleEntity_read_json, _ := json.Marshal((*(singleEntity_read.(*[]T_SingleEntity)))[0])

	if string(singleEntityCreate_read_json) != string(singleEntity_read_json) {
		t.Fatal("singleEntity_read_json: ", string(singleEntity_read_json), "\n", "singleEntityCreate_read_json: ", string(singleEntityCreate_read_json))
	}
}

// In order to update the partition key of the search/sub/find_by table, we need to use UpdateContainsSubTablePrimaryKey.
// It deletes the existing entity and then create the new one.
func Test_UpdateContainsSubTablePrimaryKey(t *testing.T) {
	singleEntityCreate := T_SingleEntity{
		Id:   "id1",
		Name: "name1",
		Alias: "alias1",
		Age:  "age1",
	}
	if err := DataAccess.Create(&singleEntityCreate); err != nil {
		t.Fatal(err.Error())
	}

	singleEntityUpdate := singleEntityCreate
	singleEntityUpdate.Age += "new"
	err := DataAccess.UpdateContainsSubTablePrimaryKey(&singleEntityUpdate)
	if err != nil {
		t.Fatal(err.Error())
	}
	singleEntity_update_json, _ := json.Marshal(singleEntityUpdate)

	searchInput := &T_SingleEntity{
		Name:   "name1",
		Id: "id1",
		Alias: "alias1",
	}

	// We have 2 sort columns in all. As a demo, we use only one of them (the first one)
	singleEntity_read, err := DataAccess.Read(&searchInput)
	if err != nil {
		t.Fatal(err.Error())
	}
	singleEntity_read_json, _ := json.Marshal(singleEntity_read.(*T_SingleEntity))

	if string(singleEntity_update_json) != string(singleEntity_read_json) {
		t.Fatal("singleEntity_read_json: ", string(singleEntity_read_json), "\n", "singleEntity_update_json: ", string(singleEntity_update_json))
	}
}