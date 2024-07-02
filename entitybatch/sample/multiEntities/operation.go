package multiEntities

import (
	"github.com/scylladb/gocqlx/v2"
	"github.com/scylladb/gocqlx/v2/entitybatch"
	"github.com/scylladb/gocqlx/v2/entitybatch/sample"
	"github.com/scylladb/gocqlx/v2/entitybatch/structures"
	"github.com/scylladb/gocqlx/v2/qb"
	"github.com/scylladb/gocqlx/v2/table"
)

const (
	TABLE1 = "table1"
	TABLE2 = "table2"
)

type T_Table1 struct {
	Id string `json:"id"`
	Name string `json:"name"`
}

type T_Table2 struct {
	Id string `json:"id"`
	Name string `json:"name"`
}

type batchTables struct {
	Table1 *T_Table1
	Table2 *T_Table2
}

var (
	DataAccess db.DbAccessBatchMultiEntities
)

func init() {
	var metadatas []table.Metadata
	metadatas = append(metadatas, table.Metadata{
		Name:    TABLE1,
		Columns: structures.GetDbFieldNames(T_Table1{}),
		PartKey: []string{"id"},
		SortKey: []string{},
	})

	metadatas = append(metadatas, table.Metadata{
		Name:    TABLE2,
		Columns: structures.GetDbFieldNames(T_Table1{}),
		PartKey: []string{"id"},
		SortKey: []string{},
	})

	generateBatch := func(entities ...interface{}) interface{} {
		table1, ok := entities[0].(*T_Table1)
		if !ok {
			panic("The type of the entity needs to be *T_Table1")
		}
		table2, ok := entities[1].(*T_Table2)
		if !ok {
			panic("The type of the entity needs to be *T_Table2")
		}
		return &batchTables{table1, table2}
	}
	DataAccess = db.CreateDbAccessMultipleEntities(metadatas, generateBatch)
}

func SelectAll(tableIndex int) (interface{}, error) {
	table := DataAccess.Tables()[tableIndex]
	stmt, names := qb.Select(table.Name()).Columns(table.Metadata().Columns...).ToCql()
	q := gocqlx.Query(sample.Cql_Session.Query(stmt), names)

	generateEntities := func() (entities interface{}) {
		switch tableIndex {
		case 0:
			entities = &[]T_Table1{}
		case 1:
			entities = &[]T_Table2{}
		}
		return entities
	}
	entities := generateEntities()
	if err := q.SelectRelease(entities); err != nil {
		return nil, err
	}
	return entities, nil
}