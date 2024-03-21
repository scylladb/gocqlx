package singleEntity

import (
	"github.com/scylladb/gocqlx/v2/entitybatch"
	"github.com/scylladb/gocqlx/v2/entitybatch/structures"
	"github.com/scylladb/gocqlx/v2/table"
)

const (
	SINGLE_ENTITY = "single_entity"
	SINGLE_ENTITY_BY_NAME = "single_entity_by_name"
	SINGLE_ENTITY_BY_AGE = "single_entity_by_age"
)

type T_SingleEntity struct {
	Id string `json:"id"`
	Name string `json:"name"`
	Alias string `json:"alias"`
	Age string `json:"age"`
	Hobby string `json:"hobby"`
}

type batchSingleEntity struct {
	SingleEntity *T_SingleEntity
	SingleEntity_by_name *T_SingleEntity
	SingleEntity_by_age *T_SingleEntity
}

type batchSingleEntity4DelAndCreate struct {
	SingleEntity_old *T_SingleEntity
	SingleEntity_by_name_old *T_SingleEntity
	SingleEntity_by_age_old *T_SingleEntity

	SingleEntity_new *T_SingleEntity
	SingleEntity_by_name_new *T_SingleEntity
	SingleEntity_by_age_new *T_SingleEntity
}

var (
	DataAccess db.DbAccessSingleEntity
)

func init() {
	var metadatas []table.Metadata
	metadatas = append(metadatas, table.Metadata{
		Name:    SINGLE_ENTITY,
		Columns: structures.GetDbFieldNames(T_SingleEntity{}),
		PartKey: []string{"id"},
		SortKey: []string{"name", "alias"},
	})

	metadatas = append(metadatas, table.Metadata{
		Name:    SINGLE_ENTITY_BY_NAME,
		Columns: structures.GetDbFieldNames(T_SingleEntity{}),
		PartKey: []string{"name"},
		SortKey: []string{"id", "alias"},
	})

	metadatas = append(metadatas, table.Metadata{
		Name:    SINGLE_ENTITY_BY_AGE,
		Columns: structures.GetDbFieldNames(T_SingleEntity{}),
		PartKey: []string{"age"},
		SortKey: []string{"id", "alias"},
	})

	generateEntity := func() (entity interface{}) {
		return &T_SingleEntity{}
	}
	generateEntities := func() (entities interface{}) {
		return &[]T_SingleEntity{}
	}
	generateBatch := func(entities ...interface{}) interface{} {
		singleEntity, ok := entities[0].(*T_SingleEntity)
		if !ok {
			panic("The type of the entities[0] needs to be *T_SingleEntity")
		}
		return &batchSingleEntity{singleEntity, singleEntity, singleEntity}
	}
	generateBatch4CreateAndDelete := func(entityDel, entityCre interface{}) (batch interface{}) {
		producttypeDel, ok := entityDel.(*T_SingleEntity)
		if !ok {
			panic("The type of the entityDel needs to be *T_SingleEntity")
		}

		producttypeCre, ok := entityCre.(*T_SingleEntity)
		if !ok {
			panic("The type of the entityCre needs to be *T_SingleEntity")
		}

		return &batchSingleEntity4DelAndCreate{
			producttypeDel, producttypeDel, producttypeDel,
			producttypeCre, producttypeCre, producttypeCre,
		}
	}
	DataAccess = db.CreateDbAccessSingleEntity(metadatas, generateEntity, generateEntities, generateBatch, generateBatch4CreateAndDelete)
}
