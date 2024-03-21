package db

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/scylladb/gocqlx/v2"
	"github.com/scylladb/gocqlx/v2/entitybatch/sample"
	"github.com/scylladb/gocqlx/v2/qb"
	"github.com/scylladb/gocqlx/v2/table"
	"strconv"
	"time"
)

type GenerateEntity func() (entity interface{})                                               // entity should be a pointer
type GenerateEntities func() (entities interface{})                                           // entityies should be a pointer to a slice. The elements in the slice can a pointer, or not.
type GenerateBatch4CreateAndDelete func(entityDel, entityCre interface{}) (batch interface{}) //  entityDel, entityCre and batch are pointers

type dbAccessSingleEntity struct {
	dbAccessBatch
	generateEntity                GenerateEntity                // entity should be a pointer
	generateEntities              GenerateEntities              // entity should be a pointer
	generateBatch4CreateAndDelete GenerateBatch4CreateAndDelete //  entityDel, entityCre and batch are pointers
}

// tables[0] must be the base table. Other elements are the so called "find_by"/sub tables.
func CreateDbAccessSingleEntity(table_metadatas []table.Metadata, generateEntity GenerateEntity, generateEntities GenerateEntities, generateBatch GenerateBatch, generateBatch4CreateAndDelete GenerateBatch4CreateAndDelete) DbAccessSingleEntity {
	if len(table_metadatas) == 0 {
		panic("Empty table_metadatas!")
	}
	da := &dbAccessSingleEntity{}
	da.setupTables(table_metadatas)
	da.preBuildOperations()

	da.generateEntity = generateEntity
	da.generateEntities = generateEntities
	da.generateBatch = generateBatch
	da.generateBatch4CreateAndDelete = generateBatch4CreateAndDelete

	return da
}

// Prebuild CRUD operations.
func (da *dbAccessSingleEntity) preBuildOperations() {
	da.dbAccessBatch.preBuildOperations()

	// We need to reset update operation using dbAccessSingleEntity's own implementation.
	// We filter out the "find_by" columns because they are the partition key of the find_by table.
	// If you want to update the "find_by" columns, please use UpdateFindByKeys.
	{
		batchBuilder := qb.Batch()
		for _, t := range da.tables {
			stmt, names := t.Update(da.getFiledsForUpdateBatch(t.Metadata().Columns)...)
			batchBuilder.AddStmtWithPrefix(t.Metadata().Name, stmt, names)
		}
		stmt, names := batchBuilder.ToCql()
		da.prebuiltOperations["Update"] = operation{stmt, names}
	}

	// Read the entity by primary key columns from the base table.
	{
		t := da.tables[0]
		stmt, names := t.Get(t.Metadata().Columns...)
		da.prebuiltOperations["Read"] = operation{stmt, names}
	}

	//Find entities from the find_by tables by partition keys.
	{
		for _, t := range da.tables {
			stmt, names := t.Select(t.Metadata().Columns...)
			da.prebuiltOperations[generateFindByPartKeyOperationName(t.Metadata().Name)] = operation{stmt, names}
		}
	}

	//Find entities from the find_by tables by partition keys and sort keys.
	{
		for _, t := range da.tables {
			for i := 0; i < len(t.Metadata().SortKey); i++ {
				stmt, names := qb.Select(t.Metadata().Name).
					Columns(t.Metadata().Columns...).
					Where(getPrimaryKeyCmp(t)[0 : i+1]...).
					ToCql()
				da.prebuiltOperations[generateFindByPartKeyAndSortKeyOperationName(t.Metadata().Name, i)] = operation{stmt, names}
			}
		}
	}
}

// In order to update the partition key of the "find_by" tables, we need to delete the existing entity and create a new one in a batch cql request.
// The execution order of the statements in a batch doesn't depends on their order in the batch cql statements.
// So we need to add time stamps to the statements in the batch cql.
// DeleteAndCreateOperation cannot be pre-built because we need to use the newest time stamp.
func (da *dbAccessSingleEntity) generateDeleteAndCreateOperation() operation {
	// deleteAndCreate
	{
		batchBuilder := qb.Batch()

		// Delete the existing product
		// We need the timestamp to order the cql statements execution in the batch.
		for _, t := range da.tables {
			primaryKeyCmp := getPrimaryKeyCmp(t)
			stmt, names := qb.Delete(t.Metadata().Name).Columns().Where(primaryKeyCmp...).Timestamp(time.Now()).ToCql()
			batchBuilder.AddStmtWithPrefix(t.Metadata().Name+"_old", stmt, names)
		}

		// Create the updated(new) product
		for _, t := range da.tables {
			stmt, names := qb.Insert(t.Metadata().Name).Columns(t.Metadata().Columns...).Timestamp(time.Now()).ToCql()
			batchBuilder.AddStmtWithPrefix(t.Metadata().Name+"_new", stmt, names)
		}
		stmt, names := batchBuilder.ToCql()
		return operation{stmt, names}
	}
}

func (da *dbAccessSingleEntity) getFiledsForUpdateBatch(columns []string) []string {
	// primary columns (partition columns and sort columns) are not updatable.
	primarykeys := make(map[string]struct{})
	for _, t := range da.tables {
		for _, partKey := range t.Metadata().PartKey {
			primarykeys[partKey] = struct{}{}
		}

		for _, sortkey := range t.Metadata().SortKey {
			primarykeys[sortkey] = struct{}{}
		}
	}

	var result []string
	for _, col := range columns {
		if _, exist := primarykeys[col]; !exist {
			result = append(result, col)
		}
	}
	return result
}

func generateFindByPartKeyOperationName(tableName string) operationName {
	return operationName("FindByPartKey_" + tableName)
}

func generateFindByPartKeyAndSortKeyOperationName(tableName string, numSortCols int) operationName {
	return operationName("FindByPartKeyAndSortKey_" + tableName + "_" + strconv.Itoa(numSortCols))
}

// entity should be a pointer.
// The returned interface{} is a pointer.
func (da *dbAccessSingleEntity) Read(entity interface{}) (interface{}, error) {
	operation, ok := da.prebuiltOperations["Read"]
	if !ok {
		return nil, errors.New(fmt.Sprintf("The operation %s is not supported", "Read"))
	}
	stmt, names := operation.stmt, operation.names
	q := gocqlx.Query(sample.Cql_Session.Query(stmt), names).BindStruct(entity)
	cql := q.String()

	result := da.generateEntity()
	if err := q.GetRelease(result); err != nil {
		errMsg := fmt.Sprintf("Query: %s, \nError: %s", cql, err.Error())
		return nil, errors.New(errMsg)
	}
	return result, nil
}

// entity should be a pointer.
// empty tbName means the base table name.
// The returned entities is an pointer to the entity array.
func (da *dbAccessSingleEntity) FindByPartKey(tbName string, entity interface{}) (interface{}, error) {
	operation, ok := da.prebuiltOperations[generateFindByPartKeyOperationName(tbName)]
	if !ok {
		return nil, errors.New(fmt.Sprintf("The operation %s is not supported", generateFindByPartKeyOperationName(tbName)))
	}

	stmt, names := operation.stmt, operation.names
	q := gocqlx.Query(sample.Cql_Session.Query(stmt), names).BindStruct(entity)
	cql := q.String()

	entities := da.generateEntities()
	if err := q.SelectRelease(entities); err != nil {
		errMsg := fmt.Sprintf("Query: %s, \nError: %s", cql, err.Error())
		return nil, errors.New(errMsg)
	}
	return entities, nil
}

// entity should be a pointer.
// empty tbName means the base table name.
// The returned entities is an pointer to the entity array.
func (da *dbAccessSingleEntity) FindByPartKeyAndSortKey(tbName string, numSortCols int, entity interface{}) (interface{}, error) {
	operation, ok := da.prebuiltOperations[generateFindByPartKeyAndSortKeyOperationName(tbName, numSortCols)]
	if !ok {
		return nil, errors.New(fmt.Sprintf("The operation %s is not supported", generateFindByPartKeyOperationName(tbName)))
	}

	stmt, names := operation.stmt, operation.names
	q := gocqlx.Query(sample.Cql_Session.Query(stmt), names).BindStruct(entity)
	cql := q.String()

	entities := da.generateEntities()
	if err := q.SelectRelease(entities); err != nil {
		errMsg := fmt.Sprintf("Query: %s, \nError: %s", cql, err.Error())
		return nil, errors.New(errMsg)
	}
	return entities, nil
}

// entityDel, entityCre should be pointers.
func (da *dbAccessSingleEntity) deleteAndCreate(entityDel, entityCre interface{}) error {
	operation := da.generateDeleteAndCreateOperation()
	stmt, names := operation.stmt, operation.names

	batch := da.generateBatch4CreateAndDelete(entityDel, entityCre)
	q := gocqlx.Query(sample.Cql_Session.Query(stmt), names).BindStruct(batch)
	cql := q.String()

	if err := q.ExecRelease(); err != nil {
		errMsg := fmt.Sprintf("Query: %s, \nError: %s", cql, err.Error())
		return errors.New(errMsg)
	}
	return nil
}

// entityCre should be pointers.
// The "find_by" columns are the partition keys of the "find_by" tables. So they cannot be updated directly.
// We need to find the existing old entity first, then delete the old and create the new one in a batch cql.
func (da *dbAccessSingleEntity) UpdateContainsSubTablePrimaryKey(entityCre interface{}) error {
	entityDel, err := da.Read(entityCre)
	if entityDel == nil {
		entityCreBytes, _ := json.Marshal(entityCre)
		msg := fmt.Sprintf("Failed to find the entity: %s. ", entityCreBytes)
		if err != nil {
			msg += "\n error: " + err.Error()
		}
		return errors.New(msg)
	}

	err = da.deleteAndCreate(entityDel, entityCre)
	return err
}

func (da *dbAccessSingleEntity) SelectAll() (interface{}, error) {
	stmt, names := qb.Select(da.tables[0].Name()).Columns(da.tables[0].Metadata().Columns...).ToCql()
	q := gocqlx.Query(sample.Cql_Session.Query(stmt), names)

	entities := da.generateEntities()
	if err := q.SelectRelease(entities); err != nil {
		return nil, err
	}
	return entities, nil
}
