package db



import (
	"errors"
	"fmt"
	"github.com/gocql/gocql"
	"github.com/scylladb/gocqlx/v2/entitybatch/sample"

	"github.com/scylladb/gocqlx/v2"
	"github.com/scylladb/gocqlx/v2/qb"
	"github.com/scylladb/gocqlx/v2/table"
)

type operationName string // The operationname should be literal the public db operation function name, such as "Create", "Update",...
type operation struct {
	stmt  string
	names []string
}

type ExecuteStatement interface {
	ExecuteStatement(stmt string) error
}

// Encapsulate batch operations(create, update, delete) of multiple entities/single entity.
type DbAccessBatch interface {
	Tables() []*table.Table
	Create(entities ...interface{}) error
	Update(entities ...interface{}) error
	Delete(entities ...interface{}) error
	ExecuteStatement
}

// In order to enable find_by, besides the main entity table, we also need to create some find_by tables.
// Encapsulate batch operations of a single entity and other read/find_by requests.
type DbAccessSingleEntity interface {
	DbAccessBatch
	UpdateContainsSubTablePrimaryKey(entityCre interface{}) error
	Read(entity interface{}) (interface{}, error)
	FindByPartKey(tbName string, entity interface{}) (interface{}, error)
	FindByPartKeyAndSortKey(tbName string, numSortCols int, entity interface{}) (interface{}, error)
	SelectAll() (interface{}, error)
}

// Encapsulate batch operations of multiple entities.
// For read/find_by requests, you need to implement them manually.
type DbAccessBatchMultiEntities interface {
	DbAccessBatch
}

type GenerateBatch func(entity ...interface{}) (batch interface{}) //  entity and batch are pointers

type dbAccessBatch struct {
	Cql_Session        *gocql.Session
	tables             []*table.Table
	prebuiltOperations map[operationName]operation
	generateBatch      GenerateBatch //  entity and batch are pointers
}

func (da *dbAccessBatch) setupTables(table_metadatas []table.Metadata) {
	da.tables = make([]*table.Table, len(table_metadatas))
	for i, tm := range table_metadatas {
		da.tables[i] = table.New(table.Metadata{
			Name:    tm.Name,
			Columns: tm.Columns,
			PartKey: tm.PartKey,
			SortKey: tm.SortKey,
		})
	}
}

// Prebuild CRUD operations.
func (da *dbAccessBatch) preBuildOperations() {
	da.prebuiltOperations = make(map[operationName]operation)

	// Create entities
	{
		batchBuilder := qb.Batch()
		for _, t := range da.tables {
			stmt, names := t.Insert()
			batchBuilder.AddStmtWithPrefix(t.Name(), stmt, names)
		}
		stmt, names := batchBuilder.ToCql()
		da.prebuiltOperations["Create"] = operation{stmt, names}
	}

	// Update entities
	{
		batchBuilder := qb.Batch()
		for _, t := range da.tables {
			stmt, names := t.Update(da.getFiledsForUpdateBatch(t)...)
			batchBuilder.AddStmtWithPrefix(t.Metadata().Name, stmt, names)
		}
		stmt, names := batchBuilder.ToCql()
		da.prebuiltOperations["Update"] = operation{stmt, names}
	}

	// Delete entities
	{
		batchBuilder := qb.Batch()
		for _, t := range da.tables {
			stmt, names := t.Delete()
			batchBuilder.AddStmtWithPrefix(t.Metadata().Name, stmt, names)
		}
		stmt, names := batchBuilder.ToCql()
		da.prebuiltOperations["Delete"] = operation{stmt, names}
	}
}

func (da *dbAccessBatch) getFiledsForUpdateBatch(t *table.Table) []string {
	columns := t.Metadata().Columns
	// primary columns (partition columns and sort columns) are not updatable.
	primarykeys := make(map[string]struct{})
	for _, partKey := range t.Metadata().PartKey {
		primarykeys[partKey] = struct{}{}
	}

	for _, sortkey := range t.Metadata().SortKey {
		primarykeys[sortkey] = struct{}{}
	}

	var result []string
	for _, col := range columns {
		if _, exist := primarykeys[col]; !exist {
			result = append(result, col)
		}
	}
	return result
}

func getPrimaryKeyCmp(table *table.Table) []qb.Cmp {
	if table == nil {
		return nil
	}
	PartKey := table.Metadata().PartKey
	SortKey := table.Metadata().SortKey

	primaryKeyCmp := make([]qb.Cmp, 0, len(PartKey)+len(SortKey))
	for _, k := range PartKey {
		primaryKeyCmp = append(primaryKeyCmp, qb.Eq(k))
	}
	for _, k := range SortKey {
		primaryKeyCmp = append(primaryKeyCmp, qb.Eq(k))
	}
	return primaryKeyCmp
}

func (da *dbAccessBatch) Tables() []*table.Table {
	return da.tables
}

// entity should be a pointer.
func (da *dbAccessBatch) Create(entities ...interface{}) error {
	operation, ok := da.prebuiltOperations["Create"]
	if !ok {
		return errors.New(fmt.Sprintf("The operation %s is not supported", "Create"))
	}
	stmt, names := operation.stmt, operation.names

	batch := da.generateBatch(entities...)
	q := gocqlx.Query(sample.Cql_Session.Query(stmt), names).BindStruct(batch)
	cql := q.String()

	if err := q.ExecRelease(); err != nil {
		errMsg := fmt.Sprintf("Query: %s, \nError: %s", cql, err.Error())
		return errors.New(errMsg)
	}
	return nil
}

// entity should be a pointer.
// The find_by columns are filtered out because they are the partition key of the find_by table.
// If you want to update find_by columns, please use UpdateFindByKeys.
func (da *dbAccessBatch) Update(entities ...interface{}) error {
	operation, ok := da.prebuiltOperations["Update"]
	if !ok {
		return errors.New(fmt.Sprintf("The operation %s is not supported", "Update"))
	}
	stmt, names := operation.stmt, operation.names

	batch := da.generateBatch(entities...)
	q := gocqlx.Query(sample.Cql_Session.Query(stmt), names).BindStruct(batch)
	cql := q.String()

	if err := q.ExecRelease(); err != nil {
		errMsg := fmt.Sprintf("Query: %s, \nError: %s", cql, err.Error())
		return errors.New(errMsg)
	}
	return nil
}

// entity should be a pointer.
func (da *dbAccessBatch) Delete(entities ...interface{}) error {
	operation, ok := da.prebuiltOperations["Delete"]
	if !ok {
		return errors.New(fmt.Sprintf("The operation %s is not supported", "Delete"))
	}
	stmt, names := operation.stmt, operation.names

	batch := da.generateBatch(entities...)
	q := gocqlx.Query(sample.Cql_Session.Query(stmt), names).BindStruct(batch)
	cql := q.String()

	if err := q.ExecRelease(); err != nil {
		errMsg := fmt.Sprintf("Query: %s, \nError: %s", cql, err.Error())
		return errors.New(errMsg)
	}
	return nil
}

func (da *dbAccessBatch) ExecuteStatement(stmt string) error {
	if len(stmt) == 0 {
		return errors.New("statement is required")
	}

	q := sample.Cql_Session.Query(stmt)
	defer q.Release()
	return q.Exec()
}
