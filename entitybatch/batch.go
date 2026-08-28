package db

import (
	"github.com/scylladb/gocqlx/v2/qb"
	"github.com/scylladb/gocqlx/v2/table"
	"strconv"
	"time"
)

// The OperationName should be literal the public db Operation function name, such as "Create", "Read", "Update", "Delete".
// For those special operation(such as delte&create, please use the tool functions, which begins with Generate* in this package.
type Operation struct {
	Stmt  string
	Names []string
}

const (
	CREATE = "Create"
	UPDATE = "Update"
	DELETE = "Delete"
	READ = "Read"
	FINDBYPARTKEY = "FindByPartKey_"
	FINDBYPARTKEYSANDSORTKEYS = "FindByPartKeyAndSortKey_"
)

type SingleEntityBatch struct {
	tables             []*table.Table
	PrebuiltOperations map[string]Operation
}

func (this *SingleEntityBatch) SetupTables(table_metadatas []table.Metadata) {
	this.tables = make([]*table.Table, len(table_metadatas))
	for i, tm := range table_metadatas {
		this.tables[i] = table.New(table.Metadata{
			Name:    tm.Name,
			Columns: tm.Columns,
			PartKey: tm.PartKey,
			SortKey: tm.SortKey,
		})
	}
}

// Prebuild batch operations:
// Create, Update, Delete
// Read the entity by primary key columns from the BASE table.
func (this *SingleEntityBatch) PreBuildOperations() {
	this.PrebuiltOperations = make(map[string]Operation)

	// Create entities
	{
		batchBuilder := qb.Batch()
		for _, t := range this.tables {
			stmt, names := t.Insert()
			batchBuilder.AddStmtWithPrefix(t.Name(), stmt, names)
		}
		stmt, names := batchBuilder.ToCql()
		this.PrebuiltOperations[CREATE] = Operation{stmt, names}
	}

	// Update entities
	{
		batchBuilder := qb.Batch()
		for _, t := range this.tables {
			stmt, names := t.Update(getFiledsForUpdateBatch(t)...)
			batchBuilder.AddStmtWithPrefix(t.Metadata().Name, stmt, names)
		}
		stmt, names := batchBuilder.ToCql()
		this.PrebuiltOperations[UPDATE] = Operation{stmt, names}
	}

	// Delete entities
	{
		batchBuilder := qb.Batch()
		for _, t := range this.tables {
			stmt, names := t.Delete()
			batchBuilder.AddStmtWithPrefix(t.Metadata().Name, stmt, names)
		}
		stmt, names := batchBuilder.ToCql()
		this.PrebuiltOperations[DELETE] = Operation{stmt, names}
	}

	// Read the entity by primary key columns from the base table.
	{
		t := this.tables[0]
		stmt, names := t.Get(t.Metadata().Columns...)
		this.PrebuiltOperations[READ] = Operation{stmt, names}
	}

	//Find entities from the find_by tables by partition keys.
	{
		for _, t := range this.tables {
			stmt, names := t.Select(t.Metadata().Columns...)
			this.PrebuiltOperations[GenerateFindByPartKeyOperationName(t.Metadata().Name)] = Operation{stmt, names}
		}
	}

	//Find entities from the find_by tables by partition keys and some of the sort keys.
	{
		for _, t := range this.tables {
			for i := 0; i < len(t.Metadata().SortKey); i++ {
				stmt, names := qb.Select(t.Metadata().Name).
					Columns(t.Metadata().Columns...).
					Where(t.PrimaryKeyCmp()[0 : i+1]...).
					ToCql()
				this.PrebuiltOperations[GenerateFindByPartKeysAndSortKeysOperationName(t.Metadata().Name, i)] = Operation{stmt, names}
			}
		}
	}
}

// Filters out search tables' primary keys because they cannot appear in the update batch's cql statements.
func getFiledsForUpdateBatch(t *table.Table) []string {
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

func (this *SingleEntityBatch) Tables() []*table.Table {
	return this.tables
}


// In order to update the partition key of the "find_by" tables, we need to delete the existing entity and create a new one in a batch cql request.
// The execution order of the statements in a batch doesn't depends on their order in the batch cql statements.
// So we need to add time stamps to the statements in the batch cql.
// DeleteAndCreateOperation CANNOT be pre-built because we need to use the latest time stamp.
func (this *SingleEntityBatch) GenerateDeleteAndCreateOperation() Operation {
	// deleteAndCreate
	{
		batchBuilder := qb.Batch()

		// Delete the existing product
		// We need the timestamp to order the cql statements execution in the batch.
		for _, t := range this.tables {
			primaryKeyCmp := t.PrimaryKeyCmp()
			stmt, names := qb.Delete(t.Metadata().Name).Columns().Where(primaryKeyCmp...).Timestamp(time.Now()).ToCql()
			batchBuilder.AddStmtWithPrefix(t.Metadata().Name+"_old", stmt, names)
		}

		// Create the updated(new) product
		for _, t := range this.tables {
			stmt, names := qb.Insert(t.Metadata().Name).Columns(t.Metadata().Columns...).Timestamp(time.Now()).ToCql()
			batchBuilder.AddStmtWithPrefix(t.Metadata().Name+"_new", stmt, names)
		}
		stmt, names := batchBuilder.ToCql()
		return Operation{stmt, names}
	}
}

func GenerateFindByPartKeyOperationName(tableName string) string {
	return FINDBYPARTKEY + tableName
}

func GenerateFindByPartKeysAndSortKeysOperationName(tableName string, numSortCols int) string {
	return FINDBYPARTKEYSANDSORTKEYS + tableName + "_" + strconv.Itoa(numSortCols)
}