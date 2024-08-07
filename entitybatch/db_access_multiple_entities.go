package db

import "github.com/scylladb/gocqlx/v2/table"

type dbAccessMultipleEntities struct {
	dbAccessBatch
}

func CreateDbAccessMultipleEntities(table_metadatas []table.Metadata, generateBatch GenerateBatch) DbAccessBatchMultiEntities {
	if len(table_metadatas) == 0 {
		panic("Empty table_metadatas!")
	}
	da := &dbAccessMultipleEntities{}
	da.setupTables(table_metadatas)
	da.preBuildOperations()

	da.generateBatch = generateBatch

	return da
}
