package qb

import (
	"../reflectx"
	"reflect"
	"regexp"
	"strings"
	"fmt"
)


type CreateTableBuilder struct {
	table   string
	columns interface{}
}

// CreateTable returns a new CreateTableBuilder with the given table name.
func CreateTable(table string) *CreateTableBuilder {
	return &CreateTableBuilder{
		table: table,
	}
}

func (b *CreateTableBuilder) Columns(columns interface{}) {
	b.columns = columns
}



// CreateTable builds a CQL query string
func (b *CreateTableBuilder) ToCql() string {
	table, columns := b.table, b.columns
	cql := fmt.Sprintf("CREATE TABLE %s(", table)
	obj := reflect.ValueOf(columns)
	result := reflectx.Deref(obj.Type())
	count := result.NumField()
	var primaries []string
	for i := 0; i < count; i++ {
		field := result.Field(i)
		cql += fmt.Sprintf("\n%s %s,", toSnakeCase(field.Name), databaseType(field.Type))
		if i == 0 {primaries = append(primaries, toSnakeCase(field.Name))}
	}
	cql += "\nPRIMARY KEY ("
	if len(primaries) > 1 {
		for i := 0; i < len(primaries); i++ {
			cql += primaries[i]
			if i < len(primaries) - 1 {cql += ","}
		}
		cql += ")"
	} else {
		cql += primaries[0] + ")"
	}
	cql += "\n);"
	return cql
}


var matchFirstCap = regexp.MustCompile("(.)([A-Z][a-z]+)")
var matchAllCap   = regexp.MustCompile("([a-z0-9])([A-Z])")

func toSnakeCase(str string) string {
	snake := matchFirstCap.ReplaceAllString(str, "${1}_${2}")
	snake  = matchAllCap.ReplaceAllString(snake, "${1}_${2}")
	return strings.ToLower(snake)
}

func databaseType(tp reflect.Type) string {
	if tp.String() == "int" {
		return "int"
	} else if tp.String() == "int64" {
		return "bigint"
	}
	return "text"
}
