package structures

import (
	"reflect"
	"strings"
)

func GetDbFieldNames(input interface{}) (db_fields []string) {

	dest_value := reflect.ValueOf(input)
	dest_type := reflect.TypeOf(input)

	for index := 0; index < dest_type.NumField(); index++ {

		if v := dest_value.Field(index); v.Kind() == reflect.Struct {
			db_fields = append(db_fields, GetDbFieldNames(v.Interface())...)
			continue
		}

		dest_field := dest_type.Field(index)
		db_tag := dest_field.Tag.Get("db")
		if strings.Contains(db_tag, ",") {
			db_tag = strings.Split(db_tag, ",")[0]
		}

		if len(db_tag) > 0 {
			db_fields = append(db_fields, db_tag)
			continue
		}

		json_tag := dest_field.Tag.Get("json")
		if strings.Contains(json_tag, ",") {
			json_tag = strings.Split(json_tag, ",")[0]
		}

		if len(json_tag) > 0 {
			db_fields = append(db_fields, json_tag)
			continue
		}

		db_fields = append(db_fields, strings.ToLower(dest_field.Name))
	}

	return
}