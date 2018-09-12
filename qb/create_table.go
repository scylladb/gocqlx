package qb



func (s *gocql.Session) CreateTable(name string, inter interface{}) {
	cql := fmt.Sprintf("create table %s", name)
	obj := reflect.ValueOf(inter)
	result := reflectx.Deref(obj.Type())
	count := result.NumField()
	for i := 0; i < count; i++ {
		field := result.Field(i)
		cql += fmt.Sprintf("\n%s %s", toSnakeCase(field.Name), databaseType(field.Type))
		if i == 0 {cql += " PRIMARYKEY"}
		if i < count - 1 {cql += ","}
	}
	cql += ";"
	s.Query(cql).Exec()
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
