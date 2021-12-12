package main

import (
	"regexp"
	"strconv"
	"strings"

	"github.com/gocql/gocql"
)

var types = map[string]string{
	"ascii":     "string",
	"bigint":    "int64",
	"blob":      "[]byte",
	"boolean":   "bool",
	"counter":   "int",
	"date":      "time.Time",
	"decimal":   "inf.Dec",
	"double":    "float64",
	"duration":  "gocql.Duration",
	"float":     "float32",
	"inet":      "string",
	"int":       "int32",
	"smallint":  "int16",
	"text":      "string",
	"time":      "time.Duration",
	"timestamp": "time.Time",
	"timeuuid":  "[16]byte",
	"tinyint":   "int8",
	"uuid":      "[16]byte",
	"varchar":   "string",
	"varint":    "int64",
}

func mapScyllaToGoType(s string) string {
	mapRegex := regexp.MustCompile(`map<([a-z]*), ([a-z]*)>`)
	setRegex := regexp.MustCompile(`set<([a-z]*)>`)
	listRegex := regexp.MustCompile(`list<([a-z]*)>`)
	tupleRegex := regexp.MustCompile(`tuple<(?:([a-z]*),? ?)*>`)
	match := mapRegex.FindAllStringSubmatch(s, -1)
	if match != nil {
		key := match[0][1]
		value := match[0][2]

		return "map[" + types[key] + "]" + types[value]
	}

	match = setRegex.FindAllStringSubmatch(s, -1)
	if match != nil {
		key := match[0][1]

		return "[]" + types[key]
	}

	match = listRegex.FindAllStringSubmatch(s, -1)
	if match != nil {
		key := match[0][1]

		return "[]" + types[key]
	}

	match = tupleRegex.FindAllStringSubmatch(s, -1)
	if match != nil {
		tuple := match[0][0]
		subStr := tuple[6 : len(tuple)-1]
		types := strings.Split(subStr, ", ")

		typeStr := "struct {\n"
		for i, t := range types {
			typeStr = typeStr + "\t\tField" + strconv.Itoa(i+1) + " " + mapScyllaToGoType(t) + "\n"
		}
		typeStr = typeStr + "\t}"

		return typeStr
	}

	t, exists := types[s]
	if exists {
		return t
	}

	return camelize(s) + "Type"
}

func getNativeTypeSting(t gocql.NativeType) string {
	return t.String()
}
