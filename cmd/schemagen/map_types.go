package main

import (
	"fmt"
	"strconv"
	"strings"
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

func mapScyllaToGoType(s string) (string, error) {
	t, _, err := goTypeForScyllaType(s, true)
	return t, err
}

func goTypeForScyllaType(s string, allowTuple bool) (goType string, isComparable bool, err error) {
	s = strings.TrimSpace(s)

	t, exists := types[s]
	if exists {
		return t, isComparableGoType(t), nil
	}

	name, args, ok := splitTypeConstructor(s)
	if ok {
		if hasEmptyTypeArg(args) {
			return "", false, fmt.Errorf("unsupported CQL type %q", s)
		}

		switch name {
		case "frozen":
			if len(args) == 1 {
				return goTypeForScyllaType(args[0], false)
			}
		case "map":
			if len(args) == 2 {
				key, isComparable, err := goTypeForScyllaType(args[0], false)
				if err != nil {
					return "", false, err
				}
				if !isComparable {
					return "", false, fmt.Errorf("unsupported non-comparable CQL map key type %q", args[0])
				}
				value, _, err := goTypeForScyllaType(args[1], false)
				if err != nil {
					return "", false, err
				}
				return "map[" + key + "]" + value, false, nil
			}
		case "set", "list":
			if len(args) == 1 {
				t, _, err := goTypeForScyllaType(args[0], false)
				if err != nil {
					return "", false, err
				}
				return "[]" + t, false, nil
			}
		case "tuple":
			if !allowTuple {
				return "", false, unsupportedTupleElementError(s)
			}
			isComparable = true
			typeStr := "struct {\n"
			for i, t := range args {
				if _, _, ok := splitTypeConstructor(t); ok {
					return "", false, unsupportedTupleElementError(s)
				}
				goType, fieldComparable, err := goTypeForScyllaType(t, false)
				if err != nil {
					return "", false, err
				}
				if !fieldComparable {
					isComparable = false
				}
				typeStr += "\t\tField" + strconv.Itoa(i+1) + " " + goType + "\n"
			}
			typeStr += "\t}"

			return typeStr, isComparable, nil
		}

		return "", false, fmt.Errorf("unsupported CQL type %q", s)
	}

	// Bare identifiers are treated as UDT references here. This parser does not
	// have UDT metadata, so UDT field comparability is validated earlier by
	// validateMapKeyTypes before templates call mapScyllaToGoType.
	return camelize(s) + "UserType", true, nil
}

func unsupportedTupleElementError(s string) error {
	return fmt.Errorf("unsupported non-flat tuple element CQL type %q; see https://github.com/scylladb/gocqlx/issues/375", s)
}

func isComparableGoType(s string) bool {
	return !strings.HasPrefix(s, "[]") && s != "inf.Dec"
}

func splitTypeConstructor(s string) (name string, args []string, ok bool) {
	start := strings.IndexByte(s, '<')
	if start < 1 || !strings.HasSuffix(s, ">") {
		return "", nil, false
	}

	return s[:start], splitTypeArgs(s[start+1 : len(s)-1]), true
}

func splitTypeArgs(s string) []string {
	args := make([]string, 0, 2)
	start := 0
	depth := 0

	for i, r := range s {
		switch r {
		case '<':
			depth++
		case '>':
			depth--
		case ',':
			if depth == 0 {
				args = append(args, strings.TrimSpace(s[start:i]))
				start = i + 1
			}
		}
	}

	return append(args, strings.TrimSpace(s[start:]))
}

func hasEmptyTypeArg(args []string) bool {
	for _, arg := range args {
		if arg == "" {
			return true
		}
	}

	return false
}
