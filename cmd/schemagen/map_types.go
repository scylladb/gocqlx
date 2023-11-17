package main

import (
	"fmt"
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

type tokenStyle int

const (
	FrozenToken tokenStyle = iota
	MapToken
	SetToken
	ListToken
	TupleToken
	CommaToken
	AnchorToken
)

type token struct {
	style tokenStyle
	count int
}

func (ts tokenStyle) String() string {
	switch ts {
	case FrozenToken:
		return "frozen"
	case MapToken:
		return "map"
	case SetToken:
		return "set"
	case ListToken:
		return "list"
	case TupleToken:
		return "tuple"
	case CommaToken:
		return "comma"
	case AnchorToken:
		return "anchor"
	default:
		return "unknown"
	}
}

func (ts tokenStyle) format(values ...string) (string, error) {
	l := len(values)
	switch ts {
	case FrozenToken:
		if l != 1 {
			return "", fmt.Errorf("Invalid values count=%d for %s", l, ts)
		}
		return values[0], nil
	case MapToken:
		if l != 2 {
			return "", fmt.Errorf("Invalid values count=%d for %s", l, ts)
		}
		return "map[" + values[0] + "]" + values[1], nil
	case SetToken:
		if l != 1 {
			return "", fmt.Errorf("Invalid values count=%d for %s", l, ts)
		}
		return "[]" + values[0], nil
	case ListToken:
		if l != 1 {
			return "", fmt.Errorf("Invalid values count=%d for %s", l, ts)
		}
		return "[]" + values[0], nil
	case TupleToken:
		if l == 0 {
			return "", fmt.Errorf("Invalid values count=%d for %s", l, ts)
		}
		tupleStr := "struct {\n"
		for i, v := range values {
			tupleStr = tupleStr + "\t\tField" + strconv.Itoa(i+1) + " " + v + "\n"
		}
		tupleStr = tupleStr + "\t}"
		return tupleStr, nil
	default:
		return "", fmt.Errorf("Invalid token type: %s", ts)
	}
}

func parseToken(s string) (*token, string) {
	regexps := make(map[tokenStyle]*regexp.Regexp)
	regexps[FrozenToken] = regexp.MustCompile(`^frozen<(.*)$`)
	regexps[MapToken] = regexp.MustCompile(`^map<(.*)$`)
	regexps[SetToken] = regexp.MustCompile(`^set<(.*)`)
	regexps[ListToken] = regexp.MustCompile(`^list<(.*)$`)
	regexps[TupleToken] = regexp.MustCompile(`^tuple<(.*)$`)
	regexps[CommaToken] = regexp.MustCompile(`^,(.*)$`)
	regexps[AnchorToken] = regexp.MustCompile(`^>(.*)$`)

	for tokenStyle, tokenRegexp := range regexps {
		match := tokenRegexp.FindStringSubmatch(s)
		if match != nil {
			return &token{tokenStyle, 0}, match[1]
		}
	}

	return nil, s
}

func parsePolishNotation(s string) (*Stack, error) {
	tokenStack := NewStack()
	notation := NewStack()
	left := s

	for {
		left = strings.TrimSpace(left)
		if len(left) == 0 {
			break
		}

		var t *token
		t, left = parseToken(left)
		if t != nil {
			switch t.style {
			case CommaToken:
				top, err := tokenStack.top()
				if err != nil {
					return nil, err
				}
				v, ok := top.(*token)
				if !ok {
					return nil, fmt.Errorf("Invalid type: %T", v)
				}
				v.count += 1
			case AnchorToken:
				prev, err := tokenStack.pop()
				if err != nil {
					return nil, err
				}
				v, ok := prev.(*token)
				if !ok {
					return nil, fmt.Errorf("Invalid type: %T", v)
				}
				v.count += 1
				notation.push(prev)
			default:
				tokenStack.push(t)
			}
		} else {
			itemRegex := regexp.MustCompile(`([^,>]+?)[,>]`)
			match := itemRegex.FindStringSubmatchIndex(left)
			if match != nil {
				item := strings.TrimSpace(left[match[2]:match[3]])
				notation.push(item)

				left = left[match[3]:]
			} else {
				notation.push(strings.TrimSpace(left))
				left = ""
			}
		}
	}

	for {
		t, err := tokenStack.pop()
		if err != nil {
			break
		}
		notation.push(t)
	}

	return notation, nil
}

func mapToGoType(s string) string {
	t, exists := types[s]
	if exists {
		return t
	}

	return camelize(s) + "UserType"
}

func calcPolishNotation(notation *Stack) (string, error) {
	outputStack := NewStack()
	for _, item := range notation.toSlice() {
		switch v := item.(type) {
		case string:
			outputStack.push(mapToGoType(v))
		case *token:
			datas, err := outputStack.popSlice(v.count)
			if err != nil {
				return "", err
			}
			var values []string
			for _, data := range datas {
				value, ok := data.(string)
				if !ok {
					return "", fmt.Errorf("Invalid output value: %v", value)
				}
				values = append(values, value)
			}
			fmtStr, err := v.style.format(values...)
			if err != nil {
				return "", err
			}
			outputStack.push(fmtStr)
		default:
			return "", fmt.Errorf("Invalid type: %T", v)
		}
	}

	if outputStack.count() != 1 {
		return "", fmt.Errorf("Invalid polish notation")
	}

	result, err := outputStack.pop()
	if err != nil {
		return "", nil
	}

	if resultStr, ok := result.(string); !ok {
		return "", fmt.Errorf("Invalid result value type: %T", result)
	} else {
		return resultStr, nil
	}
}

func mapScyllaToGoType(s string) string {
	notation, err := parsePolishNotation(s)
	if err != nil {
		panic(fmt.Sprintf("Failed to parse polish notation for %s: %v", s, err))
	}

	goTypeStr, err := calcPolishNotation(notation)
	if err != nil {
		panic(fmt.Sprintf("Failed to calculate polish notation: %v", err))
	}

	return goTypeStr
}

func typeToString(t interface{}) string {
	tType := fmt.Sprintf("%T", t)
	switch tType {
	case "gocql.NativeType":
		return t.(gocql.NativeType).String()
	case "gocql.CollectionType":
		collectionType := t.(gocql.CollectionType).String()
		collectionType = strings.Replace(collectionType, "(", "<", -1)
		collectionType = strings.Replace(collectionType, ")", ">", -1)
		return collectionType
	default:
		panic(fmt.Sprintf("Did not expect %v type in user defined type", tType))
	}
}
