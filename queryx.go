package gocqlx

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"
	"strconv"

	"github.com/gocql/gocql"
	"github.com/jmoiron/sqlx/reflectx"
)

// CompileNamedQuery compiles a named query into an unbound query using the
// '?' bindvar and a list of names.
func CompileNamedQuery(qs []byte) (stmt string, names []string, err error) {
	// guess number of names
	n := bytes.Count(qs, []byte(":"))
	if n == 0 {
		return "", nil, errors.New("expected a named query")
	}
	names = make([]string, 0, n)
	rebound := make([]byte, 0, len(qs))

	inName := false
	last := len(qs) - 1
	name := make([]byte, 0, 10)

	for i, b := range qs {
		// a ':' while we're in a name is an error
		if b == ':' {
			// if this is the second ':' in a '::' escape sequence, append a ':'
			if inName && i > 0 && qs[i-1] == ':' {
				rebound = append(rebound, ':')
				inName = false
				continue
			} else if inName {
				err = errors.New("unexpected `:` while reading named param at " + strconv.Itoa(i))
				return stmt, names, err
			}
			inName = true
			name = []byte{}
			// if we're in a name, and this is an allowed character, continue
		} else if inName && (allowedBindRune(b) || b == '_' || b == '.') && i != last {
			// append the byte to the name if we are in a name and not on the last byte
			name = append(name, b)
			// if we're in a name and it's not an allowed character, the name is done
		} else if inName {
			inName = false
			// if this is the final byte of the string and it is part of the name, then
			// make sure to add it to the name
			if i == last && allowedBindRune(b) {
				name = append(name, b)
			}
			// add the string representation to the names list
			names = append(names, string(name))
			// add a proper bindvar for the bindType
			rebound = append(rebound, '?')
			// add this byte to string unless it was not part of the name
			if i != last {
				rebound = append(rebound, b)
			} else if !allowedBindRune(b) {
				rebound = append(rebound, b)
			}
		} else {
			// this is a normal byte and should just go onto the rebound query
			rebound = append(rebound, b)
		}
	}

	return string(rebound), names, err
}

func allowedBindRune(b byte) bool {
	return (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') || (b >= '0' && b <= '9')
}

// Queryx is a wrapper around gocql.Query which adds struct binding capabilities.
type Queryx struct {
	*gocql.Query
	Names  []string
	Mapper *reflectx.Mapper
}

// BindStruct binds query named parameters using mapper.
func (q Queryx) BindStruct(arg interface{}) error {
	m := q.Mapper
	if m == nil {
		m = DefaultMapper
	}

	arglist, err := bindStructArgs(q.Names, arg, m)
	if err != nil {
		return err
	}

	q.Bind(arglist...)

	return nil
}

func bindStructArgs(names []string, arg interface{}, m *reflectx.Mapper) ([]interface{}, error) {
	arglist := make([]interface{}, 0, len(names))

	// grab the indirected value of arg
	v := reflect.ValueOf(arg)
	for v = reflect.ValueOf(arg); v.Kind() == reflect.Ptr; {
		v = v.Elem()
	}

	fields := m.TraversalsByName(v.Type(), names)
	for i, t := range fields {
		if len(t) == 0 {
			return arglist, fmt.Errorf("could not find name %s in %#v", names[i], arg)
		}
		val := reflectx.FieldByIndexesReadOnly(v, t)
		arglist = append(arglist, val.Interface())
	}

	return arglist, nil
}

// BindMap binds query named parameters using map.
func (q Queryx) BindMap(arg map[string]interface{}) error {
	arglist, err := bindMapArgs(q.Names, arg)
	if err != nil {
		return err
	}

	q.Bind(arglist...)

	return nil
}

func bindMapArgs(names []string, arg map[string]interface{}) ([]interface{}, error) {
	arglist := make([]interface{}, 0, len(names))

	for _, name := range names {
		val, ok := arg[name]
		if !ok {
			return arglist, fmt.Errorf("could not find name %s in %#v", name, arg)
		}
		arglist = append(arglist, val)
	}
	return arglist, nil
}
