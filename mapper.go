// Copyright (C) 2017 ScyllaDB
// Use of this source code is governed by a ALv2-style
// license that can be found in the LICENSE file.

package gocqlx

import (
	"fmt"
	"unicode"

	"github.com/scylladb/gocqlx/reflectx"
)

// DefaultMapper uses `db` tag and automatically converts struct field names to
// snake case. It can be set to whatever you want, but it is encouraged to be
// set before gocqlx is used as name-to-field mappings are cached after first
// use on a type.
var DefaultMapper = reflectx.NewMapperFunc("db", snakeCase)

// snakeCase converts camel case to snake case.
func snakeCase(s string) string {
	buf := []byte(s)
	out := make([]byte, 0, len(buf)+3)

	l := len(buf)
	for i := 0; i < l; i++ {
		if !(allowedBindRune(buf[i]) || buf[i] == '_') {
			panic(fmt.Sprint("not allowed name ", s))
		}

		b := rune(buf[i])

		if unicode.IsUpper(b) {
			if i > 0 && buf[i-1] != '_' && (unicode.IsLower(rune(buf[i-1])) || (i+1 < l && unicode.IsLower(rune(buf[i+1])))) {
				out = append(out, '_')
			}
			b = unicode.ToLower(b)
		}

		out = append(out, byte(b))
	}

	return string(out)
}
