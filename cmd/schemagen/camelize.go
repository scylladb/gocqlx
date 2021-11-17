// Copyright (C) 2017 ScyllaDB
// Use of this source code is governed by a ALv2-style
// license that can be found in the LICENSE file.

package main

import (
	"fmt"
	"unicode"
)

func camelize(s string) string {
	buf := []byte(s)
	out := make([]byte, 0, len(buf))
	underscoreSeen := false

	l := len(buf)
	for i := 0; i < l; i++ {
		if !(allowedBindRune(buf[i]) || buf[i] == '_') {
			panic(fmt.Sprint("not allowed name ", s))
		}

		b := rune(buf[i])

		if b == '_' {
			underscoreSeen = true
			continue
		}

		if (i == 0 || underscoreSeen) && unicode.IsLower(b) {
			b = unicode.ToUpper(b)
			underscoreSeen = false
		}

		out = append(out, byte(b))
	}

	return string(out)
}

func allowedBindRune(b byte) bool {
	return (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') || (b >= '0' && b <= '9')
}
