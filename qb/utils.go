// Copyright (C) 2017 ScyllaDB
// Use of this source code is governed by a ALv2-style
// license that can be found in the LICENSE file.

package qb

import (
	"bytes"
	"strings"
)

// placeholders returns a string with count ? placeholders joined with commas.
func placeholders(cql *bytes.Buffer, count int) {
	if count < 1 {
		return
	}

	for i := 0; i < count-1; i++ {
		cql.WriteByte('?')
		cql.WriteByte(',')
	}
	cql.WriteByte('?')
}

type columns []string

func (cols columns) writeCql(cql *bytes.Buffer) {
	cql.WriteString(strings.Join(cols, ","))
}
