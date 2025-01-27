// Copyright (C) 2017 ScyllaDB
// Use of this source code is governed by a ALv2-style
// license that can be found in the LICENSE file.

package qb

import (
	"bytes"
	"fmt"
	"strings"
	"time"
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
	for i, c := range cols {
		cql.WriteString(c)
		if i < len(cols)-1 {
			cql.WriteByte(',')
		}
	}
}

func formatDuration(d time.Duration) string {
	// Round the duration to the nearest millisecond
	// Extract hours, minutes, seconds, and milliseconds
	minutes := d / time.Minute
	d %= time.Minute
	seconds := d / time.Second
	d %= time.Second
	milliseconds := d / time.Millisecond

	// Format the duration string
	var res []string
	if minutes > 0 {
		res = append(res, fmt.Sprintf("%dm", minutes))
	}
	if seconds > 0 {
		res = append(res, fmt.Sprintf("%ds", seconds))
	}
	if milliseconds > 0 {
		res = append(res, fmt.Sprintf("%dms", milliseconds))
	}
	return strings.Join(res, "")
}
