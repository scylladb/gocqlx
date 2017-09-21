// Copyright (C) 2017 ScyllaDB
// Use of this source code is governed by a ALv2-style
// license that can be found in the LICENSE file.

package qb

import (
	"bytes"
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

// TTL converts duration to format expected in USING TTL clause.
func TTL(d time.Duration) int64 {
	return int64(d.Seconds())
}

// Timestamp converts time to format expected in USING TIMESTAMP clause.
func Timestamp(t time.Time) int64 {
	return t.UnixNano() / 1000
}
