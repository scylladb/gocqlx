// Copyright (C) 2017 ScyllaDB
// Use of this source code is governed by a ALv2-style
// license that can be found in the LICENSE file.

package qb

import (
	"bytes"
	"fmt"
	"time"
)

// TTL converts duration to format expected in USING TTL clause.
func TTL(d time.Duration) int64 {
	return int64(d.Seconds())
}

// Timestamp converts time to format expected in USING TIMESTAMP clause.
func Timestamp(t time.Time) int64 {
	return t.UnixNano() / 1000
}

type using struct {
	ttlName       string
	timestampName string
	timeoutName   string
	ttl           int64
	timestamp     int64
	timeout       time.Duration
}

func (u *using) TTL(d time.Duration) *using {
	u.ttl = TTL(d)
	if u.ttl == 0 {
		u.ttl = -1
	}
	u.timestampName = ""
	return u
}

func (u *using) TTLNamed(name string) *using {
	u.ttl = 0
	u.ttlName = name
	return u
}

func (u *using) Timestamp(t time.Time) *using {
	u.timestamp = Timestamp(t)
	u.timestampName = ""
	return u
}

func (u *using) TimestampNamed(name string) *using {
	u.timestamp = 0
	u.timestampName = name
	return u
}

func (u *using) Timeout(d time.Duration) *using {
	u.timeout = d
	u.timeoutName = ""
	return u
}

func (u *using) TimeoutNamed(name string) *using {
	u.timeout = 0
	u.timeoutName = name
	return u
}

func (u *using) writeCql(cql *bytes.Buffer) (names []string) {
	writePreamble := u.preambleWriter()

	if u.ttl != 0 {
		if u.ttl == -1 {
			u.ttl = 0
		}
		writePreamble(cql)
		fmt.Fprintf(cql, "TTL %d ", u.ttl)
	} else if u.ttlName != "" {
		writePreamble(cql)
		cql.WriteString("TTL ? ")
		names = append(names, u.ttlName)
	}

	if u.timestamp != 0 {
		writePreamble(cql)
		fmt.Fprintf(cql, "TIMESTAMP %d ", u.timestamp)
	} else if u.timestampName != "" {
		writePreamble(cql)
		cql.WriteString("TIMESTAMP ? ")
		names = append(names, u.timestampName)
	}

	if u.timeout != 0 {
		writePreamble(cql)
		fmt.Fprintf(cql, "TIMEOUT %s ", u.timeout)
	} else if u.timeoutName != "" {
		writePreamble(cql)
		cql.WriteString("TIMEOUT ? ")
		names = append(names, u.timeoutName)
	}

	return
}

func (u *using) preambleWriter() func(cql *bytes.Buffer) {
	var hasPreamble bool
	return func(cql *bytes.Buffer) {
		if hasPreamble {
			cql.WriteString("AND ")
			return
		}
		cql.WriteString("USING ")
		hasPreamble = true
	}
}
