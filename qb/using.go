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
	ttl           int64
	ttlName       string
	timestamp     int64
	timestampName string
	timeout       time.Duration
	timeoutName   string

	using bool
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
	u.using = false

	if u.ttl != 0 {
		if u.ttl == -1 {
			u.ttl = 0
		}
		u.writePreamble(cql)
		fmt.Fprintf(cql, "TTL %d ", u.ttl)
	} else if u.ttlName != "" {
		u.writePreamble(cql)
		cql.WriteString("TTL ? ")
		names = append(names, u.ttlName)
	}

	if u.timestamp != 0 {
		u.writePreamble(cql)
		fmt.Fprintf(cql, "TIMESTAMP %d ", u.timestamp)
	} else if u.timestampName != "" {
		u.writePreamble(cql)
		cql.WriteString("TIMESTAMP ? ")
		names = append(names, u.timestampName)
	}

	if u.timeout != 0 {
		u.writePreamble(cql)
		fmt.Fprintf(cql, "TIMEOUT %s ", u.timeout)
	} else if u.timeoutName != "" {
		u.writePreamble(cql)
		cql.WriteString("TIMEOUT ? ")
		names = append(names, u.timeoutName)
	}

	return
}

func (u *using) writePreamble(cql *bytes.Buffer) {
	if u.using {
		cql.WriteString("AND ")
	} else {
		cql.WriteString("USING ")
		u.using = true
	}
}
