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

func (u *using) writeCql(cql *bytes.Buffer) (names []string) {
	hasTTL := false

	if u.ttl != 0 {
		hasTTL = true
		if u.ttl == -1 {
			u.ttl = 0
		}
		cql.WriteString("USING TTL ")
		cql.WriteString(fmt.Sprint(u.ttl))
		cql.WriteByte(' ')
	} else if u.ttlName != "" {
		hasTTL = true
		cql.WriteString("USING TTL ? ")
		names = append(names, u.ttlName)
	}

	if u.timestamp != 0 {
		if hasTTL {
			cql.WriteString("AND TIMESTAMP ")
		} else {
			cql.WriteString("USING TIMESTAMP ")
		}
		cql.WriteString(fmt.Sprint(u.timestamp))
		cql.WriteByte(' ')
	} else if u.timestampName != "" {
		if hasTTL {
			cql.WriteString("AND TIMESTAMP ? ")
		} else {
			cql.WriteString("USING TIMESTAMP ? ")
		}
		names = append(names, u.timestampName)
	}

	return
}
