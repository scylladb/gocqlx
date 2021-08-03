// Copyright (C) 2017 ScyllaDB
// Use of this source code is governed by a ALv2-style
// license that can be found in the LICENSE file.

package qb

import (
	"bytes"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
)

func TestTTL(t *testing.T) {
	if TTL(time.Second*86400) != 86400 {
		t.Fatal("wrong ttl")
	}
}

func TestTimestamp(t *testing.T) {
	if Timestamp(time.Unix(0, 0).Add(time.Microsecond*123456789)) != 123456789 {
		t.Fatal("wrong timestamp")
	}
}

func TestUsing(t *testing.T) {
	table := []struct {
		B *using
		N []string
		S string
	}{
		// TTL
		{
			B: new(using).TTL(time.Second),
			S: "USING TTL 1 ",
		},
		// TTLNamed
		{
			B: new(using).TTLNamed("ttl"),
			S: "USING TTL ? ",
			N: []string{"ttl"},
		},
		// Timestamp
		{
			B: new(using).Timestamp(time.Date(2005, 05, 05, 0, 0, 0, 0, time.UTC)),
			S: "USING TIMESTAMP 1115251200000000 ",
		},
		// TimestampNamed
		{
			B: new(using).TimestampNamed("ts"),
			S: "USING TIMESTAMP ? ",
			N: []string{"ts"},
		},
		// Timeout
		{
			B: new(using).Timeout(time.Second),
			S: "USING TIMEOUT 1s ",
		},
		// TimeoutNamed
		{
			B: new(using).TimeoutNamed("to"),
			S: "USING TIMEOUT ? ",
			N: []string{"to"},
		},
		// TTL Timestamp
		{
			B: new(using).TTL(time.Second).Timestamp(time.Date(2005, 05, 05, 0, 0, 0, 0, time.UTC)),
			S: "USING TTL 1 AND TIMESTAMP 1115251200000000 ",
		},
		// TTL TimestampNamed
		{
			B: new(using).TTL(time.Second).TimestampNamed("ts"),
			S: "USING TTL 1 AND TIMESTAMP ? ",
			N: []string{"ts"},
		},
		// TTLNamed TimestampNamed
		{
			B: new(using).TTLNamed("ttl").TimestampNamed("ts"),
			S: "USING TTL ? AND TIMESTAMP ? ",
			N: []string{"ttl", "ts"},
		},
		// TTLNamed Timestamp
		{
			B: new(using).TTLNamed("ttl").Timestamp(time.Date(2005, 05, 05, 0, 0, 0, 0, time.UTC)),
			S: "USING TTL ? AND TIMESTAMP 1115251200000000 ",
			N: []string{"ttl"},
		},
		// TTL Timeout
		{
			B: new(using).TTL(time.Second).Timeout(time.Second),
			S: "USING TTL 1 AND TIMEOUT 1s ",
		},
		// TTL TimeoutNamed
		{
			B: new(using).TTL(time.Second).TimeoutNamed("to"),
			S: "USING TTL 1 AND TIMEOUT ? ",
			N: []string{"to"},
		},
		// TTL Timestamp Timeout
		{
			B: new(using).TTL(time.Second).Timestamp(time.Date(2005, 05, 05, 0, 0, 0, 0, time.UTC)).Timeout(time.Second),
			S: "USING TTL 1 AND TIMESTAMP 1115251200000000 AND TIMEOUT 1s ",
		},
		// TTL with no duration
		{
			B: new(using).TTL(0 * time.Second),
			S: "USING TTL 0 ",
		},
		{
			B: new(using).TTL(-1 * time.Second),
			S: "USING TTL 0 ",
		},
		{
			// TODO patch this maybe in the future
			B: new(using).TTL(-2 * time.Second),
			S: "USING TTL -2 ",
		},
		// TTL TTLNamed
		{
			B: new(using).TTL(time.Second).TTLNamed("ttl"),
			S: "USING TTL ? ",
			N: []string{"ttl"},
		},
		// TTLNamed TTL
		{
			B: new(using).TTLNamed("ttl").TTL(time.Second),
			S: "USING TTL 1 ",
		},
		// Timestamp TimestampNamed
		{
			B: new(using).Timestamp(time.Date(2005, 05, 05, 0, 0, 0, 0, time.UTC)).TimestampNamed("ts"),
			S: "USING TIMESTAMP ? ",
			N: []string{"ts"},
		},
		// TimestampNamed Timestamp
		{
			B: new(using).TimestampNamed("ts").Timestamp(time.Date(2005, 05, 05, 0, 0, 0, 0, time.UTC)),
			S: "USING TIMESTAMP 1115251200000000 ",
		},
	}

	for _, test := range table {
		buf := bytes.NewBuffer(nil)
		names := test.B.writeCql(buf)
		stmt := buf.String()

		if diff := cmp.Diff(test.S, stmt); diff != "" {
			t.Error(diff)
		}
		if diff := cmp.Diff(test.N, names); diff != "" {
			t.Error(diff)
		}
	}
}
