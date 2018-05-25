// Copyright (C) 2017 ScyllaDB
// Use of this source code is governed by a ALv2-style
// license that can be found in the LICENSE file.

package qb

import (
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
)

type mockBuilder struct {
	stmt  string
	names []string
}

func (b mockBuilder) ToCql() (stmt string, names []string) {
	return b.stmt, b.names
}

func TestBatchBuilder(t *testing.T) {
	m := mockBuilder{"INSERT INTO cycling.cyclist_name (id,user_uuid,firstname) VALUES (?,?,?) ", []string{"id", "user_uuid", "firstname"}}

	table := []struct {
		B *BatchBuilder
		N []string
		S string
	}{
		// Basic test for Batch
		{
			B: Batch().Add(m),
			S: "BEGIN BATCH INSERT INTO cycling.cyclist_name (id,user_uuid,firstname) VALUES (?,?,?) ; APPLY BATCH ",
			N: []string{"id", "user_uuid", "firstname"},
		},
		// Add statement
		{
			B: Batch().
				AddWithPrefix("a", m).
				AddWithPrefix("b", m),
			S: "BEGIN BATCH INSERT INTO cycling.cyclist_name (id,user_uuid,firstname) VALUES (?,?,?) ; INSERT INTO cycling.cyclist_name (id,user_uuid,firstname) VALUES (?,?,?) ; APPLY BATCH ",
			N: []string{"a.id", "a.user_uuid", "a.firstname", "b.id", "b.user_uuid", "b.firstname"},
		},
		// Add UNLOGGED
		{
			B: Batch().UnLogged(),
			S: "BEGIN UNLOGGED BATCH APPLY BATCH ",
		},
		// Add COUNTER
		{
			B: Batch().Counter(),
			S: "BEGIN COUNTER BATCH APPLY BATCH ",
		},
		// Add TTL
		{
			B: Batch().TTL(time.Second),
			S: "BEGIN BATCH USING TTL 1 APPLY BATCH ",
		},
		{
			B: Batch().TTLNamed("ttl"),
			S: "BEGIN BATCH USING TTL ? APPLY BATCH ",
			N: []string{"ttl"},
		},
		// Add TIMESTAMP
		{
			B: Batch().Timestamp(time.Date(2005, 05, 05, 0, 0, 0, 0, time.UTC)),
			S: "BEGIN BATCH USING TIMESTAMP 1115251200000000 APPLY BATCH ",
		},
		{
			B: Batch().TimestampNamed("ts"),
			S: "BEGIN BATCH USING TIMESTAMP ? APPLY BATCH ",
			N: []string{"ts"},
		},
	}

	for _, test := range table {
		stmt, names := test.B.ToCql()
		if diff := cmp.Diff(test.S, stmt); diff != "" {
			t.Error(diff)
		}
		if diff := cmp.Diff(test.N, names); diff != "" {
			t.Error(diff)
		}
	}
}
