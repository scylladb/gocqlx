// Copyright (C) 2026 ScyllaDB
// Use of this source code is governed by a ALv2-style
// license that can be found in the LICENSE file.

package gocqlx

import (
	"testing"

	"github.com/gocql/gocql"
)

func TestBatchProtocolV5Options(t *testing.T) {
	b := &Batch{Batch: &gocql.Batch{Type: gocql.LoggedBatch}}
	got := b.SetKeyspace("test_keyspace").WithNowInSeconds(123).Query("INSERT INTO test (value) VALUES (?)", 42)
	if got != b {
		t.Fatal("batch options must preserve the Batch wrapper")
	}
	if got.Keyspace() != "test_keyspace" {
		t.Errorf("keyspace = %q, want test_keyspace", got.Keyspace())
	}
	if len(got.Entries) != 1 {
		t.Fatalf("batch has %d entries, want 1", len(got.Entries))
	}
}
