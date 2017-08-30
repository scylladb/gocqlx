package qb

import (
	"testing"

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
			B: Batch().TTL(),
			S: "BEGIN BATCH USING TTL ? APPLY BATCH ",
			N: []string{"_ttl"},
		},
		// Add TIMESTAMP
		{
			B: Batch().Timestamp(),
			S: "BEGIN BATCH USING TIMESTAMP ? APPLY BATCH ",
			N: []string{"_ts"},
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

func BenchmarkBatchBuilder(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Batch().Add(mockBuilder{"INSERT INTO cycling.cyclist_name (id,user_uuid,firstname) VALUES (?,?,?) ", []string{"id", "user_uuid", "firstname"}}).ToCql()
	}
}
