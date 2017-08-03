package qb

import (
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestInsertBuilder(t *testing.T) {
	table := []struct {
		B *InsertBuilder
		N []string
		S string
	}{

		// Basic test for insert
		{
			B: Insert("cycling.cyclist_name").Columns("id", "user_uuid", "firstname"),
			S: "INSERT INTO cycling.cyclist_name (id,user_uuid,firstname) VALUES (?,?,?) ",
			N: []string{"id", "user_uuid", "firstname"},
		},
		// Change table name
		{
			B: Insert("cycling.cyclist_name").Columns("id", "user_uuid", "firstname").Into("Foobar"),
			S: "INSERT INTO Foobar (id,user_uuid,firstname) VALUES (?,?,?) ",
			N: []string{"id", "user_uuid", "firstname"},
		},
		// Add columns
		{
			B: Insert("cycling.cyclist_name").Columns("id", "user_uuid", "firstname").Columns("stars"),
			S: "INSERT INTO cycling.cyclist_name (id,user_uuid,firstname,stars) VALUES (?,?,?,?) ",
			N: []string{"id", "user_uuid", "firstname", "stars"},
		},
		// Add TTL
		{
			B: Insert("cycling.cyclist_name").Columns("id", "user_uuid", "firstname").TTL(),
			S: "INSERT INTO cycling.cyclist_name (id,user_uuid,firstname) VALUES (?,?,?) USING TTL ? ",
			N: []string{"id", "user_uuid", "firstname", "_ttl"},
		},
		// Add TIMESTAMP
		{
			B: Insert("cycling.cyclist_name").Columns("id", "user_uuid", "firstname").Timestamp(),
			S: "INSERT INTO cycling.cyclist_name (id,user_uuid,firstname) VALUES (?,?,?) USING TIMESTAMP ? ",
			N: []string{"id", "user_uuid", "firstname", "_ts"},
		},
		// Add IF NOT EXISTS
		{
			B: Insert("cycling.cyclist_name").Columns("id", "user_uuid", "firstname").Unique(),
			S: "INSERT INTO cycling.cyclist_name (id,user_uuid,firstname) VALUES (?,?,?) IF NOT EXISTS ",
			N: []string{"id", "user_uuid", "firstname"},
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

func BenchmarkInsertBuilder(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Insert("cycling.cyclist_name").Columns("id", "user_uuid", "firstname", "stars").ToCql()
	}
}
