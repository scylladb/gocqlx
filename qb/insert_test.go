package qb

import (
	"testing"
	"time"

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
			B: Insert("cycling.cyclist_name").Columns("id", "user_uuid", "firstname").TTL(time.Second * 86400),
			S: "INSERT INTO cycling.cyclist_name (id,user_uuid,firstname) VALUES (?,?,?) USING TTL 86400 ",
			N: []string{"id", "user_uuid", "firstname"},
		},
		// Add TIMESTAMP
		{
			B: Insert("cycling.cyclist_name").Columns("id", "user_uuid", "firstname").Timestamp(time.Unix(0, 0).Add(time.Microsecond * 123456789)),
			S: "INSERT INTO cycling.cyclist_name (id,user_uuid,firstname) VALUES (?,?,?) USING TIMESTAMP 123456789 ",
			N: []string{"id", "user_uuid", "firstname"},
		},
		// Add IF NOT EXISTS
		{
			B: Insert("cycling.cyclist_name").Columns("id", "user_uuid", "firstname").Unique(),
			S: "INSERT INTO cycling.cyclist_name (id,user_uuid,firstname) VALUES (?,?,?) IF NOT EXISTS ",
			N: []string{"id", "user_uuid", "firstname"},
		},
	}

	for _, test := range table {
		stmt, names, err := test.B.ToCql()
		if err != nil {
			t.Error("unexpected error", err)
		}
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
		Insert("foo").Columns("name", "age", "first", "last").ToCql()
	}
}
