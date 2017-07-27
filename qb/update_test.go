package qb

import (
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
)

func TestUpdateBuilder(t *testing.T) {
	w := EqNamed("id", "expr")

	table := []struct {
		B *UpdateBuilder
		N []string
		S string
	}{
		// Basic test for update
		{
			B: Update("cycling.cyclist_name").Set("id", "user_uuid", "firstname").Where(w),
			S: "UPDATE cycling.cyclist_name SET id=?,user_uuid=?,firstname=? WHERE id=? ",
			N: []string{"id", "user_uuid", "firstname", "expr"},
		},
		// Change table name
		{
			B: Update("cycling.cyclist_name").Set("id", "user_uuid", "firstname").Where(w).Table("Foobar"),
			S: "UPDATE Foobar SET id=?,user_uuid=?,firstname=? WHERE id=? ",
			N: []string{"id", "user_uuid", "firstname", "expr"},
		},
		// Add SET
		{
			B: Update("cycling.cyclist_name").Set("id", "user_uuid", "firstname").Where(w).Set("stars"),
			S: "UPDATE cycling.cyclist_name SET id=?,user_uuid=?,firstname=?,stars=? WHERE id=? ",
			N: []string{"id", "user_uuid", "firstname", "stars", "expr"},
		},
		// Add WHERE
		{
			B: Update("cycling.cyclist_name").Set("id", "user_uuid", "firstname").Where(w, Gt("firstname")),
			S: "UPDATE cycling.cyclist_name SET id=?,user_uuid=?,firstname=? WHERE id=? AND firstname>? ",
			N: []string{"id", "user_uuid", "firstname", "expr", "firstname"},
		},
		// Add IF
		{
			B: Update("cycling.cyclist_name").Set("id", "user_uuid", "firstname").Where(w).If(Gt("firstname")),
			S: "UPDATE cycling.cyclist_name SET id=?,user_uuid=?,firstname=? WHERE id=? IF firstname>? ",
			N: []string{"id", "user_uuid", "firstname", "expr", "firstname"},
		},
		// Add TTL
		{
			B: Update("cycling.cyclist_name").Set("id", "user_uuid", "firstname").Where(w).TTL(time.Second * 86400),
			S: "UPDATE cycling.cyclist_name USING TTL 86400 SET id=?,user_uuid=?,firstname=? WHERE id=? ",
			N: []string{"id", "user_uuid", "firstname", "expr"},
		},
		// Add TIMESTAMP
		{
			B: Update("cycling.cyclist_name").Set("id", "user_uuid", "firstname").Where(w).Timestamp(time.Unix(0, 0).Add(time.Microsecond * 123456789)),
			S: "UPDATE cycling.cyclist_name USING TIMESTAMP 123456789 SET id=?,user_uuid=?,firstname=? WHERE id=? ",
			N: []string{"id", "user_uuid", "firstname", "expr"},
		},
		// Add IF EXISTS
		{
			B: Update("cycling.cyclist_name").Set("id", "user_uuid", "firstname").Where(w).Existing(),
			S: "UPDATE cycling.cyclist_name SET id=?,user_uuid=?,firstname=? WHERE id=? IF EXISTS ",
			N: []string{"id", "user_uuid", "firstname", "expr"},
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

func BenchmarkUpdateBuilder(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Update("cycling.cyclist_name").Set("id", "user_uuid", "firstname", "stars").Where(Eq("id"))
	}
}
