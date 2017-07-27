package qb

import (
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
)

func TestUpdateBuilder(t *testing.T) {
	m := mockExpr{
		cql:   "expr",
		names: []string{"expr"},
	}

	table := []struct {
		B *UpdateBuilder
		N []string
		S string
	}{
		// Basic test for update
		{
			B: Update("cycling.cyclist_name").Set("id", "user_uuid", "firstname").Where(m),
			S: "UPDATE cycling.cyclist_name SET id=?,user_uuid=?,firstname=? WHERE expr ",
			N: []string{"id", "user_uuid", "firstname", "expr"},
		},
		// Change table name
		{
			B: Update("cycling.cyclist_name").Set("id", "user_uuid", "firstname").Where(m).Table("Foobar"),
			S: "UPDATE Foobar SET id=?,user_uuid=?,firstname=? WHERE expr ",
			N: []string{"id", "user_uuid", "firstname", "expr"},
		},
		// Add SET
		{
			B: Update("cycling.cyclist_name").Set("id", "user_uuid", "firstname").Where(m).Set("stars"),
			S: "UPDATE cycling.cyclist_name SET id=?,user_uuid=?,firstname=?,stars=? WHERE expr ",
			N: []string{"id", "user_uuid", "firstname", "stars", "expr"},
		},
		// Add WHERE
		{
			B: Update("cycling.cyclist_name").Set("id", "user_uuid", "firstname").Where(m).Where(mockExpr{
				cql:   "expr_1",
				names: []string{"expr_1"},
			}, mockExpr{
				cql:   "expr_2",
				names: []string{"expr_2"},
			}),
			S: "UPDATE cycling.cyclist_name SET id=?,user_uuid=?,firstname=? WHERE expr AND expr_1 AND expr_2 ",
			N: []string{"id", "user_uuid", "firstname", "expr", "expr_1", "expr_2"},
		},
		// Add IF
		{
			B: Update("cycling.cyclist_name").Set("id", "user_uuid", "firstname").Where(m).If(mockExpr{
				cql:   "expr_1",
				names: []string{"expr_1"},
			}, mockExpr{
				cql:   "expr_2",
				names: []string{"expr_2"},
			}),
			S: "UPDATE cycling.cyclist_name SET id=?,user_uuid=?,firstname=? WHERE expr IF expr_1 AND expr_2 ",
			N: []string{"id", "user_uuid", "firstname", "expr", "expr_1", "expr_2"},
		},
		// Add TTL
		{
			B: Update("cycling.cyclist_name").Set("id", "user_uuid", "firstname").Where(m).TTL(time.Second * 86400),
			S: "UPDATE cycling.cyclist_name USING TTL 86400 SET id=?,user_uuid=?,firstname=? WHERE expr ",
			N: []string{"id", "user_uuid", "firstname", "expr"},
		},
		// Add TIMESTAMP
		{
			B: Update("cycling.cyclist_name").Set("id", "user_uuid", "firstname").Where(m).Timestamp(time.Unix(0, 0).Add(time.Microsecond * 123456789)),
			S: "UPDATE cycling.cyclist_name USING TIMESTAMP 123456789 SET id=?,user_uuid=?,firstname=? WHERE expr ",
			N: []string{"id", "user_uuid", "firstname", "expr"},
		},
		// Add IF EXISTS
		{
			B: Update("cycling.cyclist_name").Set("id", "user_uuid", "firstname").Where(m).Existing(),
			S: "UPDATE cycling.cyclist_name SET id=?,user_uuid=?,firstname=? WHERE expr IF EXISTS ",
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
	m := mockExpr{
		cql:   "expr",
		names: []string{"expr"},
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Update("cycling.cyclist_name").Set("id", "user_uuid", "firstname", "stars").Where(m)
	}
}
