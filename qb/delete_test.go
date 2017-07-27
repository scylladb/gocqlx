package qb

import (
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
)

func TestDeleteBuilder(t *testing.T) {
	m := mockExpr{
		cql:   "expr",
		names: []string{"expr"},
	}

	table := []struct {
		B *DeleteBuilder
		N []string
		S string
	}{
		// Basic test for delete
		{
			B: Delete("cycling.cyclist_name").Where(m),
			S: "DELETE FROM cycling.cyclist_name WHERE expr ",
			N: []string{"expr"},
		},
		// Change table name
		{
			B: Delete("cycling.cyclist_name").Where(m).From("Foobar"),
			S: "DELETE FROM Foobar WHERE expr ",
			N: []string{"expr"},
		},
		// Add column
		{
			B: Delete("cycling.cyclist_name").Where(m).Columns("stars"),
			S: "DELETE stars FROM cycling.cyclist_name WHERE expr ",
			N: []string{"expr"},
		},
		// Add WHERE
		{
			B: Delete("cycling.cyclist_name").Where(m).Where(mockExpr{
				cql:   "expr_1",
				names: []string{"expr_1"},
			}, mockExpr{
				cql:   "expr_2",
				names: []string{"expr_2"},
			}),
			S: "DELETE FROM cycling.cyclist_name WHERE expr AND expr_1 AND expr_2 ",
			N: []string{"expr", "expr_1", "expr_2"},
		},
		// Add IF
		{
			B: Delete("cycling.cyclist_name").Where(m).If(mockExpr{
				cql:   "expr_1",
				names: []string{"expr_1"},
			}, mockExpr{
				cql:   "expr_2",
				names: []string{"expr_2"},
			}),
			S: "DELETE FROM cycling.cyclist_name WHERE expr IF expr_1 AND expr_2 ",
			N: []string{"expr", "expr_1", "expr_2"},
		},
		// Add TIMESTAMP
		{
			B: Delete("cycling.cyclist_name").Where(m).Timestamp(time.Unix(0, 0).Add(time.Microsecond * 123456789)),
			S: "DELETE FROM cycling.cyclist_name USING TIMESTAMP 123456789 WHERE expr ",
			N: []string{"expr"},
		},
		// Add IF EXISTS
		{
			B: Delete("cycling.cyclist_name").Where(m).Existing(),
			S: "DELETE FROM cycling.cyclist_name WHERE expr IF EXISTS ",
			N: []string{"expr"},
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

func BenchmarkDeleteBuilder(b *testing.B) {
	m := mockExpr{
		cql:   "expr",
		names: []string{"expr"},
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Delete("cycling.cyclist_name").Columns("id", "user_uuid", "firstname", "stars").Where(m)
	}
}
