package qb

import (
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestSelectBuilder(t *testing.T) {
	m := mockExpr{
		cql:   "expr",
		names: []string{"expr"},
	}

	table := []struct {
		B *SelectBuilder
		N []string
		S string
	}{
		// Basic test for select *
		{
			B: Select("cycling.cyclist_name"),
			S: "SELECT * FROM cycling.cyclist_name ",
		},
		// Basic test for select columns
		{
			B: Select("cycling.cyclist_name").Columns("id", "user_uuid", "firstname"),
			S: "SELECT id,user_uuid,firstname FROM cycling.cyclist_name ",
		},
		// Basic test for select distinct
		{
			B: Select("cycling.cyclist_name").Distinct("id"),
			S: "SELECT DISTINCT id FROM cycling.cyclist_name ",
		},
		// Change table name
		{
			B: Select("cycling.cyclist_name").From("Foobar"),
			S: "SELECT * FROM Foobar ",
		},
		// Add WHERE
		{
			B: Select("cycling.cyclist_name").Where(m).Where(mockExpr{
				cql:   "expr_1",
				names: []string{"expr_1"},
			}, mockExpr{
				cql:   "expr_2",
				names: []string{"expr_2"},
			}),
			S: "SELECT * FROM cycling.cyclist_name WHERE expr AND expr_1 AND expr_2 ",
			N: []string{"expr", "expr_1", "expr_2"},
		},
		// Add GROUP BY
		{
			B: Select("cycling.cyclist_name").Columns("MAX(stars) as max_stars").GroupBy("id"),
			S: "SELECT id,MAX(stars) as max_stars FROM cycling.cyclist_name GROUP BY id ",
		},
		// Add ORDER BY
		{
			B: Select("cycling.cyclist_name").Where(m).OrderBy("firstname", ASC),
			S: "SELECT * FROM cycling.cyclist_name WHERE expr ORDER BY firstname ASC ",
			N: []string{"expr"},
		},
		// Add LIMIT
		{
			B: Select("cycling.cyclist_name").Where(m).Limit(10),
			S: "SELECT * FROM cycling.cyclist_name WHERE expr LIMIT 10 ",
			N: []string{"expr"},
		},
		// Add PER PARTITION LIMIT
		{
			B: Select("cycling.cyclist_name").Where(m).LimitPerPartition(10),
			S: "SELECT * FROM cycling.cyclist_name WHERE expr PER PARTITION LIMIT 10 ",
			N: []string{"expr"},
		},
		// Add ALLOW FILTERING
		{
			B: Select("cycling.cyclist_name").Where(m).AllowFiltering(),
			S: "SELECT * FROM cycling.cyclist_name WHERE expr ALLOW FILTERING ",
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

func BenchmarkSelectBuilder(b *testing.B) {
	m := mockExpr{
		cql:   "expr",
		names: []string{"expr"},
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Select("cycling.cyclist_name").Columns("id", "user_uuid", "firstname", "stars").Where(m)
	}
}
