// Copyright (C) 2017 ScyllaDB
// Use of this source code is governed by a ALv2-style
// license that can be found in the LICENSE file.

package qb

import (
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
)

func TestSelectBuilder(t *testing.T) {
	w := EqNamed("id", "expr")

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
		// Add a SELECT AS column
		{
			B: Select("cycling.cyclist_name").Columns("id", "user_uuid", As("firstname", "name")),
			S: "SELECT id,user_uuid,firstname AS name FROM cycling.cyclist_name ",
		},
		// Basic test for select columns as JSON
		{
			B: Select("cycling.cyclist_name").Columns("id", "user_uuid", "firstname").Json(),
			S: "SELECT JSON id,user_uuid,firstname FROM cycling.cyclist_name ",
		},
		// Add a SELECT AS column as JSON
		{
			B: Select("cycling.cyclist_name").Columns("id", "user_uuid", As("firstname", "name")).Json(),
			S: "SELECT JSON id,user_uuid,firstname AS name FROM cycling.cyclist_name ",
		},
		// Add a SELECT AS column 2
		{
			B: Select("cycling.cyclist_name").
				Columns(As("firstname", "name"), "id", As("user_uuid", "user")),
			S: "SELECT firstname AS name,id,user_uuid AS user FROM cycling.cyclist_name ",
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
			B: Select("cycling.cyclist_name").Where(w, Gt("firstname")),
			S: "SELECT * FROM cycling.cyclist_name WHERE id=? AND firstname>? ",
			N: []string{"expr", "firstname"},
		},
		// Add WHERE with tuple
		{
			B: Select("cycling.cyclist_name").Where(EqTuple("id", 2), Gt("firstname")),
			S: "SELECT * FROM cycling.cyclist_name WHERE id=(?,?) AND firstname>? ",
			N: []string{"id[0]", "id[1]", "firstname"},
		},
		// Add WHERE with only tuples
		{
			B: Select("cycling.cyclist_name").Where(EqTuple("id", 2), GtTuple("firstname", 2)),
			S: "SELECT * FROM cycling.cyclist_name WHERE id=(?,?) AND firstname>(?,?) ",
			N: []string{"id[0]", "id[1]", "firstname[0]", "firstname[1]"},
		},
		// Add TIMEOUT
		{
			B: Select("cycling.cyclist_name").Where(w, Gt("firstname")).Timeout(time.Second),
			S: "SELECT * FROM cycling.cyclist_name WHERE id=? AND firstname>? USING TIMEOUT 1s ",
			N: []string{"expr", "firstname"},
		},
		{
			B: Select("cycling.cyclist_name").Where(w, Gt("firstname")).TimeoutNamed("to"),
			S: "SELECT * FROM cycling.cyclist_name WHERE id=? AND firstname>? USING TIMEOUT ? ",
			N: []string{"expr", "firstname", "to"},
		},
		// Add GROUP BY
		{
			B: Select("cycling.cyclist_name").Columns("MAX(stars) as max_stars").GroupBy("id"),
			S: "SELECT id,MAX(stars) as max_stars FROM cycling.cyclist_name GROUP BY id ",
		},
		// Add GROUP BY
		{
			B: Select("cycling.cyclist_name").GroupBy("id"),
			S: "SELECT id FROM cycling.cyclist_name GROUP BY id ",
		},
		// Add GROUP BY two columns
		{
			B: Select("cycling.cyclist_name").GroupBy("id", "user_uuid"),
			S: "SELECT id,user_uuid FROM cycling.cyclist_name GROUP BY id,user_uuid ",
		},
		// Add ORDER BY
		{
			B: Select("cycling.cyclist_name").Where(w).OrderBy("firstname", ASC),
			S: "SELECT * FROM cycling.cyclist_name WHERE id=? ORDER BY firstname ASC ",
			N: []string{"expr"},
		},
		// Add ORDER BY
		{
			B: Select("cycling.cyclist_name").Where(w).OrderBy("firstname", DESC),
			S: "SELECT * FROM cycling.cyclist_name WHERE id=? ORDER BY firstname DESC ",
			N: []string{"expr"},
		},
		// Add ORDER BY two columns
		{
			B: Select("cycling.cyclist_name").Where(w).OrderBy("firstname", ASC).OrderBy("lastname", DESC),
			S: "SELECT * FROM cycling.cyclist_name WHERE id=? ORDER BY firstname ASC,lastname DESC ",
			N: []string{"expr"},
		},
		// Add LIMIT
		{
			B: Select("cycling.cyclist_name").Where(w).Limit(10),
			S: "SELECT * FROM cycling.cyclist_name WHERE id=? LIMIT 10 ",
			N: []string{"expr"},
		},
		// Add named LIMIT
		{
			B: Select("cycling.cyclist_name").Where(w).LimitNamed("limit"),
			S: "SELECT * FROM cycling.cyclist_name WHERE id=? LIMIT ? ",
			N: []string{"expr", "limit"},
		},
		// Add PER PARTITION LIMIT
		{
			B: Select("cycling.cyclist_name").Where(w).LimitPerPartition(10),
			S: "SELECT * FROM cycling.cyclist_name WHERE id=? PER PARTITION LIMIT 10 ",
			N: []string{"expr"},
		},
		// Add named PER PARTITION LIMIT
		{
			B: Select("cycling.cyclist_name").Where(w).LimitPerPartitionNamed("partition_limit"),
			S: "SELECT * FROM cycling.cyclist_name WHERE id=? PER PARTITION LIMIT ? ",
			N: []string{"expr", "partition_limit"},
		},
		// Add PER PARTITION LIMIT and LIMIT
		{
			B: Select("cycling.cyclist_name").Where(w).LimitPerPartition(2).Limit(10),
			S: "SELECT * FROM cycling.cyclist_name WHERE id=? PER PARTITION LIMIT 2 LIMIT 10 ",
			N: []string{"expr"},
		},
		// Add named PER PARTITION LIMIT and LIMIT
		{
			B: Select("cycling.cyclist_name").Where(w).LimitPerPartitionNamed("partition_limit").LimitNamed("limit"),
			S: "SELECT * FROM cycling.cyclist_name WHERE id=? PER PARTITION LIMIT ? LIMIT ? ",
			N: []string{"expr", "partition_limit", "limit"},
		},
		// Add ALLOW FILTERING
		{
			B: Select("cycling.cyclist_name").Where(w).AllowFiltering(),
			S: "SELECT * FROM cycling.cyclist_name WHERE id=? ALLOW FILTERING ",
			N: []string{"expr"},
		},
		// Add ALLOW FILTERING and BYPASS CACHE
		{
			B: Select("cycling.cyclist_name").Where(w).AllowFiltering().BypassCache(),
			S: "SELECT * FROM cycling.cyclist_name WHERE id=? ALLOW FILTERING BYPASS CACHE ",
			N: []string{"expr"},
		},
		// Add BYPASS CACHE
		{
			B: Select("cycling.cyclist_name").Where(w).BypassCache(),
			S: "SELECT * FROM cycling.cyclist_name WHERE id=? BYPASS CACHE ",
			N: []string{"expr"},
		},
		// Add COUNT all
		{
			B: Select("cycling.cyclist_name").CountAll().Where(Gt("stars")),
			S: "SELECT count(*) FROM cycling.cyclist_name WHERE stars>? ",
			N: []string{"stars"},
		},
		// Add COUNT with GROUP BY
		{
			B: Select("cycling.cyclist_name").Count("stars").GroupBy("id"),
			S: "SELECT id,count(stars) FROM cycling.cyclist_name GROUP BY id ",
		},
		// Add Min
		{
			B: Select("cycling.cyclist_name").Min("stars"),
			S: "SELECT min(stars) FROM cycling.cyclist_name ",
		},
		// Add Sum
		{
			B: Select("cycling.cyclist_name").Sum("*"),
			S: "SELECT sum(*) FROM cycling.cyclist_name ",
		},
		// Add Avg
		{
			B: Select("cycling.cyclist_name").Avg("stars"),
			S: "SELECT avg(stars) FROM cycling.cyclist_name ",
		},
		// Add Max
		{
			B: Select("cycling.cyclist_name").Max("stars"),
			S: "SELECT max(stars) FROM cycling.cyclist_name ",
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
