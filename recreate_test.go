// Copyright (C) 2017 ScyllaDB

package gocqlx_test

import (
	"io/ioutil"
	"sort"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/scylladb/gocqlx"
	. "github.com/scylladb/gocqlx/gocqlxtest"
)

func TestRecreateSchema(t *testing.T) {
	session := CreateSession(t)
	defer session.Close()

	tcs := []struct {
		Name     string
		Keyspace string
		Input    string
		Golden   string
	}{
		{
			Name:     "Keyspace",
			Keyspace: "gocqlx_keyspace",
			Input:    "testdata/recreate/keyspace.cql",
			Golden:   "testdata/recreate/keyspace_golden.cql",
		},
		{
			Name:     "Table",
			Keyspace: "gocqlx_table",
			Input:    "testdata/recreate/table.cql",
			Golden:   "testdata/recreate/table_golden.cql",
		},
		{
			Name:     "Materialized Views",
			Keyspace: "gocqlx_mv",
			Input:    "testdata/recreate/materialized_views.cql",
			Golden:   "testdata/recreate/materialized_views_golden.cql",
		},
		{
			Name:     "Index",
			Keyspace: "gocqlx_idx",
			Input:    "testdata/recreate/index.cql",
			Golden:   "testdata/recreate/index_golden.cql",
		},
		{
			Name:     "Secondary Index",
			Keyspace: "gocqlx_sec_idx",
			Input:    "testdata/recreate/secondary_index.cql",
			Golden:   "testdata/recreate/secondary_index_golden.cql",
		},
		{
			Name:     "UDT",
			Keyspace: "gocqlx_udt",
			Input:    "testdata/recreate/udt.cql",
			Golden:   "testdata/recreate/udt_golden.cql",
		},
	}

	for i := range tcs {
		test := tcs[i]
		t.Run(test.Name, func(t *testing.T) {
			cleanup(t, session, test.Keyspace)

			in, err := ioutil.ReadFile(test.Input)
			if err != nil {
				t.Fatal(err)
			}

			queries := trimQueries(strings.Split(string(in), ";"))
			for _, q := range queries {
				if err := session.ExecStmt(q); err != nil {
					t.Fatal("invalid input query", q, err)
				}
			}

			dump, err := session.Schema(test.Keyspace)
			if err != nil {
				t.Fatal("dump schema", err)
			}

			golden, err := ioutil.ReadFile(test.Golden)
			if err != nil {
				t.Fatal(err)
			}

			goldenQueries := sortQueries(strings.Split(string(golden), ";"))
			dumpQueries := sortQueries(strings.Split(dump, ";"))

			// Compare with golden
			if !cmp.Equal(goldenQueries, dumpQueries) {
				t.Error("dump doesn't match golden", cmp.Diff(string(golden), dump))
			}
		})
	}
}

func TestRecreatedSchemaCorrectness(t *testing.T) {
	const (
		inputPath = "testdata/recreate/correctness.cql"
		keyspace  = "gocqlx_correctness"
	)

	session := CreateSession(t)
	defer session.Close()

	cleanup(t, session, keyspace)

	in, err := ioutil.ReadFile(inputPath)
	if err != nil {
		t.Fatal(err)
	}

	queries := trimQueries(strings.Split(string(in), ";"))
	for _, q := range queries {
		if err := session.ExecStmt(q); err != nil {
			t.Fatal("invalid input query", q, err)
		}
	}

	firstDump, err := session.Schema(keyspace)
	if err != nil {
		t.Fatal("dump schema", err)
	}

	cleanup(t, session, keyspace)

	// Close session in order to clear metadata cache
	session.Close()
	session = CreateSession(t)

	for _, q := range trimQueries(strings.Split(firstDump, ";")) {
		if err := session.ExecStmt(q); err != nil {
			t.Fatal("invalid dump query", q, err)
		}
	}

	secondDump, err := session.Schema(keyspace)
	if err != nil {
		t.Fatal("dump schema", err)
	}

	dumpQueries := sortQueries(strings.Split(firstDump, ";"))
	secondDumpQueries := sortQueries(strings.Split(secondDump, ";"))

	// Compare with golden
	if !cmp.Equal(secondDumpQueries, dumpQueries) {
		t.Error("second dump doesn't match first one", cmp.Diff(dumpQueries, secondDumpQueries))
	}
}

func cleanup(t *testing.T, session gocqlx.Session, keyspace string) {
	err := session.ExecStmt(`DROP KEYSPACE IF EXISTS ` + keyspace)
	if err != nil {
		t.Fatalf("unable to drop keyspace: %v", err)
	}
}

func sortQueries(in []string) []string {
	q := trimQueries(in)
	sort.Strings(q)
	return q
}

func trimQueries(in []string) []string {
	queries := in[:0]
	for _, q := range in {
		q = strings.TrimSpace(q)
		if len(q) != 0 {
			queries = append(queries, q)
		}
	}
	return queries
}
