package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"testing"

	"github.com/gocql/gocql"
	"github.com/google/go-cmp/cmp"

	"github.com/scylladb/gocqlx/v2/gocqlxtest"
)

var flagUpdate = flag.Bool("update", false, "update golden file")

func TestSchemagen(t *testing.T) {
	flag.Parse()
	createTestSchema(t)

	// add ignored types and table
	*flagIgnoreNames = strings.Join([]string{
		"composers",
		"composers_by_name",
		"label",
	}, ",")
	*flagIgnoreIndexes = true

	b := runSchemagen(t, "foobar")

	const goldenFile = "testdata/models.go.txt"
	if *flagUpdate {
		if err := ioutil.WriteFile(goldenFile, b, os.ModePerm); err != nil {
			t.Fatal(err)
		}
	}
	golden, err := ioutil.ReadFile(goldenFile)
	if err != nil {
		t.Fatal(err)
	}

	if diff := cmp.Diff(string(golden), string(b)); diff != "" {
		t.Fatalf(diff)
	}
}

func Test_usedInTables(t *testing.T) {
	tests := map[string]struct {
		columnValidator string
		typeName        string
	}{
		"matches given a frozen collection": {
			columnValidator: "frozen<album>",
			typeName:        "album",
		},
		"matches given a set": {
			columnValidator: "set<artist>",
			typeName:        "artist",
		},
		"matches given a list": {
			columnValidator: "list<song>",
			typeName:        "song",
		},
		"matches given a tuple: first of two elements": {
			columnValidator: "tuple<first, second>",
			typeName:        "first",
		},
		"matches given a tuple: second of two elements": {
			columnValidator: "tuple<first, second>",
			typeName:        "second",
		},
		"matches given a tuple: first of three elements": {
			columnValidator: "tuple<first, second, third>",
			typeName:        "first",
		},
		"matches given a tuple: second of three elements": {
			columnValidator: "tuple<first, second, third>",
			typeName:        "second",
		},
		"matches given a tuple: third of three elements": {
			columnValidator: "tuple<first, second, third>",
			typeName:        "third",
		},
		"matches given a frozen set": {
			columnValidator: "set<frozen<album>>",
			typeName:        "album",
		},
		"matches snake_case names given a nested map": {
			columnValidator: "map<album, tuple<first, map<map_key, map-value>, third>>",
			typeName:        "map_key",
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			tables := map[string]*gocql.TableMetadata{
				"table": {Columns: map[string]*gocql.ColumnMetadata{
					"column": {Type: tt.columnValidator},
				}},
			}
			if !usedInTables(tt.typeName, tables) {
				t.Fatal()
			}
		})
	}

	t.Run("doesn't panic with empty type name", func(t *testing.T) {
		tables := map[string]*gocql.TableMetadata{
			"table": {Columns: map[string]*gocql.ColumnMetadata{
				"column": {Type: "map<text, album>"},
			}},
		}
		usedInTables("", tables)
	})
}

func createTestSchema(t *testing.T) {
	t.Helper()

	session := gocqlxtest.CreateSession(t)
	defer session.Close()

	err := session.ExecStmt(`CREATE KEYSPACE IF NOT EXISTS schemagen WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}`)
	if err != nil {
		t.Fatal("create keyspace:", err)
	}

	err = session.ExecStmt(`CREATE TABLE IF NOT EXISTS schemagen.songs (
		id uuid PRIMARY KEY,
		title text,
		album text,
		artist text,
		tags set<text>,
		data blob)`)
	if err != nil {
		t.Fatal("create table:", err)
	}

	err = session.ExecStmt(`CREATE TYPE IF NOT EXISTS schemagen.album (
		name text,
		songwriters set<text>,)`)
	if err != nil {
		t.Fatal("create type:", err)
	}

	err = session.ExecStmt(`CREATE TABLE IF NOT EXISTS schemagen.playlists (
		id uuid,
		title text,
		album frozen<album>, 
		artist text,
		song_id uuid,
		PRIMARY KEY (id, title, album, artist))`)
	if err != nil {
		t.Fatal("create table:", err)
	}

	err = session.ExecStmt(`CREATE INDEX IF NOT EXISTS songs_title ON schemagen.songs (title)`)
	if err != nil {
		t.Fatal("create index:", err)
	}

	err = session.ExecStmt(`CREATE TABLE IF NOT EXISTS schemagen.composers (
		id uuid PRIMARY KEY,
		name text)`)
	if err != nil {
		t.Fatal("create table:", err)
	}

	err = session.ExecStmt(`CREATE MATERIALIZED VIEW IF NOT EXISTS schemagen.composers_by_name AS
    	SELECT id, name
    	FROM composers
    	WHERE id IS NOT NULL AND name IS NOT NULL
    	PRIMARY KEY (id, name)`)
	if err != nil {
		t.Fatal("create view:", err)
	}

	err = session.ExecStmt(`CREATE TYPE IF NOT EXISTS schemagen.label (
		name text,
		artists set<text>)`)
	if err != nil {
		t.Fatal("create type:", err)
	}
}

func runSchemagen(t *testing.T, pkgname string) []byte {
	t.Helper()

	dir, err := os.MkdirTemp("", "gocqlx")
	if err != nil {
		t.Fatal(err)
	}
	keyspace := "schemagen"

	flagKeyspace = &keyspace
	flagPkgname = &pkgname
	flagOutput = &dir

	if err := schemagen(); err != nil {
		t.Fatalf("schemagen() error %s", err)
	}

	f := fmt.Sprintf("%s/%s.go", dir, pkgname)
	b, err := os.ReadFile(f)
	if err != nil {
		t.Fatalf("%s: %s", f, err)
	}
	return b
}
