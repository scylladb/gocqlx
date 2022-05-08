package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/scylladb/gocqlx/v2/gocqlxtest"
)

var flagUpdate = flag.Bool("update", false, "update golden file")

func TestSchemagen(t *testing.T) {
	flag.Parse()
	createTestSchema(t)
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
