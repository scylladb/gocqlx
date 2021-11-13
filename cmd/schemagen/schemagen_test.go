package main

import (
	"fmt"
	"github.com/scylladb/gocqlx/v2/gocqlxtest"
	"io"
	"os"
	"os/exec"
	"path"
	"strings"
	"testing"
)

func TestCamelize(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"hello", "Hello"},
		{"_hello", "Hello"},
		{"__hello", "Hello"},
		{"hello_", "Hello"},
		{"hello_world", "HelloWorld"},
		{"hello__world", "HelloWorld"},
		{"_hello_world", "HelloWorld"},
		{"helloWorld", "HelloWorld"},
		{"HelloWorld", "HelloWorld"},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			if got := camelize(tt.input); got != tt.want {
				t.Errorf("camelize() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_schemagen_defaultParams(t *testing.T) {
	cleanup(t, "models")
	defer cleanup(t, "models")
	createTestSchema(t)
	runSchemagen(t, "", "")
	assertResult(t, "models", "models")
}

func Test_schemagen_customParams(t *testing.T) {
	cleanup(t, "asdf")
	defer cleanup(t, "asdf")
	createTestSchema(t)
	runSchemagen(t, "qwer", "asdf")
	assertResult(t, "qwer", "asdf")
}

func cleanup(t *testing.T, output string) {
	err := os.RemoveAll(output)
	if err != nil {
		t.Fatalf("could not delete %s directory: %v\n", output, err)
	}

	err = os.Remove("./schemagen")
	if err != nil {
		t.Fatalf("could not delete binary: %v\n", err)
	}

	cmd := exec.Command("go", "build")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("could not build binary for schemagen: %v\nOutput:\n%v\n", err, string(out))
	}
}

func createTestSchema(t *testing.T) {
	session := gocqlxtest.CreateSession(t)
	defer session.Close()

	err := session.ExecStmt(`CREATE KEYSPACE IF NOT EXISTS examples WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}`)
	if err != nil {
		t.Fatal("create keyspace:", err)
	}

	err = session.ExecStmt(`CREATE TABLE IF NOT EXISTS examples.songs (
		id uuid PRIMARY KEY,
		title text,
		album text,
		artist text,
		tags set<text>,
		data blob)`)
	if err != nil {
		t.Fatal("create table:", err)
	}

	err = session.ExecStmt(`CREATE TABLE IF NOT EXISTS examples.playlists (
		id uuid,
		title text,
		album text, 
		artist text,
		song_id uuid,
		PRIMARY KEY (id, title, album, artist))`)
	if err != nil {
		t.Fatal("create table:", err)
	}
}

func runSchemagen(t *testing.T, pkgname, output string) {
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}

	args := []string{"-keyspace=examples"}
	for _, arg := range os.Args {
		if strings.HasPrefix(arg, "-cluster") {
			args = append(args, arg)
		}
	}

	if pkgname != "" {
		args = append(args, fmt.Sprintf("-pkgname=%s", pkgname))
	}

	if output != "" {
		args = append(args, fmt.Sprintf("-output=%s", output))
	}

	cmd := exec.Command(path.Join(dir, "schemagen"), args...)
	err = cmd.Run()
	if err != nil {
		t.Fatal(err)
	}
}

func assertResult(t *testing.T, pkgname, output string) {
	path := fmt.Sprintf("%s/%s.go", output, pkgname)
	res, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("can't read output file (%s): %s\n", path, err)
	}

	want := resultWant(t, pkgname)

	if string(res) != want {
		t.Fatalf("unexpected result: %s\nWanted:\n%s\n", string(res), want)
	}
}

func resultWant(t *testing.T, pkgname string) string {
	f, err := os.Open("testdata/models.go.txt")
	if err != nil {
		t.Fatalf("can't open testdata/models.go.txt")
	}
	defer f.Close()

	b, err := io.ReadAll(f)
	if err != nil {
		t.Fatalf("can't read testdata/models.go.txt")
	}

	return strings.Replace(string(b), "{{pkgname}}", pkgname, 1)
}
