package main

import (
	"strings"
	"testing"
)

func TestCreateSchema(t *testing.T) {
	session := &recordingExecStmtSession{}

	if err := createSchema(session, `"example"`); err != nil {
		t.Fatal(err)
	}

	want := []string{
		`CREATE KEYSPACE IF NOT EXISTS "example" WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}`,
		`CREATE TABLE IF NOT EXISTS "example".people (
		id uuid PRIMARY KEY,
		first_name text,
		last_name text)`,
	}
	if len(session.stmts) != len(want) {
		t.Fatalf("ExecStmt call count = %d, want %d", len(session.stmts), len(want))
	}
	for i := range want {
		if normalizeWhitespace(session.stmts[i]) != normalizeWhitespace(want[i]) {
			t.Errorf("ExecStmt call %d = %q, want %q", i, session.stmts[i], want[i])
		}
	}
}

func normalizeWhitespace(s string) string {
	return strings.Join(strings.Fields(s), " ")
}

func TestQuoteKeyspace(t *testing.T) {
	tests := []struct {
		name     string
		keyspace string
		want     string
		wantErr  bool
	}{
		{
			name:     "lowercase",
			keyspace: "example",
			want:     `"example"`,
		},
		{
			name:     "mixed case reserved word",
			keyspace: "Select",
			want:     `"Select"`,
		},
		{
			name:     "maximum length",
			keyspace: strings.Repeat("a", 48),
			want:     `"` + strings.Repeat("a", 48) + `"`,
		},
		{
			name:    "empty",
			wantErr: true,
		},
		{
			name:     "too long",
			keyspace: strings.Repeat("a", 49),
			wantErr:  true,
		},
		{
			name:     "invalid character",
			keyspace: "example;DROP_KEYSPACE",
			wantErr:  true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := quoteKeyspace(test.keyspace)
			if (err != nil) != test.wantErr {
				t.Fatalf("quoteKeyspace(%q) error = %v, wantErr %v", test.keyspace, err, test.wantErr)
			}
			if got != test.want {
				t.Fatalf("quoteKeyspace(%q) = %q, want %q", test.keyspace, got, test.want)
			}
		})
	}
}

func TestPeopleTableUsesQuotedKeyspace(t *testing.T) {
	quotedKeyspace, err := quoteKeyspace("Team")
	if err != nil {
		t.Fatal(err)
	}

	stmt, _ := newPeopleTable(quotedKeyspace).Insert()
	want := `INSERT INTO "Team".people (id,first_name,last_name) VALUES (?,?,?) `
	if stmt != want {
		t.Fatalf("Insert statement = %q, want %q", stmt, want)
	}
}

type recordingExecStmtSession struct {
	stmts []string
}

func (s *recordingExecStmtSession) ExecStmt(stmt string) error {
	s.stmts = append(s.stmts, stmt)
	return nil
}
