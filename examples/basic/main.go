package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/gocql/gocql"

	"github.com/scylladb/gocqlx/v3"
	"github.com/scylladb/gocqlx/v3/table"
)

const schemaAgreementTimeout = 10 * time.Second

type Person struct {
	FirstName string
	LastName  string
	ID        gocql.UUID
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	hosts := flag.String("hosts", "127.0.0.1:9042", "comma-separated Scylla/Cassandra 4+ hosts")
	keyspace := flag.String("keyspace", "gocqlx_example", "example keyspace")
	flag.Parse()

	quotedKeyspace, err := quoteKeyspace(*keyspace)
	if err != nil {
		return err
	}

	cluster := gocql.NewCluster(splitHosts(*hosts)...)
	cluster.Timeout = 5 * time.Second
	cluster.MaxWaitSchemaAgreement = schemaAgreementTimeout

	session, err := gocqlx.WrapSession(cluster.CreateSession())
	if err != nil {
		return fmt.Errorf("create session: %w", err)
	}
	defer session.Close()

	if err := createSchema(session, quotedKeyspace); err != nil {
		return fmt.Errorf("create schema: %w", err)
	}
	schemaCtx, cancel := context.WithTimeout(context.Background(), schemaAgreementTimeout)
	defer cancel()
	if err := session.AwaitSchemaAgreement(schemaCtx); err != nil {
		return fmt.Errorf("await schema agreement: %w", err)
	}

	people := newPeopleTable(quotedKeyspace)

	person := Person{
		ID:        gocql.TimeUUID(),
		FirstName: "Ada",
		LastName:  "Lovelace",
	}
	if err := people.InsertQuery(session).BindStruct(person).ExecRelease(); err != nil {
		return fmt.Errorf("insert person: %w", err)
	}

	var rows []Person
	if err := people.SelectQuery(session).BindStruct(person).SelectRelease(&rows); err != nil {
		return fmt.Errorf("select people: %w", err)
	}
	if len(rows) != 1 {
		return fmt.Errorf("select person: got %d rows, want 1", len(rows))
	}
	if rows[0] != person {
		return fmt.Errorf("select person: got %+v, want %+v", rows[0], person)
	}
	row := rows[0]
	//nolint:forbidigo // This example prints the selected row for the user.
	fmt.Printf("%s %s %s\n", row.ID, row.FirstName, row.LastName)
	return nil
}

func splitHosts(hosts string) []string {
	parts := strings.Split(hosts, ",")
	out := parts[:0]
	for _, part := range parts {
		if host := strings.TrimSpace(part); host != "" {
			out = append(out, host)
		}
	}
	return out
}

func quoteKeyspace(keyspace string) (string, error) {
	if keyspace == "" || len(keyspace) > 48 {
		return "", fmt.Errorf("keyspace %q must contain 1-48 ASCII letters, digits, or underscores", keyspace)
	}
	for _, r := range keyspace {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_' {
			continue
		}
		return "", fmt.Errorf("keyspace %q must contain 1-48 ASCII letters, digits, or underscores", keyspace)
	}
	return `"` + keyspace + `"`, nil
}

func newPeopleTable(quotedKeyspace string) *table.Table {
	return table.New(table.Metadata{
		Name:    quotedKeyspace + ".people",
		Columns: []string{"id", "first_name", "last_name"},
		PartKey: []string{"id"},
	})
}

type execStmtSession interface {
	ExecStmt(stmt string) error
}

func createSchema(session execStmtSession, quotedKeyspace string) error {
	// Scylla and Cassandra 4+ expand replication_factor to every data center.
	stmt := fmt.Sprintf(`CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}`, quotedKeyspace)
	if err := session.ExecStmt(stmt); err != nil {
		return err
	}
	return session.ExecStmt(fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s.people (
		id uuid PRIMARY KEY,
		first_name text,
		last_name text)`, quotedKeyspace))
}
