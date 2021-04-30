// Copyright (C) 2017 ScyllaDB
// Use of this source code is governed by a ALv2-style
// license that can be found in the LICENSE file.

// +build all integration

package example

import (
	"context"
	"testing"

	"github.com/scylladb/gocqlx/v2"
	"github.com/scylladb/gocqlx/v2/gocqlxtest"
	"github.com/scylladb/gocqlx/v2/migrate"
	"github.com/scylladb/gocqlx/v2/migrate/example/cql"
)

// Running examples locally:
// make run-scylla
// make run-examples
func TestExample(t *testing.T) {
	const ks = "migrate_example"

	// Create keyspace
	cluster := gocqlxtest.CreateCluster()
	cluster.Keyspace = ks
	if err := gocqlxtest.CreateKeyspace(cluster, ks); err != nil {
		t.Fatal("CreateKeyspace:", err)
	}

	// Create session using the keyspace
	session, err := gocqlx.WrapSession(cluster.CreateSession())
	if err != nil {
		t.Fatal("CreateSession:", err)
	}
	defer session.Close()

	// Add callback prints
	log := func(ctx context.Context, session gocqlx.Session, ev migrate.CallbackEvent, name string) error {
		t.Log(ev, name)
		return nil
	}
	reg := migrate.CallbackRegister{}
	reg.Add(migrate.BeforeMigration, "m1.cql", log)
	reg.Add(migrate.AfterMigration, "m1.cql", log)
	reg.Add(migrate.CallComment, "1", log)
	reg.Add(migrate.CallComment, "2", log)
	reg.Add(migrate.CallComment, "3", log)
	migrate.Callback = reg.Callback

	// First run prints data
	if err := migrate.FromFS(context.Background(), session, cql.Files); err != nil {
		t.Fatal("Migrate:", err)
	}

	// Second run skips the processed files
	if err := migrate.FromFS(context.Background(), session, cql.Files); err != nil {
		t.Fatal("Migrate:", err)
	}
}
