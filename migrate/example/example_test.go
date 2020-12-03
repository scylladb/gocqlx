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
)

// Running examples locally:
// make run-scylla
// make run-examples
func TestExample(t *testing.T) {
	const ks = "migrate_example"

	cluster := gocqlxtest.CreateCluster()
	cluster.Keyspace = ks

	if err := gocqlxtest.CreateKeyspace(cluster, ks); err != nil {
		t.Fatal("CreateKeyspace:", err)
	}
	session, err := gocqlx.WrapSession(cluster.CreateSession())
	if err != nil {
		t.Fatal("CreateSession:", err)
	}
	defer session.Close()

	// Add callback prints
	printEvent := func(ctx context.Context, session gocqlx.Session, ev migrate.CallbackEvent, name string) error {
		t.Log(ev, name)
		return nil
	}

	reg := migrate.CallbackRegister{}
	reg.Add(migrate.BeforeMigration, "m1.cql", printEvent)
	reg.Add(migrate.AfterMigration, "m1.cql", printEvent)
	reg.Add(migrate.CallComment, "1", printEvent)
	reg.Add(migrate.CallComment, "2", printEvent)
	reg.Add(migrate.CallComment, "3", printEvent)

	migrate.Callback = reg.Callback

	// First run prints data
	if err := migrate.Migrate(context.Background(), session, "migrations"); err != nil {
		t.Fatal("Migrate:", err)
	}

	// Second run skips the processed files
	if err := migrate.Migrate(context.Background(), session, "migrations"); err != nil {
		t.Fatal("Migrate:", err)
	}
}
