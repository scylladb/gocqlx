// Copyright (C) 2026 ScyllaDB
// Use of this source code is governed by a ALv2-style
// license that can be found in the LICENSE file.

//go:build all || integration
// +build all integration

package gocqlx_test

import (
	"testing"
	"time"

	"github.com/gocql/gocql"

	"github.com/scylladb/gocqlx/v3"
	"github.com/scylladb/gocqlx/v3/gocqlxtest"
)

func TestProtocolV5Options(t *testing.T) {
	cluster := gocqlxtest.CreateCluster()
	if cluster.ProtoVersion < 5 {
		t.Skip("run with -proto=5 to test protocol v5 query and batch options")
	}

	const keyspace = "gocqlx_v5_options"
	if err := gocqlxtest.CreateKeyspace(cluster, keyspace); err != nil {
		t.Fatal(err)
	}
	// Keep the session without a keyspace so every unqualified query needs the override.
	session, err := gocqlx.WrapSession(cluster.CreateSession())
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	if err := session.ExecStmt("CREATE TABLE " + keyspace + ".options (id int PRIMARY KEY, value text)"); err != nil {
		t.Fatal(err)
	}

	now := int(time.Now().Unix())
	t.Run("query", func(t *testing.T) {
		err := session.Query("INSERT INTO options (id, value) VALUES (?, ?) USING TTL 20", []string{"id", "value"}).
			SetKeyspace(keyspace).WithNowInSeconds(now).BindMap(map[string]interface{}{"id": 1, "value": "query"}).ExecRelease()
		if err != nil {
			t.Fatal(err)
		}
	})
	t.Run("batch", func(t *testing.T) {
		batch := session.Batch(gocql.LoggedBatch).SetKeyspace(keyspace).WithNowInSeconds(now).
			Query("INSERT INTO options (id, value) VALUES (?, ?) USING TTL 20", 2, "batch")
		if err := session.ExecuteBatch(batch); err != nil {
			t.Fatal(err)
		}
	})

	for _, id := range []int{1, 2} {
		var ttl int
		err := session.Query("SELECT TTL(value) FROM options WHERE id = ?", []string{"id"}).
			SetKeyspace(keyspace).WithNowInSeconds(now + 10).Bind(id).Scan(&ttl)
		if err != nil {
			t.Fatal(err)
		}
		if ttl != 10 {
			t.Errorf("id %d: remaining TTL = %d, want 10", id, ttl)
		}
	}
}
