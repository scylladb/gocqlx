// Copyright (C) 2017 ScyllaDB
// Use of this source code is governed by a ALv2-style
// license that can be found in the LICENSE file.

package gocqlxtest

import (
	"flag"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/scylladb/gocqlx/v2"
)

var (
	flagCluster      = flag.String("cluster", "127.0.0.1", "a comma-separated list of host:port tuples")
	flagKeyspace     = flag.String("keyspace", "gocqlx_test", "keyspace name")
	flagProto        = flag.Int("proto", 0, "protcol version")
	flagCQL          = flag.String("cql", "3.0.0", "CQL version")
	flagRF           = flag.Int("rf", 1, "replication factor for test keyspace")
	flagRetry        = flag.Int("retries", 5, "number of times to retry queries")
	flagCompressTest = flag.String("compressor", "", "compressor to use")
	flagTimeout      = flag.Duration("gocql.timeout", 5*time.Second, "sets the connection `timeout` for all operations")
)

var initOnce sync.Once

// CreateSession creates a new gocqlx session from flags.
func CreateSession(tb testing.TB) gocqlx.Session {
	cluster := CreateCluster()
	return createSessionFromCluster(cluster, tb)
}

// CreateCluster creates gocql ClusterConfig from flags.
func CreateCluster() *gocql.ClusterConfig {
	if !flag.Parsed() {
		flag.Parse()
	}
	clusterHosts := strings.Split(*flagCluster, ",")

	cluster := gocql.NewCluster(clusterHosts...)
	cluster.ProtoVersion = *flagProto
	cluster.CQLVersion = *flagCQL
	cluster.Timeout = *flagTimeout
	cluster.Consistency = gocql.Quorum
	cluster.MaxWaitSchemaAgreement = 2 * time.Minute // travis might be slow
	if *flagRetry > 0 {
		cluster.RetryPolicy = &gocql.SimpleRetryPolicy{NumRetries: *flagRetry}
	}

	switch *flagCompressTest {
	case "snappy":
		cluster.Compressor = &gocql.SnappyCompressor{}
	case "":
	default:
		panic("invalid compressor: " + *flagCompressTest)
	}

	return cluster
}

// CreateKeyspace creates keyspace with SimpleStrategy and RF derived from flags.
func CreateKeyspace(cluster *gocql.ClusterConfig, keyspace string) error {
	c := *cluster
	c.Keyspace = "system"
	c.Timeout = 30 * time.Second

	session, err := gocqlx.WrapSession(c.CreateSession())
	if err != nil {
		return err
	}
	defer session.Close()

	{
		err := session.ExecStmt(`DROP KEYSPACE IF EXISTS ` + keyspace)
		if err != nil {
			return fmt.Errorf("drop keyspace: %w", err)
		}
	}

	{
		err := session.ExecStmt(fmt.Sprintf(`CREATE KEYSPACE %s WITH replication = {'class' : 'SimpleStrategy', 'replication_factor' : %d}`, keyspace, *flagRF))
		if err != nil {
			return fmt.Errorf("create keyspace: %w", err)
		}
	}

	return nil
}

func createSessionFromCluster(cluster *gocql.ClusterConfig, tb testing.TB) gocqlx.Session {
	if !flag.Parsed() {
		flag.Parse()
	}
	// Drop and re-create the keyspace once. Different tests should use their own
	// individual tables, but can assume that the table does not exist before.
	initOnce.Do(func() {
		if err := CreateKeyspace(cluster, *flagKeyspace); err != nil {
			tb.Fatal(err)
		}
	})

	cluster.Keyspace = *flagKeyspace
	session, err := gocqlx.WrapSession(cluster.CreateSession())
	if err != nil {
		tb.Fatal("CreateSession:", err)
	}
	return session
}
