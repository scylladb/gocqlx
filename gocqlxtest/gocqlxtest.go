// Copyright (C) 2017 ScyllaDB
// Use of this source code is governed by a ALv2-style
// license that can be found in the LICENSE file.

package gocqlxtest

import (
	"flag"
	"fmt"
	"log"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gocql/gocql"
)

var (
	flagCluster      = flag.String("cluster", "127.0.0.1", "a comma-separated list of host:port tuples")
	flagProto        = flag.Int("proto", 0, "protcol version")
	flagCQL          = flag.String("cql", "3.0.0", "CQL version")
	flagRF           = flag.Int("rf", 1, "replication factor for test keyspace")
	flagRetry        = flag.Int("retries", 5, "number of times to retry queries")
	flagCompressTest = flag.String("compressor", "", "compressor to use")
	flagTimeout      = flag.Duration("gocql.timeout", 5*time.Second, "sets the connection `timeout` for all operations")

	clusterHosts []string
)

func init() {
	flag.Parse()
	clusterHosts = strings.Split(*flagCluster, ",")
	log.SetFlags(log.Lshortfile | log.LstdFlags)
}

var initOnce sync.Once

// CreateSession creates a new gocql session from flags.
func CreateSession(tb testing.TB) *gocql.Session {
	cluster := createCluster()
	return createSessionFromCluster(cluster, tb)
}

func createCluster() *gocql.ClusterConfig {
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

func createSessionFromCluster(cluster *gocql.ClusterConfig, tb testing.TB) *gocql.Session {
	// Drop and re-create the keyspace once. Different tests should use their own
	// individual tables, but can assume that the table does not exist before.
	initOnce.Do(func() {
		createKeyspace(tb, cluster, "gocqlx_test")
	})

	cluster.Keyspace = "gocqlx_test"
	session, err := cluster.CreateSession()
	if err != nil {
		tb.Fatal("CreateSession:", err)
	}

	return session
}

func createKeyspace(tb testing.TB, cluster *gocql.ClusterConfig, keyspace string) {
	c := *cluster
	c.Keyspace = "system"
	c.Timeout = 30 * time.Second
	session, err := c.CreateSession()
	if err != nil {
		panic(err)
	}
	defer session.Close()

	err = ExecStmt(session, `DROP KEYSPACE IF EXISTS `+keyspace)
	if err != nil {
		panic(fmt.Sprintf("unable to drop keyspace: %v", err))
	}

	err = ExecStmt(session, fmt.Sprintf(`CREATE KEYSPACE %s
	WITH replication = {
		'class' : 'SimpleStrategy',
		'replication_factor' : %d
	}`, keyspace, *flagRF))

	if err != nil {
		panic(fmt.Sprintf("unable to create keyspace: %v", err))
	}
}

// ExecStmt executes a statement string.
func ExecStmt(s *gocql.Session, stmt string) error {
	q := s.Query(stmt).RetryPolicy(nil)
	defer q.Release()
	return q.Exec()
}
