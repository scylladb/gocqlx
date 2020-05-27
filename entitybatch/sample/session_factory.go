package sample

import (
	"time"

	"github.com/gocql/gocql"
)

var (
	cluster_config *gocql.ClusterConfig
	Cql_Session    *gocql.Session
)

func NewSession() (err error) {
	cluster_config = gocql.NewCluster("127.0.0.1:9042")
	cluster_config.Keyspace = "kaicloud_dev"
	cluster_config.Consistency = gocql.Quorum
	cluster_config.ProtoVersion = 4
	cluster_config.ConnectTimeout = time.Second * 20
	cluster_config.Timeout = time.Second * 20

	Cql_Session, err = cluster_config.CreateSession()

	return
}
