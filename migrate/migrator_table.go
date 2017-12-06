package migrate

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/gocql/gocql"
	"github.com/scylladb/gocqlx"
	"github.com/scylladb/gocqlx/qb"
)

const (
	infoSchema = `CREATE TABLE IF NOT EXISTS gocqlx_migrate (
	name text,
	checksum text,
	done tinyint,
	start_time timestamp,
	end_time timestamp,
	PRIMARY KEY(name)
)`
	selectInfo = "SELECT * FROM gocqlx_migrate"
)

// MigratorTable interface operates on the gocqlx_migrate table.
// Tests can substitute a mock implementation.
type MigratorTable interface {
	List(ctx context.Context) ([]*Info, error)
	Execute(ctx context.Context, stmt string, info Info) error
}

type cassandraSessionMigrator struct {
	session *gocql.Session
	stmt    string
	names   []string
	iq      *gocqlx.Queryx
}

// ErrNoSessionProvided is returned when trying to create a Cassandra migrator without a session.
var ErrNoSessionProvided = errors.New("no session provided to the Cassandra migrator")

// ErrInvalidCqlStmt is returned when trying to execute a non valid cql statment
var ErrInvalidCqlStmt = errors.New("invalid cql statment")

// ErrNoCqlStmt is returned when trying to execute a non cql statment i.e. "This is not cql"
var ErrNoCqlStmt = errors.New("no cql statment found")

// NewCassandraSessionMigrator creates a migrator that persits into and reads from a Cassandra database.
func NewCassandraSessionMigrator(session *gocql.Session) (MigratorTable, error) {
	if session == nil {
		return nil, ErrNoSessionProvided
	}
	csm := &cassandraSessionMigrator{session: session}
	csm.stmt, csm.names = qb.Insert("gocqlx_migrate").Columns(
		"name",
		"checksum",
		"done",
		"start_time",
		"end_time",
	).ToCql()
	csm.iq = gocqlx.Query(csm.session.Query(csm.stmt) /*.WithContext(ctx)*/, csm.names)
	return csm, nil
}

// Info contains information on migration applied on a database.
type Info struct {
	Name      string
	Checksum  string
	Done      int
	StartTime time.Time
	EndTime   time.Time
}

// List returns already applied migrations.
func (csm *cassandraSessionMigrator) List(ctx context.Context) (v []*Info, err error) {
	if err = ensureInfoTable(ctx, csm.session); err != nil {
		return nil, err
	}

	if err = gocqlx.Select(&v, csm.session.Query(selectInfo).WithContext(ctx)); err != nil {
		if err == gocql.ErrNotFound {
			return nil, nil
		}
		return nil, err
	}

	sort.Slice(v, func(i, j int) bool {
		return v[i].Name < v[j].Name
	})
	return v, err
}

func ensureInfoTable(ctx context.Context, session *gocql.Session) error {
	return gocqlx.Query(session.Query(infoSchema).WithContext(ctx), nil).ExecRelease()
}

// Execute applies a single statement from a migration and, if successful, records it in the migrations table.
func (csm *cassandraSessionMigrator) Execute(ctx context.Context, stmt string, info Info) error {
	// execute
	q := gocqlx.Query(csm.session.Query(stmt).RetryPolicy(nil).WithContext(ctx), nil)
	if err := q.ExecRelease(); err != nil {
		return fmt.Errorf("statement %d failed: %s", info.Done, err)
	}

	// update info
	info.EndTime = time.Now()
	if err := csm.iq.BindStruct(info).Exec(); err != nil {
		return ErrInvalidCqlStmt
	}
	return nil
}
