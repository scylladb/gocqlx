// Copyright (C) 2017 ScyllaDB
// Use of this source code is governed by a ALv2-style
// license that can be found in the LICENSE file.

package gocqlx

import (
	"context"

	"github.com/gocql/gocql"
	"github.com/scylladb/go-reflectx"
)

// Session wraps gocql.Session and provides a modified Query function that
// returns Queryx instance.
// The original Session instance can be accessed as Session.
// The default mapper uses `db` tag and automatically converts struct field
// names to snake case. If needed package reflectx provides constructors
// for other types of mappers.
type Session struct {
	*gocql.Session
	Mapper *reflectx.Mapper
}

// NewSession wraps existing gocql.session.
func NewSession(session *gocql.Session) Session {
	return Session{
		Session: session,
		Mapper:  DefaultMapper,
	}
}

// WrapSession should be called on CreateSession() gocql function to convert
// the created session to gocqlx.Session.
//
// Example:
//     session, err := gocqlx.WrapSession(cluster.CreateSession())
func WrapSession(session *gocql.Session, err error) (Session, error) {
	return Session{
		Session: session,
		Mapper:  DefaultMapper,
	}, err
}

// ContextQuery is a helper function that allows to pass context when creating
// a query, see the "Query" function .
func (s Session) ContextQuery(ctx context.Context, stmt string, names []string) *Queryx {
	return &Queryx{
		Query:  s.Session.Query(stmt).WithContext(ctx),
		Names:  names,
		Mapper: s.Mapper,
	}
}

// Query creates a new Queryx using the session mapper.
// The stmt and names parameters are typically result of a query builder
// (package qb) ToCql() function or come from table model (package table).
// The names parameter is a list of query parameters' names and it's used for
// binding.
func (s Session) Query(stmt string, names []string) *Queryx {
	return &Queryx{
		Query:  s.Session.Query(stmt),
		Names:  names,
		Mapper: s.Mapper,
	}
}

// ExecStmt creates query and executes the given statement.
func (s Session) ExecStmt(stmt string) error {
	return s.Query(stmt, nil).ExecRelease()
}
