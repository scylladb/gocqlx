// Copyright (C) 2017 ScyllaDB
// Use of this source code is governed by a ALv2-style
// license that can be found in the LICENSE file.

package gocqlxtest

import (
	"errors"
	"reflect"
	"testing"
)

func TestExecCreateKeyspaceStmtRetriesWithTabletsDisabled(t *testing.T) {
	tests := []struct {
		name string
		stmt string
		want []string
	}{
		{
			name: "create keyspace",
			stmt: `CREATE KEYSPACE gocqlx_test WITH replication = {'class' : 'SimpleStrategy', 'replication_factor' : 1}`,
			want: []string{
				`CREATE KEYSPACE gocqlx_test WITH replication = {'class' : 'SimpleStrategy', 'replication_factor' : 1}`,
				`CREATE KEYSPACE gocqlx_test WITH replication = {'class' : 'SimpleStrategy', 'replication_factor' : 1} AND tablets = {'enabled': false}`,
			},
		},
		{
			name: "create keyspace if not exists",
			stmt: `CREATE KEYSPACE IF NOT EXISTS gocqlx_test WITH replication = {'class' : 'SimpleStrategy', 'replication_factor' : 1};`,
			want: []string{
				`CREATE KEYSPACE IF NOT EXISTS gocqlx_test WITH replication = {'class' : 'SimpleStrategy', 'replication_factor' : 1};`,
				`CREATE KEYSPACE IF NOT EXISTS gocqlx_test WITH replication = {'class' : 'SimpleStrategy', 'replication_factor' : 1} AND tablets = {'enabled': false}`,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			session := &recordingExecStmtSession{
				errs: []error{
					errors.New("SimpleStrategy doesn't support tablet replication"),
					nil,
				},
			}

			if err := execCreateKeyspaceStmt(session, tt.stmt); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(session.stmts, tt.want) {
				t.Fatalf("ExecStmt calls = %#v, want %#v", session.stmts, tt.want)
			}
		})
	}
}

type recordingExecStmtSession struct {
	stmts []string
	errs  []error
}

func (s *recordingExecStmtSession) ExecStmt(stmt string) error {
	s.stmts = append(s.stmts, stmt)
	if len(s.errs) == 0 {
		return nil
	}

	err := s.errs[0]
	s.errs = s.errs[1:]
	return err
}
