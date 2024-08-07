// test_setup.go is intended to be used by all the tests inside data/db
// this way we can easily setup and teardown stuff inside a func TestMain(...)
// DO NOT USE THE EXPORTED FUNCIONS OUTSIDE OF A TEST
package sample

import (
	"errors"
)

func Setup() error {
	NewSession()
	return nil
}

func Execute_Statement(stmt string) error {

	if len(stmt) == 0 {
		return errors.New("statement is required")
	}

	q := Cql_Session.Query(stmt)
	defer q.Release()
	return q.Exec()
}
