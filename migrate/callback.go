// Copyright (C) 2017 ScyllaDB
// Use of this source code is governed by a ALv2-style
// license that can be found in the LICENSE file.

package migrate

import (
	"context"
	"errors"

	"github.com/scylladb/gocqlx/v3"
)

// CallbackEvent specifies type of the event when calling CallbackFunc.
type CallbackEvent uint8

// enumeration of CallbackEvents
const (
	BeforeMigration CallbackEvent = iota
	AfterMigration
	CallComment
)

// CallbackFunc enables execution of arbitrary Go code during migration.
// If error is returned the migration is aborted.
// BeforeMigration is triggered before processing each migration file and again
// before processing its remaining statements after a partial migration resumes.
// AfterMigration is attempted after the final statement's progress is recorded
// and is not retried if it returns an error, including one caused by context
// cancellation or deadline expiry.
// CallComment is triggered for each comment in a form `-- CALL <name>;` (note the semicolon).
// BeforeMigration and AfterMigration receive the caller's context. CallComment
// receives a context detached from caller cancellation and deadlines so a callback
// that is part of a started migration operation completes with its progress update.
type CallbackFunc func(ctx context.Context, session gocqlx.Session, ev CallbackEvent, name string) error

// Callback is means of executing Go code during migrations.
// Use this variable to register a global callback dispatching function.
// See CallbackFunc for details.
var Callback CallbackFunc

type nameEvent struct {
	name  string
	event CallbackEvent
}

// CallbackRegister allows to register a handlers for an event type and a name.
// It dispatches calls to the registered handlers.
// If there is no handler registered for CallComment an error is returned.
type CallbackRegister map[nameEvent]CallbackFunc

// Add registers a callback handler.
func (r CallbackRegister) Add(ev CallbackEvent, name string, f CallbackFunc) {
	r[nameEvent{name, ev}] = f
}

// Find returns the registered handler.
func (r CallbackRegister) Find(ev CallbackEvent, name string) CallbackFunc {
	return r[nameEvent{name, ev}]
}

// Callback is CallbackFunc.
func (r CallbackRegister) Callback(ctx context.Context, session gocqlx.Session, ev CallbackEvent, name string) error {
	f, ok := r[nameEvent{name, ev}]
	if !ok {
		if ev == CallComment {
			return errors.New("missing handler")
		}
		return nil
	}
	return f(ctx, session, ev, name)
}
