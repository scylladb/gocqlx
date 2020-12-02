// Copyright (C) 2017 ScyllaDB
// Use of this source code is governed by a ALv2-style
// license that can be found in the LICENSE file.

package migrate

import (
	"context"

	"github.com/scylladb/gocqlx/v2"
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
// BeforeMigration and AfterMigration are triggered before and after processing
// of each migration file respectively.
// CallComment is triggered for each comment in a form `-- CALL <name>;` (note the semicolon).
type CallbackFunc func(ctx context.Context, session gocqlx.Session, ev CallbackEvent, name string) error

// Callback is means of executing Go code during migrations.
// Use this variable to register a global callback dispatching function.
// See CallbackFunc for details.
var Callback CallbackFunc
