// Copyright (C) 2017 ScyllaDB
// Use of this source code is governed by a ALv2-style
// license that can be found in the LICENSE file.

package migrate

import (
	"context"

	"github.com/gocql/gocql"
)

// CallbackEvent specifies type of the event when calling CallbackFunc.
type CallbackEvent uint8

// enumeration of CallbackEvents
const (
	BeforeMigration CallbackEvent = iota
	AfterMigration
)

// CallbackFunc enables interrupting the migration process and executing code
// while migrating. If error is returned the migration is aborted.
type CallbackFunc func(ctx context.Context, session *gocql.Session, ev CallbackEvent, name string) error

// Callback is called before and after each migration.
// See CallbackFunc for details.
var Callback CallbackFunc
