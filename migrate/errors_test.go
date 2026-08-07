// Copyright (C) 2026 ScyllaDB
// Use of this source code is governed by a ALv2-style
// license that can be found in the LICENSE file.

package migrate

import (
	"context"
	"errors"
	"testing"

	"github.com/scylladb/gocqlx/v3"
)

func TestNoMigrationsError(t *testing.T) {
	err := &NoMigrationsError{Pattern: "*.cql"}
	if !errors.Is(err, ErrNoMigrations) {
		t.Fatalf("errors.Is(%v, ErrNoMigrations) = false", err)
	}
	var target *NoMigrationsError
	if !errors.As(err, &target) || target != err {
		t.Fatalf("errors.As(%v) did not return the original error", err)
	}
	if got, want := err.Error(), `no migration files found matching "*.cql"`; got != want {
		t.Fatalf("Error() = %q, expected %q", got, want)
	}
}

func TestDatabaseAheadError(t *testing.T) {
	err := &DatabaseAheadError{
		Applied:   3,
		Available: 2,
	}
	if !errors.Is(err, ErrDatabaseAhead) {
		t.Fatalf("errors.Is(%v, ErrDatabaseAhead) = false", err)
	}
	var target *DatabaseAheadError
	if !errors.As(err, &target) || target != err {
		t.Fatalf("errors.As(%v) did not return the original error", err)
	}
	if got, want := err.Error(), "database is ahead: 3 migrations applied, 2 available"; got != want {
		t.Fatalf("Error() = %q, expected %q", got, want)
	}
}

func TestInconsistentMigrationError(t *testing.T) {
	err := &InconsistentMigrationError{
		Expected: "001.cql",
		Actual:   "002.cql",
		Index:    1,
	}
	if !errors.Is(err, ErrInconsistentMigrations) {
		t.Fatalf("errors.Is(%v, ErrInconsistentMigrations) = false", err)
	}
	var target *InconsistentMigrationError
	if !errors.As(err, &target) || target != err {
		t.Fatalf("errors.As(%v) did not return the original error", err)
	}
	if got, want := err.Error(), `inconsistent migrations found, expected "001.cql" got "002.cql" at 1`; got != want {
		t.Fatalf("Error() = %q, expected %q", got, want)
	}
}

func TestChecksumMismatchError(t *testing.T) {
	err := &ChecksumMismatchError{
		Path:     "001.cql",
		Expected: "expected",
		Actual:   "actual",
	}
	if !errors.Is(err, ErrChecksumMismatch) {
		t.Fatalf("errors.Is(%v, ErrChecksumMismatch) = false", err)
	}
	var target *ChecksumMismatchError
	if !errors.As(err, &target) || target != err {
		t.Fatalf("errors.As(%v) did not return the original error", err)
	}
	if got, want := err.Error(), `file "001.cql" was tampered with, expected md5 expected, got actual`; got != want {
		t.Fatalf("Error() = %q, expected %q", got, want)
	}
}

func TestNoMigrationStatementsError(t *testing.T) {
	err := &NoMigrationStatementsError{Path: "001.cql"}
	if !errors.Is(err, ErrNoMigrationStatements) {
		t.Fatalf("errors.Is(%v, ErrNoMigrationStatements) = false", err)
	}
	var target *NoMigrationStatementsError
	if !errors.As(err, &target) || target != err {
		t.Fatalf("errors.As(%v) did not return the original error", err)
	}
	if got, want := err.Error(), `no migration statements found in "001.cql"`; got != want {
		t.Fatalf("Error() = %q, expected %q", got, want)
	}
}

func TestMissingCallbackHandlerError(t *testing.T) {
	err := CallbackRegister{}.Callback(context.Background(), gocqlx.Session{}, CallComment, "seed")
	if !errors.Is(err, ErrMissingCallbackHandler) {
		t.Fatalf("errors.Is(%v, ErrMissingCallbackHandler) = false", err)
	}
	var target *MissingCallbackHandlerError
	if !errors.As(err, &target) {
		t.Fatalf("errors.As(%v) did not return MissingCallbackHandlerError", err)
	}
	if target.Name != "seed" {
		t.Fatalf("MissingCallbackHandlerError.Name = %q, expected %q", target.Name, "seed")
	}
	if target.Statement != 0 {
		t.Fatalf("MissingCallbackHandlerError.Statement = %d, expected 0", target.Statement)
	}
	if got, want := err.Error(), `missing callback handler for "seed"`; got != want {
		t.Fatalf("Error() = %q, expected %q", got, want)
	}
}
