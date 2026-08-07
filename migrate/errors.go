// Copyright (C) 2026 ScyllaDB
// Use of this source code is governed by a ALv2-style
// license that can be found in the LICENSE file.

package migrate

import (
	"errors"
	"fmt"
)

var (
	// ErrNoMigrations indicates that the migration file system contains no CQL files.
	ErrNoMigrations = errors.New("no migration files found")
	// ErrDatabaseAhead indicates that the database contains more applied migrations
	// than the migration file system.
	ErrDatabaseAhead = errors.New("database is ahead")
	// ErrInconsistentMigrations indicates that an applied migration does not match
	// the migration at the same position in the file system.
	ErrInconsistentMigrations = errors.New("inconsistent migrations")
	// ErrChecksumMismatch indicates that an applied migration file's checksum has changed.
	ErrChecksumMismatch = errors.New("migration checksum mismatch")
	// ErrNoMigrationStatements indicates that a migration file contains no statements.
	ErrNoMigrationStatements = errors.New("no migration statements found")
	// ErrMissingCallbackHandler indicates that a CALL comment has no registered handler.
	ErrMissingCallbackHandler = errors.New("missing callback handler")
)

// NoMigrationsError describes the file pattern for which no migrations were found.
type NoMigrationsError struct {
	Pattern string
}

func (e *NoMigrationsError) Error() string {
	return fmt.Sprintf("no migration files found matching %q", e.Pattern)
}

// Unwrap allows errors.Is to match ErrNoMigrations.
func (e *NoMigrationsError) Unwrap() error {
	return ErrNoMigrations
}

// DatabaseAheadError describes migration counts when the database contains
// more applied migrations than the file system.
type DatabaseAheadError struct {
	Applied   int
	Available int
}

func (e *DatabaseAheadError) Error() string {
	return fmt.Sprintf("database is ahead: %d migrations applied, %d available", e.Applied, e.Available)
}

// Unwrap allows errors.Is to match ErrDatabaseAhead.
func (e *DatabaseAheadError) Unwrap() error {
	return ErrDatabaseAhead
}

// InconsistentMigrationError describes a mismatch between the applied and
// file-system migration sequences.
type InconsistentMigrationError struct {
	Expected string
	Actual   string
	Index    int
}

func (e *InconsistentMigrationError) Error() string {
	return fmt.Sprintf("inconsistent migrations found, expected %q got %q at %d", e.Expected, e.Actual, e.Index)
}

// Unwrap allows errors.Is to match ErrInconsistentMigrations.
func (e *InconsistentMigrationError) Unwrap() error {
	return ErrInconsistentMigrations
}

// ChecksumMismatchError describes a migration file whose checksum differs
// from the checksum recorded in the database.
type ChecksumMismatchError struct {
	Path     string
	Expected string
	Actual   string
}

func (e *ChecksumMismatchError) Error() string {
	return fmt.Sprintf("file %q was tampered with, expected md5 %s, got %s", e.Path, e.Expected, e.Actual)
}

// Unwrap allows errors.Is to match ErrChecksumMismatch.
func (e *ChecksumMismatchError) Unwrap() error {
	return ErrChecksumMismatch
}

// NoMigrationStatementsError describes a migration file containing no statements.
type NoMigrationStatementsError struct {
	Path string
}

func (e *NoMigrationStatementsError) Error() string {
	return fmt.Sprintf("no migration statements found in %q", e.Path)
}

// Unwrap allows errors.Is to match ErrNoMigrationStatements.
func (e *NoMigrationStatementsError) Unwrap() error {
	return ErrNoMigrationStatements
}

// MissingCallbackHandlerError describes a CALL comment for which no callback
// handler is registered.
type MissingCallbackHandlerError struct {
	Name      string
	Statement int
}

func (e *MissingCallbackHandlerError) Error() string {
	if e.Statement > 0 {
		return fmt.Sprintf("statement %d: missing callback handler for %q", e.Statement, e.Name)
	}
	return fmt.Sprintf("missing callback handler for %q", e.Name)
}

// Unwrap allows errors.Is to match ErrMissingCallbackHandler.
func (e *MissingCallbackHandlerError) Unwrap() error {
	return ErrMissingCallbackHandler
}
