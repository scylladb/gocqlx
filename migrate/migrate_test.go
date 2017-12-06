package migrate

import (
	"context"
	"errors"
	"testing"
)

// errInvalidCQLStatement is returned when trying to execute a non valid cql statement
var errInvalidCQLStatement = errors.New("invalid CQL statement")

type mockMigratorTable struct {
	ListFunc    func(ctx context.Context) ([]*Info, error)
	ExecuteFunc func(ctx context.Context, stmt string, info Info) error
	*testing.T
}

func (m *mockMigratorTable) List(ctx context.Context) ([]*Info, error) {
	if m.ListFunc == nil {
		m.T.Fatalf("Mock called without the ListFunc being set")
		return nil, nil
	}
	return m.ListFunc(ctx)
}

func (m *mockMigratorTable) Execute(ctx context.Context, stmt string, info Info) error {
	if m.ExecuteFunc == nil {
		m.T.Fatalf("Mock called without the ExecuteFunc being set")
		return nil
	}
	return m.ExecuteFunc(ctx, stmt, info)
}

func TestNoSessionSet(t *testing.T) {
	err := Migrate(context.Background(), nil, "")
	if err != ErrNoSessionProvided {
		t.Fatalf("Error should be ErrNoSessionProvided but was %+v", err)
	}
}

func TestListFailureAbortsEarly(t *testing.T) {
	m := &mockMigratorTable{T: t}
	SetMigratorTable(m)
	m.ListFunc = func(ctx context.Context) ([]*Info, error) {
		return nil, errors.New("list failed")
	}
	err := Migrate(context.Background(), nil, ".")
	if err.Error() != "failed to list migrations: list failed" {
		t.Fatalf("Error should not be %q", err.Error())
	}
}

func TestNoMigrationsToApply(t *testing.T) {
	m := &mockMigratorTable{T: t}
	SetMigratorTable(m)
	m.ListFunc = func(ctx context.Context) ([]*Info, error) {
		return nil, nil
	}
	err := Migrate(context.Background(), nil, "testdata/does_not_exist")
	if err.Error() != "no migrations were found" {
		t.Fatalf("Error should not be %q", err.Error())
	}
}

func TestDatabaseAhead(t *testing.T) {
	m := &mockMigratorTable{T: t}
	SetMigratorTable(m)
	m.ListFunc = func(ctx context.Context) ([]*Info, error) {
		var existingMigrations = []*Info{
			&Info{},
			&Info{},
			&Info{},
		}
		return existingMigrations, nil
	}
	err := Migrate(context.Background(), nil, "testdata/stage1")
	if err.Error() != "database is ahead of \"testdata/stage1\"" {
		t.Fatalf("Error should not be %q", err.Error())
	}
}

func TestInvalidCQLStatement(t *testing.T) {
	m := &mockMigratorTable{T: t}
	SetMigratorTable(m)
	m.ListFunc = func(ctx context.Context) ([]*Info, error) {
		return nil, nil
	}
	m.ExecuteFunc = func(ctx context.Context, stmt string, info Info) error {
		return errInvalidCQLStatement
	}
	err := Migrate(context.Background(), nil, "testdata/invalid")
	if err.Error() != "failed to apply migration \"testdata/invalid/not_valid_cql.cql\": invalid CQL statement" {
		t.Fatalf("Error should not be %v", err)
	}
}

func TestNonCQLStatement(t *testing.T) {
	m := &mockMigratorTable{T: t}
	SetMigratorTable(m)
	m.ListFunc = func(ctx context.Context) ([]*Info, error) {
		return nil, nil
	}
	err := Migrate(context.Background(), nil, "testdata/noncql")
	if err.Error() != "failed to apply migration \"testdata/noncql/not_cql.cql\": no CQL statement found" {
		t.Fatalf("Error should not be %v", err)
	}
}

func TestValidCQLStatement(t *testing.T) {
	m := &mockMigratorTable{T: t}
	SetMigratorTable(m)
	m.ListFunc = func(ctx context.Context) ([]*Info, error) {
		return nil, nil
	}
	m.ExecuteFunc = func(ctx context.Context, stmt string, info Info) error {
		return nil
	}
	err := Migrate(context.Background(), nil, "testdata/valid")
	if err != nil {
		t.Fatalf("Error should not be %v", err)
	}
}
