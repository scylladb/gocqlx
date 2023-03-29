package gocqlx

import (
	"github.com/gocql/gocql"
)

type Batch struct {
	*gocql.Batch
}

// NewBatch creates a new batch operation using defaults defined in the cluster.
func (s *Session) NewBatch(bt gocql.BatchType) *Batch {
	return &Batch{
		Batch: s.Session.NewBatch(bt),
	}
}

// BindStruct binds query named parameters to values from arg using a mapper.
// If value cannot be found an error is reported.
func (b *Batch) BindStruct(qry *Queryx, arg interface{}) error {
	args, err := qry.bindStructArgs(arg, nil)
	if err != nil {
		return err
	}
	b.Query(qry.Statement(), args...)
	return nil
}

// BindMap binds query named parameters to values from arg using a mapper.
// If value cannot be found an error is reported.
func (b *Batch) BindMap(qry *Queryx, arg map[string]interface{}) error {
	args, err := qry.bindMapArgs(arg)
	if err != nil {
		return err
	}
	b.Query(qry.Statement(), args...)
	return nil
}

// BindStructMap binds query named parameters to values from arg0 and arg1 using a mapper.
// If value cannot be found an error is reported.
func (b *Batch) BindStructMap(qry *Queryx, arg0 interface{}, arg1 map[string]interface{}) error {
	args, err := qry.bindStructArgs(arg0, arg1)
	if err != nil {
		return err
	}
	b.Query(qry.Statement(), args...)
	return nil
}

// ExecuteBatch executes a batch operation and returns nil if successful
// otherwise an error describing the failure.
func (s *Session) ExecuteBatch(batch *Batch) error {
	return s.Session.ExecuteBatch(batch.Batch)
}

// ExecuteBatchCAS executes a batch operation and returns true if successful and
// an iterator (to scan additional rows if more than one conditional statement)
// was sent.
// Further scans on the interator must also remember to include
// the applied boolean as the first argument to *Iter.Scan
func (s *Session) ExecuteBatchCAS(batch *Batch, dest ...interface{}) (applied bool, iter *gocql.Iter, err error) {
	return s.Session.ExecuteBatchCAS(batch.Batch, dest...)
}

// MapExecuteBatchCAS executes a batch operation much like ExecuteBatchCAS,
// however it accepts a map rather than a list of arguments for the initial
// scan.
func (s *Session) MapExecuteBatchCAS(batch *Batch, dest map[string]interface{}) (applied bool, iter *gocql.Iter, err error) {
	return s.Session.MapExecuteBatchCAS(batch.Batch, dest)
}
