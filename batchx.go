package gocqlx

import (
	"context"
	"fmt"
	"time"

	"github.com/gocql/gocql"
)

// Batch is a wrapper around gocql.Batch
type Batch struct {
	*gocql.Batch
}

// NewBatch creates a new batch operation using defaults defined in the cluster.
//
// Deprecated: use session.Batch instead
func (s *Session) NewBatch(bt gocql.BatchType) *Batch {
	return &Batch{
		Batch: s.Session.Batch(bt),
	}
}

// Batch creates a new batch operation using defaults defined in the cluster.
func (s *Session) Batch(bt gocql.BatchType) *Batch {
	return &Batch{
		Batch: s.Session.Batch(bt),
	}
}

// ContextBatch creates a new batch operation using defaults defined in the cluster with attached context.
func (s *Session) ContextBatch(ctx context.Context, bt gocql.BatchType) *Batch {
	return &Batch{
		Batch: s.Session.Batch(bt).WithContext(ctx),
	}
}

// GetRequestTimeout returns time driver waits for single server response
// This timeout is applied to preparing statement request and for query execution requests
func (b *Batch) GetRequestTimeout() time.Duration {
	return b.Batch.GetRequestTimeout()
}

// SetRequestTimeout sets time driver waits for server to respond
// This timeout is applied to preparing statement request and for query execution requests
func (b *Batch) SetRequestTimeout(timeout time.Duration) *Batch {
	b.Batch.SetRequestTimeout(timeout)
	return b
}

// SetHostID allows to define the host the query should be executed against. If the
// host was filtered or otherwise unavailable, then the query will error. If an empty
// string is sent, the default behavior, using the configured HostSelectionPolicy will
// be used. A hostID can be obtained from HostInfo.HostID() after calling GetHosts().
func (b *Batch) SetHostID(hostID string) *Batch {
	b.Batch.SetHostID(hostID)
	return b
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

// Bind binds query parameters to values from args.
// If value cannot be found an error is reported.
func (b *Batch) Bind(qry *Queryx, args ...interface{}) error {
	if len(qry.Names) != len(args) {
		return fmt.Errorf("query requires %d arguments, but %d provided", len(qry.Names), len(args))
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

// DefaultTimestamp will enable the with default timestamp flag on the query.
// If enabled, this will replace the server side assigned
// timestamp as default timestamp. Note that a timestamp in the query itself
// will still override this timestamp. This is entirely optional.
//
// Only available on protocol >= 3
func (b *Batch) DefaultTimestamp(enable bool) *Batch {
	b.Batch.DefaultTimestamp(enable)
	return b
}

// Observer enables batch-level observer on this batch.
// The provided observer will be called every time this batched query is executed.
func (b *Batch) Observer(observer gocql.BatchObserver) *Batch {
	b.Batch.Observer(observer)
	return b
}

// RetryPolicy sets the retry policy to use when executing the batch operation
func (b *Batch) RetryPolicy(policy gocql.RetryPolicy) *Batch {
	b.Batch.RetryPolicy(policy)
	return b
}

// SerialConsistency sets the consistency level for the
// serial phase of conditional updates. That consistency can only be
// either SERIAL or LOCAL_SERIAL and if not present, it defaults to
// SERIAL. This option will be ignored for anything else that a
// conditional update/insert.
//
// Only available for protocol 3 and above
func (b *Batch) SerialConsistency(cons gocql.Consistency) *Batch {
	b.Batch.SerialConsistency(cons)
	return b
}

// SpeculativeExecutionPolicy sets the speculative execution policy to use when executing the batch operation
func (b *Batch) SpeculativeExecutionPolicy(policy gocql.SpeculativeExecutionPolicy) *Batch {
	b.Batch.SpeculativeExecutionPolicy(policy)
	return b
}

// Trace enables tracing of this batch. Look at the documentation of the
// gocql.Tracer interface to learn more about tracing.
func (b *Batch) Trace(trace gocql.Tracer) *Batch {
	b.Batch.Trace(trace)
	return b
}

// WithContext returns a shallow copy of b with its context
// set to ctx.
//
// The provided context controls the entire lifetime of executing a
// query, queries will be canceled and return once the context is
// canceled.
func (b *Batch) WithContext(ctx context.Context) *Batch {
	return &Batch{
		Batch: b.Batch.WithContext(ctx),
	}
}

// WithTimestamp will enable the with default timestamp flag on the query
// like DefaultTimestamp does. But also allows to define value for timestamp.
// It works the same way as USING TIMESTAMP in the query itself, but
// should not break prepared query optimization.
//
// Only available on protocol >= 3
func (b *Batch) WithTimestamp(timestamp int64) *Batch {
	b.Batch.WithTimestamp(timestamp)
	return b
}

// Query adds the query to the batch operation
func (b *Batch) Query(stmt string, args ...interface{}) *Batch {
	b.Batch.Query(stmt, args...)
	return b
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
