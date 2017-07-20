package gocqlx

import (
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/gocql/gocql"
	"github.com/jmoiron/sqlx/reflectx"
)

// DefaultMapper uses `db` tag and strings.ToLower to lowercase struct field
// names.  It can be set to whatever you want, but it is encouraged to be set
// before gocqlx is used as name-to-field mappings are cached after first
// use on a type.
var DefaultMapper = reflectx.NewMapperFunc("db", strings.ToLower)

// Get is a convenience function for creating iterator and calling Get on it.
func Get(q *gocql.Query, dest interface{}) error {
	return Iter(q).Get(dest)
}

// Select is a convenience function for creating iterator and calling Select on it.
func Select(q *gocql.Query, dest interface{}) error {
	return Iter(q).Select(dest)
}

// Iterx is a wrapper around gocql.Iter which adds struct scanning capabilities.
type Iterx struct {
	*gocql.Iter
	unsafe bool
	Mapper *reflectx.Mapper
	// these fields cache memory use for a rows during iteration w/ structScan
	started bool
	fields  [][]int
	values  []interface{}
	err     error
}

// Iter creates a new Iterx from gocql.Query using a default mapper.
func Iter(q *gocql.Query) *Iterx {
	return &Iterx{
		Iter:   q.Iter(),
		Mapper: DefaultMapper,
	}
}

// Get scans first row into a destination and closes the iterator.  If the
// destination type is a Struct, then StructScan will be used.  If the
// destination is some other type, then the row must only have one column which
// can scan into that type.
func (iter *Iterx) Get(dest interface{}) error {
	if err := iter.scanAny(dest, false); err != nil {
		iter.err = err
	}

	if err := iter.Close(); err != nil {
		iter.err = err
	}

	return iter.err
}

func (iter *Iterx) scanAny(dest interface{}, structOnly bool) error {
	v := reflect.ValueOf(dest)
	if v.Kind() != reflect.Ptr {
		return errors.New("must pass a pointer, not a value, to StructScan destination")
	}
	if v.IsNil() {
		return errors.New("nil pointer passed to StructScan destination")
	}

	base := reflectx.Deref(v.Type())
	scannable := isScannable(base)

	if structOnly && scannable {
		return structOnlyError(base)
	}

	if scannable && len(iter.Columns()) > 1 {
		return fmt.Errorf("scannable dest type %s with >1 columns (%d) in result", base.Kind(), len(iter.Columns()))
	}

	if !scannable {
		iter.StructScan(dest)
	} else {
		iter.Scan(dest)
	}

	return nil
}

// Select scans all rows into a destination, which must be a slice of any type
// and closes the iterator.  If the destination slice type is a Struct, then
// StructScan will be used on each row.  If the destination is some other type,
// then each row must only have one column which can scan into that type.
func (iter *Iterx) Select(dest interface{}) error {
	if err := iter.scanAll(dest, false); err != nil {
		iter.err = err
	}

	if err := iter.Close(); err != nil {
		iter.err = err
	}

	return iter.err
}

func (iter *Iterx) scanAll(dest interface{}, structOnly bool) error {
	var v, vp reflect.Value

	value := reflect.ValueOf(dest)

	// json.Unmarshal returns errors for these
	if value.Kind() != reflect.Ptr {
		return errors.New("must pass a pointer, not a value, to StructScan destination")
	}
	if value.IsNil() {
		return errors.New("nil pointer passed to StructScan destination")
	}
	direct := reflect.Indirect(value)

	slice, err := baseType(value.Type(), reflect.Slice)
	if err != nil {
		return err
	}

	isPtr := slice.Elem().Kind() == reflect.Ptr
	base := reflectx.Deref(slice.Elem())
	scannable := isScannable(base)

	if structOnly && scannable {
		return structOnlyError(base)
	}

	// if it's a base type make sure it only has 1 column;  if not return an error
	if scannable && len(iter.Columns()) > 1 {
		return fmt.Errorf("non-struct dest type %s with >1 columns (%d)", base.Kind(), len(iter.Columns()))
	}

	if !scannable {
		for {
			// create a new struct type (which returns PtrTo) and indirect it
			vp = reflect.New(base)
			v = reflect.Indirect(vp)
			// scan into the struct field pointers and append to our results
			if ok := iter.StructScan(vp.Interface()); !ok {
				break
			}

			if isPtr {
				direct.Set(reflect.Append(direct, vp))
			} else {
				direct.Set(reflect.Append(direct, v))
			}
		}
	} else {
		for {
			vp = reflect.New(base)
			if ok := iter.Scan(vp.Interface()); !ok {
				break
			}

			// append
			if isPtr {
				direct.Set(reflect.Append(direct, vp))
			} else {
				direct.Set(reflect.Append(direct, reflect.Indirect(vp)))
			}
		}
	}

	return iter.Err()
}

// StructScan is like gocql.Scan, but scans a single row into a single Struct.
// Use this and iterate manually when the memory load of Select() might be
// prohibitive.  StructScan caches the reflect work of matching up column
// positions to fields to avoid that overhead per scan, which means it is not
// safe to run StructScan on the same Iterx instance with different struct
// types.
func (iter *Iterx) StructScan(dest interface{}) bool {
	v := reflect.ValueOf(dest)
	if v.Kind() != reflect.Ptr {
		iter.err = errors.New("must pass a pointer, not a value, to StructScan destination")
		return false
	}

	if !iter.started {
		columns := columnNames(iter.Iter.Columns())
		m := iter.Mapper

		iter.fields = m.TraversalsByName(v.Type(), columns)
		// if we are not unsafe and are missing fields, return an error
		if f, err := missingFields(iter.fields); err != nil && !iter.unsafe {
			iter.err = fmt.Errorf("missing destination name %s in %T", columns[f], dest)
			return false
		}
		iter.values = make([]interface{}, len(columns))
		iter.started = true
	}

	err := fieldsByTraversal(v, iter.fields, iter.values, true)
	if err != nil {
		iter.err = err
		return false
	}
	// scan into the struct field pointers and append to our results
	return iter.Iter.Scan(iter.values...)
}

func columnNames(ci []gocql.ColumnInfo) []string {
	r := make([]string, len(ci))
	for i, column := range ci {
		r[i] = column.Name
	}
	return r
}

// Err returns the error encountered while scanning.
func (iter *Iterx) Err() error {
	return iter.err
}

// structOnlyError returns an error appropriate for type when a non-scannable
// struct is expected but something else is given
func structOnlyError(t reflect.Type) error {
	isStruct := t.Kind() == reflect.Struct
	isScanner := reflect.PtrTo(t).Implements(_unmarshallerInterface)
	if !isStruct {
		return fmt.Errorf("expected %s but got %s", reflect.Struct, t.Kind())
	}
	if isScanner {
		return fmt.Errorf("structscan expects a struct dest but the provided struct type %s implements unmarshaler", t.Name())
	}
	return fmt.Errorf("expected a struct, but struct %s has no exported fields", t.Name())
}

// reflect helpers

var _unmarshallerInterface = reflect.TypeOf((*gocql.Unmarshaler)(nil)).Elem()

func baseType(t reflect.Type, expected reflect.Kind) (reflect.Type, error) {
	t = reflectx.Deref(t)
	if t.Kind() != expected {
		return nil, fmt.Errorf("expected %s but got %s", expected, t.Kind())
	}
	return t, nil
}

// isScannable takes the reflect.Type and the actual dest value and returns
// whether or not it's Scannable. Something is scannable if:
//   * it is not a struct
//   * it implements gocql.Unmarshaler
//   * it has no exported fields
func isScannable(t reflect.Type) bool {
	if reflect.PtrTo(t).Implements(_unmarshallerInterface) {
		return true
	}
	if t.Kind() != reflect.Struct {
		return true
	}

	// it's not important that we use the right mapper for this particular object,
	// we're only concerned on how many exported fields this struct has
	m := DefaultMapper
	if len(m.TypeMap(t).Index) == 0 {
		return true
	}
	return false
}

// fieldsByName fills a values interface with fields from the passed value based
// on the traversals in int.  If ptrs is true, return addresses instead of values.
// We write this instead of using FieldsByName to save allocations and map lookups
// when iterating over many rows.  Empty traversals will get an interface pointer.
// Because of the necessity of requesting ptrs or values, it's considered a bit too
// specialized for inclusion in reflectx itself.
func fieldsByTraversal(v reflect.Value, traversals [][]int, values []interface{}, ptrs bool) error {
	v = reflect.Indirect(v)
	if v.Kind() != reflect.Struct {
		return errors.New("argument not a struct")
	}

	for i, traversal := range traversals {
		if len(traversal) == 0 {
			values[i] = new(interface{})
			continue
		}
		f := reflectx.FieldByIndexes(v, traversal)
		if ptrs {
			values[i] = f.Addr().Interface()
		} else {
			values[i] = f.Interface()
		}
	}
	return nil
}

func missingFields(transversals [][]int) (field int, err error) {
	for i, t := range transversals {
		if len(t) == 0 {
			return i, errors.New("missing field")
		}
	}
	return 0, nil
}
