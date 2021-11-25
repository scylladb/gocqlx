// Copyright (C) 2017 ScyllaDB
// Use of this source code is governed by a ALv2-style
// license that can be found in the LICENSE file.

package gocqlx

import (
	"reflect"

	"github.com/gocql/gocql"
)

// Transformer transforms the value of the named parameter to another value.
type Transformer func(name string, val interface{}) interface{}

// DefaultBindTransformer just do nothing.
//
// A custom transformer can always be set per Query.
var DefaultBindTransformer Transformer

// UnsetEmptyTransformer unsets all empty parameters.
// It helps to avoid tombstones when using the same insert/update
// statement for filled and partially filled named parameters.
var UnsetEmptyTransformer = func(name string, val interface{}) interface{} {
	v := reflect.ValueOf(val)
	if v.IsZero() {
		return gocql.UnsetValue
	}
	return val
}
