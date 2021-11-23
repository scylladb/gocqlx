// Copyright (C) 2017 ScyllaDB
// Use of this source code is governed by a ALv2-style
// license that can be found in the LICENSE file.

package gocqlx

// Transformer transforms the value of the named parameter to another value.
type Transformer func(name string, val interface{}) interface{}

// DefaultBindTransformer just do nothing.
//
// A custom transformer can always be set per Query.
var DefaultBindTransformer Transformer
