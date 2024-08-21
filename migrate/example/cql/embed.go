// Copyright (C) 2017 ScyllaDB
// Use of this source code is governed by a ALv2-style
// license that can be found in the LICENSE file.

//go:build all || integration
// +build all integration

package cql

import "embed"

// Files contains *.cql schema migration files.
//
//go:embed *.cql
var Files embed.FS
