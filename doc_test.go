// Copyright (C) 2017 ScyllaDB
// Use of this source code is governed by a ALv2-style
// license that can be found in the LICENSE file.

package gocqlx_test

import (
	"github.com/gocql/gocql"
	"github.com/scylladb/gocqlx/v2"
	"github.com/scylladb/gocqlx/v2/qb"
)

func ExampleSession() {
	cluster := gocql.NewCluster("host")
	session, err := gocqlx.WrapSession(cluster.CreateSession())
	if err != nil {
		// handle error
	}

	builder := qb.Select("foo")
	session.Query(builder.ToCql())
}

func ExampleUDT() {
	// Just add gocqlx.UDT to a type, no need to implement marshalling functions
	type FullName struct {
		gocqlx.UDT
		FirstName string
		LastName  string
	}
}

func ExampleUDT_wraper() {
	type FullName struct {
		FirstName string
		LastName  string
	}

	// Create new UDT wrapper type
	type FullNameUDT struct {
		gocqlx.UDT
		*FullName
	}
}
