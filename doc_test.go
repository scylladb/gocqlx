package gocqlx_test

import (
	"github.com/scylladb/gocqlx"
)

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
