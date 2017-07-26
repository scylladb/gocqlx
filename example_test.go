// +build all integration

package gocqlx_test

import (
	"fmt"
	"testing"

	"github.com/gocql/gocql"
	"github.com/scylladb/gocqlx"
)

var personSchema = `
CREATE TABLE gocqlx_test.person (
    first_name text,
    last_name text,
    email list<text>,
    PRIMARY KEY(first_name, last_name)
)`

var placeSchema = `
CREATE TABLE gocqlx_test.place (
    country text,
    city text,
    code int,
    PRIMARY KEY(country, city)
)`

// Field names are converted to camel case by default, no need to add
// `db:"first_name"`, if you want to disable a filed add `db:"-"` tag
type Person struct {
	FirstName string
	LastName  string
	Email     []string
}

type Place struct {
	Country string
	City    string
	TelCode int `db:"code"`
}

func TestExample(t *testing.T) {
	session := createSession(t)
	defer session.Close()

	mustExec := func(q *gocql.Query) {
		if err := q.Exec(); err != nil {
			t.Fatal("insert:", q, err)
		}
	}

	// Fill person table
	{
		mustExec(session.Query(personSchema))

		q := session.Query("INSERT INTO gocqlx_test.person (first_name, last_name, email) VALUES (?, ?, ?)")
		mustExec(q.Bind("Jason", "Moiron", []string{"jmoiron@jmoiron.net"}))
		mustExec(q.Bind("John", "Doe", []string{"johndoeDNE@gmail.net"}))
		q.Release()
	}

	// Fill place table
	{
		mustExec(session.Query(placeSchema))

		q := session.Query("INSERT INTO gocqlx_test.place (country, city, code) VALUES (?, ?, ?)")
		mustExec(q.Bind("United States", "New York", 1))
		mustExec(q.Bind("Hong Kong", "", 852))
		mustExec(q.Bind("Singapore", "", 65))
		q.Release()
	}

	// Query the database, storing results in a []Person (wrapped in []interface{})
	{
		people := []Person{}
		if err := gocqlx.Select(&people, session.Query("SELECT * FROM person")); err != nil {
			t.Fatal("select:", err)
		}

		fmt.Printf("%#v\n%#v\n", people[0], people[1])
		// gocqlx_test.Person{FirstName:"John", LastName:"Doe", Email:[]string{"johndoeDNE@gmail.net"}}
		// gocqlx_test.Person{FirstName:"Jason", LastName:"Moiron", Email:[]string{"jmoiron@jmoiron.net"}}
	}

	// Get a single result, a la QueryRow
	{
		var jason Person
		if err := gocqlx.Get(&jason, session.Query("SELECT * FROM person WHERE first_name=?", "Jason")); err != nil {
			t.Fatal("get:", err)
		}
		fmt.Printf("%#v\n", jason)
		// gocqlx_test.Person{FirstName:"Jason", LastName:"Moiron", Email:[]string{"jmoiron@jmoiron.net"}}
	}

	// Loop through rows using only one struct
	{
		var place Place
		iter := gocqlx.Iter(session.Query("SELECT * FROM place"))
		for iter.StructScan(&place) {
			fmt.Printf("%#v\n", place)
		}
		if err := iter.Close(); err != nil {
			t.Fatal("iter:", err)
		}
		iter.ReleaseQuery()
		// gocqlx_test.Place{Country:"Hong Kong", City:"", TelCode:852}
		// gocqlx_test.Place{Country:"United States", City:"New York", TelCode:1}
		// gocqlx_test.Place{Country:"Singapore", City:"", TelCode:65}
	}

	// Named queries, using `:name` as the bindvar
	{
		stmt, names, err := gocqlx.CompileNamedQuery([]byte("INSERT INTO person (first_name, last_name, email) VALUES (:first_name, :last_name, :email)"))
		if err != nil {
			t.Fatal("compile:", err)
		}

		q := gocqlx.Queryx{
			Query: session.Query(stmt),
			Names: names,
		}

		if err := q.BindStruct(&Person{
			"Jane",
			"Citizen",
			[]string{"jane.citzen@gocqlx_test.com"},
		}); err != nil {
			t.Fatal("bind:", err)
		}
		mustExec(q.Query)

		if err := q.BindMap(map[string]interface{}{
			"first_name": "Bin",
			"last_name":  "Smuth",
			"email":      []string{"bensmith@allblacks.nz"},
		}); err != nil {
			t.Fatal("bind:", err)
		}
		mustExec(q.Query)
	}
}
