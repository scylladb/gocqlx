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
    telcode int,
    PRIMARY KEY(country, city)
)`

type Person struct {
	FirstName string `db:"first_name"`
	LastName  string `db:"last_name"`
	Email     []string
}

type Place struct {
	Country string
	City    string
	TelCode int
}

func TestExample(t *testing.T) {
	session := createSession(t)
	defer session.Close()

	// Exec the schema or fail
	mustExec := func(q *gocql.Query) {
		if err := q.Exec(); err != nil {
			t.Fatal("insert:", q, err)
		}
	}
	mustExec(session.Query(personSchema))
	mustExec(session.Query(placeSchema))

	mustExec(session.Query("INSERT INTO gocqlx_test.person (first_name, last_name, email) VALUES (?, ?, ?)", "Jason", "Moiron", []string{"jmoiron@jmoiron.net"}))
	mustExec(session.Query("INSERT INTO gocqlx_test.person (first_name, last_name, email) VALUES (?, ?, ?)", "John", "Doe", []string{"johndoeDNE@gmail.net"}))
	mustExec(session.Query("INSERT INTO gocqlx_test.place (country, city, telcode) VALUES (?, ?, ?)", "United States", "New York", 1))
	mustExec(session.Query("INSERT INTO gocqlx_test.place (country, city, telcode) VALUES (?, ?, ?)", "Hong Kong", "", 852))
	mustExec(session.Query("INSERT INTO gocqlx_test.place (country, city, telcode) VALUES (?, ?, ?)", "Singapore", "", 65))

	// TODO
	// tx.NamedExec("INSERT INTO person (first_name, last_name, email) VALUES (:first_name, :last_name, :email)", &Person{"Jane", "Citizen", "jane.citzen@gocqlx_test.com"})

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
}
