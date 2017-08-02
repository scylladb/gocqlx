// +build all integration

package gocqlx_test

import (
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/scylladb/gocqlx"
	"github.com/scylladb/gocqlx/qb"
)

var personSchema = `
CREATE TABLE IF NOT EXISTS gocqlx_test.person (
    first_name text,
    last_name text,
    email list<text>,
    PRIMARY KEY(first_name, last_name)
)`

var placeSchema = `
CREATE TABLE IF NOT EXISTS gocqlx_test.place (
    country text,
    city text,
    code int,
    PRIMARY KEY(country, city)
)`

// Field names are converted to camel case by default, no need to add
// `db:"first_name"`, if you want to disable a filed add `db:"-"` tag.
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
			t.Fatal("query:", q, err)
		}
	}

	// Fill person table.
	{
		if err := createTable(session, personSchema); err != nil {
			t.Fatal("create table:", err)
		}

		q := session.Query("INSERT INTO gocqlx_test.person (first_name, last_name, email) VALUES (?, ?, ?)")
		mustExec(q.Bind("Jason", "Moiron", []string{"jmoiron@jmoiron.net"}))
		mustExec(q.Bind("John", "Doe", []string{"johndoeDNE@gmail.net"}))
		q.Release()
	}

	// Fill place table.
	{
		if err := createTable(session, placeSchema); err != nil {
			t.Fatal("create table:", err)
		}

		q := session.Query("INSERT INTO gocqlx_test.place (country, city, code) VALUES (?, ?, ?)")
		mustExec(q.Bind("United States", "New York", 1))
		mustExec(q.Bind("Hong Kong", "", 852))
		mustExec(q.Bind("Singapore", "", 65))
		q.Release()
	}

	// Query the database, storing results in a []Person (wrapped in []interface{}).
	{
		var people []Person
		if err := gocqlx.Select(&people, session.Query("SELECT * FROM gocqlx_test.person")); err != nil {
			t.Fatal("select:", err)
		}
		t.Log(people)

		// [{John Doe [johndoeDNE@gmail.net]} {Jason Moiron [jmoiron@jmoiron.net]}]
	}

	// Get a single result.
	{
		var jason Person
		if err := gocqlx.Get(&jason, session.Query("SELECT * FROM gocqlx_test.person WHERE first_name=?", "Jason")); err != nil {
			t.Fatal("get:", err)
		}
		t.Log(jason)

		// Jason Moiron [jmoiron@jmoiron.net]}
	}

	// Loop through rows using only one struct.
	{
		var place Place
		iter := gocqlx.Iter(session.Query("SELECT * FROM gocqlx_test.place"))
		for iter.StructScan(&place) {
			t.Log(place)
		}
		if err := iter.Close(); err != nil {
			t.Fatal("iter:", err)
		}
		iter.ReleaseQuery()

		// {Hong Kong  852}
		// {United States New York 1}
		// {Singapore  65}
	}

	// Query builder, using DSL to build queries, using `:name` as the bindvar.
	{
		p := &Person{
			"Patricia",
			"Citizen",
			[]string{"patricia.citzen@gocqlx_test.com"},
		}

		// Insert
		{
			stmt, names := qb.Insert("gocqlx_test.person").Columns("first_name", "last_name", "email").ToCql()
			q := gocqlx.Query(session.Query(stmt), names)

			if err := q.BindStruct(p).Exec(); err != nil {
				t.Fatal(err)
			}
		}

		// Insert with TTL
		{
			stmt, names := qb.Insert("gocqlx_test.person").Columns("first_name", "last_name", "email").TTL().ToCql()
			q := gocqlx.Query(session.Query(stmt), names)

			if err := q.BindStructMap(p, qb.M{"_ttl": qb.TTL(86400 * time.Second)}).Exec(); err != nil {
				t.Fatal(err)
			}
		}

		// Update
		{
			p.Email = append(p.Email, "patricia1.citzen@gocqlx_test.com")

			stmt, names := qb.Update("gocqlx_test.person").Set("email").Where(qb.Eq("first_name"), qb.Eq("last_name")).ToCql()
			q := gocqlx.Query(session.Query(stmt), names)

			if err := q.BindStruct(p).Exec(); err != nil {
				t.Fatal(err)
			}
		}

		// Select
		{
			stmt, names := qb.Select("gocqlx_test.person").Where(qb.In("first_name")).ToCql()
			q := gocqlx.Query(session.Query(stmt), names)

			q.BindMap(qb.M{"first_name": []string{"Patricia", "John"}})
			if err := q.Err(); err != nil {
				t.Fatal(err)
			}

			var people []Person
			if err := gocqlx.Select(&people, q.Query); err != nil {
				t.Fatal("select:", err)
			}
			t.Log(people)

			// [{Patricia Citizen [patricia.citzen@gocqlx_test.com patricia1.citzen@gocqlx_test.com]} {John Doe [johndoeDNE@gmail.net]}]
		}
	}

	// Named queries, using `:name` as the bindvar.
	{
		// compile query to valid gocqlx query and list of named parameters
		stmt, names, err := gocqlx.CompileNamedQuery([]byte("INSERT INTO gocqlx_test.person (first_name, last_name, email) VALUES (:first_name, :last_name, :email)"))
		if err != nil {
			t.Fatal("compile:", err)
		}
		q := gocqlx.Query(session.Query(stmt), names)

		// bind named parameters from a struct
		{
			p := &Person{
				"Jane",
				"Citizen",
				[]string{"jane.citzen@gocqlx_test.com"},
			}

			if err := q.BindStruct(p).Exec(); err != nil {
				t.Fatal(err)
			}
		}

		// bind named parameters from a map
		{
			m := map[string]interface{}{
				"first_name": "Bin",
				"last_name":  "Smuth",
				"email":      []string{"bensmith@allblacks.nz"},
			}

			if err := q.BindMap(m).Exec(); err != nil {
				t.Fatal(err)
			}
		}
	}
}
