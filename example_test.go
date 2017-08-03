// +build all integration

package gocqlx_test

import (
	"testing"
	"time"

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

// Field names are converted to camel case by default, no need to add
// `db:"first_name"`, if you want to disable a filed add `db:"-"` tag.
type Person struct {
	FirstName string
	LastName  string
	Email     []string
}

func TestExample(t *testing.T) {
	session := createSession(t)
	defer session.Close()

	if err := createTable(session, personSchema); err != nil {
		t.Fatal("create table:", err)
	}

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

	// Batch
	{
		i := qb.Insert("gocqlx_test.person").Columns("first_name", "last_name", "email")

		stmt, names := qb.Batch().
			Add("a.", i).
			Add("b.", i).
			ToCql()
		q := gocqlx.Query(session.Query(stmt), names)

		b := struct {
			A Person
			B Person
		}{
			A: Person{
				"Igy",
				"Citizen",
				[]string{"ian.citzen@gocqlx_test.com"},
			},
			B: Person{
				"Ian",
				"Citizen",
				[]string{"igy.citzen@gocqlx_test.com"},
			},
		}

		if err := q.BindStruct(&b).Exec(); err != nil {
			t.Fatal(err)
		}
	}

	// Get
	{
		var p Person
		if err := gocqlx.Get(&p, session.Query("SELECT * FROM gocqlx_test.person WHERE first_name=?", "Patricia")); err != nil {
			t.Fatal("get:", err)
		}
		t.Log(p)

		// {Patricia Citizen [patricia.citzen@gocqlx_test.com patricia1.citzen@gocqlx_test.com]}
	}

	// Select
	{
		stmt, names := qb.Select("gocqlx_test.person").Where(qb.In("first_name")).ToCql()
		q := gocqlx.Query(session.Query(stmt), names)

		q.BindMap(qb.M{"first_name": []string{"Patricia", "Igy", "Ian"}})
		if err := q.Err(); err != nil {
			t.Fatal(err)
		}

		var people []Person
		if err := gocqlx.Select(&people, q.Query); err != nil {
			t.Fatal("select:", err)
		}
		t.Log(people)

		// [{Ian Citizen [igy.citzen@gocqlx_test.com]} {Igy Citizen [ian.citzen@gocqlx_test.com]} {Patricia Citizen [patricia.citzen@gocqlx_test.com patricia1.citzen@gocqlx_test.com]}]
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
