package main

import (
	"flag"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/gocql/gocql"
	"github.com/google/go-cmp/cmp"

	"github.com/scylladb/gocqlx/v3"
	"github.com/scylladb/gocqlx/v3/gocqlxtest"
)

var flagUpdate = flag.Bool("update", false, "update golden file")

func TestSchemagen(t *testing.T) {
	flag.Parse()
	createTestSchema(t)

	// add ignored types and table
	*flagIgnoreNames = strings.Join([]string{
		"composers",
		"composers_by_name",
		"label",
	}, ",")

	// NOTE Only this generated models is used in real tests.
	t.Run("IgnoreIndexes", func(t *testing.T) {
		*flagIgnoreIndexes = true
		b := runSchemagen(t, "schemagentest")
		assertDiff(t, b, "testdata/models.go")
	})

	t.Run("NoIgnoreIndexes", func(t *testing.T) {
		*flagIgnoreIndexes = false
		b := runSchemagen(t, "schemagentest")
		assertDiff(t, b, "testdata/no_ignore_indexes/models.go")
	})
}

func Test_usedInTables(t *testing.T) {
	tests := map[string]struct {
		columnValidator string
		typeName        string
	}{
		"matches given a frozen collection": {
			columnValidator: "frozen<album>",
			typeName:        "album",
		},
		"matches given a set": {
			columnValidator: "set<artist>",
			typeName:        "artist",
		},
		"matches given a list": {
			columnValidator: "list<song>",
			typeName:        "song",
		},
		"matches given a tuple: first of two elements": {
			columnValidator: "tuple<first, second>",
			typeName:        "first",
		},
		"matches given a tuple: second of two elements": {
			columnValidator: "tuple<first, second>",
			typeName:        "second",
		},
		"matches given a tuple: first of three elements": {
			columnValidator: "tuple<first, second, third>",
			typeName:        "first",
		},
		"matches given a tuple: second of three elements": {
			columnValidator: "tuple<first, second, third>",
			typeName:        "second",
		},
		"matches given a tuple: third of three elements": {
			columnValidator: "tuple<first, second, third>",
			typeName:        "third",
		},
		"matches given a frozen set": {
			columnValidator: "set<frozen<album>>",
			typeName:        "album",
		},
		"matches snake_case names given a nested map": {
			columnValidator: "map<album, tuple<first, map<map_key, map-value>, third>>",
			typeName:        "map_key",
		},
		"matches a nested map without spaces": {
			columnValidator: "map<album,set<song>>",
			typeName:        "song",
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			tables := map[string]*gocql.TableMetadata{
				"table": {Columns: map[string]*gocql.ColumnMetadata{
					"column": {Type: tt.columnValidator},
				}},
			}
			if !usedInTables(tt.typeName, tables) {
				t.Fatal()
			}
		})
	}

	t.Run("doesn't panic with empty type name", func(t *testing.T) {
		tables := map[string]*gocql.TableMetadata{
			"table": {Columns: map[string]*gocql.ColumnMetadata{
				"column": {Type: "map<text, album>"},
			}},
		}
		usedInTables("", tables)
	})
}

func TestRenderTemplateRetainsNestedUserTypes(t *testing.T) {
	pkgname := *flagPkgname
	ignoreNames := *flagIgnoreNames
	ignoreIndexes := *flagIgnoreIndexes
	t.Cleanup(func() {
		*flagPkgname = pkgname
		*flagIgnoreNames = ignoreNames
		*flagIgnoreIndexes = ignoreIndexes
	})

	*flagPkgname = "schemagentest"
	*flagIgnoreNames = ""
	*flagIgnoreIndexes = false

	stateColumn := &gocql.ColumnMetadata{Name: "state", Type: "frozen<state>"}
	b, err := renderTemplate(&gocql.KeyspaceMetadata{
		Tables: map[string]*gocql.TableMetadata{
			"states": {
				Name:           "states",
				Columns:        map[string]*gocql.ColumnMetadata{"state": stateColumn},
				OrderedColumns: []string{"state"},
				PartitionKey:   []*gocql.ColumnMetadata{stateColumn},
			},
		},
		Types: map[string]*gocql.TypeMetadata{
			"state": {
				Name:       "state",
				FieldNames: []string{"rca", "status", "rolecustom"},
				FieldTypes: []string{
					"frozen<set<rcas>>",
					"frozen<incidentstatus>",
					"frozen<map<incidentcustomuserrole, set<userid>>>",
				},
			},
			"rcas": {
				Name:       "rcas",
				FieldNames: []string{"id", "created", "elapsed", "score"},
				FieldTypes: []string{"text", "timestamp", "duration", "decimal"},
			},
			"incidentstatus": {
				Name:       "incidentstatus",
				FieldNames: []string{"id"},
				FieldTypes: []string{"text"},
			},
			"incidentcustomuserrole": {
				Name:       "incidentcustomuserrole",
				FieldNames: []string{"id"},
				FieldTypes: []string{"text"},
			},
			"userid": {
				Name:       "userid",
				FieldNames: []string{"id"},
				FieldTypes: []string{"text"},
			},
			"orphan": {
				Name:       "orphan",
				FieldNames: []string{"id"},
				FieldTypes: []string{"text"},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	source := string(b)
	normalizedSource := strings.Join(strings.Fields(source), " ")
	for _, want := range []string{
		"type StateUserType struct {",
		"Rca []RcasUserType `cql:\"rca\"`",
		"Status IncidentstatusUserType `cql:\"status\"`",
		"Rolecustom map[IncidentcustomuserroleUserType][]UseridUserType `cql:\"rolecustom\"`",
		`"github.com/gocql/gocql"`,
		"type RcasUserType struct {",
		"Id string `cql:\"id\"`",
		"Created time.Time `cql:\"created\"`",
		"Elapsed gocql.Duration `cql:\"elapsed\"`",
		"Score inf.Dec `cql:\"score\"`",
		"type IncidentstatusUserType struct {",
		"type IncidentcustomuserroleUserType struct {",
		"type UseridUserType struct {",
		`"gopkg.in/inf.v0"`,
		`"time"`,
	} {
		if !strings.Contains(normalizedSource, want) {
			t.Fatalf("missing generated source %q:\n%s", want, source)
		}
	}
	if strings.Contains(normalizedSource, "type OrphanUserType struct {") {
		t.Fatalf("orphan UDT should not be generated:\n%s", source)
	}
}

func TestRenderTemplateIgnoresPaxosTables(t *testing.T) {
	pkgname := *flagPkgname
	ignoreNames := *flagIgnoreNames
	ignoreIndexes := *flagIgnoreIndexes
	t.Cleanup(func() {
		*flagPkgname = pkgname
		*flagIgnoreNames = ignoreNames
		*flagIgnoreIndexes = ignoreIndexes
	})

	*flagPkgname = "schemagentest"
	*flagIgnoreNames = ""
	*flagIgnoreIndexes = false

	idColumn := &gocql.ColumnMetadata{Name: "id", Type: "uuid"}
	emailColumn := &gocql.ColumnMetadata{Name: "email", Type: "text"}
	paxosColumn := &gocql.ColumnMetadata{Name: "row_key", Type: "blob"}
	b, err := renderTemplate(&gocql.KeyspaceMetadata{
		Tables: map[string]*gocql.TableMetadata{
			"users": {
				Name:           "users",
				Columns:        map[string]*gocql.ColumnMetadata{"id": idColumn, "email": emailColumn},
				OrderedColumns: []string{"id", "email"},
				PartitionKey:   []*gocql.ColumnMetadata{idColumn},
			},
			"users_by_email$paxos": {
				Name:           "users_by_email$paxos",
				Columns:        map[string]*gocql.ColumnMetadata{"row_key": paxosColumn},
				OrderedColumns: []string{"row_key"},
				PartitionKey:   []*gocql.ColumnMetadata{paxosColumn},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	source := string(b)
	if !strings.Contains(source, "Users = table.New") {
		t.Fatalf("generated source does not include user table:\n%s", source)
	}
	if strings.Contains(source, "paxos") {
		t.Fatalf("generated source includes internal Paxos table:\n%s", source)
	}
}

func TestRenderTemplateRejectsNonComparableMapKeys(t *testing.T) {
	pkgname := *flagPkgname
	ignoreNames := *flagIgnoreNames
	ignoreIndexes := *flagIgnoreIndexes
	t.Cleanup(func() {
		*flagPkgname = pkgname
		*flagIgnoreNames = ignoreNames
		*flagIgnoreIndexes = ignoreIndexes
	})

	*flagPkgname = "schemagentest"
	*flagIgnoreNames = ""
	*flagIgnoreIndexes = false

	tests := []struct {
		name string
		md   *gocql.KeyspaceMetadata
		want string
	}{
		{
			name: "collection key",
			md: &gocql.KeyspaceMetadata{
				Tables: map[string]*gocql.TableMetadata{
					"bad_table": {
						Name: "bad_table",
						Columns: map[string]*gocql.ColumnMetadata{
							"bad": &gocql.ColumnMetadata{Name: "bad", Type: "map<frozen<set<int>>, text>"},
						},
					},
				},
			},
			want: `table bad_table column bad: unsupported non-comparable CQL map key type "frozen<set<int>>"`,
		},
		{
			name: "UDT key with collection field",
			md: &gocql.KeyspaceMetadata{
				Tables: map[string]*gocql.TableMetadata{
					"states": {
						Name: "states",
						Columns: map[string]*gocql.ColumnMetadata{
							"state": &gocql.ColumnMetadata{Name: "state", Type: "state"},
						},
					},
				},
				Types: map[string]*gocql.TypeMetadata{
					"state": {
						Name:       "state",
						FieldNames: []string{"roles"},
						FieldTypes: []string{"map<role, text>"},
					},
					"role": {
						Name:       "role",
						FieldNames: []string{"ids"},
						FieldTypes: []string{"set<text>"},
					},
				},
			},
			want: `UDT state field roles: unsupported non-comparable CQL map key type "role"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := renderTemplate(tt.md)
			if err == nil {
				t.Fatal("expected error")
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestGeneratedUserTypesRoundTripNestedUDTs(t *testing.T) {
	textType := gocql.NewNativeType(4, gocql.TypeText)
	rcasType := gocql.NewUDTType(4, "rcas", "", gocql.UDTField{Name: "id", Type: textType})
	statusType := gocql.NewUDTType(4, "incidentstatus", "", gocql.UDTField{Name: "id", Type: textType})
	roleType := gocql.NewUDTType(4, "incidentcustomuserrole", "", gocql.UDTField{Name: "id", Type: textType})
	userIDType := gocql.NewUDTType(4, "userid", "", gocql.UDTField{Name: "id", Type: textType})
	userIDSetType := gocql.NewCollectionType(gocql.NewNativeType(4, gocql.TypeSet), nil, userIDType)
	stateType := gocql.NewUDTType(4, "state", "",
		gocql.UDTField{
			Name: "rca",
			Type: gocql.NewCollectionType(gocql.NewNativeType(4, gocql.TypeSet), nil, rcasType),
		},
		gocql.UDTField{Name: "status", Type: statusType},
		gocql.UDTField{
			Name: "rolecustom",
			Type: gocql.NewCollectionType(gocql.NewNativeType(4, gocql.TypeMap), roleType, userIDSetType),
		},
	)

	original := generatedStateUserType{
		Rca:    []generatedRcasUserType{{Id: "rca-1"}},
		Status: generatedIncidentstatusUserType{Id: "status-1"},
		Rolecustom: map[generatedIncidentcustomuserroleUserType][]generatedUseridUserType{
			{Id: "role-1"}: {{Id: "user-1"}},
		},
	}

	data, err := gocql.Marshal(stateType, newTestGocqlxUDT(original))
	if err != nil {
		t.Fatal(err)
	}

	var decoded generatedStateUserType
	if err := gocql.Unmarshal(stateType, data, newTestGocqlxUDT(&decoded)); err != nil {
		t.Fatal(err)
	}

	if diff := cmp.Diff(original, decoded); diff != "" {
		t.Fatal(diff)
	}
}

type testGocqlxUDT struct {
	field map[string]reflect.Value
}

func newTestGocqlxUDT(value interface{}) testGocqlxUDT {
	v := reflect.ValueOf(value)
	for v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	return testGocqlxUDT{
		field: gocqlx.DefaultMapper.FieldMap(v),
	}
}

func (u testGocqlxUDT) MarshalUDT(name string, info gocql.TypeInfo) ([]byte, error) {
	value, ok := u.field[name]
	if ok {
		return gocql.Marshal(info, value.Interface())
	}
	return nil, nil
}

func (u testGocqlxUDT) UnmarshalUDT(name string, info gocql.TypeInfo, data []byte) error {
	value, ok := u.field[name]
	if ok {
		return gocql.Unmarshal(info, data, value.Addr().Interface())
	}
	return nil
}

type generatedStateUserType struct {
	gocqlx.UDT
	Rca        []generatedRcasUserType                                               `cql:"rca"`
	Status     generatedIncidentstatusUserType                                       `cql:"status"`
	Rolecustom map[generatedIncidentcustomuserroleUserType][]generatedUseridUserType `cql:"rolecustom"`
}

type generatedRcasUserType struct {
	gocqlx.UDT
	Id string `cql:"id"`
}

type generatedIncidentstatusUserType struct {
	gocqlx.UDT
	Id string `cql:"id"`
}

type generatedIncidentcustomuserroleUserType struct {
	gocqlx.UDT
	Id string `cql:"id"`
}

type generatedUseridUserType struct {
	gocqlx.UDT
	Id string `cql:"id"`
}

func assertDiff(t *testing.T, actual []byte, goldenFile string) {
	t.Helper()

	if *flagUpdate {
		if err := os.WriteFile(goldenFile, actual, os.ModePerm); err != nil {
			t.Fatal(err)
		}
	}
	golden, err := os.ReadFile(goldenFile)
	if err != nil {
		t.Fatal(err)
	}

	if diff := cmp.Diff(string(golden), string(actual)); diff != "" {
		t.Fatal(diff)
	}
}

func createTestSchema(t *testing.T) {
	t.Helper()

	session := gocqlxtest.CreateSession(t)
	defer session.Close()

	if err := gocqlxtest.CreateKeyspaceIfNotExists(session, "schemagen"); err != nil {
		t.Fatal("create keyspace:", err)
	}

	err := session.ExecStmt(`CREATE TABLE IF NOT EXISTS schemagen.songs (
		id uuid PRIMARY KEY,
		title text,
		album text,
		artist text,
		duration duration,
		tags set<text>,
		data blob)`)
	if err != nil {
		t.Fatal("create table:", err)
	}

	err = session.ExecStmt(`CREATE TYPE IF NOT EXISTS schemagen.album (
		name text,
		songwriters set<text>,)`)
	if err != nil {
		t.Fatal("create type:", err)
	}

	err = session.ExecStmt(`CREATE TABLE IF NOT EXISTS schemagen.playlists (
		id uuid,
		title text,
		album frozen<album>, 
		artist text,
		song_id uuid,
		PRIMARY KEY (id, title, album, artist))`)
	if err != nil {
		t.Fatal("create table:", err)
	}

	err = session.ExecStmt(`CREATE INDEX IF NOT EXISTS songs_title ON schemagen.songs (title)`)
	if err != nil {
		t.Fatal("create index:", err)
	}

	err = session.ExecStmt(`CREATE TABLE IF NOT EXISTS schemagen.composers (
		id uuid PRIMARY KEY,
		name text)`)
	if err != nil {
		t.Fatal("create table:", err)
	}

	err = session.ExecStmt(`CREATE MATERIALIZED VIEW IF NOT EXISTS schemagen.composers_by_name AS
    	SELECT id, name
    	FROM composers
    	WHERE id IS NOT NULL AND name IS NOT NULL
    	PRIMARY KEY (id, name)`)
	if err != nil {
		t.Fatal("create view:", err)
	}

	err = session.ExecStmt(`CREATE TYPE IF NOT EXISTS schemagen.label (
		name text,
		artists set<text>)`)
	if err != nil {
		t.Fatal("create type:", err)
	}
}

func runSchemagen(t *testing.T, pkgname string) []byte {
	t.Helper()

	dir, err := os.MkdirTemp("", "gocqlx")
	if err != nil {
		t.Fatal(err)
	}
	keyspace := "schemagen"
	cl := "127.0.1.1"

	flagCluster = &cl
	flagKeyspace = &keyspace
	flagPkgname = &pkgname
	flagOutput = &dir

	if err := schemagen(); err != nil {
		t.Fatalf("schemagen() error %s", err)
	}

	f := fmt.Sprintf("%s/%s.go", dir, pkgname)
	b, err := os.ReadFile(f)
	if err != nil {
		t.Fatalf("%s: %s", f, err)
	}
	return b
}
