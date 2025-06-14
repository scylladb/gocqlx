package main

import (
	"bytes"
	_ "embed"
	"flag"
	"fmt"
	"go/format"
	"html/template"
	"io/fs"
	"log"
	"os"
	"path"
	"regexp"
	"sort"
	"strings"

	"github.com/gocql/gocql"

	"github.com/scylladb/gocqlx/v3"
	_ "github.com/scylladb/gocqlx/v3/table"
)

var defaultClusterConfig = gocql.NewCluster()

var (
	defaultQueryTimeout      = defaultClusterConfig.Timeout
	defaultConnectionTimeout = defaultClusterConfig.ConnectTimeout
)

var (
	cmd                           = flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	flagCluster                   = cmd.String("cluster", "127.0.0.1", "a comma-separated list of host:port tuples")
	flagKeyspace                  = cmd.String("keyspace", "", "keyspace to inspect")
	flagPkgname                   = cmd.String("pkgname", "models", "the name you wish to assign to your generated package")
	flagOutput                    = cmd.String("output", "models", "the name of the folder to output to")
	flagOutputDirPerm             = cmd.Uint64("output-dir-perm", 0o755, "output directory permissions")
	flagOutputFilePerm            = cmd.Uint64("output-file-perm", 0o644, "output file permissions")
	flagUser                      = cmd.String("user", "", "user for password authentication")
	flagPassword                  = cmd.String("password", "", "password for password authentication")
	flagIgnoreNames               = cmd.String("ignore-names", "", "a comma-separated list of table, view or index names to ignore")
	flagIgnoreIndexes             = cmd.Bool("ignore-indexes", false, "don't generate types for indexes")
	flagQueryTimeout              = cmd.Duration("query-timeout", defaultQueryTimeout, "query timeout ( in seconds )")
	flagConnectionTimeout         = cmd.Duration("connection-timeout", defaultConnectionTimeout, "connection timeout ( in seconds )")
	flagSSLEnableHostVerification = cmd.Bool("ssl-enable-host-verification", false, "don't check server ssl certificate")
	flagSSLCAPath                 = cmd.String("ssl-ca-path", "", "path to ssl CA certificates")
	flagSSLCertPath               = cmd.String("ssl-cert-path", "", "path to ssl certificate")
	flagSSLKeyPath                = cmd.String("ssl-key-path", "", "path to ssl key")
)

//go:embed keyspace.tmpl
var keyspaceTmpl string

func main() {
	err := cmd.Parse(os.Args[1:])
	if err != nil {
		log.Fatalln("can't parse flags")
	}

	if *flagKeyspace == "" {
		log.Fatalln("missing required flag: keyspace")
	}

	if err := schemagen(); err != nil {
		log.Fatalf("failed to generate schema: %s", err)
	}
}

func schemagen() error {
	if err := os.MkdirAll(*flagOutput, os.FileMode(*flagOutputDirPerm)); err != nil {
		return fmt.Errorf("create output directory: %w", err)
	}

	session, err := createSession()
	if err != nil {
		return fmt.Errorf("open output file: %w", err)
	}
	metadata, err := session.KeyspaceMetadata(*flagKeyspace)
	if err != nil {
		return fmt.Errorf("fetch keyspace metadata: %w", err)
	}
	b, err := renderTemplate(metadata)
	if err != nil {
		return fmt.Errorf("render template: %w", err)
	}
	outputPath := path.Join(*flagOutput, *flagPkgname+".go")

	return os.WriteFile(outputPath, b, fs.FileMode(*flagOutputFilePerm))
}

func renderTemplate(md *gocql.KeyspaceMetadata) ([]byte, error) {
	t, err := template.
		New("keyspace.tmpl").
		Funcs(template.FuncMap{"camelize": camelize}).
		Funcs(template.FuncMap{"mapScyllaToGoType": mapScyllaToGoType}).
		Parse(keyspaceTmpl)
	if err != nil {
		log.Fatalln("unable to parse models template:", err)
	}

	// First of all, drop all indicies in metadata if option `-ignore-indexes`
	// is specified.
	if *flagIgnoreIndexes {
		md.Indexes = nil
	}

	// Then remove all tables, views, and indices if their names match the
	// filter.
	ignoredNames := make(map[string]struct{})
	for _, ignoredName := range strings.Split(*flagIgnoreNames, ",") {
		ignoredNames[ignoredName] = struct{}{}
	}
	for name := range ignoredNames {
		delete(md.Tables, name)
		delete(md.Views, name)
		delete(md.Indexes, name)
	}

	// Delete a user-defined type (UDT) if it is not used any column (i.e.
	// table, view, or index).
	orphanedTypes := make(map[string]struct{})
	for userTypeName := range md.Types {
		if !usedInTables(userTypeName, md.Tables) &&
			!usedInViews(userTypeName, md.Views) &&
			!usedInIndices(userTypeName, md.Indexes) {
			orphanedTypes[userTypeName] = struct{}{}
		}
	}
	for typeName := range orphanedTypes {
		delete(md.Types, typeName)
	}

	imports := make([]string, 0)
	if len(md.Types) != 0 {
		imports = append(imports, "github.com/scylladb/gocqlx/v3")
	}

	updateImports := func(columns map[string]*gocql.ColumnMetadata) {
		for _, c := range columns {
			if (c.Type == "timestamp" || c.Type == "date" || c.Type == "time") && !existsInSlice(imports, "time") {
				imports = append(imports, "time")
			}
			if c.Type == "decimal" && !existsInSlice(imports, "gopkg.in/inf.v0") {
				imports = append(imports, "gopkg.in/inf.v0")
			}
			if c.Type == "duration" && !existsInSlice(imports, "github.com/gocql/gocql") {
				imports = append(imports, "github.com/gocql/gocql")
			}
		}
	}

	// Ensure that for each table, view, and index
	//
	// 1. ordered columns are sorted alphabetically;
	// 2. imports are resolves for column types.
	for _, t := range md.Tables {
		sort.Strings(t.OrderedColumns)
		updateImports(t.Columns)
	}
	for _, v := range md.Views {
		sort.Strings(v.OrderedColumns)
		updateImports(v.Columns)
	}
	for _, i := range md.Indexes {
		sort.Strings(i.OrderedColumns)
		updateImports(i.Columns)
	}

	buf := &bytes.Buffer{}
	data := map[string]interface{}{
		"PackageName": *flagPkgname,
		"Tables":      md.Tables,
		"Views":       md.Views,
		"Indexes":     md.Indexes,
		"UserTypes":   md.Types,
		"Imports":     imports,
	}

	if err = t.Execute(buf, data); err != nil {
		return nil, fmt.Errorf("template: %w", err)
	}
	return format.Source(buf.Bytes())
}

func createSession() (gocqlx.Session, error) {
	cluster := gocql.NewCluster(clusterHosts()...)

	if *flagUser != "" {
		cluster.Authenticator = gocql.PasswordAuthenticator{
			Username: *flagUser,
			Password: *flagPassword,
		}
	}

	if *flagQueryTimeout >= 0 {
		cluster.Timeout = *flagQueryTimeout
	}
	if *flagConnectionTimeout >= 0 {
		cluster.ConnectTimeout = *flagConnectionTimeout
	}

	if *flagSSLCAPath != "" || *flagSSLCertPath != "" || *flagSSLKeyPath != "" {
		cluster.SslOpts = &gocql.SslOptions{
			EnableHostVerification: *flagSSLEnableHostVerification,
			CaPath:                 *flagSSLCAPath,
			CertPath:               *flagSSLCertPath,
			KeyPath:                *flagSSLKeyPath,
		}
	}

	return gocqlx.WrapSession(cluster.CreateSession())
}

func clusterHosts() []string {
	return strings.Split(*flagCluster, ",")
}

func existsInSlice(s []string, v string) bool {
	for _, i := range s {
		if v == i {
			return true
		}
	}

	return false
}

// userTypes finds Cassandra schema types enclosed in angle brackets.
// Calling FindAllStringSubmatch on it will return a slice of string slices containing two elements.
// The second element contains the name of the type.
//
//	[["<my_type,", "my_type"] ["my_other_type>", "my_other_type"]]
var userTypes = regexp.MustCompile(`(?:<|\s)(\w+)[>,]`) // match all types contained in set<X>, list<X>, tuple<A, B> etc.

// usedInColumns tests whether the typeName is used in any of columns of the
// provided tables.
func usedInColumns(typeName string, columns map[string]*gocql.ColumnMetadata) bool {
	for _, column := range columns {
		if typeName == column.Type {
			return true
		}
		matches := userTypes.FindAllStringSubmatch(column.Type, -1)
		for _, s := range matches {
			if s[1] == typeName {
				return true
			}
		}
	}
	return false
}

// usedInTables tests whether the typeName is used in any of columns of the
// provided tables.
func usedInTables(typeName string, tables map[string]*gocql.TableMetadata) bool {
	for _, table := range tables {
		if usedInColumns(typeName, table.Columns) {
			return true
		}
	}
	return false
}

// usedInViews tests whether the typeName is used in any of columns of the
// provided views.
func usedInViews(typeName string, tables map[string]*gocql.ViewMetadata) bool {
	for _, table := range tables {
		if usedInColumns(typeName, table.Columns) {
			return true
		}
	}
	return false
}

// usedInIndices tests whether the typeName is used in any of columns of the
// provided indices.
func usedInIndices(typeName string, tables map[string]*gocql.IndexMetadata) bool {
	for _, table := range tables {
		if usedInColumns(typeName, table.Columns) {
			return true
		}
	}
	return false
}
