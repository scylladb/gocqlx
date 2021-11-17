package main

import (
	"bytes"
	_ "embed"
	"flag"
	"fmt"
	"go/format"
	"html/template"
	"io/ioutil"
	"log"
	"os"
	"path"
	"strings"

	"github.com/gocql/gocql"
	"github.com/scylladb/gocqlx/v2"
	_ "github.com/scylladb/gocqlx/v2/table"
)

var (
	cmd          = flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	flagCluster  = cmd.String("cluster", "127.0.0.1", "a comma-separated list of host:port tuples")
	flagKeyspace = cmd.String("keyspace", "", "keyspace to inspect")
	flagPkgname  = cmd.String("pkgname", "models", "the name you wish to assign to your generated package")
	flagOutput   = cmd.String("output", "models", "the name of the folder to output to")
)

var (
	//go:embed keyspace.tmpl
	keyspaceTmpl string
)

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
	if err := os.MkdirAll(*flagOutput, os.ModePerm); err != nil {
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

	return ioutil.WriteFile(outputPath, b, os.ModePerm)
}

func renderTemplate(md *gocql.KeyspaceMetadata) ([]byte, error) {
	t, err := template.
		New("keyspace.tmpl").
		Funcs(template.FuncMap{"camelize": camelize}).
		Parse(keyspaceTmpl)

	if err != nil {
		log.Fatalln("unable to parse models template:", err)
	}

	buf := &bytes.Buffer{}
	data := map[string]interface{}{
		"PackageName": *flagPkgname,
		"Tables":      md.Tables,
	}

	if err = t.Execute(buf, data); err != nil {
		return nil, fmt.Errorf("template: %w", err)
	}
	return format.Source(buf.Bytes())
}

func createSession() (gocqlx.Session, error) {
	cluster := gocql.NewCluster(clusterHosts()...)
	return gocqlx.WrapSession(cluster.CreateSession())
}

func clusterHosts() []string {
	return strings.Split(*flagCluster, ",")
}
