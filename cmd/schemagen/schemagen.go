package main

import (
	"bytes"
	_ "embed"
	"flag"
	"fmt"
	"go/format"
	"html/template"
	"io"
	"log"
	"os"
	"path"
	"strings"
	"unicode"

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

	schemagen()
}

func schemagen() {
	err := os.MkdirAll(*flagOutput, os.ModePerm)
	if err != nil {
		log.Fatalln("unable to create output directory:", err)
	}

	outputPath := path.Join(*flagOutput, *flagPkgname+".go")
	f, err := os.OpenFile(outputPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0755)
	if err != nil {
		log.Fatalln("unable to open output file:", err)
	}

	metadata := fetchMetadata(createSession())

	if err = renderTemplate(f, metadata); err != nil {
		log.Fatalln("unable to output template:", err)
	}

	if err = f.Close(); err != nil {
		log.Fatalln("unable to close output file:", err)
	}

	log.Println("File written to", outputPath)
}

func fetchMetadata(s *gocqlx.Session) *gocql.KeyspaceMetadata {
	md, err := s.KeyspaceMetadata(*flagKeyspace)
	if err != nil {
		log.Fatalln("unable to fetch keyspace metadata:", err)
	}

	return md
}

func renderTemplate(w io.Writer, md *gocql.KeyspaceMetadata) error {
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

	err = t.Execute(buf, data)
	if err != nil {
		log.Fatalln("unable to execute models template:", err)
	}

	res, err := format.Source(buf.Bytes())
	if err != nil {
		log.Fatalln("template output is not a valid go code:", err)
	}

	_, err = w.Write(res)

	return err
}

func createSession() *gocqlx.Session {
	cluster := createCluster()
	s, err := gocqlx.WrapSession(cluster.CreateSession())
	if err != nil {
		log.Fatalln("unable to create scylla session:", err)
	}
	return &s
}

func createCluster() *gocql.ClusterConfig {
	clusterHosts := getClusterHosts()
	return gocql.NewCluster(clusterHosts...)
}

func getClusterHosts() []string {
	return strings.Split(*flagCluster, ",")
}

func camelize(s string) string {
	buf := []byte(s)
	out := make([]byte, 0, len(buf))
	underscoreSeen := false

	l := len(buf)
	for i := 0; i < l; i++ {
		if !(allowedBindRune(buf[i]) || buf[i] == '_') {
			panic(fmt.Sprint("not allowed name ", s))
		}

		b := rune(buf[i])

		if b == '_' {
			underscoreSeen = true
			continue
		}

		if (i == 0 || underscoreSeen) && unicode.IsLower(b) {
			b = unicode.ToUpper(b)
			underscoreSeen = false
		}

		out = append(out, byte(b))
	}

	return string(out)
}

func allowedBindRune(b byte) bool {
	return (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') || (b >= '0' && b <= '9')
}
