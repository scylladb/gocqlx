// Copyright (C) 2017 ScyllaDB

package gocqlx

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
	"text/template"

	"github.com/gocql/gocql"
)

type schemaRecreator struct {
	km *gocql.KeyspaceMetadata
}

func (sr schemaRecreator) Recreate() (string, error) {
	sb := &strings.Builder{}

	if err := sr.recreateKeyspace(sb); err != nil {
		return "", err
	}

	sortedTypes := sr.typesSortedTopologically()
	for _, tm := range sortedTypes {
		if err := sr.recreateUserTypes(sb, tm); err != nil {
			return "", err
		}
	}

	for _, tm := range sr.km.Tables {
		if err := sr.recreateTable(sb, sr.km.Name, tm); err != nil {
			return "", err
		}
	}

	for _, im := range sr.km.Indexes {
		if err := sr.recreateIndex(sb, im); err != nil {
			return "", err
		}
	}

	for _, fm := range sr.km.Functions {
		if err := sr.recreateFunction(sb, sr.km.Name, fm); err != nil {
			return "", err
		}
	}

	for _, am := range sr.km.Aggregates {
		if err := sr.recreateAggregate(sb, am); err != nil {
			return "", err
		}
	}

	for _, vm := range sr.km.Views {
		if err := sr.recreateView(sb, vm); err != nil {
			return "", err
		}
	}

	return sb.String(), nil
}

func (sr schemaRecreator) typesSortedTopologically() []*gocql.TypeMetadata {
	sortedTypes := make([]*gocql.TypeMetadata, 0, len(sr.km.Types))
	for _, tm := range sr.km.Types {
		sortedTypes = append(sortedTypes, tm)
	}
	sort.Slice(sortedTypes, func(i, j int) bool {
		for _, ft := range sortedTypes[j].FieldTypes {
			if strings.Contains(ft, sortedTypes[i].Name) {
				return true
			}
		}
		return false
	})
	return sortedTypes
}

func (sr schemaRecreator) recreateTable(w io.Writer, kn string, tm *gocql.TableMetadata) error {
	t := template.Must(template.New("table").
		Funcs(map[string]interface{}{
			"escape":                  sr.escape,
			"recreateTableColumns":    sr.recreateTableColumns,
			"recreateTableProperties": sr.recreateTableProperties,
		}).
		Parse(`
CREATE TABLE {{ .KeyspaceName }}.{{ .Tm.Name }} (
  {{ recreateTableColumns .Tm }}
) WITH {{ recreateTableProperties .Tm.ClusteringColumns .Tm.Options .Tm.Flags }};
`))
	if err := t.Execute(w, map[string]interface{}{
		"Tm":           tm,
		"KeyspaceName": kn,
	}); err != nil {
		return err
	}
	return nil
}

func (sr schemaRecreator) recreateTableColumns(tm *gocql.TableMetadata) string {
	sb := strings.Builder{}

	var columns []string
	for _, cn := range tm.OrderedColumns {
		cm := tm.Columns[cn]
		column := fmt.Sprintf("%s %s", cn, cm.Type)
		if cm.Kind == gocql.ColumnStatic {
			column += " static"
		}
		columns = append(columns, column)
	}
	if len(tm.PartitionKey) == 1 && len(tm.ClusteringColumns) == 0 && len(columns) > 0 {
		columns[0] += " PRIMARY KEY"
	}

	sb.WriteString(strings.Join(columns, ",\n  "))

	if len(tm.PartitionKey) > 1 || len(tm.ClusteringColumns) > 0 {
		sb.WriteString(",\n  PRIMARY KEY (")
		sb.WriteString(sr.partitionKeyString(tm.PartitionKey, tm.ClusteringColumns))
		sb.WriteRune(')')
	}

	return sb.String()
}

func (sr schemaRecreator) partitionKeyString(pks, cks []*gocql.ColumnMetadata) string {
	sb := strings.Builder{}

	if len(pks) > 1 {
		sb.WriteRune('(')
		for i, pk := range pks {
			if i != 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(pk.Name)
		}
		sb.WriteRune(')')
	} else {
		sb.WriteString(pks[0].Name)
	}

	if len(cks) > 0 {
		sb.WriteString(", ")
		for i, ck := range cks {
			if i != 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(ck.Name)
		}
	}

	return sb.String()
}

func (sr schemaRecreator) recreateTableProperties(cks []*gocql.ColumnMetadata, opts gocql.TableMetadataOptions, flags []string) (string, error) {
	sb := strings.Builder{}

	var properties []string

	compactStorage := len(flags) > 0 && (contains(flags, gocql.TableFlagDense) ||
		contains(flags, gocql.TableFlagSuper) ||
		!contains(flags, gocql.TableFlagCompound))

	if compactStorage {
		properties = append(properties, "COMPACT STORAGE")
	}

	if len(cks) > 0 {
		var inner []string
		for _, col := range cks {
			inner = append(inner, fmt.Sprintf("%s %s", col.Name, col.ClusteringOrder))
		}
		properties = append(properties, fmt.Sprintf("CLUSTERING ORDER BY (%s)", strings.Join(inner, ", ")))
	}

	options, err := sr.recreateTableOptions(opts)
	if err != nil {
		return "", err
	}
	properties = append(properties, options...)

	sb.WriteString(strings.Join(properties, "\n    AND "))
	return sb.String(), nil
}

func (sr schemaRecreator) recreateTableOptions(ops gocql.TableMetadataOptions) ([]string, error) {
	opts := map[string]interface{}{
		"bloom_filter_fp_chance":      ops.BloomFilterFpChance,
		"comment":                     ops.Comment,
		"crc_check_chance":            ops.CrcCheckChance,
		"dclocal_read_repair_chance":  ops.DcLocalReadRepairChance,
		"default_time_to_live":        ops.DefaultTimeToLive,
		"gc_grace_seconds":            ops.GcGraceSeconds,
		"max_index_interval":          ops.MaxIndexInterval,
		"memtable_flush_period_in_ms": ops.MemtableFlushPeriodInMs,
		"min_index_interval":          ops.MinIndexInterval,
		"read_repair_chance":          ops.ReadRepairChance,
		"speculative_retry":           ops.SpeculativeRetry,
	}

	caching, err := json.Marshal(ops.Caching)
	if err != nil {
		return nil, err
	}

	compaction, err := json.Marshal(ops.Compaction)
	if err != nil {
		return nil, err
	}

	compression, err := json.Marshal(ops.Compression)
	if err != nil {
		return nil, err
	}

	cdc, err := json.Marshal(ops.CDC)
	if err != nil {
		return nil, err
	}

	opts["compression"] = bytes.ReplaceAll(compression, []byte(`"`), []byte(`'`))
	opts["compaction"] = bytes.ReplaceAll(compaction, []byte(`"`), []byte(`'`))
	opts["caching"] = bytes.ReplaceAll(caching, []byte(`"`), []byte(`'`))
	if string(cdc) != "null" {
		opts["cdc"] = bytes.ReplaceAll(cdc, []byte(`"`), []byte(`'`))
	}

	out := make([]string, 0, len(opts))
	for key, opt := range opts {
		out = append(out, fmt.Sprintf("%s = %s", key, sr.escape(opt)))
	}

	sort.Strings(out)
	return out, nil
}

func (sr schemaRecreator) recreateFunction(w io.Writer, keyspaceName string, fm *gocql.FunctionMetadata) error {
	t := template.Must(template.New("functions").
		Funcs(map[string]interface{}{
			"escape":      sr.escape,
			"zip":         sr.zip,
			"stripFrozen": sr.stripFrozen,
		}).
		Parse(`
CREATE FUNCTION {{ escape .keyspaceName }}.{{ escape .fm.Name }} ( 
  {{- range $i, $args := zip .fm.ArgumentNames .fm.ArgumentTypes }}
  {{- if ne $i 0 }}, {{ end }}
  {{- escape (index $args 0) }} {{ stripFrozen (index $args 1) }}
  {{- end -}})
  {{ if .fm.CalledOnNullInput }}CALLED{{ else }}RETURNS NULL{{ end }} ON NULL INPUT
  RETURNS {{ .fm.ReturnType }}
  LANGUAGE {{ .fm.Language }}
  AS $${{ .fm.Body }}$$;
`))

	if err := t.Execute(w, map[string]interface{}{
		"fm":           fm,
		"keyspaceName": keyspaceName,
	}); err != nil {
		return err
	}
	return nil
}

func (sr schemaRecreator) recreateView(w io.Writer, vm *gocql.ViewMetadata) error {
	t := template.Must(template.New("views").
		Funcs(map[string]interface{}{
			"zip":                     sr.zip,
			"partitionKeyString":      sr.partitionKeyString,
			"recreateTableProperties": sr.recreateTableProperties,
		}).
		Parse(`
CREATE MATERIALIZED VIEW {{ .vm.KeyspaceName }}.{{ .vm.ViewName }} AS
SELECT {{ if .vm.IncludeAllColumns }}*{{ else }}
{{- range $i, $col := .vm.OrderedColumns }}
{{- if ne $i 0 }}, {{ end }}
{{- $col }}
{{- end }}
{{- end }}
FROM {{ .vm.KeyspaceName }}.{{ .vm.BaseTableName }}
WHERE {{ .vm.WhereClause }}
PRIMARY KEY ({{ partitionKeyString .vm.PartitionKey .vm.ClusteringColumns }})
WITH {{ recreateTableProperties .vm.ClusteringColumns .vm.Options .flags }};
`))
	if err := t.Execute(w, map[string]interface{}{
		"vm":    vm,
		"flags": []string{},
	}); err != nil {
		return err
	}
	return nil
}

func (sr schemaRecreator) recreateAggregate(w io.Writer, am *gocql.AggregateMetadata) error {
	t := template.Must(template.New("aggregate").
		Funcs(map[string]interface{}{
			"stripFrozen": sr.stripFrozen,
		}).
		Parse(`
CREATE AGGREGATE {{ .Keyspace }}.{{ .Name }}( 
  {{- range $arg, $i := .ArgumentTypes }}
  {{- if ne $i 0 }}, {{ end }}
  {{- stripFrozen $arg }}
  {{- end -}})
  SFUNC {{ .StateFunc.Name }}
  STYPE {{ stripFrozen .State }}
  {{- if ne .FinalFunc.Name "" }}
  FINALFUNC {{ .FinalFunc.Name }}
  {{- end -}}
  {{- if ne .InitCond "" }}
  INITCOND {{ .InitCond }}
  {{- end -}}
);
`))

	if err := t.Execute(w, am); err != nil {
		return err
	}
	return nil
}

func (sr schemaRecreator) recreateUserTypes(w io.Writer, tm *gocql.TypeMetadata) error {
	t := template.Must(template.New("types").
		Funcs(map[string]interface{}{
			"zip": sr.zip,
		}).
		Parse(`
CREATE TYPE {{ .Keyspace }}.{{ .Name }} ( 
  {{- range $i, $fields := zip .FieldNames .FieldTypes }} {{- if ne $i 0 }},{{ end }}
  {{ index $fields 0 }} {{ index $fields 1 }}
  {{- end }}
);
`))

	if err := t.Execute(w, tm); err != nil {
		return err
	}
	return nil
}

func (sr schemaRecreator) recreateIndex(w io.Writer, im *gocql.IndexMetadata) error {
	// Scylla doesn't support any custom indexes
	if im.Kind != gocql.IndexKindCustom {
		options := im.Options
		indexTarget := options["target"]

		// secondary index
		si := struct {
			ClusteringKeys []string `json:"ck"`
			PartitionKeys  []string `json:"pk"`
		}{}

		if err := json.Unmarshal([]byte(indexTarget), &si); err == nil {
			indexTarget = fmt.Sprintf("(%s), %s",
				strings.Join(si.PartitionKeys, ","),
				strings.Join(si.ClusteringKeys, ","),
			)
		}

		_, err := fmt.Fprintf(w, "\nCREATE INDEX %s ON %s.%s (%s);\n",
			im.Name,
			im.KeyspaceName,
			im.TableName,
			indexTarget,
		)
		if err != nil {
			return err
		}
	}

	return nil
}

func (sr schemaRecreator) recreateKeyspace(w io.Writer) error {
	fixStrategy := func(v string) string {
		return strings.TrimPrefix(v, "org.apache.cassandra.locator.")
	}

	t := template.Must(template.New("keyspace").
		Funcs(map[string]interface{}{
			"escape":      sr.escape,
			"fixStrategy": fixStrategy,
		}).
		Parse(`
CREATE KEYSPACE {{ .Name }} WITH replication = {
  'class': {{ escape ( fixStrategy .StrategyClass) }}
  {{- range $key, $value := .StrategyOptions }},
  {{ escape $key }}: {{ escape $value }}
  {{- end }}
}{{ if not .DurableWrites }} AND durable_writes = 'false'{{ end }};
`))

	if err := t.Execute(w, sr.km); err != nil {
		return err
	}
	return nil
}

func contains(in []string, v string) bool {
	for _, e := range in {
		if e == v {
			return true
		}
	}
	return false
}

func (sr schemaRecreator) zip(a []string, b []string) [][]string {
	m := make([][]string, len(a))
	for i := range a {
		m[i] = []string{a[i], b[i]}
	}
	return m
}

func (sr schemaRecreator) escape(e interface{}) string {
	switch v := e.(type) {
	case int, float64:
		return fmt.Sprint(v)
	case bool:
		if v {
			return "true"
		}
		return "false"
	case string:
		return "'" + strings.ReplaceAll(v, "'", "''") + "'"
	case []byte:
		return string(v)
	}
	return ""
}

func (sr schemaRecreator) stripFrozen(v string) string {
	return strings.TrimSuffix(strings.TrimPrefix(v, "frozen<"), ">")
}
