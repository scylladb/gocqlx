// Copyright (C) 2017 ScyllaDB
// Use of this source code is governed by a ALv2-style
// license that can be found in the LICENSE file.

package qb

import (
	"bytes"
	"fmt"
	"strings"
	"time"
)

// placeholders returns a string with count ? placeholders joined with commas.
func placeholders(cql *bytes.Buffer, count int) {
	if count < 1 {
		return
	}

	for i := 0; i < count-1; i++ {
		cql.WriteByte('?')
		cql.WriteByte(',')
	}
	cql.WriteByte('?')
}

type columns []string

func (cols columns) writeCql(cql *bytes.Buffer) {
	for i, c := range cols {
		cql.WriteString(c)
		if i < len(cols)-1 {
			cql.WriteByte(',')
		}
	}
}

func quoteTableName(table string) string {
	if !strings.ContainsAny(table, `."`) {
		return quoteIdentifier(table)
	}

	parts := splitTableName(table)
	for i := range parts {
		parts[i] = quoteIdentifier(parts[i])
	}
	return strings.Join(parts, ".")
}

func splitTableName(table string) []string {
	var parts []string
	start := 0
	quoted := false

	for i := 0; i < len(table); i++ {
		switch table[i] {
		case '"':
			switch {
			case quoted && i+1 < len(table) && table[i+1] == '"':
				i++
			case quoted:
				quoted = false
			case i == start:
				quoted = true
			}
		case '.':
			if !quoted {
				parts = append(parts, table[start:i])
				start = i + 1
			}
		}
	}

	return append(parts, table[start:])
}

func quoteIdentifier(identifier string) string {
	if isQuotedIdentifier(identifier) || !needsQuoting(identifier) {
		return identifier
	}
	return `"` + strings.ReplaceAll(identifier, `"`, `""`) + `"`
}

func isQuotedIdentifier(identifier string) bool {
	if len(identifier) < 2 || identifier[0] != '"' || identifier[len(identifier)-1] != '"' {
		return false
	}

	for i := 1; i < len(identifier)-1; i++ {
		if identifier[i] != '"' {
			continue
		}
		if i+1 < len(identifier)-1 && identifier[i+1] == '"' {
			i++
			continue
		}
		return false
	}
	return true
}

// Reserved words from ScyllaDB CQL Appendix A.
var reservedCQLKeywords = map[string]struct{}{
	"add":          {},
	"allow":        {},
	"alter":        {},
	"and":          {},
	"apply":        {},
	"asc":          {},
	"authorize":    {},
	"batch":        {},
	"begin":        {},
	"by":           {},
	"columnfamily": {},
	"create":       {},
	"delete":       {},
	"desc":         {},
	"describe":     {},
	"drop":         {},
	"entries":      {},
	"execute":      {},
	"from":         {},
	"full":         {},
	"grant":        {},
	"if":           {},
	"in":           {},
	"index":        {},
	"infinity":     {},
	"insert":       {},
	"into":         {},
	"keyspace":     {},
	"limit":        {},
	"modify":       {},
	"nan":          {},
	"norecursive":  {},
	"not":          {},
	"null":         {},
	"of":           {},
	"on":           {},
	"or":           {},
	"order":        {},
	"primary":      {},
	"rename":       {},
	"replace":      {},
	"revoke":       {},
	"schema":       {},
	"select":       {},
	"set":          {},
	"table":        {},
	"to":           {},
	"token":        {},
	"truncate":     {},
	"unlogged":     {},
	"update":       {},
	"use":          {},
	"using":        {},
	"where":        {},
	"with":         {},
}

func needsQuoting(identifier string) bool {
	if identifier == "" {
		return false
	}

	for i, r := range identifier {
		switch {
		case r >= 'a' && r <= 'z':
		case r >= '0' && r <= '9' && i > 0:
		case r == '_' && i > 0:
		default:
			return true
		}
	}
	if _, ok := reservedCQLKeywords[strings.ToLower(identifier)]; ok {
		return true
	}
	return false
}

func formatDuration(d time.Duration) string {
	// Round the duration to the nearest millisecond
	// Extract hours, minutes, seconds, and milliseconds
	minutes := d / time.Minute
	d %= time.Minute
	seconds := d / time.Second
	d %= time.Second
	milliseconds := d / time.Millisecond

	// Format the duration string
	var res []string
	if minutes > 0 {
		res = append(res, fmt.Sprintf("%dm", minutes))
	}
	if seconds > 0 {
		res = append(res, fmt.Sprintf("%ds", seconds))
	}
	if milliseconds > 0 {
		res = append(res, fmt.Sprintf("%dms", milliseconds))
	}
	return strings.Join(res, "")
}
