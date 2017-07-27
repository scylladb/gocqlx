package qb

import (
	"bytes"
	"fmt"
	"strings"
	"time"
)

type expr interface {
	// WriteCql writes a CQL representation of the expr to a buffer and returns
	// slice of parameter names.
	WriteCql(cql *bytes.Buffer) (names []string)
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

type using struct {
	timestamp time.Time
	ttl       time.Duration
}

func (u using) writeCql(cql *bytes.Buffer) {
	var v []string
	if !u.timestamp.IsZero() {
		v = append(v, fmt.Sprint("TIMESTAMP ", u.timestamp.UnixNano()/1000))
	}
	if u.ttl != 0 {
		v = append(v, fmt.Sprint("TTL ", int(u.ttl.Seconds())))
	}
	if len(v) > 0 {
		cql.WriteString("USING ")
		cql.WriteString(strings.Join(v, ","))
		cql.WriteByte(' ')
	}
}

type where []expr

func (w where) writeCql(cql *bytes.Buffer) (names []string) {
	if len(w) == 0 {
		return
	}

	cql.WriteString("WHERE ")
	return writeCql(w, cql)
}

type _if []expr

func (w _if) writeCql(cql *bytes.Buffer) (names []string) {
	if len(w) == 0 {
		return
	}

	cql.WriteString("IF ")
	return writeCql(w, cql)
}

func writeCql(es []expr, cql *bytes.Buffer) (names []string) {
	for i, c := range es {
		names = append(names, c.WriteCql(cql)...)
		if i < len(es)-1 {
			cql.WriteString(" AND ")
		}
	}
	cql.WriteByte(' ')
	return
}
