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

type using struct {
	timestamp time.Time
	ttl       time.Duration
}

func (u using) WriteCql(cql *bytes.Buffer) (names []string) {
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
		cql.WriteString(" ")
	}

	return
}
