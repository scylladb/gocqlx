package qb

import (
	"bytes"
	"fmt"
	"time"
)

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
	ts := !u.timestamp.IsZero()

	if ts {
		cql.WriteString("USING TIMESTAMP ")
		cql.WriteString(fmt.Sprint(u.timestamp.UnixNano() / 1000))
		cql.WriteByte(' ')
	}

	if u.ttl != 0 {
		if ts {
			cql.WriteString("AND TTL ")
		} else {
			cql.WriteString("USING TTL ")
		}
		cql.WriteString(fmt.Sprint(int(u.ttl.Seconds())))
		cql.WriteByte(' ')
	}
}

type where cmps

func (w where) writeCql(cql *bytes.Buffer) (names []string) {
	if len(w) == 0 {
		return
	}

	cql.WriteString("WHERE ")
	return cmps(w).writeCql(cql)
}

type _if cmps

func (w _if) writeCql(cql *bytes.Buffer) (names []string) {
	if len(w) == 0 {
		return
	}

	cql.WriteString("IF ")
	return cmps(w).writeCql(cql)
}
