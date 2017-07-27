package qb

import "bytes"

type mockExpr struct {
	cql   string
	names []string
}

func (m mockExpr) WriteCql(cql *bytes.Buffer) (names []string) {
	cql.WriteString(m.cql)
	names = m.names
	return
}
