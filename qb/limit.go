// Copyright (C) 2017 ScyllaDB
// Use of this source code is governed by a ALv2-style
// license that can be found in the LICENSE file.

package qb

import (
	"bytes"
	"strconv"
)

type limit struct {
	value        value
	perPartition bool
}

func limitLit(l uint, perPartition bool) limit {
	val := strconv.FormatUint(uint64(l), 10)
	return limit{
		value:        lit(val),
		perPartition: perPartition,
	}
}

func limitNamed(name string, perPartition bool) limit {
	return limit{
		value:        param(name),
		perPartition: perPartition,
	}
}

func (l limit) writeCql(cql *bytes.Buffer) (names []string) {
	if l.value == nil {
		return nil
	}

	if l.perPartition {
		cql.WriteString("PER PARTITION ")
	}
	cql.WriteString("LIMIT ")

	names = l.value.writeCql(cql)
	cql.WriteByte(' ')
	return
}
