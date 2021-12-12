// Copyright (C) 2017 ScyllaDB
// Use of this source code is governed by a ALv2-style
// license that can be found in the LICENSE file.

package main

import (
	"testing"
)

func TestMapScyllaToGoType(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"ascii", "string"},
		{"bigint", "int64"},
		{"blob", "[]byte"},
		{"boolean", "bool"},
		{"counter", "int"},
		{"date", "time.Time"},
		{"decimal", "inf.Dec"},
		{"double", "float64"},
		{"duration", "gocql.Duration"},
		{"float", "float32"},
		{"inet", "string"},
		{"int", "int32"},
		{"smallint", "int16"},
		{"text", "string"},
		{"time", "time.Duration"},
		{"timestamp", "time.Time"},
		{"timeuuid", "[16]byte"},
		{"tinyint", "int8"},
		{"uuid", "[16]byte"},
		{"varchar", "string"},
		{"varint", "int64"},
		{"map<int, text>", "map[int32]string"},
		{"list<int>", "[]int32"},
		{"set<int>", "[]int32"},
		{"tuple<boolean, int, smallint>", "struct {\n\t\tField1 bool\n\t\tField2 int32\n\t\tField3 int16\n\t}"},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			if got := mapScyllaToGoType(tt.input); got != tt.want {
				t.Errorf("mapScyllaToGoType() = %v, want %v", got, tt.want)
			}
		})
	}
}
