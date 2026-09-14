// Copyright (C) 2017 ScyllaDB
// Use of this source code is governed by a ALv2-style
// license that can be found in the LICENSE file.

package main

import (
	"strings"
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
		{"frozen<set<rcas>>", "[]RcasUserType"},
		{"frozen<map<incidentcustomuserrole, set<userid>>>", "map[IncidentcustomuserroleUserType][]UseridUserType"},
		{"map<text, frozen<list<album>>>", "map[string][]AlbumUserType"},
		{"tuple<boolean, int, smallint>", "struct {\n\t\tField1 bool\n\t\tField2 int32\n\t\tField3 int16\n\t}"},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, err := mapScyllaToGoType(tt.input)
			if err != nil {
				t.Fatal(err)
			}
			if got != tt.want {
				t.Errorf("mapScyllaToGoType() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMapScyllaToGoTypeRejectsNonComparableMapKeys(t *testing.T) {
	tests := []string{
		"map<blob, text>",
		"map<decimal, text>",
		"map<frozen<set<int>>, text>",
	}
	for _, tt := range tests {
		t.Run(tt, func(t *testing.T) {
			_, err := mapScyllaToGoType(tt)
			if err == nil {
				t.Fatal("expected error")
			}
			if !strings.Contains(err.Error(), "unsupported non-comparable CQL map key type") {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestMapScyllaToGoTypeRejectsExpandedTupleShapes(t *testing.T) {
	// These valid CQL shapes are intentionally rejected until gocqlx#375
	// settles end-to-end generated tuple support.
	// https://github.com/scylladb/gocqlx/issues/375
	tests := []string{
		"frozen<tuple<int>>",
		"list<tuple<int>>",
		"set<tuple<int>>",
		"map<text, tuple<int>>",
		"map<tuple<int>, text>",
		"tuple<boolean, map<text, frozen<set<album>>>>",
		"tuple<frozen<album>>",
		"tuple<frozen<set<album>>>",
		"tuple<list<int>>",
		"tuple<tuple<int>>",
	}
	for _, tt := range tests {
		t.Run(tt, func(t *testing.T) {
			_, err := mapScyllaToGoType(tt)
			if err == nil {
				t.Fatal("expected error")
			}
			if !strings.Contains(err.Error(), "unsupported non-flat tuple element CQL type") {
				t.Fatalf("unexpected error: %v", err)
			}
			if !strings.Contains(err.Error(), "https://github.com/scylladb/gocqlx/issues/375") {
				t.Fatalf("error does not include issue link: %v", err)
			}
		})
	}
}

func TestMapScyllaToGoTypeRejectsUnsupportedConstructors(t *testing.T) {
	tests := []string{
		"vector<float, 3>",
		"map<int>",
		"map<int, text, boolean>",
		"list<int, text>",
		"frozen<>",
	}
	for _, tt := range tests {
		t.Run(tt, func(t *testing.T) {
			_, err := mapScyllaToGoType(tt)
			if err == nil {
				t.Fatal("expected error")
			}
			if !strings.Contains(err.Error(), "unsupported CQL type") {
				t.Fatalf("unexpected error: %v", err)
			}
			if !strings.Contains(err.Error(), tt) {
				t.Fatalf("error does not include type %q: %v", tt, err)
			}
		})
	}
}
