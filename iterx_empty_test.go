// Copyright (C) 2017 ScyllaDB
// Use of this source code is governed by a ALv2-style
// license that can be found in the LICENSE file.

package gocqlx

import (
	"testing"

	"github.com/gocql/gocql"
)

func TestIterxSelectEmptyResultClearsDest(t *testing.T) {
	dest := []int{42, 99}
	iter := &Iterx{Iter: &gocql.Iter{}, Mapper: DefaultMapper}
	if err := iter.Select(&dest); err != nil {
		t.Fatal(err)
	}
	if len(dest) != 0 {
		t.Fatalf("expected empty dest for empty result, got %v", dest)
	}
}
