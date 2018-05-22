// Copyright (C) 2017 ScyllaDB
// Use of this source code is governed by a ALv2-style
// license that can be found in the LICENSE file.

package gocqlx

import (
	"strings"
	"testing"
)

func BenchmarkSnakeCase(b *testing.B) {
	for i := 0; i < b.N; i++ {
		snakeCase(snakeTable[b.N%len(snakeTable)].N)
	}
}

func BenchmarkToLower(b *testing.B) {
	for i := 0; i < b.N; i++ {
		strings.ToLower(snakeTable[b.N%len(snakeTable)].N)
	}
}
