// Copyright (C) 2017 ScyllaDB
// Use of this source code is governed by a ALv2-style
// license that can be found in the LICENSE file.

package qb

import "testing"

func BenchmarkDeleteBuilder(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Delete("cycling.cyclist_name").Columns("id", "user_uuid", "firstname", "stars").Where(Eq("id")).ToCql()
	}
}
