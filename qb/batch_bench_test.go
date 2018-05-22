// Copyright (C) 2017 ScyllaDB
// Use of this source code is governed by a ALv2-style
// license that can be found in the LICENSE file.

package qb

import "testing"

func BenchmarkBatchBuilder(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Batch().Add(mockBuilder{"INSERT INTO cycling.cyclist_name (id,user_uuid,firstname) VALUES (?,?,?) ", []string{"id", "user_uuid", "firstname"}}).ToCql()
	}
}
