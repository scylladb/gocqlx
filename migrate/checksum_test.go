// Copyright (C) 2017 ScyllaDB
// Use of this source code is governed by a ALv2-style
// license that can be found in the LICENSE file.

package migrate

import "testing"

func TestFileChecksum(t *testing.T) {
	c, err := fileChecksum("testdata/file")
	if err != nil {
		t.Fatal(err)
	}
	if c != "bbe02f946d5455d74616fc9777557c22" {
		t.Fatal(c)
	}
}
