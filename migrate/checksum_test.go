// Copyright (C) 2017 ScyllaDB
// Use of this source code is governed by a ALv2-style
// license that can be found in the LICENSE file.

package migrate

import (
	"errors"
	"io/fs"
	"os"
	"testing"
	"testing/fstest"
)

func TestFileChecksum(t *testing.T) {
	c, err := fileChecksum(os.DirFS("testdata"), "file")
	if err != nil {
		t.Fatal(err)
	}
	if c != "bbe02f946d5455d74616fc9777557c22" {
		t.Fatal(c)
	}
}

func TestFileChecksumOpenError(t *testing.T) {
	_, err := fileChecksum(fstest.MapFS{}, "missing")
	if !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("fileChecksum() error = %v, expected fs.ErrNotExist", err)
	}
}
