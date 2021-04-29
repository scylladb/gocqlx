// Copyright (C) 2017 ScyllaDB
// Use of this source code is governed by a ALv2-style
// license that can be found in the LICENSE file.

package migrate

import (
	"crypto/md5"
	"encoding/hex"
	"io"
	"io/fs"
)

var encode = hex.EncodeToString

func checksum(b []byte) string {
	v := md5.Sum(b)
	return encode(v[:])
}

func fileChecksum(f fs.FS, path string) (string, error) {
	file, err := f.Open(path)
	if err != nil {
		return "", nil
	}
	defer file.Close()

	h := md5.New()
	if _, err := io.Copy(h, file); err != nil {
		return "", err
	}
	v := h.Sum(nil)
	return encode(v), nil
}
