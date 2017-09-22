// Copyright (C) 2017 ScyllaDB
// Use of this source code is governed by a ALv2-style
// license that can be found in the LICENSE file.

package migrate

import (
	"crypto/md5"
	"encoding/hex"
	"io"
	"os"
)

var encode = hex.EncodeToString

func checksum(b []byte) string {
	v := md5.Sum(b)
	return encode(v[:])
}

func fileChecksum(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", nil
	}
	defer f.Close()

	h := md5.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	v := h.Sum(nil)
	return encode(v[:]), nil
}
