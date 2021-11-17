// Copyright (C) 2017 ScyllaDB
// Use of this source code is governed by a ALv2-style
// license that can be found in the LICENSE file.

package main

import "testing"

func TestCamelize(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"hello", "Hello"},
		{"_hello", "Hello"},
		{"__hello", "Hello"},
		{"hello_", "Hello"},
		{"hello_world", "HelloWorld"},
		{"hello__world", "HelloWorld"},
		{"_hello_world", "HelloWorld"},
		{"helloWorld", "HelloWorld"},
		{"HelloWorld", "HelloWorld"},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			if got := camelize(tt.input); got != tt.want {
				t.Errorf("camelize() = %v, want %v", got, tt.want)
			}
		})
	}
}
