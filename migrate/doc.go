// Copyright (C) 2017 ScyllaDB
// Use of this source code is governed by a ALv2-style
// license that can be found in the LICENSE file.

// Package migrate reads migrations from a flat directory containing CQL files.
// There is no imposed naming schema. Migration name is file name.
// The order of migrations is the lexicographical order of file names in the directory.
// You can inject execution of Go code before processing of a migration file, after processing of a migration file, or between statements in a migration file.
package migrate
