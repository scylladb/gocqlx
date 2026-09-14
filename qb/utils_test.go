package qb

import (
	"testing"
	"time"
)

func TestQuoteTableName(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{
			name: "simple qualified name",
			in:   "cycling.cyclist_name",
			want: "cycling.cyclist_name",
		},
		{
			name: "mixed case table",
			in:   "ks.tableName",
			want: `ks."tableName"`,
		},
		{
			name: "mixed case keyspace and table",
			in:   "keySpace.tableName",
			want: `"keySpace"."tableName"`,
		},
		{
			name: "reserved keyword table",
			in:   "ks.select",
			want: `ks."select"`,
		},
		{
			name: "reserved keyword keyspace",
			in:   "select.tbl_name",
			want: `"select".tbl_name`,
		},
		{
			name: "non-reserved keyword table",
			in:   "ks.aggregate",
			want: `ks.aggregate`,
		},
		{
			name: "already quoted table",
			in:   `ks."tableName"`,
			want: `ks."tableName"`,
		},
		{
			name: "dot inside quoted keyspace",
			in:   `"key.space".tableName`,
			want: `"key.space"."tableName"`,
		},
		{
			name: "quote inside unquoted keyspace",
			in:   `key"space.tableName`,
			want: `"key""space"."tableName"`,
		},
		{
			name: "escaped quote inside quoted keyspace",
			in:   `"key""space".tableName`,
			want: `"key""space"."tableName"`,
		},
		{
			name: "unescaped quote inside quoted table",
			in:   `ks."table"name"`,
			want: `ks."""table""name"""`,
		},
		{
			name: "unescaped quotes inside quoted table",
			in:   `"table"; DROP TABLE "other"`,
			want: `"""table""; DROP TABLE ""other"""`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := quoteTableName(tt.in); got != tt.want {
				t.Errorf("got %q, want %q", got, tt.want)
			}
		})
	}
}

func TestFormatDuration(t *testing.T) {
	tests := []struct {
		name     string
		input    time.Duration
		expected string
	}{
		{
			name:     "Zero duration",
			input:    0,
			expected: "",
		},
		{
			input:    500 * time.Millisecond,
			expected: "500ms",
		},
		{
			input:    10 * time.Second,
			expected: "10s",
		},
		{
			input:    3 * time.Minute,
			expected: "3m",
		},
		{
			input:    (2 * time.Minute) + (30 * time.Second),
			expected: "2m30s",
		},
		{
			input:    (15 * time.Second) + (250 * time.Millisecond),
			expected: "15s250ms",
		},
		{
			input:    (1 * time.Minute) + (45 * time.Second) + (123 * time.Millisecond),
			expected: "1m45s123ms",
		},
		{
			input:    (5 * time.Minute) + (1 * time.Second) + (999 * time.Millisecond),
			expected: "5m1s999ms",
		},
		{
			input:    (2 * time.Second) + (1500 * time.Millisecond), // 3 seconds, 500ms
			expected: "3s500ms",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual := formatDuration(tt.input)
			if actual != tt.expected {
				t.Errorf("got %q, want %q", actual, tt.expected)
			}
		})
	}
}
