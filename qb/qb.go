package qb

import (
	"strings"
)

// placeholders returns a string with count ? placeholders joined with commas.
func placeholders(count int) string {
	if count < 1 {
		return ""
	}

	return strings.Repeat(",?", count)[1:]
}
