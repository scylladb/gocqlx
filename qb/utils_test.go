package qb

import (
	"testing"
	"time"
)

func TestTTL(t *testing.T) {
	if TTL(time.Second*86400) != 86400 {
		t.Fatal("wrong ttl")
	}
}

func TestTimestamp(t *testing.T) {
	if Timestamp(time.Unix(0, 0).Add(time.Microsecond*123456789)) != 123456789 {
		t.Fatal("wrong timestamp")
	}
}
