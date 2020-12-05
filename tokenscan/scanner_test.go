package tokenscan

import (
	"math"
	"testing"

	"github.com/scylladb/gocqlx/qb"
)

func Test_tokenSelect(t *testing.T) {
	type args struct {
		sb           *qb.SelectBuilder
		tokenColumns []string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "test",
			args: args{
				sb:           qb.Select("testtable").Columns("column1", "column2"),
				tokenColumns: []string{"token_column"},
			},
			want: "SELECT column1,column2 FROM testtable WHERE token(token_column)>=? AND token(token_column)<=? ",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got, _ := tokenSelect(tt.args.sb, tt.args.tokenColumns...).ToCql(); got != tt.want {
				t.Errorf("tokenSelect() = %v, overallRange %v", got, tt.want)
			}
		})
	}
}

func Test_getShuffledTokenRanges(t *testing.T) {
	type args struct {
		parallelism int
	}
	tests := []struct {
		name         string
		args         args
		overallRange tokenRange
	}{
		{
			name: "create random token range with parallelism 1",
			args: args{parallelism: 1},
			overallRange: tokenRange{
				startRange: math.MinInt64,
				endRange:   math.MaxInt64,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ranges := getShuffledTokenRanges(tt.args.parallelism)
			min, max := int64(0), int64(0)
			for i, tokenRange := range ranges {
				if tokenRange.startRange < min {
					min = tokenRange.startRange
				}
				if tokenRange.endRange > max {
					max = tokenRange.endRange
				}
				t.Logf("range(%d) = (%d,%d)", i, tokenRange.startRange, tokenRange.endRange)
			}
			if min != tt.overallRange.startRange {
				t.Errorf("expected the generated token ranges includes %d as startRange, but it's lowest bound is %d", tt.overallRange.startRange, min)
			}
			if max != tt.overallRange.endRange {
				t.Errorf("expected the generated token ranges includes %d as endRange, but it's highest bound is %d", tt.overallRange.endRange, max)
			}
		})
	}
}
