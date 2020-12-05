package tokenscan

import (
	"context"
	"math"
	"math/rand"
	"sync"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/gocql/gocql"

	"github.com/scylladb/gocqlx"
	"golang.org/x/sync/errgroup"

	"github.com/scylladb/gocqlx/qb"
)

const (
	// parallelism = nodesInCluster * coresInNode * 3
	defaultParallelism = 72
)

type tokenRange struct {
	startRange int64
	endRange   int64
}

// SessionBuilder creates a gocql.Session
type SessionBuilder interface {
	CreateSession() (*gocql.Session, error)
}

// ApplyFunc is a callback function that is called with each retrieved scan item.
// The scan is aborted if the ApplyFunc returns false
type ApplyFunc func(context.Context, map[string]interface{}) bool

// Scanner is a scylla token range scanner
type Scanner struct {
	sessionBuilder SessionBuilder
	tokenRanges    []*tokenRange
	parallelism    int
	results        prometheus.Counter
	errors         prometheus.Counter
}

// Opt is an option func
type Opt func(*Scanner)

// WithParallelism configures the scan parallelism
func WithParallelism(parallelism int) Opt {
	return func(s *Scanner) {
		s.parallelism = parallelism
	}
}

// New creates a new scylla token range scanner
func New(sessionBuilder SessionBuilder, opts ...Opt) *Scanner {
	s := &Scanner{
		sessionBuilder: sessionBuilder,

		parallelism: defaultParallelism,
		results: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "scyllascanner_results_total",
			Help: "Number of items returned by the scylla scanner",
		}),
		errors: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "scyllascaller_errors_total",
			Help: "Number of errors encountered while scanning",
		}),
	}
	for _, o := range opts {
		o(s)
	}

	s.tokenRanges = getShuffledTokenRanges(s.parallelism)
	return s
}

// Describe implements the prometheus.Collector interface
func (s *Scanner) Describe(ch chan<- *prometheus.Desc) {
	s.results.Describe(ch)
	s.errors.Describe(ch)
}

// Collect implements the prometheus.Collector interface
func (s *Scanner) Collect(ch chan<- prometheus.Metric) {
	s.results.Collect(ch)
	s.errors.Collect(ch)
}

// Scan performs a token range scan with the provided select builder
// The scanned items are passed to the ApplyFunc. If the ApplyFunc returns ErrAbort the scanning process
// is aborted. All other ApplyFunc errors are just logged
func (s *Scanner) Scan(
	ctx context.Context,
	selectBuilder *qb.SelectBuilder,
	partitionKeyColumns []string,
	applyFunc ApplyFunc,
) error {
	// send all available token ranges to a channel
	rangesChannel := make(chan *tokenRange, len(s.tokenRanges))
	for i := range s.tokenRanges {
		rangesChannel <- s.tokenRanges[i]
	}
	close(rangesChannel)

	// create a cancelable context
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var sessionCreationWaitGroup sync.WaitGroup
	errGroup := errgroup.Group{}
	sessionCreationWaitGroup.Add(s.parallelism)

	iterRows := make(chan map[string]interface{}) // TODO: channel cap?
	defer close(iterRows)

	go func() {
		for row := range iterRows {
			if abort := applyFunc(ctx, row); abort {
				cancel()
				return
			}
		}
	}()

	for i := 0; i < s.parallelism; i++ {
		errGroup.Go(func() error {
			session, err := s.sessionBuilder.CreateSession()
			if err != nil {
				cancel()
				return err
			}
			defer session.Close()

			// synchronize start of token range scans, i.e. wait for all sessions to be created
			sessionCreationWaitGroup.Done()
			// Read ranges from the channel, until we have completed accessing all ranges
			for r := range rangesChannel {
				stmt, names := tokenSelect(selectBuilder, partitionKeyColumns...).ToCql()
				q := gocqlx.Query(session.Query(stmt, names).WithContext(ctx), names).BindMap(qb.M{
					"lower": r.startRange,
					"upper": r.endRange,
				})
				iter := q.Iter()
				for {
					row := make(map[string]interface{})
					if !iter.MapScan(row) {
						break
					}
					select {
					case iterRows <- row: // send the result to the iterRows channel to be passed to the ApplyFunc
						s.results.Inc()
					case <-ctx.Done():
						return ctx.Err()
					}
				}
				if err := iter.Close(); err != nil {
					s.errors.Inc()
					cancel()
					return err
				}
			}
			return nil
		})
	}
	if err := errGroup.Wait(); err != nil {
		// context is cancelled by defer statement
		return err
	}

	return nil
}

func tokenSelect(sb *qb.SelectBuilder, tokenColumns ...string) *qb.SelectBuilder {
	tokenBuilder := qb.Token(tokenColumns...)
	lowerBound := tokenBuilder.GtOrEqValueNamed("lower")
	upperBound := tokenBuilder.LtOrEqValueNamed("upper")
	s := *sb
	return s.Where(lowerBound, upperBound)
}

// Calculates the token range values to be executed in parallel
// see https://www.scylladb.com/2017/03/28/parallel-efficient-full-table-scan-scylla/
func getShuffledTokenRanges(parallelism int) []*tokenRange {
	var numberOfRanges = int64(parallelism * 100)
	var maxSize uint64 = math.MaxInt64 * 2
	var rangeSize = maxSize / uint64(numberOfRanges)

	var start int64 = math.MinInt64
	var end int64
	var shouldBreak = false

	var ranges = make([]*tokenRange, numberOfRanges)

	for i := int64(0); i < numberOfRanges; i++ {
		end = start + int64(rangeSize)
		if start > 0 && end < 0 {
			end = math.MaxInt64
			shouldBreak = true
		}

		ranges[i] = &tokenRange{startRange: start, endRange: end}

		if shouldBreak {
			break
		}

		start = end + 1
	}

	// shuffle the ranges
	for i := 1; i < len(ranges); i++ {
		r := rand.Intn(i + 1)
		if i != r {
			ranges[r], ranges[i] = ranges[i], ranges[r]
		}
	}

	return ranges
}
