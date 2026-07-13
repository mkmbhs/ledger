// Command loadtest hammers a running ledger with concurrent random transfers and
// then proves the books still balance: it reports throughput and latency, and
// asserts that the sum of every account balance equals the total opening sum —
// because a correct double-entry ledger never creates or destroys money, no
// matter how much concurrency you throw at it.
//
// Example (with `docker compose up` serving REST on :8080):
//
//	go run ./cmd/loadtest -base http://localhost:8080 -accounts 50 -workers 32 -duration 10s
//
// It exits non-zero if the conservation invariant fails.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand/v2"
	"net/http"
	"sort"
	"sync"
	"time"
)

func main() {
	if err := run(); err != nil {
		log.Fatalf("loadtest: %v", err)
	}
}

type config struct {
	base     string
	accounts int
	workers  int
	duration time.Duration
	currency string
	opening  int64
}

func run() error {
	cfg := config{}
	flag.StringVar(&cfg.base, "base", "http://localhost:8080", "base URL of the ledger REST API")
	flag.IntVar(&cfg.accounts, "accounts", 50, "number of accounts to seed")
	flag.IntVar(&cfg.workers, "workers", 32, "number of concurrent transfer workers")
	flag.DurationVar(&cfg.duration, "duration", 10*time.Second, "how long to drive load")
	flag.StringVar(&cfg.currency, "currency", "USD", "currency for every seeded account")
	flag.Int64Var(&cfg.opening, "opening", 1_000_000, "opening balance per account, in minor units")
	flag.Parse()

	if cfg.accounts < 2 {
		return fmt.Errorf("-accounts must be at least 2 (transfers need two distinct accounts)")
	}
	if cfg.workers < 1 {
		return fmt.Errorf("-workers must be at least 1")
	}

	// One shared client with bounded, explicit timeouts and a connection pool
	// sized for the worker count — the default MaxIdleConnsPerHost of 2 would
	// otherwise serialise the workers behind a handful of reused connections.
	client := &http.Client{
		Timeout: 10 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        cfg.workers * 2,
			MaxIdleConnsPerHost: cfg.workers,
			IdleConnTimeout:     90 * time.Second,
		},
	}

	ctx := context.Background()

	// Phase 1 — seed.
	openingSum := int64(cfg.accounts) * cfg.opening
	log.Printf("seeding %d accounts at %d %s each (opening sum = %d)...", cfg.accounts, cfg.opening, cfg.currency, openingSum)
	if err := seed(ctx, client, cfg); err != nil {
		return fmt.Errorf("seed: %w", err)
	}

	// Phase 2 — load. The deadline cancels every in-flight request at once.
	log.Printf("driving load for %s with %d workers...", cfg.duration, cfg.workers)
	loadCtx, cancel := context.WithTimeout(ctx, cfg.duration)
	defer cancel()
	agg, elapsed := drive(loadCtx, client, cfg)

	// Phase 3 — report.
	report(cfg, agg, elapsed)

	// Phase 4 — conservation check (the point of the whole exercise).
	finalSum, err := totalBalance(ctx, client, cfg)
	if err != nil {
		return fmt.Errorf("conservation check: %w", err)
	}
	fmt.Println("--- conservation ---")
	delta := finalSum - openingSum
	if delta != 0 {
		fmt.Printf("INVARIANT FAILURE: money not conserved (opening=%d final=%d delta=%+d)\n", openingSum, finalSum, delta)
		return fmt.Errorf("conservation invariant violated: delta=%+d", delta)
	}
	fmt.Printf("INVARIANT OK: money conserved (sum=%d)\n", finalSum)
	return nil
}

// runPrefix makes each run's accounts unique. Account creation is idempotent —
// re-creating an account whose balance has moved is refused — so a re-run
// against a persistent stack must seed fresh accounts rather than reset the
// previous run's. Conservation is then checked over exactly this run's money.
var runPrefix = fmt.Sprintf("acct-%d", time.Now().UnixMilli())

// accountID renders a run-scoped, zero-padded id like "acct-1752000000000-0007".
func accountID(i int) string { return fmt.Sprintf("%s-%04d", runPrefix, i) }

// seed creates every account with the configured opening balance.
func seed(ctx context.Context, client *http.Client, cfg config) error {
	type accountReq struct {
		ID       string `json:"id"`
		Currency string `json:"currency"`
		Opening  int64  `json:"opening"`
	}
	for i := 0; i < cfg.accounts; i++ {
		body := accountReq{ID: accountID(i), Currency: cfg.currency, Opening: cfg.opening}
		status, err := postJSON(ctx, client, cfg.base+"/v1/accounts", body)
		if err != nil {
			return fmt.Errorf("create %s: %w", body.ID, err)
		}
		if status != http.StatusCreated {
			return fmt.Errorf("create %s: unexpected status %d", body.ID, status)
		}
	}
	return nil
}

// transferReq is the POST /v1/transfers body.
type transferReq struct {
	IdempotencyKey string `json:"idempotency_key"`
	FromAccountID  string `json:"from_account_id"`
	ToAccountID    string `json:"to_account_id"`
	Amount         int64  `json:"amount"`
}

// drive runs the worker pool until ctx is cancelled and returns the merged
// stats plus the wall-clock time the load phase actually took.
func drive(ctx context.Context, client *http.Client, cfg config) (*stats, time.Duration) {
	// Bound total latency memory: split a fixed budget of samples across workers
	// and let each worker keep an unbiased reservoir of that size.
	const totalSampleBudget = 100_000
	perWorker := totalSampleBudget / cfg.workers
	if perWorker < 1 {
		perWorker = 1
	}

	start := time.Now()
	var wg sync.WaitGroup
	results := make([]*stats, cfg.workers)
	for w := 0; w < cfg.workers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			// Per-worker RNG and counter: no shared state, so no contention and
			// no atomics on the hot path. (workerID, n) is globally unique, which
			// makes every idempotency key unique — so no transfer is ever
			// rejected as a duplicate.
			rng := rand.New(rand.NewPCG(uint64(workerID)+1, uint64(time.Now().UnixNano())))
			st := newStats(perWorker, rng)
			n := 0
			for ctx.Err() == nil {
				from, to := distinctPair(rng, cfg.accounts)
				req := transferReq{
					IdempotencyKey: fmt.Sprintf("lt-%d-%d", workerID, n),
					FromAccountID:  accountID(from),
					ToAccountID:    accountID(to),
					Amount:         int64(rng.IntN(100) + 1), // 1..100
				}
				n++

				t0 := time.Now()
				status, err := postJSON(ctx, client, cfg.base+"/v1/transfers", req)
				latency := time.Since(t0)
				if err != nil {
					// Cancellation at the deadline is expected, not a failure.
					if ctx.Err() != nil {
						break
					}
					st.record(0, latency) // transport error: status 0
					continue
				}
				st.record(status, latency)
			}
			results[workerID] = st
		}(w)
	}
	wg.Wait()
	elapsed := time.Since(start)

	merged := newStats(0, nil)
	for _, st := range results {
		merged.merge(st)
	}
	return merged, elapsed
}

// distinctPair returns two distinct account indices in [0, n).
func distinctPair(rng *rand.Rand, n int) (int, int) {
	a := rng.IntN(n)
	b := rng.IntN(n - 1)
	if b >= a {
		b++
	}
	return a, b
}

// report prints throughput and latency percentiles to stdout.
func report(cfg config, s *stats, elapsed time.Duration) {
	fmt.Println("=== ledger load test ===")
	fmt.Printf("target:    %s\n", cfg.base)
	fmt.Printf("accounts:  %d\n", cfg.accounts)
	fmt.Printf("workers:   %d\n", cfg.workers)

	fmt.Println("--- throughput ---")
	fmt.Printf("total requests: %d\n", s.total)
	fmt.Printf("  201 created:  %d\n", s.byStatus[http.StatusCreated])
	for _, code := range sortedKeys(s.byStatus) {
		if code == http.StatusCreated {
			continue
		}
		// Non-201 includes legitimate business rejections (e.g. 400
		// insufficient_funds), which do not move money, so they never break the
		// conservation invariant.
		fmt.Printf("  other %3d:    %d\n", code, s.byStatus[code])
	}
	fmt.Printf("elapsed:        %s\n", elapsed.Round(time.Millisecond))
	if secs := elapsed.Seconds(); secs > 0 {
		fmt.Printf("throughput:     %.0f req/s\n", float64(s.total)/secs)
	}

	fmt.Println("--- latency ---")
	if len(s.lat) == 0 {
		fmt.Println("(no samples)")
		return
	}
	sort.Slice(s.lat, func(i, j int) bool { return s.lat[i] < s.lat[j] })
	fmt.Printf("sampled:        %d\n", len(s.lat))
	fmt.Printf("p50:            %s\n", percentile(s.lat, 0.50).Round(time.Microsecond))
	fmt.Printf("p90:            %s\n", percentile(s.lat, 0.90).Round(time.Microsecond))
	fmt.Printf("p99:            %s\n", percentile(s.lat, 0.99).Round(time.Microsecond))
	fmt.Printf("max:            %s\n", s.lat[len(s.lat)-1].Round(time.Microsecond))
}

// percentile returns the q-quantile (0..1) of an already-sorted slice.
func percentile(sorted []time.Duration, q float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(q * float64(len(sorted)-1))
	return sorted[idx]
}

// totalBalance reads every account back and sums the settled balances.
func totalBalance(ctx context.Context, client *http.Client, cfg config) (int64, error) {
	var sum int64
	for i := 0; i < cfg.accounts; i++ {
		id := accountID(i)
		var acc struct {
			Balance int64 `json:"balance"`
		}
		if err := getJSON(ctx, client, cfg.base+"/v1/accounts/"+id, &acc); err != nil {
			return 0, fmt.Errorf("get %s: %w", id, err)
		}
		sum += acc.Balance
	}
	return sum, nil
}

// --- HTTP helpers ---

// postJSON encodes body as JSON, POSTs it, drains and closes the response, and
// returns the status code.
func postJSON(ctx context.Context, client *http.Client, url string, body any) (int, error) {
	buf, err := json.Marshal(body)
	if err != nil {
		return 0, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(buf))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return 0, err
	}
	// Drain and close so the connection can be reused by the idle pool.
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	return resp.StatusCode, nil
}

// getJSON GETs url and decodes a 2xx body into v.
func getJSON(ctx context.Context, client *http.Client, url string, v any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status %d", resp.StatusCode)
	}
	return json.NewDecoder(resp.Body).Decode(v)
}

// --- stats ---

// stats holds per-worker (and, once merged, aggregate) counters plus an unbiased
// reservoir of latency samples.
type stats struct {
	total    int64
	byStatus map[int]int64
	lat      []time.Duration
	cap      int
	seen     int64 // total observations offered to the reservoir
	rng      *rand.Rand
}

func newStats(capacity int, rng *rand.Rand) *stats {
	return &stats{
		byStatus: make(map[int]int64),
		lat:      make([]time.Duration, 0, capacity),
		cap:      capacity,
		rng:      rng,
	}
}

// record counts one request and feeds its latency to the reservoir (Algorithm R:
// every observation has an equal chance of being retained, so percentiles aren't
// biased toward the warm-up burst).
func (s *stats) record(status int, d time.Duration) {
	s.total++
	s.byStatus[status]++
	if s.cap <= 0 {
		s.seen++
		return
	}
	if len(s.lat) < s.cap {
		s.lat = append(s.lat, d)
	} else if j := s.rng.IntN(int(s.seen) + 1); j < s.cap {
		s.lat[j] = d
	}
	s.seen++
}

// merge folds another stats into s. Latency reservoirs are simply concatenated;
// each is already a fair sample of its worker, so the union represents the run.
func (s *stats) merge(other *stats) {
	if other == nil {
		return
	}
	s.total += other.total
	for code, n := range other.byStatus {
		s.byStatus[code] += n
	}
	s.lat = append(s.lat, other.lat...)
}

// sortedKeys returns the map keys in ascending order for stable output.
func sortedKeys(m map[int]int64) []int {
	keys := make([]int, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Ints(keys)
	return keys
}
