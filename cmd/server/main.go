// Command server runs the ledger as a real service: a PostgreSQL-backed Service
// exposed over REST and gRPC, with Prometheus metrics, and an in-process outbox
// relay shipping transfer.posted events to Kafka.
//
// Configuration is via environment variables (12-factor); every long-running
// component is shut down gracefully on SIGINT/SIGTERM.
package main

import (
	"context"
	"errors"
	"io/fs"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	"github.com/mkmbhs/ledger"
	"github.com/mkmbhs/ledger/internal/metrics"
	"github.com/mkmbhs/ledger/internal/outbox"
	"github.com/mkmbhs/ledger/internal/transport/grpcsvc"
	"github.com/mkmbhs/ledger/internal/transport/rest"
	"github.com/mkmbhs/ledger/migrations"
	"github.com/mkmbhs/ledger/postgres"
)

type config struct {
	databaseURL  string
	kafkaBrokers []string
	kafkaTopic   string
	httpAddr     string
	grpcAddr     string
	relayEvery   time.Duration

	// Retention (migrations/0005). pruneInterval gates the ticker: zero (the
	// default, and the demo stack's setting) disables pruning entirely.
	pruneInterval   time.Duration
	keyRetention    time.Duration
	outboxRetention time.Duration
}

func loadConfig() config {
	return config{
		databaseURL:  env("DATABASE_URL", "postgres://ledger:ledger@localhost:5432/ledger?sslmode=disable"),
		kafkaBrokers: strings.Split(env("KAFKA_BROKERS", "localhost:9092"), ","),
		kafkaTopic:   env("KAFKA_TOPIC", "ledger.transfers"),
		httpAddr:     env("HTTP_ADDR", ":8080"),
		grpcAddr:     env("GRPC_ADDR", ":9090"),
		relayEvery:   time.Second,

		pruneInterval:   envDuration("PRUNE_INTERVAL", 0),
		keyRetention:    envDuration("KEY_RETENTION", 30*24*time.Hour),
		outboxRetention: envDuration("OUTBOX_RETENTION", 7*24*time.Hour),
	}
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// envDuration reads a Go duration (e.g. "1h", "720h") from the environment. A
// malformed value is a configuration bug worth failing loudly over.
func envDuration(key string, def time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		log.Fatalf("invalid %s %q: %v", key, v, err)
	}
	return d
}

func main() {
	if err := run(); err != nil {
		log.Fatalf("server: %v", err)
	}
}

func run() error {
	// Cancel everything on SIGINT/SIGTERM.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg := loadConfig()

	store, pool, err := postgres.Connect(ctx, cfg.databaseURL)
	if err != nil {
		return err
	}
	defer pool.Close()

	if err := migrate(ctx, pool); err != nil {
		return err
	}

	svc := ledger.New(store)

	// Outbox relay as an in-process goroutine. This is the right model for a
	// single service; FOR UPDATE SKIP LOCKED keeps it safe if it is ever split
	// into a separate worker for horizontal scale.
	pub := outbox.NewKafkaPublisher(cfg.kafkaBrokers, cfg.kafkaTopic)
	defer pub.Close()
	relayDone := startRelay(ctx, outbox.NewRelay(pool, pub), pool, cfg.relayEvery)

	// Retention: an opt-in ticker applying ledger_prune (migrations/0005).
	// Disabled unless PRUNE_INTERVAL is set, so the demo stack keeps full
	// history. Retention must exceed the longest client retry horizon: a retry
	// arriving after its key was pruned becomes a new operation.
	var pruneDone <-chan struct{}
	if cfg.pruneInterval > 0 {
		// Fail at boot, not on every tick: retentions must be sane and the
		// schema must actually have ledger_prune (a database provisioned before
		// migration 0005 and never re-migrated would not).
		if cfg.keyRetention <= 0 || cfg.outboxRetention <= 0 {
			return errors.New("KEY_RETENTION and OUTBOX_RETENTION must be positive when PRUNE_INTERVAL is set")
		}
		var fn *string
		if err := pool.QueryRow(ctx, `SELECT to_regprocedure('ledger_prune(interval, interval)')::text`).Scan(&fn); err != nil {
			return err
		}
		if fn == nil {
			return errors.New("retention enabled but ledger_prune is missing: the schema predates migration 0005 — apply migrations/0005_retention.sql")
		}
		log.Printf("retention: pruning every %s (keys %s, published outbox %s)",
			cfg.pruneInterval, cfg.keyRetention, cfg.outboxRetention)
		pruneDone = startPruner(ctx, store, cfg)
	} else {
		closed := make(chan struct{})
		close(closed)
		pruneDone = closed
	}

	// HTTP: /metrics plus the REST API wrapped in the metrics middleware.
	mux := http.NewServeMux()
	mux.Handle("/metrics", metrics.Handler())
	mux.Handle("/", metrics.HTTPMiddleware(rest.NewHandler(svc)))
	httpSrv := &http.Server{Addr: cfg.httpAddr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}

	// gRPC with the metrics interceptor.
	grpcSrv := grpc.NewServer(grpc.UnaryInterceptor(metrics.UnaryServerInterceptor()))
	grpcsvc.Register(grpcSrv, svc)
	reflection.Register(grpcSrv) // lets grpcurl and other tools introspect the API
	lis, err := net.Listen("tcp", cfg.grpcAddr)
	if err != nil {
		return err
	}

	errc := make(chan error, 2)
	go func() {
		log.Printf("HTTP listening on %s", cfg.httpAddr)
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errc <- err
		}
	}()
	go func() {
		log.Printf("gRPC listening on %s", cfg.grpcAddr)
		if err := grpcSrv.Serve(lis); err != nil {
			errc <- err
		}
	}()

	// Wait for a shutdown signal or a fatal server error.
	select {
	case <-ctx.Done():
		log.Println("shutdown signal received")
	case err := <-errc:
		return err
	}

	// Graceful shutdown, bounded.
	shCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = httpSrv.Shutdown(shCtx)
	grpcSrv.GracefulStop()
	<-relayDone // the relay loop stops when ctx is cancelled
	<-pruneDone
	log.Println("stopped cleanly")
	return nil
}

// startPruner applies the retention policy on a ticker until ctx is cancelled,
// returning a channel closed once the loop has exited. Pruning is maintenance,
// not correctness: an error is logged and retried on the next tick.
func startPruner(ctx context.Context, store *postgres.Store, cfg config) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		t := time.NewTicker(cfg.pruneInterval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				st, err := store.Prune(context.Background(), cfg.keyRetention, cfg.outboxRetention)
				if err != nil {
					log.Printf("prune: %v", err)
				} else if st.TransferKeysNulled+st.HoldKeysNulled+st.OutboxRowsDeleted > 0 {
					log.Printf("prune: nulled %d transfer + %d hold key(s), deleted %d outbox row(s)",
						st.TransferKeysNulled, st.HoldKeysNulled, st.OutboxRowsDeleted)
				}
			}
		}
	}()
	return done
}

// startRelay drains the outbox to Kafka on a ticker until ctx is cancelled,
// recording throughput and backlog metrics on each tick. It returns a channel
// closed once the loop has exited.
func startRelay(ctx context.Context, relay *outbox.Relay, pool *pgxpool.Pool, every time.Duration) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		t := time.NewTicker(every)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				n, err := relay.Drain(context.Background(), 100)
				metrics.AddOutboxPublished(n) // counts rows delivered even when the batch later failed
				if err != nil {
					log.Printf("relay: %v", err)
				} else if n > 0 {
					log.Printf("relay: published %d event(s)", n)
				}
				// Backlog gauges: how many rows still await publication and how
				// old the oldest is. The partial index on unpublished rows keeps
				// this a cheap scan of just the tail.
				var unpublished int64
				var lagSeconds float64
				if err := pool.QueryRow(context.Background(), `
					SELECT count(*), coalesce(extract(epoch FROM now() - min(created_at)), 0)
					FROM outbox WHERE published_at IS NULL`).Scan(&unpublished, &lagSeconds); err != nil {
					log.Printf("outbox backlog probe: %v", err)
				} else {
					metrics.SetOutboxBacklog(unpublished, time.Duration(lagSeconds*float64(time.Second)))
				}
			}
		}
	}()
	return done
}

// migrate applies the embedded schema once. It is a no-op if the schema is
// already present (so restarts against a persistent volume are safe). A real
// deployment would use a versioned migration tool; this keeps the demo
// self-contained.
func migrate(ctx context.Context, pool *pgxpool.Pool) error {
	var present *string
	if err := pool.QueryRow(ctx, `SELECT to_regclass('public.accounts')`).Scan(&present); err != nil {
		return err
	}
	if present != nil {
		log.Println("schema already present, skipping migrations")
		return nil
	}
	files, err := fs.Glob(migrations.FS, "*.sql")
	if err != nil {
		return err
	}
	sort.Strings(files)
	for _, f := range files {
		sql, err := migrations.FS.ReadFile(f)
		if err != nil {
			return err
		}
		if _, err := pool.Exec(ctx, string(sql)); err != nil {
			return err
		}
		log.Printf("applied migration %s", f)
	}
	return nil
}
