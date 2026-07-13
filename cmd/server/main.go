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
}

func loadConfig() config {
	return config{
		databaseURL:  env("DATABASE_URL", "postgres://ledger:ledger@localhost:5432/ledger?sslmode=disable"),
		kafkaBrokers: strings.Split(env("KAFKA_BROKERS", "localhost:9092"), ","),
		kafkaTopic:   env("KAFKA_TOPIC", "ledger.transfers"),
		httpAddr:     env("HTTP_ADDR", ":8080"),
		grpcAddr:     env("GRPC_ADDR", ":9090"),
		relayEvery:   time.Second,
	}
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
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
	relayDone := startRelay(ctx, outbox.NewRelay(pool, pub), cfg.relayEvery)

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
	log.Println("stopped cleanly")
	return nil
}

// startRelay drains the outbox to Kafka on a ticker until ctx is cancelled. It
// returns a channel closed once the loop has exited.
func startRelay(ctx context.Context, relay *outbox.Relay, every time.Duration) <-chan struct{} {
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
				if n, err := relay.Drain(context.Background(), 100); err != nil {
					log.Printf("relay: %v", err)
				} else if n > 0 {
					log.Printf("relay: published %d event(s)", n)
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
