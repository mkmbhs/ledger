// Package metrics provides Prometheus instrumentation for the ledger's HTTP and
// gRPC transports. It keeps a private registry so the ledger's series never
// collide with another process that shares the global default registry, and
// exposes that registry through Handler. The HTTP middleware and the gRPC
// interceptor are the two hooks cmd/server wires into each transport, so both
// surfaces report request counts and latency the same way.
package metrics

import (
	"context"
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
)

// registry is the ledger's own collector registry. Using a dedicated registry
// (rather than prometheus.DefaultRegisterer) keeps the exported metrics scoped
// to this package and avoids duplicate-registration panics when embedded.
var registry = prometheus.NewRegistry()

// factory registers every metric below against the private registry.
var factory = promauto.With(registry)

var (
	httpRequests = factory.NewCounterVec(
		prometheus.CounterOpts{
			Name: "ledger_http_requests_total",
			Help: "Total HTTP requests by method, route and status code.",
		},
		[]string{"method", "route", "status"},
	)
	httpLatency = factory.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "ledger_http_request_duration_seconds",
			Help:    "HTTP request latency in seconds by method and route.",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"method", "route"},
	)
	grpcRequests = factory.NewCounterVec(
		prometheus.CounterOpts{
			Name: "ledger_grpc_requests_total",
			Help: "Total gRPC requests by method and response code.",
		},
		[]string{"method", "code"},
	)
	grpcLatency = factory.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "ledger_grpc_request_duration_seconds",
			Help:    "gRPC request latency in seconds by method.",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"method"},
	)
)

// Handler returns the /metrics endpoint that exposes the private registry.
func Handler() http.Handler {
	return promhttp.HandlerFor(registry, promhttp.HandlerOpts{})
}

// HTTPMiddleware wraps next, counting every request and observing its latency,
// labelled by method, matched route and status code. It must be mounted outside
// the router so the router can populate the request's matched Pattern before the
// labels are read.
func HTTPMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r)
		route := routeOf(r)
		httpRequests.WithLabelValues(r.Method, route, strconv.Itoa(rec.status)).Inc()
		httpLatency.WithLabelValues(r.Method, route).Observe(time.Since(start).Seconds())
	})
}

// UnaryServerInterceptor returns a gRPC interceptor that counts every unary call
// and observes its latency, labelled by full method and gRPC status code.
func UnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		start := time.Now()
		resp, err := handler(ctx, req)
		grpcRequests.WithLabelValues(info.FullMethod, status.Code(err).String()).Inc()
		grpcLatency.WithLabelValues(info.FullMethod).Observe(time.Since(start).Seconds())
		return resp, err
	}
}

// routeOf returns the router's matched pattern as a low-cardinality route label.
// Unmatched requests (404s) collapse to a single bucket so raw paths never
// explode the metric's cardinality.
func routeOf(r *http.Request) string {
	if r.Pattern != "" {
		return r.Pattern
	}
	return "unmatched"
}

// statusRecorder captures the response status code for the metrics labels while
// transparently delegating writes to the wrapped ResponseWriter.
type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (s *statusRecorder) WriteHeader(code int) {
	s.status = code
	s.ResponseWriter.WriteHeader(code)
}
