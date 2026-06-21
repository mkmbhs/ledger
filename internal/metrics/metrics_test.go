package metrics

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestHandler_Serves(t *testing.T) {
	rec := httptest.NewRecorder()
	Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("/metrics status = %d, want 200", rec.Code)
	}
}

func TestHTTPMiddleware_CountsRequests(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /ping", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	srv := HTTPMiddleware(mux)

	counter := httpRequests.WithLabelValues(http.MethodGet, "GET /ping", "200")
	before := testutil.ToFloat64(counter)

	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/ping", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("downstream status = %d, want 200", rec.Code)
	}
	if got := testutil.ToFloat64(counter); got != before+1 {
		t.Fatalf("counter = %v, want %v", got, before+1)
	}
}

func TestUnaryServerInterceptor_CountsByCode(t *testing.T) {
	interceptor := UnaryServerInterceptor()
	info := &grpc.UnaryServerInfo{FullMethod: "/ledger.Ledger/Transfer"}
	failing := func(context.Context, any) (any, error) {
		return nil, status.Error(codes.NotFound, "missing")
	}

	counter := grpcRequests.WithLabelValues(info.FullMethod, codes.NotFound.String())
	before := testutil.ToFloat64(counter)

	_, err := interceptor(context.Background(), nil, info, failing)
	if err == nil {
		t.Fatal("expected an error from the failing handler")
	}
	if status.Code(err) != codes.NotFound {
		t.Fatalf("err code = %v, want NotFound", status.Code(err))
	}
	if got := testutil.ToFloat64(counter); got != before+1 {
		t.Fatalf("counter = %v, want %v", got, before+1)
	}
}
