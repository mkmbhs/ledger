# Multi-stage build → a tiny, non-root, distroless image.
# Distroless (not scratch) because the service makes outbound TLS-capable
# connections and distroless ships CA certificates + a nonroot user (UID 65532)
# while still having no shell or package manager.

# --- build stage ---
FROM golang:1.26 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
# Static binary: CGO off, symbols/DWARF stripped.
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /ledger-server ./cmd/server

# --- run stage ---
FROM gcr.io/distroless/static:nonroot
COPY --from=build /ledger-server /ledger-server
USER nonroot:nonroot
EXPOSE 8080 9090
ENTRYPOINT ["/ledger-server"]
