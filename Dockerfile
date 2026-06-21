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
# Static binaries: CGO off, symbols/DWARF stripped. The image carries the server
# (default entrypoint) and the example Kafka consumer; the compose `consumer`
# service just overrides the entrypoint to /ledger-consumer.
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /ledger-server   ./cmd/server
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /ledger-consumer ./cmd/consumer

# --- run stage ---
FROM gcr.io/distroless/static:nonroot
COPY --from=build /ledger-server   /ledger-server
COPY --from=build /ledger-consumer /ledger-consumer
USER nonroot:nonroot
EXPOSE 8080 9090
ENTRYPOINT ["/ledger-server"]
