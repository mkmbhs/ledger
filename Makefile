.PHONY: test race vet run cover

# Run the unit tests.
test:
	go test ./...

# Run the tests with the race detector (proves the concurrency claims).
race:
	go test -race ./...

vet:
	go vet ./...

# Run the demo.
run:
	go run ./cmd/ledger

# Test coverage summary.
cover:
	go test -cover ./...
