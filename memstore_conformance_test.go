package ledger_test

import (
	"testing"

	"github.com/mkmbhs/ledger"
	"github.com/mkmbhs/ledger/ledgertest"
)

// TestMemStore_Conformance runs the exported conformance suite (ledgertest)
// against the in-memory reference implementation: the specification must pass
// its own spec — untagged, with no external dependencies, on every machine
// that can run `go test ./...`, including under the race detector.
func TestMemStore_Conformance(t *testing.T) {
	ledgertest.Run(t, func(t *testing.T) ledger.Store { return ledger.NewMemStore() })
}
