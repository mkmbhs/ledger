// Package ledgertest is the store-agnostic conformance suite for
// implementations of ledger.Store: the same executable specification this
// module's own stores — the in-memory reference (ledger.MemStore) and the
// PostgreSQL store — are held to, packaged so any other implementation can be
// held to it too.
//
// Usage:
//
//	func TestMyStoreConformance(t *testing.T) {
//		ledgertest.Run(t, func(t *testing.T) ledger.Store {
//			return mystore.New(...) // a fresh, EMPTY store per call
//		})
//	}
//
// An implementation may claim it "passes mkmbhs/ledger conformance v1" when
// Run is green, including under the race detector (go test -race). The
// scenario catalog is add-only while Version stays the same — scenarios are
// never changed or removed, so the claim keeps a stable meaning. The catalog
// version governs the scenarios, not compilation: the module itself follows
// SemVer v0.x, so the Store interface may still evolve between minor releases.
//
// See the package README for the scenario catalog and versioning policy.
package ledgertest

import (
	"testing"

	"github.com/mkmbhs/ledger"
)

// Version identifies the conformance contract. Scenarios are only added —
// never changed or removed — while Version stays the same, so a claim of
// "passes conformance v1" has a stable meaning.
const Version = "v1"

// Factory returns a fresh, empty ledger.Store for one scenario. Run calls it
// exactly once per scenario, so calls must not share state: return a new
// in-memory instance, or a store pointing at a truncated schema. Register
// teardown with t.Cleanup; fail setup with t.Fatal.
type Factory func(t *testing.T) ledger.Store

// Scenario is one named conformance check. The full catalog comes from
// Scenarios; each entry is runnable à la carte with its own subtest and a
// Factory.
type Scenario struct {
	Name string
	Run  func(t *testing.T, f Factory)
}

// Run executes every conformance scenario as a subtest of t. This is the
// whole benchmark: a Store that passes exhibits the atomicity, idempotency,
// hold-lifecycle, and double-entry conservation semantics the ledger core
// relies on.
func Run(t *testing.T, f Factory) {
	for _, s := range Scenarios() {
		t.Run(s.Name, func(t *testing.T) { s.Run(t, f) })
	}
}
