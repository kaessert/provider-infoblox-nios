/*
Copyright 2021 Upbound Inc.
*/

package split

import (
	"k8s.io/apimachinery/pkg/types"
)

// resetForTest resets the package-level globals that accumulate cross-reconcile
// state so tests do not leak state into one another. It lives in a _test.go file
// so it is compiled only for tests and never ships in the provider binary.
func resetForTest() {
	mu.Lock()
	readSetup = nil
	readOTS = nil
	configured = false
	mu.Unlock()

	wsMu.Lock()
	postWrite = map[types.UID]struct{}{}
	wsMu.Unlock()
}

// seedPostWrite marks a UID as post-write, so tests can drive the convergence
// gate without going through a full Create/Update.
func seedPostWrite(uid types.UID) { markPostWrite(uid) }

// isPostWrite reports whether a UID is currently in the post-write set, so tests
// can assert the marker was set/cleared as expected.
func isPostWrite(uid types.UID) bool { return inPostWrite(uid) }
