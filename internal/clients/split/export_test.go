/*
Copyright 2021 Upbound Inc.
*/

package split

import (
	"time"

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
	wsByUID = map[types.UID]*writeState{}
	wsMu.Unlock()

	now = time.Now
}
