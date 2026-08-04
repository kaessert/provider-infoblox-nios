/*
Copyright 2025 The Crossplane Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	authv1 "k8s.io/api/authorization/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

// errBoom is a sentinel error used to assert that hard errors returned by the
// Kubernetes API are propagated rather than swallowed as a plain denial.
var errBoom = errors.New("boom")

// sarResponse describes how a fake SelfSubjectAccessReview Create call
// should behave for a given verb.
type sarResponse struct {
	allowed bool
	err     error
}

// stubManager satisfies ctrl.Manager by embedding it (nil) and overriding
// only the two methods canWatchCRD and hasCRDWatchPermissions actually call:
// GetClient and GetScheme. Any other method is never invoked by the code
// under test, so leaving them to panic on the nil embedded interface is
// safe and keeps the stub to the two lines that matter.
type stubManager struct {
	ctrl.Manager
	client client.Client
	scheme *runtime.Scheme
}

func (m *stubManager) GetClient() client.Client   { return m.client }
func (m *stubManager) GetScheme() *runtime.Scheme { return m.scheme }

// newStubManager builds a stubManager backed by a fake controller-runtime
// client. respond is invoked once per SelfSubjectAccessReview Create call
// with the verb being checked, and controls whether that verb is allowed,
// denied, or fails with a hard error. It returns the manager along with a
// counter of how many Create calls were made, so tests can assert on
// short-circuit behaviour.
func newStubManager(t *testing.T, respond func(verb string) sarResponse) (*stubManager, *int32) {
	t.Helper()

	scheme := runtime.NewScheme()
	if err := authv1.AddToScheme(scheme); err != nil {
		t.Fatalf("add authv1 to scheme: %v", err)
	}

	var calls int32
	fc := fake.NewClientBuilder().
		WithScheme(scheme).
		WithInterceptorFuncs(interceptor.Funcs{
			Create: func(_ context.Context, _ client.WithWatch, obj client.Object, _ ...client.CreateOption) error {
				sar, ok := obj.(*authv1.SelfSubjectAccessReview)
				if !ok {
					t.Fatalf("unexpected object type passed to Create: %T", obj)
				}
				atomic.AddInt32(&calls, 1)
				res := respond(sar.Spec.ResourceAttributes.Verb)
				if res.err != nil {
					return res.err
				}
				sar.Status.Allowed = res.allowed
				return nil
			},
		}).
		Build()

	return &stubManager{client: fc, scheme: scheme}, &calls
}

func TestHasCRDWatchPermissions(t *testing.T) {
	cases := map[string]struct {
		reason      string
		respond     func(verb string) sarResponse
		wantAllowed bool
		wantErr     bool
		wantCalls   int32
	}{
		"AllVerbsAllowed": {
			reason: "every verb allowed should return true with no error, checking all three verbs",
			respond: func(string) sarResponse {
				return sarResponse{allowed: true}
			},
			wantAllowed: true,
			wantErr:     false,
			wantCalls:   3,
		},
		"SingleVerbDenied": {
			reason: "a denial on any verb should return false without checking the remaining verbs",
			respond: func(verb string) sarResponse {
				if verb == "list" {
					return sarResponse{allowed: false}
				}
				return sarResponse{allowed: true}
			},
			wantAllowed: false,
			wantErr:     false,
			// get (allowed) then list (denied) - watch is never reached.
			wantCalls: 2,
		},
		"CreateHardError": {
			reason: "an error from the SAR Create call must propagate, not be swallowed as a denial",
			respond: func(verb string) sarResponse {
				if verb == "get" {
					return sarResponse{err: errBoom}
				}
				return sarResponse{allowed: true}
			},
			wantAllowed: false,
			wantErr:     true,
			wantCalls:   1,
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			mgr, calls := newStubManager(t, tc.respond)

			allowed, err := hasCRDWatchPermissions(context.Background(), mgr)

			if tc.wantErr && err == nil {
				t.Fatalf("%s: expected an error, got none", tc.reason)
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("%s: unexpected error: %v", tc.reason, err)
			}
			if tc.wantErr && !errors.Is(err, errBoom) {
				t.Fatalf("%s: expected error to wrap errBoom, got: %v", tc.reason, err)
			}
			if allowed != tc.wantAllowed {
				t.Fatalf("%s: allowed = %v, want %v", tc.reason, allowed, tc.wantAllowed)
			}
			if got := atomic.LoadInt32(calls); got != tc.wantCalls {
				t.Fatalf("%s: SAR Create called %d times, want %d", tc.reason, got, tc.wantCalls)
			}
		})
	}
}

// TestCanWatchCRD covers the retry/backoff/timeout behaviour layered on top
// of hasCRDWatchPermissions. safeStartPrecheckInterval is a package
// constant (not a test seam), so the eventually-allowed case genuinely
// waits out one real interval - the test still stays well clear of the
// production 30s timeout, and the always-denied and hard-error cases use a
// short caller-supplied context so they never wait an interval at all.
func TestCanWatchCRD(t *testing.T) {
	t.Run("EventuallyAllowedWithinDeadline", func(t *testing.T) {
		// Denied on the first poll round, allowed from the second round
		// onward. round advances each time the "get" verb - always the
		// first verb checked in a round - is requested.
		var round int32
		respond := func(verb string) sarResponse {
			if verb == "get" {
				atomic.AddInt32(&round, 1)
			}
			if atomic.LoadInt32(&round) <= 1 {
				return sarResponse{allowed: false}
			}
			return sarResponse{allowed: true}
		}
		mgr, _ := newStubManager(t, respond)

		// One retry costs one safeStartPrecheckInterval (2s); leave enough
		// headroom for that single retry without approaching the
		// production 30s timeout.
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		start := time.Now()
		allowed, err := canWatchCRD(ctx, mgr)
		elapsed := time.Since(start)

		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !allowed {
			t.Fatalf("allowed = false, want true once the second poll round grants every verb")
		}
		if elapsed >= 10*time.Second {
			t.Fatalf("canWatchCRD took %v, expected to succeed well before the context deadline", elapsed)
		}
	})

	t.Run("AlwaysDeniedUntilDeadline", func(t *testing.T) {
		respond := func(string) sarResponse {
			return sarResponse{allowed: false}
		}
		mgr, _ := newStubManager(t, respond)

		// Shorter than safeStartPrecheckInterval so the poll never reaches
		// a second round - the immediate first check is denied and the
		// context deadline is then reached without a real interval wait.
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()

		start := time.Now()
		allowed, err := canWatchCRD(ctx, mgr)
		elapsed := time.Since(start)

		if err != nil {
			t.Fatalf("expected a denial (wait.Interrupted trapped internally), not an error: %v", err)
		}
		if allowed {
			t.Fatalf("allowed = true, want false when every poll round is denied until the deadline")
		}
		if elapsed >= time.Second {
			t.Fatalf("canWatchCRD took %v, expected to return shortly after the context deadline", elapsed)
		}
	})

	t.Run("HardErrorAbortsImmediately", func(t *testing.T) {
		respond := func(verb string) sarResponse {
			if verb == "get" {
				return sarResponse{err: errBoom}
			}
			return sarResponse{allowed: true}
		}
		mgr, calls := newStubManager(t, respond)

		// Deliberately long enough to prove a retry never happens - if the
		// error were swallowed and retried, this test would hang until the
		// deadline instead of returning immediately.
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		start := time.Now()
		allowed, err := canWatchCRD(ctx, mgr)
		elapsed := time.Since(start)

		if err == nil {
			t.Fatalf("expected the SAR Create error to abort canWatchCRD, got nil error")
		}
		if !errors.Is(err, errBoom) {
			t.Fatalf("expected error to wrap errBoom, got: %v", err)
		}
		if allowed {
			t.Fatalf("allowed = true, want false alongside a hard error")
		}
		if got := atomic.LoadInt32(calls); got != 1 {
			t.Fatalf("SAR Create called %d times, want exactly 1 - a hard error must not be retried", got)
		}
		if elapsed >= time.Second {
			t.Fatalf("canWatchCRD took %v to abort on a hard error, want an immediate return", elapsed)
		}
	})
}
