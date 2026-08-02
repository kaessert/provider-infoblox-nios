// Package statemetrics wraps crossplane-runtime's managed-resource state
// recorder so a transient RESTMapper miss at controller startup cannot
// crash the whole provider process.
//
// Every resource controller registers a statemetrics.MRStateRecorder as a
// controller-runtime Runnable. Its ticker calls client.List on the
// resource's list type once per poll-state-metric interval. The provider
// uses SafeStart gating (see the CRD gate wired up in cmd/provider) so a
// resource's controller — and therefore its state recorder — is only
// registered once that resource's CRD reports Established=true. But
// "Established" on the CRD object and the API server's RESTMapper having
// caught up with that CRD are two different clocks: the RESTMapper is
// refreshed on its own cache interval, and can still return
// "no matches for kind" for a few seconds after the CRD condition flips
// true. The upstream recorder's Record method treats any List error —
// including that transient one — as fatal, and a controller-runtime
// Runnable that returns an error from Start aborts the whole manager.
// The result: the provider crashes moments after a CRD is installed and
// only recovers because Kubernetes restarts the pod.
//
// ResilientRecorder runs the same ticker loop but swallows (logs, does
// not fail on) the specific "no matches for kind" condition, letting the
// next tick retry once the RESTMapper has resolved the kind. Every other
// error is still returned, preserving the upstream behavior for real
// failures.
package statemetrics

import (
	"context"
	"time"

	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/crossplane/crossplane-runtime/v2/pkg/logging"
	"github.com/crossplane/crossplane-runtime/v2/pkg/resource"
	"github.com/crossplane/crossplane-runtime/v2/pkg/statemetrics"
)

// ResilientRecorder is a controller-runtime Runnable equivalent to
// statemetrics.MRStateRecorder that tolerates a transient
// "no matches for kind" error from the wrapped recorder's List call
// instead of treating it as fatal.
type ResilientRecorder struct {
	inner    *statemetrics.MRStateRecorder
	log      logging.Logger
	interval time.Duration
}

// NewResilientRecorder returns a Runnable that records managed-resource
// state metrics on the given interval, the same as
// statemetrics.NewMRStateRecorder, except that a "no matches for kind"
// error — the RESTMapper not yet having caught up with a just-installed
// CRD — is logged and retried on the next tick rather than returned as a
// fatal error.
func NewResilientRecorder(c client.Client, log logging.Logger, metrics *statemetrics.MRStateMetrics, managedList resource.ManagedList, interval time.Duration) *ResilientRecorder {
	return &ResilientRecorder{
		inner:    statemetrics.NewMRStateRecorder(c, log, metrics, managedList, interval),
		log:      log,
		interval: interval,
	}
}

// Start runs the state-recording ticker loop until ctx is cancelled. A
// "no matches for kind" error from a single Record call is logged and
// swallowed so the loop keeps ticking; any other error still aborts the
// loop (and, per controller-runtime Runnable semantics, the manager).
func (r *ResilientRecorder) Start(ctx context.Context) error {
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := r.inner.Record(ctx); err != nil {
				if apimeta.IsNoMatchError(err) {
					r.log.Debug("managed resource kind not yet established in the API server's RESTMapper, will retry", "error", err)
					continue
				}
				return err
			}
		case <-ctx.Done():
			return nil
		}
	}
}
