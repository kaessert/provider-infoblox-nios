package driftdetection

import (
	"context"
	"sync"
	"testing"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"

	"github.com/crossplane/crossplane-runtime/v2/pkg/reconciler/managed"

	dnsviewv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/dnsview/v1alpha1"
	arecordv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/recorda/v1alpha1"
	dd "github.com/crossplane-contrib/provider-infoblox-nios/apis/common/driftdetection"
)

// fakeRemote stands in for a real controller talking to the WAPI backend.
//
// Its update is a whole-object replace, the dominant shape in this fleet: a
// payload built from spec reverts whatever the external owner set. Modelling
// that here keeps the write-path assertions meaningful. Only comment is
// compared for up-to-dateness, mirroring how a real controller compares a
// small subset of fields; the rest are still carried through Update so the
// "owned field gets corrected, ignored field does not" assertions have
// something to check.
type fakeRemote struct {
	state    arecordv1alpha1.ARecordParameters
	observes int
	updates  int
}

func ps(s string) *string { return &s }

func strEq(a, b *string) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}

func (r *fakeRemote) client() managed.TypedExternalClient[*arecordv1alpha1.ARecord] {
	return managed.TypedExternalClientFns[*arecordv1alpha1.ARecord]{
		ObserveFn: func(_ context.Context, mg *arecordv1alpha1.ARecord) (managed.ExternalObservation, error) {
			r.observes++
			mg.Status.AtProvider = arecordv1alpha1.ARecordObservation{
				Name:     r.state.Name,
				IPv4Addr: r.state.IPv4Addr,
				View:     r.state.View,
				Comment:  r.state.Comment,
				TTL:      r.state.TTL,
				UseTTL:   r.state.UseTTL,
				ExtAttrs: r.state.ExtAttrs,
			}
			// Mirrors a real controller: only one field is compared.
			return managed.ExternalObservation{
				ResourceExists:   true,
				ResourceUpToDate: strEq(r.state.Comment, mg.Spec.ForProvider.Comment),
			}, nil
		},
		UpdateFn: func(_ context.Context, mg *arecordv1alpha1.ARecord) (managed.ExternalUpdate, error) {
			r.updates++
			r.state = *mg.Spec.ForProvider.DeepCopy()
			return managed.ExternalUpdate{}, nil
		},
		CreateFn: func(_ context.Context, mg *arecordv1alpha1.ARecord) (managed.ExternalCreation, error) {
			r.state = *mg.Spec.ForProvider.DeepCopy()
			return managed.ExternalCreation{}, nil
		},
		DeleteFn: func(_ context.Context, _ *arecordv1alpha1.ARecord) (managed.ExternalDelete, error) {
			return managed.ExternalDelete{}, nil
		},
		DisconnectFn: func(_ context.Context) error { return nil },
	}
}

func mr(cfg *dd.DriftDetection, params arecordv1alpha1.ARecordParameters) *arecordv1alpha1.ARecord {
	cr := &arecordv1alpha1.ARecord{}
	cr.Spec.DriftDetection = cfg
	cr.Spec.ForProvider = params
	return cr
}

func config(mode dd.Mode, paths ...string) *dd.DriftDetection {
	c := &dd.DriftDetection{Mode: mode}
	if len(paths) > 0 {
		c.Ignore = []dd.IgnoreRule{{Paths: paths}}
	}
	return c
}

func params(comment string) arecordv1alpha1.ARecordParameters {
	return arecordv1alpha1.ARecordParameters{
		Name:     ps("host.example.com"),
		IPv4Addr: ps("10.0.0.1"),
		View:     ps("default"),
		Comment:  ps(comment),
	}
}

func driftReason(cr *arecordv1alpha1.ARecord) (corev1.ConditionStatus, string, bool) {
	for _, c := range cr.Status.Conditions {
		if c.Type == TypeDriftDetected {
			return c.Status, string(c.Reason), true
		}
	}
	return "", "", false
}

const ignoreComment = "forProvider.comment"

// ─── behaviour ───────────────────────────────────────────────────────────────

// With no driftDetection block the wrapper is transparent. Adding the field to
// an API must not change how existing resources reconcile.
func TestNoConfigIsTransparent(t *testing.T) {
	r := &fakeRemote{state: params("set-by-someone-else")}
	cr := mr(nil, params("mine"))

	obs, err := WrapClient(r.client()).Observe(context.Background(), cr)
	if err != nil {
		t.Fatalf("Observe: %v", err)
	}
	if obs.ResourceUpToDate {
		t.Error("want drift reported when nothing is ignored")
	}
	if r.observes != 1 {
		t.Errorf("want 1 inner Observe, got %d", r.observes)
	}
}

func TestIgnoredPathSuppressesDrift(t *testing.T) {
	r := &fakeRemote{state: params("owned-externally")}
	cr := mr(config(dd.ModeEnabled, ignoreComment), params("seed"))

	obs, err := WrapClient(r.client()).Observe(context.Background(), cr)
	if err != nil {
		t.Fatalf("Observe: %v", err)
	}
	if !obs.ResourceUpToDate {
		t.Error("want up to date: the only difference is on an ignored path")
	}
	if *cr.Spec.ForProvider.Comment != "seed" {
		t.Errorf("user spec was mutated: got %q", *cr.Spec.ForProvider.Comment)
	}
	if got := *cr.Status.AtProvider.Comment; got != "owned-externally" {
		t.Errorf("atProvider must report the truth, got %q", got)
	}
	if st, reason, ok := driftReason(cr); !ok || st != corev1.ConditionTrue || reason != string(ReasonIgnored) {
		t.Errorf("want DriftDetected=True/DriftIgnored, got %v/%v (present=%v)", st, reason, ok)
	}
}

func TestIgnoredPathDoesNotMaskOtherDrift(t *testing.T) {
	// useTtl is ignored but never compared by the fake; drive real drift
	// through the field the fake does compare (comment), so the test proves
	// the ignore list does not accidentally suppress it.
	r := &fakeRemote{state: params("upstream")}
	cr := mr(config(dd.ModeEnabled, "forProvider.useTtl"), params("mine"))

	obs, err := WrapClient(r.client()).Observe(context.Background(), cr)
	if err != nil {
		t.Fatalf("Observe: %v", err)
	}
	if obs.ResourceUpToDate {
		t.Error("want drift reported: comment differs and is not ignored")
	}
	if _, reason, _ := driftReason(cr); reason != string(ReasonDrifted) {
		t.Errorf("want reason Drifted, got %v", reason)
	}
}

// The write path: correcting drift must not revert an ignored field.
func TestUpdateCarriesObservedValueForIgnoredPath(t *testing.T) {
	r := &fakeRemote{state: arecordv1alpha1.ARecordParameters{
		Name: ps("host.example.com"), View: ps("default"),
		IPv4Addr: ps("10.0.0.9"), Comment: ps("owned-externally"),
	}}
	cr := mr(config(dd.ModeEnabled, ignoreComment), params("seed")) // ipv4Addr 10.0.0.1: genuine drift

	e := WrapClient(r.client())
	ctx := context.Background()
	if _, err := e.Observe(ctx, cr); err != nil {
		t.Fatalf("Observe: %v", err)
	}
	if _, err := e.Update(ctx, cr); err != nil {
		t.Fatalf("Update: %v", err)
	}

	if *r.state.IPv4Addr != "10.0.0.1" {
		t.Errorf("owned field not reconciled: want 10.0.0.1, got %q", *r.state.IPv4Addr)
	}
	if *r.state.Comment != "owned-externally" {
		t.Errorf("ignored field reverted by the update: want owned-externally, got %q "+
			"(the failure a compare-only ignore produces under a whole-object write)", *r.state.Comment)
	}
	if *cr.Spec.ForProvider.Comment != "seed" {
		t.Errorf("user spec was mutated: got %q", *cr.Spec.ForProvider.Comment)
	}
}

func TestCreateSendsSeedValue(t *testing.T) {
	r := &fakeRemote{}
	cr := mr(config(dd.ModeEnabled, ignoreComment), params("seed"))

	if _, err := WrapClient(r.client()).Create(context.Background(), cr); err != nil {
		t.Fatalf("Create: %v", err)
	}
	if *r.state.Comment != "seed" {
		t.Errorf("want seed sent on create, got %q", *r.state.Comment)
	}
}

func TestWarnReportsWithoutCorrecting(t *testing.T) {
	r := &fakeRemote{state: params("upstream")}
	cr := mr(config(dd.ModeWarn), params("mine"))

	obs, err := WrapClient(r.client()).Observe(context.Background(), cr)
	if err != nil {
		t.Fatalf("Observe: %v", err)
	}
	if !obs.ResourceUpToDate {
		t.Error("warn must not trigger an update")
	}
	if st, reason, ok := driftReason(cr); !ok || st != corev1.ConditionTrue || reason != string(ReasonDrifted) {
		t.Errorf("want DriftDetected=True/Drifted, got %v/%v (present=%v)", st, reason, ok)
	}
	if r.observes != 1 {
		t.Errorf("warn must not cost a second read, got %d", r.observes)
	}
}

func TestDisabledSuppressesEverything(t *testing.T) {
	r := &fakeRemote{state: params("upstream")}
	cr := mr(config(dd.ModeDisabled), params("mine"))

	obs, err := WrapClient(r.client()).Observe(context.Background(), cr)
	if err != nil {
		t.Fatalf("Observe: %v", err)
	}
	if !obs.ResourceUpToDate {
		t.Error("disabled must never report drift")
	}
	if _, _, ok := driftReason(cr); ok {
		t.Error("disabled must not set a drift condition")
	}
}

func TestNoSecondReadWhenInSync(t *testing.T) {
	r := &fakeRemote{state: params("same")}
	cr := mr(config(dd.ModeEnabled, ignoreComment), params("same"))

	if _, err := WrapClient(r.client()).Observe(context.Background(), cr); err != nil {
		t.Fatalf("Observe: %v", err)
	}
	if r.observes != 1 {
		t.Errorf("want 1 inner Observe when in sync, got %d", r.observes)
	}
}

// ─── freshness and race safety ───────────────────────────────────────────────

// Update must consume the observation captured during Observe, never re-read
// status.atProvider. Re-reading would pick up whatever the object carries at
// that moment, which for any path that does not repopulate it is a previous
// reconcile's value.
func TestUpdateUsesSnapshotNotStatusReread(t *testing.T) {
	r := &fakeRemote{state: arecordv1alpha1.ARecordParameters{
		Name: ps("host.example.com"), View: ps("default"),
		IPv4Addr: ps("10.0.0.9"), Comment: ps("observed-now"),
	}}
	cr := mr(config(dd.ModeEnabled, ignoreComment), params("seed"))

	e := WrapClient(r.client())
	ctx := context.Background()
	if _, err := e.Observe(ctx, cr); err != nil {
		t.Fatalf("Observe: %v", err)
	}

	// Simulate anything that rewrites status between Observe and Update:
	// a stale cache decode, a status patch, a second controller.
	cr.Status.AtProvider.Comment = ps("STALE-OR-TAMPERED")

	if _, err := e.Update(ctx, cr); err != nil {
		t.Fatalf("Update: %v", err)
	}
	if *r.state.Comment != "observed-now" {
		t.Errorf("Update read status.atProvider instead of the captured snapshot: got %q", *r.state.Comment)
	}
}

// Update without a preceding Observe must fail closed. Falling back to the spec
// value would revert the external owner outright.
func TestUpdateFailsClosedWithoutObserve(t *testing.T) {
	r := &fakeRemote{state: params("owned-externally")}
	cr := mr(config(dd.ModeEnabled, ignoreComment), params("seed"))

	if _, err := WrapClient(r.client()).Update(context.Background(), cr); err == nil {
		t.Error("want Update to fail closed when no observation was captured")
	}
	if r.updates != 0 {
		t.Errorf("want no write issued, got %d", r.updates)
	}
	if *r.state.Comment != "owned-externally" {
		t.Errorf("external owner's value was overwritten: got %q", *r.state.Comment)
	}
}

// Disconnect ends the per-reconcile lifetime; a client reused afterwards must
// not apply a stale snapshot.
func TestDisconnectClearsSnapshot(t *testing.T) {
	r := &fakeRemote{state: params("observed")}
	cr := mr(config(dd.ModeEnabled, ignoreComment), params("seed"))

	e := WrapClient(r.client())
	ctx := context.Background()
	if _, err := e.Observe(ctx, cr); err != nil {
		t.Fatalf("Observe: %v", err)
	}
	if err := e.Disconnect(ctx); err != nil {
		t.Fatalf("Disconnect: %v", err)
	}
	if _, err := e.Update(ctx, cr); err == nil {
		t.Error("want Update to fail closed after Disconnect dropped the snapshot")
	}
}

// The reconciler's managed resource must never be mutated by substitution: it
// may be shared with a cache, and a mutated spec would be persisted by the
// ResourceLateInitialized path.
func TestInputResourceIsNeverMutated(t *testing.T) {
	r := &fakeRemote{state: arecordv1alpha1.ARecordParameters{
		Name: ps("host.example.com"), View: ps("default"),
		IPv4Addr: ps("10.0.0.9"), Comment: ps("upstream"),
	}}
	cr := mr(config(dd.ModeEnabled, ignoreComment), params("seed"))
	before := cr.Spec.DeepCopy()

	e := WrapClient(r.client())
	ctx := context.Background()
	if _, err := e.Observe(ctx, cr); err != nil {
		t.Fatalf("Observe: %v", err)
	}
	if _, err := e.Update(ctx, cr); err != nil {
		t.Fatalf("Update: %v", err)
	}
	if diff := cmp.Diff(before, cr.Spec.DeepCopy()); diff != "" {
		t.Errorf("spec was mutated (-want +got):\n%s", diff)
	}
}

// The production shape: one client per reconcile, many reconciles in flight for
// different resources. Run under -race.
func TestConcurrentReconcilesAreIndependent(t *testing.T) {
	const n = 32
	var wg sync.WaitGroup
	errs := make(chan error, n)

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			comment := "owner-" + string(rune('a'+i%26))
			r := &fakeRemote{state: arecordv1alpha1.ARecordParameters{
				Name: ps("host.example.com"), View: ps("default"),
				IPv4Addr: ps("10.0.0.9"), Comment: ps(comment),
			}}
			cr := mr(config(dd.ModeEnabled, ignoreComment), params("seed"))

			e := WrapClient(r.client()) // one wrapper per reconcile, as WrapConnector does
			ctx := context.Background()
			if _, err := e.Observe(ctx, cr); err != nil {
				errs <- err
				return
			}
			if _, err := e.Update(ctx, cr); err != nil {
				errs <- err
				return
			}
			if *r.state.Comment != comment {
				t.Errorf("snapshot crossed between reconciles: want %q, got %q", comment, *r.state.Comment)
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Errorf("concurrent reconcile: %v", err)
	}
}

// A mis-wiring that shares one client across reconciles must still be free of
// data races, even though the snapshot semantics would then be wrong. Run under
// -race.
func TestSharedClientIsRaceFree(t *testing.T) {
	r := &fakeRemote{state: params("upstream")}
	var mu sync.Mutex
	inner := managed.TypedExternalClientFns[*arecordv1alpha1.ARecord]{
		ObserveFn: func(ctx context.Context, mg *arecordv1alpha1.ARecord) (managed.ExternalObservation, error) {
			mu.Lock()
			defer mu.Unlock()
			return r.client().Observe(ctx, mg)
		},
		UpdateFn: func(ctx context.Context, mg *arecordv1alpha1.ARecord) (managed.ExternalUpdate, error) {
			mu.Lock()
			defer mu.Unlock()
			return r.client().Update(ctx, mg)
		},
		DisconnectFn: func(_ context.Context) error { return nil },
	}
	e := WrapClient[*arecordv1alpha1.ARecord](inner)

	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cr := mr(config(dd.ModeEnabled, ignoreComment), params("seed"))
			ctx := context.Background()
			_, _ = e.Observe(ctx, cr)
			_, _ = e.Update(ctx, cr)
		}()
	}
	wg.Wait()
}

// ─── configuration ─────────────────────────────────────────────────────────

// A driftDetection block with a mode set but no ignore paths is the common
// shape: mode alone, no ignore list. Ignore is a slice field with no
// omitempty tag, so an unset list marshals as "ignore":null rather than
// being absent from the object entirely, and that must not be treated as an
// error.
func TestReadConfigDriftDetectionWithoutIgnorePaths(t *testing.T) {
	for _, mode := range []dd.Mode{dd.ModeEnabled, dd.ModeWarn, dd.ModeDisabled} {
		cr := mr(config(mode), params("seed"))
		cfg, err := ReadConfig(cr)
		if err != nil {
			t.Errorf("ReadConfig(mode=%s): %v", mode, err)
			continue
		}
		if cfg.Mode != Mode(mode) || len(cfg.IgnorePaths) != 0 {
			t.Errorf("ReadConfig(mode=%s) = %+v, want Mode=%s with no ignore paths", mode, cfg, mode)
		}
	}
}

func TestUnobservedIgnoredPathKeepsSeedAndIsReported(t *testing.T) {
	r := &fakeRemote{state: params("seed")}
	// useTtl exists on ARecordObservation but the fake never populates it
	// (state.UseTTL is left nil). No other field drifts, so the unobserved
	// path is the only thing to report.
	cr := mr(config(dd.ModeEnabled, "forProvider.useTtl"), params("seed"))

	e := WrapClient(r.client())
	ctx := context.Background()
	if _, err := e.Observe(ctx, cr); err != nil {
		t.Fatalf("Observe: %v", err)
	}
	if _, reason, ok := driftReason(cr); !ok || reason != string(ReasonUnobserved) {
		t.Errorf("want an IgnoredPathUnobserved condition, got %v (present=%v)", reason, ok)
	}
	if _, err := e.Update(ctx, cr); err != nil {
		t.Fatalf("Update: %v", err)
	}
	if *r.state.Comment != "seed" {
		t.Errorf("want the seed sent when the server reports nothing, got %q", *r.state.Comment)
	}
}

func TestPathGrammar(t *testing.T) {
	cases := []struct {
		in      string
		want    string
		wantErr bool
	}{
		{in: "forProvider.comment", want: "forProvider.comment"},
		{in: "/forProvider/comment", want: "forProvider.comment"},
		{in: "forProvider.a.b.c", want: "forProvider.a.b.c"},
		{in: "forProvider.rules[0].ttl", wantErr: true},
		{in: "forProvider.rules[*].ttl", wantErr: true},
		{in: "status.atProvider.comment", wantErr: true},
		{in: "", wantErr: true},
	}
	for _, tc := range cases {
		got, err := normalizePath(tc.in)
		switch {
		case tc.wantErr && err == nil:
			t.Errorf("normalizePath(%q): want error, got %q", tc.in, got)
		case !tc.wantErr && err != nil:
			t.Errorf("normalizePath(%q): %v", tc.in, err)
		case !tc.wantErr && got != tc.want:
			t.Errorf("normalizePath(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestMalformedPathFailsClosed(t *testing.T) {
	r := &fakeRemote{state: params("upstream")}
	cr := mr(config(dd.ModeEnabled, "forProvider.rules[*].ttl"), params("seed"))

	e := WrapClient(r.client())
	ctx := context.Background()
	if _, err := e.Observe(ctx, cr); err == nil {
		t.Error("want Observe to fail on an unusable ignore path")
	}
	if _, err := e.Update(ctx, cr); err == nil {
		t.Error("want Update to fail closed on an unusable ignore path")
	}
	if r.updates != 0 {
		t.Errorf("want no write issued, got %d", r.updates)
	}
}

// The mechanism reads configuration off any managed resource without knowing
// its type; a type whose schema has no driftDetection block is unaffected.
func TestReadConfigIsTypeAgnostic(t *testing.T) {
	cfg, err := ReadConfig(&arecordv1alpha1.ARecord{})
	if err != nil {
		t.Fatalf("ReadConfig: %v", err)
	}
	if cfg.Mode != ModeEnabled || len(cfg.IgnorePaths) != 0 || cfg.Active() {
		t.Errorf("want inert default config, got %+v", cfg)
	}
}

// ─── eligibility ─────────────────────────────────────────────────────────────

// The identity-bearing fields the allowlist exists to refuse must be rejected
// outright, even though they are well-formed paths that exist on the
// resource.
func TestEligibilityRejectsIdentityFields(t *testing.T) {
	for _, path := range []string{"forProvider.name", "forProvider.view"} {
		cr := mr(config(dd.ModeEnabled, path), params("seed"))
		if _, err := ReadConfig(cr); err == nil {
			t.Errorf("ReadConfig(%s): want rejection, got nil error", path)
		}
	}
}

// Every field on the allowlist that is actually present on the resource must
// be accepted.
func TestEligibilityAcceptsListedFieldsPresentOnResource(t *testing.T) {
	for _, path := range []string{"forProvider.comment", "forProvider.extAttrs", "forProvider.ttl", "forProvider.useTtl"} {
		cr := mr(config(dd.ModeEnabled, path), params("seed"))
		cfg, err := ReadConfig(cr)
		if err != nil {
			t.Errorf("ReadConfig(%s): %v", path, err)
			continue
		}
		if len(cfg.IgnorePaths) != 1 || cfg.IgnorePaths[0] != path {
			t.Errorf("ReadConfig(%s): want ignore paths [%s], got %v", path, path, cfg.IgnorePaths)
		}
	}
}

// disable is on the allowlist and present on DNSView (unlike ARecord), so it
// must be accepted there.
func TestEligibilityAcceptsDisableOnResourceThatHasIt(t *testing.T) {
	cr := &dnsviewv1alpha1.DNSView{}
	cr.Spec.DriftDetection = config(dd.ModeEnabled, "forProvider.disable")

	cfg, err := ReadConfig(cr)
	if err != nil {
		t.Fatalf("ReadConfig: %v", err)
	}
	if len(cfg.IgnorePaths) != 1 || cfg.IgnorePaths[0] != "forProvider.disable" {
		t.Errorf("want ignore paths [forProvider.disable], got %v", cfg.IgnorePaths)
	}
}

// An allowlisted field name that does not exist on this particular resource's
// configurable fields must be rejected, not silently accepted as a no-op.
// ttl is on the allowlist, but DNSView has no ttl field.
func TestEligibilityRejectsListedFieldAbsentFromResource(t *testing.T) {
	cr := &dnsviewv1alpha1.DNSView{}
	cr.Spec.DriftDetection = config(dd.ModeEnabled, "forProvider.ttl")

	if _, err := ReadConfig(cr); err == nil {
		t.Error("want rejection: ttl is allowlisted but does not exist on DNSView's configurable fields")
	}
}

// One ineligible path in a list must fail the whole configuration — there is
// no partial application. This is also the guard against the allowlist check
// degrading from "reject" to "skip and continue": a skip implementation would
// accept the eligible path and silently drop the ineligible one instead of
// erroring here.
func TestEligibilityRejectionIsTotal(t *testing.T) {
	cr := mr(config(dd.ModeEnabled, "forProvider.comment", "forProvider.name"), params("seed"))

	cfg, err := ReadConfig(cr)
	if err == nil {
		t.Fatalf("want rejection of the whole config, got cfg=%+v", cfg)
	}
}
