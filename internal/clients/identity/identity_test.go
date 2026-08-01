package identity

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	ibclient "github.com/infobloxopen/infoblox-go-client/v2"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

// testUID and testComment are shared fixtures across the ladder-row
// tests below.
const (
	testUID     = "uid-1"
	testComment = "hello"
)

// ── fake connector ──────────────────────────────────────────────────────
//
// Per the mocks-only testing convention this factory uses, every test
// builds its own lightweight fake rather than reaching for a mocking
// framework. fakeConnector implements ibclient.IBConnector against
// *ibclient.RecordA — the same concrete type used throughout this test
// file — because Resolve's search call unmarshals into a concrete []T,
// not an interface slice, so a fake has to commit to one T per test file
// just as a real controller commits to one T per resource.

type fakeConnector struct {
	byRef      map[string]*ibclient.RecordA
	byRefErr   map[string]error
	search     []*ibclient.RecordA
	searchErr  error
	refCalls   []string
	searchCall int
}

func (f *fakeConnector) CreateObject(_ ibclient.IBObject) (string, error) { return "", nil }
func (f *fakeConnector) DeleteObject(_ string) (string, error)            { return "", nil }
func (f *fakeConnector) UpdateObject(_ ibclient.IBObject, _ string) (string, error) {
	return "", nil
}

func (f *fakeConnector) GetObject(_ ibclient.IBObject, ref string, _ *ibclient.QueryParams, res interface{}) error {
	if ref != "" {
		f.refCalls = append(f.refCalls, ref)
		if err, ok := f.byRefErr[ref]; ok {
			return err
		}
		rec, ok := f.byRef[ref]
		if !ok {
			return ibclient.NewNotFoundError("not found")
		}
		ptr, ok := res.(**ibclient.RecordA)
		if !ok {
			return fmt.Errorf("fakeConnector: unexpected res type %T for get-by-ref", res)
		}
		*ptr = rec
		return nil
	}

	f.searchCall++
	if f.searchErr != nil {
		return f.searchErr
	}
	ptr, ok := res.(*[]*ibclient.RecordA)
	if !ok {
		return fmt.Errorf("fakeConnector: unexpected res type %T for search", res)
	}
	*ptr = f.search
	if len(f.search) == 0 {
		return ibclient.NewNotFoundError("not found")
	}
	return nil
}

func newEmptyRecordA() *ibclient.RecordA {
	return ibclient.NewEmptyRecordA()
}

func recordAWithEA(ref string, ea ibclient.EA) *ibclient.RecordA {
	rec := ibclient.NewEmptyRecordA()
	rec.Ref = ref
	rec.Ea = ea
	return rec
}

// ── ladder rows ──────────────────────────────────────────────────────────

func TestResolveRefResolvesEAMatches(t *testing.T) {
	const uid = testUID
	const ref = "record:a/abc:host.example.com/default"
	conn := &fakeConnector{byRef: map[string]*ibclient.RecordA{
		ref: recordAWithEA(ref, ibclient.EA{EAKey: uid}),
	}}

	obj, outcome, err := Resolve[*ibclient.RecordA](context.Background(), conn, newEmptyRecordA, ref, uid)
	if err != nil {
		t.Fatalf("Resolve returned error: %v", err)
	}
	if outcome != OutcomeResolved {
		t.Fatalf("outcome = %s, want Resolved", outcome)
	}
	if obj == nil || obj.Ref != ref {
		t.Fatalf("obj = %+v, want Ref %q", obj, ref)
	}
	if conn.searchCall != 0 {
		t.Fatalf("search called %d times, want 0 — a matching ref must never fall through to a search", conn.searchCall)
	}
}

func TestResolveRefResolvesEAAbsentAdopts(t *testing.T) {
	const uid = testUID
	const ref = "record:a/abc:host.example.com/default"
	conn := &fakeConnector{byRef: map[string]*ibclient.RecordA{
		ref: recordAWithEA(ref, ibclient.EA{}),
	}}

	baseline := testutil.ToFloat64(adoptRestamps.WithLabelValues("record:a"))

	obj, outcome, err := Resolve[*ibclient.RecordA](context.Background(), conn, newEmptyRecordA, ref, uid)
	if err != nil {
		t.Fatalf("Resolve returned error: %v", err)
	}
	if outcome != OutcomeAdopted {
		t.Fatalf("outcome = %s, want Adopted", outcome)
	}
	if obj == nil || obj.Ref != ref {
		t.Fatalf("obj = %+v, want Ref %q", obj, ref)
	}
	if got := testutil.ToFloat64(adoptRestamps.WithLabelValues("record:a")) - baseline; got != 1 {
		t.Fatalf("adoptRestamps delta = %v, want 1", got)
	}
}

func TestResolveRefResolvesUIDMismatchRefusesHandleReuse(t *testing.T) {
	const ref = "record:a/abc:host.example.com/default"
	conn := &fakeConnector{byRef: map[string]*ibclient.RecordA{
		ref: recordAWithEA(ref, ibclient.EA{EAKey: "someone-elses-uid"}),
	}}

	obj, outcome, err := Resolve[*ibclient.RecordA](context.Background(), conn, newEmptyRecordA, ref, testUID)
	if err == nil {
		t.Fatal("Resolve returned nil error, want a HandleReuseError")
	}
	var reuse *HandleReuseError
	if !errors.As(err, &reuse) {
		t.Fatalf("err = %v (%T), want *HandleReuseError", err, err)
	}
	if reuse.WantUID != testUID || reuse.GotUID != "someone-elses-uid" {
		t.Fatalf("reuse = %+v, want WantUID=uid-1 GotUID=someone-elses-uid", reuse)
	}
	if outcome != OutcomeNotFound {
		t.Fatalf("outcome = %s, want NotFound (refusal, no object returned)", outcome)
	}
	if obj != nil {
		t.Fatalf("obj = %+v, want nil — a refusal must not hand back the foreign object", obj)
	}
	if conn.searchCall != 0 {
		t.Fatalf("search called %d times, want 0 — a UID mismatch must refuse, not fall through to a search", conn.searchCall)
	}
}

func TestResolveRef404RecoversSingleMatchRotated(t *testing.T) {
	const uid = testUID
	const staleRef = "record:a/stale:host.example.com/default"
	const newRef = "record:a/fresh:host.example.com/default"
	conn := &fakeConnector{
		byRef:  map[string]*ibclient.RecordA{}, // staleRef 404s
		search: []*ibclient.RecordA{recordAWithEA(newRef, ibclient.EA{EAKey: uid})},
	}

	baseline := testutil.ToFloat64(refMissRecoveries.WithLabelValues("record:a"))

	obj, outcome, err := Resolve[*ibclient.RecordA](context.Background(), conn, newEmptyRecordA, staleRef, uid)
	if err != nil {
		t.Fatalf("Resolve returned error: %v", err)
	}
	if outcome != OutcomeRotated {
		t.Fatalf("outcome = %s, want Rotated", outcome)
	}
	if obj == nil || obj.Ref != newRef {
		t.Fatalf("obj = %+v, want Ref %q", obj, newRef)
	}
	if got := testutil.ToFloat64(refMissRecoveries.WithLabelValues("record:a")) - baseline; got != 1 {
		t.Fatalf("refMissRecoveries delta = %v, want 1", got)
	}
}

func TestResolveRef404MultipleMatchesRefusesAmbiguous(t *testing.T) {
	const uid = testUID
	const staleRef = "record:a/stale:host.example.com/default"
	conn := &fakeConnector{
		byRef: map[string]*ibclient.RecordA{},
		search: []*ibclient.RecordA{
			recordAWithEA("record:a/one:host.example.com/default", ibclient.EA{EAKey: uid}),
			recordAWithEA("record:a/two:host.example.com/default", ibclient.EA{EAKey: uid}),
		},
	}

	obj, outcome, err := Resolve[*ibclient.RecordA](context.Background(), conn, newEmptyRecordA, staleRef, uid)
	if err == nil {
		t.Fatal("Resolve returned nil error, want an AmbiguousMatchError")
	}
	var ambiguous *AmbiguousMatchError
	if !errors.As(err, &ambiguous) {
		t.Fatalf("err = %v (%T), want *AmbiguousMatchError", err, err)
	}
	if ambiguous.Count != 2 {
		t.Fatalf("ambiguous.Count = %d, want 2", ambiguous.Count)
	}
	if outcome != OutcomeNotFound {
		t.Fatalf("outcome = %s, want NotFound (refusal, no object returned)", outcome)
	}
	if obj != nil {
		t.Fatalf("obj = %+v, want nil — an ambiguous match must never take [0]", obj)
	}
}

func TestResolveRef404NoMatchesNotFound(t *testing.T) {
	const staleRef = "record:a/stale:host.example.com/default"
	conn := &fakeConnector{byRef: map[string]*ibclient.RecordA{}, search: nil}

	obj, outcome, err := Resolve[*ibclient.RecordA](context.Background(), conn, newEmptyRecordA, staleRef, testUID)
	if err != nil {
		t.Fatalf("Resolve returned error: %v, want nil (genuinely absent is not an error)", err)
	}
	if outcome != OutcomeNotFound {
		t.Fatalf("outcome = %s, want NotFound", outcome)
	}
	if obj != nil {
		t.Fatalf("obj = %+v, want nil", obj)
	}
}

func TestResolveEmptyRefSingleMatchFoundByUID(t *testing.T) {
	const uid = testUID
	const foundRef = "record:a/fresh:host.example.com/default"
	conn := &fakeConnector{search: []*ibclient.RecordA{recordAWithEA(foundRef, ibclient.EA{EAKey: uid})}}

	baseline := testutil.ToFloat64(foundByUID.WithLabelValues("record:a"))

	obj, outcome, err := Resolve[*ibclient.RecordA](context.Background(), conn, newEmptyRecordA, "", uid)
	if err != nil {
		t.Fatalf("Resolve returned error: %v", err)
	}
	if outcome != OutcomeFoundByUID {
		t.Fatalf("outcome = %s, want FoundByUID", outcome)
	}
	if obj == nil || obj.Ref != foundRef {
		t.Fatalf("obj = %+v, want Ref %q", obj, foundRef)
	}
	if len(conn.refCalls) != 0 {
		t.Fatalf("refCalls = %v, want none — an empty ref must never issue a get-by-ref call", conn.refCalls)
	}
	if got := testutil.ToFloat64(foundByUID.WithLabelValues("record:a")) - baseline; got != 1 {
		t.Fatalf("foundByUID delta = %v, want 1", got)
	}
}

func TestResolveEmptyRefMultipleMatchesRefusesAmbiguous(t *testing.T) {
	const uid = testUID
	conn := &fakeConnector{search: []*ibclient.RecordA{
		recordAWithEA("record:a/one:host.example.com/default", ibclient.EA{EAKey: uid}),
		recordAWithEA("record:a/two:host.example.com/default", ibclient.EA{EAKey: uid}),
		recordAWithEA("record:a/three:host.example.com/default", ibclient.EA{EAKey: uid}),
	}}

	_, outcome, err := Resolve[*ibclient.RecordA](context.Background(), conn, newEmptyRecordA, "", uid)
	var ambiguous *AmbiguousMatchError
	if !errors.As(err, &ambiguous) {
		t.Fatalf("err = %v (%T), want *AmbiguousMatchError", err, err)
	}
	if ambiguous.Count != 3 {
		t.Fatalf("ambiguous.Count = %d, want 3", ambiguous.Count)
	}
	if outcome != OutcomeNotFound {
		t.Fatalf("outcome = %s, want NotFound", outcome)
	}
}

func TestResolveEmptyRefNoMatchesNotFound(t *testing.T) {
	conn := &fakeConnector{search: nil}

	obj, outcome, err := Resolve[*ibclient.RecordA](context.Background(), conn, newEmptyRecordA, "", testUID)
	if err != nil {
		t.Fatalf("Resolve returned error: %v, want nil", err)
	}
	if outcome != OutcomeNotFound {
		t.Fatalf("outcome = %s, want NotFound", outcome)
	}
	if obj != nil {
		t.Fatalf("obj = %+v, want nil", obj)
	}
}

// ── hard-error guards ─────────────────────────────────────────────────

func TestResolveEmptyUIDIsHardError(t *testing.T) {
	conn := &fakeConnector{}

	for _, uid := range []string{"", "   "} {
		_, outcome, err := Resolve[*ibclient.RecordA](context.Background(), conn, newEmptyRecordA, "record:a/x:y/default", uid)
		if err == nil {
			t.Fatalf("uid %q: Resolve returned nil error, want a hard error", uid)
		}
		if outcome != OutcomeNotFound {
			t.Fatalf("uid %q: outcome = %s, want NotFound", uid, outcome)
		}
	}
	if len(conn.refCalls) != 0 || conn.searchCall != 0 {
		t.Fatalf("connector was called (refCalls=%v, searchCall=%d) — a blank uid must short-circuit before any network call",
			conn.refCalls, conn.searchCall)
	}
}

// unsupportedObject implements ibclient.IBObject but does not follow the
// Ref/Ea shape every cataloged NIOS type in this provider uses — it
// stands in for a hypothetical future object type this package has never
// been taught about, to prove the reflection-based accessors fail loudly
// rather than silently skipping identity verification.
type unsupportedObject struct {
	ibclient.IBBase
}

func (unsupportedObject) ObjectType() string { return "unsupported:type" }

func newEmptyUnsupportedObject() *unsupportedObject { return &unsupportedObject{} }

type unsupportedConnector struct{ calls int }

func (c *unsupportedConnector) CreateObject(_ ibclient.IBObject) (string, error) { return "", nil }
func (c *unsupportedConnector) DeleteObject(_ string) (string, error)            { return "", nil }
func (c *unsupportedConnector) UpdateObject(_ ibclient.IBObject, _ string) (string, error) {
	return "", nil
}
func (c *unsupportedConnector) GetObject(_ ibclient.IBObject, _ string, _ *ibclient.QueryParams, _ interface{}) error {
	c.calls++
	return nil
}

func TestResolveUnsupportedObjectTypeFailsLoudly(t *testing.T) {
	conn := &unsupportedConnector{}

	_, outcome, err := Resolve[*unsupportedObject](context.Background(), conn, newEmptyUnsupportedObject, "some-ref", testUID)
	if err == nil {
		t.Fatal("Resolve returned nil error, want a loud failure for an unsupported object type")
	}
	if outcome != OutcomeNotFound {
		t.Fatalf("outcome = %s, want NotFound", outcome)
	}
	if conn.calls != 0 {
		t.Fatalf("connector called %d times, want 0 — an unsupported type must fail before any network call, not silently skip identity verification", conn.calls)
	}
}

// ── metrics discipline ───────────────────────────────────────────────

func TestResolveNormalPathDoesNotIncrementRecoveryCounters(t *testing.T) {
	const uid = testUID
	const ref = "record:a/abc:host.example.com/default"
	conn := &fakeConnector{byRef: map[string]*ibclient.RecordA{
		ref: recordAWithEA(ref, ibclient.EA{EAKey: uid}),
	}}

	rotatedBefore := testutil.ToFloat64(refMissRecoveries.WithLabelValues("record:a"))
	adoptedBefore := testutil.ToFloat64(adoptRestamps.WithLabelValues("record:a"))
	foundBefore := testutil.ToFloat64(foundByUID.WithLabelValues("record:a"))

	if _, _, err := Resolve[*ibclient.RecordA](context.Background(), conn, newEmptyRecordA, ref, uid); err != nil {
		t.Fatalf("Resolve returned error: %v", err)
	}

	if got := testutil.ToFloat64(refMissRecoveries.WithLabelValues("record:a")); got != rotatedBefore {
		t.Fatalf("refMissRecoveries changed on the normal path: before=%v after=%v", rotatedBefore, got)
	}
	if got := testutil.ToFloat64(adoptRestamps.WithLabelValues("record:a")); got != adoptedBefore {
		t.Fatalf("adoptRestamps changed on the normal path: before=%v after=%v", adoptedBefore, got)
	}
	if got := testutil.ToFloat64(foundByUID.WithLabelValues("record:a")); got != foundBefore {
		t.Fatalf("foundByUID changed on the normal path: before=%v after=%v", foundBefore, got)
	}
}

// ── Stamp / Strip ────────────────────────────────────────────────────

func TestStampAddsIdentityWithoutMutatingInput(t *testing.T) {
	in := ibclient.EA{"comment": testComment}
	out := Stamp(in, testUID)

	if _, ok := in[EAKey]; ok {
		t.Fatalf("Stamp mutated its input: %v", in)
	}
	if out[EAKey] != testUID {
		t.Fatalf("out[%q] = %v, want uid-1", EAKey, out[EAKey])
	}
	if out["comment"] != testComment {
		t.Fatalf("out[comment] = %v, want hello", out["comment"])
	}
}

func TestStripRemovesIdentityOnly(t *testing.T) {
	in := ibclient.EA{"comment": testComment, EAKey: testUID}
	out := Strip(in)

	if _, ok := out[EAKey]; ok {
		t.Fatalf("Strip left %q in the output: %v", EAKey, out)
	}
	if out["comment"] != testComment {
		t.Fatalf("out[comment] = %v, want hello", out["comment"])
	}
	if _, ok := in[EAKey]; !ok {
		t.Fatalf("Strip mutated its input: %v", in)
	}
}

func TestStampStripRoundTrip(t *testing.T) {
	in := ibclient.EA{"comment": testComment}
	stamped := Stamp(in, testUID)
	stripped := Strip(stamped)

	if len(stripped) != len(in) {
		t.Fatalf("stripped = %v, want same shape as %v", stripped, in)
	}
	for k, v := range in {
		if stripped[k] != v {
			t.Fatalf("stripped[%s] = %v, want %v", k, stripped[k], v)
		}
	}
}

// ── credential-bridge shape ──────────────────────────────────────────

func TestNewManagerAndConnectorExposesBoth(t *testing.T) {
	conn := &fakeConnector{}

	got := NewManagerAndConnector(conn)

	if got.Manager == nil {
		t.Fatal("Manager is nil")
	}
	if got.Connector != conn {
		t.Fatalf("Connector = %v, want the same instance passed in", got.Connector)
	}
}

// ── error classification passthrough ─────────────────────────────────

func TestResolveGetByRefNonNotFoundErrorPropagates(t *testing.T) {
	const ref = "record:a/abc:host.example.com/default"
	boom := errors.New("WAPI request error: 500('internal server error')")
	conn := &fakeConnector{byRefErr: map[string]error{ref: boom}}

	obj, outcome, err := Resolve[*ibclient.RecordA](context.Background(), conn, newEmptyRecordA, ref, testUID)
	if err == nil {
		t.Fatal("Resolve returned nil error, want the wrapped 500")
	}
	if outcome != OutcomeNotFound {
		t.Fatalf("outcome = %s, want NotFound", outcome)
	}
	if obj != nil {
		t.Fatalf("obj = %+v, want nil", obj)
	}
	if conn.searchCall != 0 {
		t.Fatalf("search called %d times, want 0 — a non-404 error must not fall through to a search", conn.searchCall)
	}
}

func TestResolveSearchNonNotFoundErrorPropagates(t *testing.T) {
	boom := errors.New("WAPI request error: 403('forbidden')")
	conn := &fakeConnector{searchErr: boom}

	_, outcome, err := Resolve[*ibclient.RecordA](context.Background(), conn, newEmptyRecordA, "", testUID)
	if err == nil {
		t.Fatal("Resolve returned nil error, want the wrapped 403")
	}
	if outcome != OutcomeNotFound {
		t.Fatalf("outcome = %s, want NotFound", outcome)
	}
}

// ── error message content ────────────────────────────────────────────

func TestHandleReuseErrorMessageNamesCauseAndOptions(t *testing.T) {
	err := &HandleReuseError{ObjectType: "record:a", Ref: "record:a/abc:x", WantUID: "want-uid", GotUID: "got-uid"}
	msg := err.Error()
	for _, want := range []string{"record:a", "want-uid", "got-uid", EAKey} {
		if !strings.Contains(msg, want) {
			t.Fatalf("message %q does not mention %q", msg, want)
		}
	}
}

func TestAmbiguousMatchErrorMessageNamesCauseAndOptions(t *testing.T) {
	err := &AmbiguousMatchError{ObjectType: "record:a", UID: testUID, Count: 3}
	msg := err.Error()
	for _, want := range []string{"record:a", testUID, "3", EAKey} {
		if !strings.Contains(msg, want) {
			t.Fatalf("message %q does not mention %q", msg, want)
		}
	}
}

// ── Outcome stringification ──────────────────────────────────────────

func TestOutcomeString(t *testing.T) {
	cases := map[Outcome]string{
		OutcomeResolved:   "Resolved",
		OutcomeAdopted:    "Adopted",
		OutcomeRotated:    "Rotated",
		OutcomeFoundByUID: "FoundByUID",
		OutcomeNotFound:   "NotFound",
		Outcome(99):       "Unknown",
	}
	for outcome, want := range cases {
		if got := outcome.String(); got != want {
			t.Fatalf("Outcome(%d).String() = %q, want %q", outcome, got, want)
		}
	}
}
