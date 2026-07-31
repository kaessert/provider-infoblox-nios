// Package zoneauth — additional unit tests closing coverage gaps left by
// controller_test.go: the credential-bridge error paths, the EA value
// stringifier's non-string branches, the grid/external server round-trip
// through both scopes' Observe methods, and the isUpToDate/
// lateInitializeCollections branches not otherwise exercised by a full
// end-to-end Observe call.
package zoneauth

import (
	"context"
	"net/http/httptest"
	"testing"

	xpv1 "github.com/crossplane/crossplane-runtime/v2/apis/common/v1"
	ibclient "github.com/infobloxopen/infoblox-go-client/v2"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	clusterv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/zoneauth/v1alpha1"
	namespacedv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/namespaced/zoneauth/v1alpha1"
)

// ── extractCredentials ───────────────────────────────────────────────────

func TestExtractCredentials(t *testing.T) {
	const ns = "crossplane-system"

	cases := map[string]struct {
		reason    string
		source    xpv1.CredentialsSource
		secretRef *xpv1.SecretKeySelector
		objects   []runtime.Object
		wantErr   bool
	}{
		"Success": {
			reason: "A Secret carrying all three keys resolves cleanly.",
			source: xpv1.CredentialsSourceSecret,
			secretRef: &xpv1.SecretKeySelector{
				SecretReference: xpv1.SecretReference{Name: "creds", Namespace: ns},
			},
			objects: []runtime.Object{credentialsSecret(ns, "creds", "grid.example.com", "admin", "s3cr3t")},
		},
		"UnsupportedSource": {
			reason:  "Only CredentialsSourceSecret is supported.",
			source:  xpv1.CredentialsSourceNone,
			wantErr: true,
		},
		"NilSecretRef": {
			reason:  "A Secret source with no secretRef is rejected.",
			source:  xpv1.CredentialsSourceSecret,
			wantErr: true,
		},
		"SecretNotFound": {
			reason: "A secretRef pointing at a Secret that doesn't exist surfaces the Get error.",
			source: xpv1.CredentialsSourceSecret,
			secretRef: &xpv1.SecretKeySelector{
				SecretReference: xpv1.SecretReference{Name: "missing", Namespace: ns},
			},
			wantErr: true,
		},
		"MissingHostKey": {
			reason: "A Secret missing the host key is rejected.",
			source: xpv1.CredentialsSourceSecret,
			secretRef: &xpv1.SecretKeySelector{
				SecretReference: xpv1.SecretReference{Name: "partial", Namespace: ns},
			},
			objects: []runtime.Object{&corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: "partial", Namespace: ns},
				Data: map[string][]byte{
					"username": []byte("admin"),
					"password": []byte("s3cr3t"),
				},
			}},
			wantErr: true,
		},
		"MissingUsernameKey": {
			reason: "A Secret missing the username key is rejected.",
			source: xpv1.CredentialsSourceSecret,
			secretRef: &xpv1.SecretKeySelector{
				SecretReference: xpv1.SecretReference{Name: "partial", Namespace: ns},
			},
			objects: []runtime.Object{&corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: "partial", Namespace: ns},
				Data: map[string][]byte{
					"host":     []byte("grid.example.com"),
					"password": []byte("s3cr3t"),
				},
			}},
			wantErr: true,
		},
		"MissingPasswordKey": {
			reason: "A Secret missing the password key is rejected.",
			source: xpv1.CredentialsSourceSecret,
			secretRef: &xpv1.SecretKeySelector{
				SecretReference: xpv1.SecretReference{Name: "partial", Namespace: ns},
			},
			objects: []runtime.Object{&corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: "partial", Namespace: ns},
				Data: map[string][]byte{
					"host":     []byte("grid.example.com"),
					"username": []byte("admin"),
				},
			}},
			wantErr: true,
		},
		"FallbackNamespaceUsed": {
			reason: "A secretRef with no namespace falls back to the caller-supplied namespace.",
			source: xpv1.CredentialsSourceSecret,
			secretRef: &xpv1.SecretKeySelector{
				SecretReference: xpv1.SecretReference{Name: "creds"},
			},
			objects: []runtime.Object{credentialsSecret(ns, "creds", "grid.example.com", "admin", "s3cr3t")},
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			scheme := runtime.NewScheme()
			if err := corev1.AddToScheme(scheme); err != nil {
				t.Fatalf("%s: cannot build scheme: %v", tc.reason, err)
			}
			kube := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(tc.objects...).Build()

			got, err := extractCredentials(context.Background(), kube, tc.source, tc.secretRef, ns)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("%s: extractCredentials: want error, got nil", tc.reason)
				}
				return
			}
			if err != nil {
				t.Fatalf("%s: extractCredentials: unexpected error: %v", tc.reason, err)
			}
			if got.Host != "grid.example.com" || got.Username != "admin" || got.Password != "s3cr3t" {
				t.Errorf("%s: extractCredentials = %+v, want host/username/password populated", tc.reason, got)
			}
		})
	}
}

// ── stringifyEAValue ─────────────────────────────────────────────────────

func TestStringifyEAValue(t *testing.T) {
	cases := map[string]struct {
		reason string
		in     interface{}
		want   string
	}{
		"Nil": {
			reason: "A nil EA value renders as an empty string.",
			in:     nil,
			want:   "",
		},
		"String": {
			reason: "A plain string value passes through unchanged.",
			in:     "prod",
			want:   "prod",
		},
		"BoolTrue": {
			reason: "ibclient.Bool(true) renders as the WAPI 'True' convention.",
			in:     ibclient.Bool(true),
			want:   "True",
		},
		"BoolFalse": {
			reason: "ibclient.Bool(false) renders as the WAPI 'False' convention.",
			in:     ibclient.Bool(false),
			want:   "False",
		},
		"StringSlice": {
			reason: "A []string value (multi-value EA) is comma-joined.",
			in:     []string{"a", "b", "c"},
			want:   "a,b,c",
		},
		"DefaultFallback": {
			reason: "Any other type falls back to fmt.Sprintf('%v', ...).",
			in:     42,
			want:   "42",
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			if got := stringifyEAValue(tc.in); got != tc.want {
				t.Errorf("%s: stringifyEAValue(%#v) = %q, want %q", tc.reason, tc.in, got, tc.want)
			}
		})
	}
}

// ── uint32OrZero ─────────────────────────────────────────────────────────

func TestUint32OrZero(t *testing.T) {
	if got := uint32OrZero(nil); got != 0 {
		t.Errorf("uint32OrZero(nil) = %d, want 0", got)
	}
	v := uint32(3600)
	if got := uint32OrZero(&v); got != 3600 {
		t.Errorf("uint32OrZero(&3600) = %d, want 3600", got)
	}
}

// ── externalServerValuesEqual ────────────────────────────────────────────

func TestExternalServerValuesEqualDetectsFieldMismatch(t *testing.T) {
	a := []externalServerValue{{Address: "10.0.0.1", Name: "ns1.example.com"}}
	b := []externalServerValue{{Address: "10.0.0.2", Name: "ns1.example.com"}}
	if externalServerValuesEqual(a, b) {
		t.Error("externalServerValuesEqual: want false for differing Address, got true")
	}
}

// TestExternalServerValuesEqualIgnoresTsigKeyNameWhenFlagOff proves an
// ExternalPrimaries/ExternalSecondaries entry ignores a tsig_key_name
// mismatch while its own use_tsig_key_name is off. The SDK's NameServer
// type documents use_tsig_key_name as the use flag for tsig_key_name — off
// means the appliance does not apply tsig_key_name to this external
// server, so it is not something the user's spec can drive, and comparing
// it unconditionally can never converge.
func TestExternalServerValuesEqualIgnoresTsigKeyNameWhenFlagOff(t *testing.T) {
	a := []externalServerValue{{Address: "10.0.0.1", Name: "ns1.example.com", UseTsigKeyName: false, TsigKeyName: "key-a"}}
	b := []externalServerValue{{Address: "10.0.0.1", Name: "ns1.example.com", UseTsigKeyName: false, TsigKeyName: "key-b"}}
	if !externalServerValuesEqual(a, b) {
		t.Error("externalServerValuesEqual: want true (use_tsig_key_name off, tsig_key_name is server-owned), got false")
	}
}

// TestExternalServerValuesEqualDetectsTsigKeyNameWhenFlagOn is the
// flag-on counterpart: the same mismatch is real drift once the flag is
// on.
func TestExternalServerValuesEqualDetectsTsigKeyNameWhenFlagOn(t *testing.T) {
	a := []externalServerValue{{Address: "10.0.0.1", Name: "ns1.example.com", UseTsigKeyName: true, TsigKeyName: "key-a"}}
	b := []externalServerValue{{Address: "10.0.0.1", Name: "ns1.example.com", UseTsigKeyName: true, TsigKeyName: "key-b"}}
	if externalServerValuesEqual(a, b) {
		t.Error("externalServerValuesEqual: want false (use_tsig_key_name on, tsig_key_name differs), got true")
	}
}

// TestExternalServerValuesEqualDetectsUseTsigKeyNameTransition proves the
// per-item flag comparison stays unconditional even though the value
// comparison is gated, so a false -> true transition is still detected.
func TestExternalServerValuesEqualDetectsUseTsigKeyNameTransition(t *testing.T) {
	a := []externalServerValue{{Address: "10.0.0.1", Name: "ns1.example.com", UseTsigKeyName: false}}
	b := []externalServerValue{{Address: "10.0.0.1", Name: "ns1.example.com", UseTsigKeyName: true, TsigKeyName: "key-b"}}
	if externalServerValuesEqual(a, b) {
		t.Error("externalServerValuesEqual: want false (use_tsig_key_name transitioned false -> true), got true")
	}
}

// TestMemberServerValuesEqualIgnoresNestedTsigKeyNameWhenFlagOff proves
// the gate also applies through the nested PreferredPrimaries
// ([]externalServerValue) inside GridPrimary/GridSecondaries entries.
func TestMemberServerValuesEqualIgnoresNestedTsigKeyNameWhenFlagOff(t *testing.T) {
	a := []memberServerValue{{
		Name:               "member1.example.com",
		PreferredPrimaries: []externalServerValue{{Address: "10.0.0.1", UseTsigKeyName: false, TsigKeyName: "key-a"}},
	}}
	b := []memberServerValue{{
		Name:               "member1.example.com",
		PreferredPrimaries: []externalServerValue{{Address: "10.0.0.1", UseTsigKeyName: false, TsigKeyName: "key-b"}},
	}}
	if !memberServerValuesEqual(a, b) {
		t.Error("memberServerValuesEqual: want true (use_tsig_key_name off on nested PreferredPrimaries entry), got false")
	}
}

// ── isUpToDate: exhaustive field-mismatch coverage ──────────────────────
//
// isUpToDate short-circuits on the first mismatched field, so a single
// desired/observed pair only ever exercises one "return false" branch.
// This table drives every comparison branch independently.

func TestIsUpToDateFieldMismatches(t *testing.T) {
	base := zoneAuthFields{
		Comment:        strPtrOrNil("hello"),
		Disable:        boolPtr(false),
		SoaDefaultTTL:  uint32Ptr(28800),
		SoaExpire:      uint32Ptr(2419200),
		SoaNegativeTTL: uint32Ptr(900),
		SoaRefresh:     uint32Ptr(10800),
		SoaRetry:       uint32Ptr(3600),
		NsGroup:        strPtrOrNil("default-nsgroup"),
		ExtAttrs:       map[string]string{"env": "prod"},
		GridPrimary:    []memberServerValue{{Name: "member1.example.com"}},
		GridSecondaries: []memberServerValue{
			{Name: "member2.example.com"},
		},
		ExternalPrimaries: []externalServerValue{
			{Address: "10.0.0.1", Name: "ns1.example.com"},
		},
		ExternalSecondaries: []externalServerValue{
			{Address: "10.0.0.2", Name: "ns2.example.com"},
		},
	}

	// Baseline sanity check: an identical copy must compare equal.
	if !isUpToDate(base, base) {
		t.Fatal("isUpToDate(base, base): want true, got false")
	}

	cases := map[string]struct {
		reason string
		mutate func(zoneAuthFields) zoneAuthFields
	}{
		"Disable": {
			reason: "Disable mismatch must be detected.",
			mutate: func(f zoneAuthFields) zoneAuthFields { f.Disable = boolPtr(true); return f },
		},
		"SoaExpire": {
			reason: "SoaExpire mismatch must be detected.",
			mutate: func(f zoneAuthFields) zoneAuthFields { f.SoaExpire = uint32Ptr(1); return f },
		},
		"SoaNegativeTTL": {
			reason: "SoaNegativeTTL mismatch must be detected.",
			mutate: func(f zoneAuthFields) zoneAuthFields { f.SoaNegativeTTL = uint32Ptr(1); return f },
		},
		"SoaRefresh": {
			reason: "SoaRefresh mismatch must be detected.",
			mutate: func(f zoneAuthFields) zoneAuthFields { f.SoaRefresh = uint32Ptr(1); return f },
		},
		"SoaRetry": {
			reason: "SoaRetry mismatch must be detected.",
			mutate: func(f zoneAuthFields) zoneAuthFields { f.SoaRetry = uint32Ptr(1); return f },
		},
		"NsGroup": {
			reason: "NsGroup mismatch must be detected.",
			mutate: func(f zoneAuthFields) zoneAuthFields { f.NsGroup = strPtrOrNil("other-nsgroup"); return f },
		},
		"GridSecondaries": {
			reason: "GridSecondaries mismatch must be detected.",
			mutate: func(f zoneAuthFields) zoneAuthFields {
				f.GridSecondaries = []memberServerValue{{Name: "different.example.com"}}
				return f
			},
		},
		"ExternalPrimaries": {
			reason: "ExternalPrimaries mismatch must be detected.",
			mutate: func(f zoneAuthFields) zoneAuthFields {
				f.ExternalPrimaries = []externalServerValue{{Address: "10.0.0.9", Name: "different.example.com"}}
				return f
			},
		},
		"ExternalSecondaries": {
			reason: "ExternalSecondaries mismatch must be detected.",
			mutate: func(f zoneAuthFields) zoneAuthFields {
				f.ExternalSecondaries = []externalServerValue{{Address: "10.0.0.9", Name: "different.example.com"}}
				return f
			},
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			desired := tc.mutate(base)
			if isUpToDate(desired, base) {
				t.Errorf("%s: isUpToDate: want false, got true", tc.reason)
			}
		})
	}
}

// ── lateInitializeCollections ────────────────────────────────────────────

func TestLateInitializeCollectionsBackfillsServerLists(t *testing.T) {
	desired := zoneAuthFields{}
	observed := zoneAuthFields{
		GridPrimary:         []memberServerValue{{Name: "member1.example.com"}},
		GridSecondaries:     []memberServerValue{{Name: "member2.example.com"}},
		ExternalPrimaries:   []externalServerValue{{Address: "10.0.0.1", Name: "ns1.example.com"}},
		ExternalSecondaries: []externalServerValue{{Address: "10.0.0.2", Name: "ns2.example.com"}},
	}

	updated, changed := lateInitializeCollections(desired, observed)
	if !changed {
		t.Fatal("lateInitializeCollections: want changed=true, got false")
	}
	if !memberServerValuesEqual(updated.GridPrimary, observed.GridPrimary) {
		t.Errorf("GridPrimary = %+v, want %+v", updated.GridPrimary, observed.GridPrimary)
	}
	if !memberServerValuesEqual(updated.GridSecondaries, observed.GridSecondaries) {
		t.Errorf("GridSecondaries = %+v, want %+v", updated.GridSecondaries, observed.GridSecondaries)
	}
	if !externalServerValuesEqual(updated.ExternalPrimaries, observed.ExternalPrimaries) {
		t.Errorf("ExternalPrimaries = %+v, want %+v", updated.ExternalPrimaries, observed.ExternalPrimaries)
	}
	if !externalServerValuesEqual(updated.ExternalSecondaries, observed.ExternalSecondaries) {
		t.Errorf("ExternalSecondaries = %+v, want %+v", updated.ExternalSecondaries, observed.ExternalSecondaries)
	}
}

func TestLateInitializeCollectionsDoesNotOverwriteSetLists(t *testing.T) {
	userGridPrimary := []memberServerValue{{Name: "user-set.example.com"}}
	desired := zoneAuthFields{GridPrimary: userGridPrimary}
	observed := zoneAuthFields{GridPrimary: []memberServerValue{{Name: "server-default.example.com"}}}

	updated, changed := lateInitializeCollections(desired, observed)
	if changed {
		t.Error("lateInitializeCollections: want changed=false when spec already set, got true")
	}
	if !memberServerValuesEqual(updated.GridPrimary, userGridPrimary) {
		t.Errorf("GridPrimary = %+v, want user-set value preserved: %+v", updated.GridPrimary, userGridPrimary)
	}
}

// ── Observe: grid/external server round-trip (both scopes) ──────────────
//
// Exercises clusterMemberServersFromValues/clusterExternalServersFromValues
// and namespacedMemberServersFromValues/namespacedExternalServersFromValues
// on their non-nil path (the mock server records seeded elsewhere in this
// package never populate grid/external server lists), plus the nested
// PreferredPrimaries conversion inside memberServerValuesFromSDK.

func seededGridAndExternalServers() ([]*ibclient.Memberserver, []*ibclient.Memberserver, []ibclient.NameServer, []ibclient.NameServer) {
	gridPrimary := []*ibclient.Memberserver{
		{
			Name:          "member1.example.com",
			Stealth:       true,
			GridReplicate: false,
			Lead:          true,
			PreferredPrimaries: []ibclient.NameServer{
				{Address: "10.0.0.1", Name: "ns1.example.com"},
			},
			EnablePreferredPrimaries: true,
		},
	}
	gridSecondaries := []*ibclient.Memberserver{
		{Name: "member2.example.com"},
	}
	externalPrimaries := []ibclient.NameServer{
		{Address: "10.0.0.9", Name: "ext1.example.com", TsigKeyName: "key1", UseTsigKeyName: true},
	}
	externalSecondaries := []ibclient.NameServer{
		{Address: "10.0.0.10", Name: "ext2.example.com"},
	}
	return gridPrimary, gridSecondaries, externalPrimaries, externalSecondaries
}

func TestClusterObserveWithGridAndExternalServers(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	gridPrimary, gridSecondaries, externalPrimaries, externalSecondaries := seededGridAndExternalServers()
	ref := m.seed(&ibclient.ZoneAuth{
		Fqdn:                "example.com",
		View:                stringPtr("default"),
		GridPrimary:         gridPrimary,
		GridSecondaries:     gridSecondaries,
		ExternalPrimaries:   externalPrimaries,
		ExternalSecondaries: externalSecondaries,
	})

	e := &clusterExternal{conn: newTestConnector(t, srv)}
	cr := newClusterZoneAuth("my-zoneauth", ref)
	// Mirror the seeded server state in spec so ResourceUpToDate is true —
	// this also exercises clusterMemberServerValues/clusterExternalServerValues
	// on the spec->fields direction for the same data.
	cr.Spec.ForProvider.GridPrimary = []clusterv1alpha1.MemberServer{
		{
			Name:                     stringPtr("member1.example.com"),
			Stealth:                  boolPtr(true),
			GridReplicate:            boolPtr(false),
			Lead:                     boolPtr(true),
			PreferredPrimaries:       []clusterv1alpha1.ExternalServer{{Address: stringPtr("10.0.0.1"), Name: stringPtr("ns1.example.com")}},
			EnablePreferredPrimaries: boolPtr(true),
		},
	}
	cr.Spec.ForProvider.GridSecondaries = []clusterv1alpha1.MemberServer{
		{Name: stringPtr("member2.example.com"), Stealth: boolPtr(false), GridReplicate: boolPtr(false), Lead: boolPtr(false), EnablePreferredPrimaries: boolPtr(false)},
	}
	cr.Spec.ForProvider.ExternalPrimaries = []clusterv1alpha1.ExternalServer{
		{Address: stringPtr("10.0.0.9"), Name: stringPtr("ext1.example.com"), TsigKeyName: stringPtr("key1"), UseTsigKeyName: boolPtr(true)},
	}
	cr.Spec.ForProvider.ExternalSecondaries = []clusterv1alpha1.ExternalServer{
		{Address: stringPtr("10.0.0.10"), Name: stringPtr("ext2.example.com")},
	}

	got, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatalf("Observe: unexpected error: %v", err)
	}
	if !got.ResourceExists {
		t.Fatal("Observe: want ResourceExists=true, got false")
	}
	if !got.ResourceUpToDate {
		t.Error("Observe: want ResourceUpToDate=true when spec mirrors server state, got false")
	}

	ap := cr.Status.AtProvider
	if len(ap.GridPrimary) != 1 || ap.GridPrimary[0].Name == nil || *ap.GridPrimary[0].Name != "member1.example.com" {
		t.Errorf("AtProvider.GridPrimary = %+v, want member1.example.com", ap.GridPrimary)
	}
	if len(ap.GridPrimary) == 1 && len(ap.GridPrimary[0].PreferredPrimaries) != 1 {
		t.Errorf("AtProvider.GridPrimary[0].PreferredPrimaries = %+v, want 1 entry", ap.GridPrimary[0].PreferredPrimaries)
	}
	if len(ap.ExternalPrimaries) != 1 || ap.ExternalPrimaries[0].Address == nil || *ap.ExternalPrimaries[0].Address != "10.0.0.9" {
		t.Errorf("AtProvider.ExternalPrimaries = %+v, want 10.0.0.9", ap.ExternalPrimaries)
	}
	if len(ap.ExternalSecondaries) != 1 {
		t.Errorf("AtProvider.ExternalSecondaries = %+v, want 1 entry", ap.ExternalSecondaries)
	}
}

func TestNamespacedObserveWithGridAndExternalServers(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	gridPrimary, gridSecondaries, externalPrimaries, externalSecondaries := seededGridAndExternalServers()
	ref := m.seed(&ibclient.ZoneAuth{
		Fqdn:                "example.com",
		View:                stringPtr("default"),
		GridPrimary:         gridPrimary,
		GridSecondaries:     gridSecondaries,
		ExternalPrimaries:   externalPrimaries,
		ExternalSecondaries: externalSecondaries,
	})

	e := &namespacedExternal{conn: newTestConnector(t, srv)}
	cr := newNamespacedZoneAuth("default", "my-zoneauth", ref, "ProviderConfig")
	cr.Spec.ForProvider.GridPrimary = []namespacedv1alpha1.MemberServer{
		{
			Name:                     stringPtr("member1.example.com"),
			Stealth:                  boolPtr(true),
			GridReplicate:            boolPtr(false),
			Lead:                     boolPtr(true),
			PreferredPrimaries:       []namespacedv1alpha1.ExternalServer{{Address: stringPtr("10.0.0.1"), Name: stringPtr("ns1.example.com")}},
			EnablePreferredPrimaries: boolPtr(true),
		},
	}
	cr.Spec.ForProvider.GridSecondaries = []namespacedv1alpha1.MemberServer{
		{Name: stringPtr("member2.example.com"), Stealth: boolPtr(false), GridReplicate: boolPtr(false), Lead: boolPtr(false), EnablePreferredPrimaries: boolPtr(false)},
	}
	cr.Spec.ForProvider.ExternalPrimaries = []namespacedv1alpha1.ExternalServer{
		{Address: stringPtr("10.0.0.9"), Name: stringPtr("ext1.example.com"), TsigKeyName: stringPtr("key1"), UseTsigKeyName: boolPtr(true)},
	}
	cr.Spec.ForProvider.ExternalSecondaries = []namespacedv1alpha1.ExternalServer{
		{Address: stringPtr("10.0.0.10"), Name: stringPtr("ext2.example.com")},
	}

	got, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatalf("Observe: unexpected error: %v", err)
	}
	if !got.ResourceExists {
		t.Fatal("Observe: want ResourceExists=true, got false")
	}
	if !got.ResourceUpToDate {
		t.Error("Observe: want ResourceUpToDate=true when spec mirrors server state, got false")
	}

	ap := cr.Status.AtProvider
	if len(ap.GridPrimary) != 1 || ap.GridPrimary[0].Name == nil || *ap.GridPrimary[0].Name != "member1.example.com" {
		t.Errorf("AtProvider.GridPrimary = %+v, want member1.example.com", ap.GridPrimary)
	}
	if len(ap.ExternalPrimaries) != 1 || ap.ExternalPrimaries[0].Address == nil || *ap.ExternalPrimaries[0].Address != "10.0.0.9" {
		t.Errorf("AtProvider.ExternalPrimaries = %+v, want 10.0.0.9", ap.ExternalPrimaries)
	}
}
