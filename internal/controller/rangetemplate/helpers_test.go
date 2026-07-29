// Additional unit tests for the RangeTemplate controller's scope-neutral
// currency conversions, extensible-attribute value coercion, credential
// extraction, and the _ref-change branches of Update. These complement
// controller_test.go's mock-server-driven CRUD tests by exercising the
// unexported helpers directly, and by seeding the mock WAPI server with
// fully-populated records (every optional field set) so the "value
// present" branches of the observation mapping are covered in addition to
// the "value absent" branches already covered by
// TestClusterObserveMinimalResponse / TestNamespacedObserveSuccess.
package rangetemplate

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	xpv1 "github.com/crossplane/crossplane-runtime/v2/apis/common/v1"
	"github.com/crossplane/crossplane-runtime/v2/pkg/meta"
	"github.com/crossplane/crossplane-runtime/v2/pkg/resource"
	ibclient "github.com/infobloxopen/infoblox-go-client/v2"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	clusterv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/rangetemplate/v1alpha1"
	clusterpcv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/v1alpha1"
	namespacedv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/namespaced/rangetemplate/v1alpha1"
	namespacedpcv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/namespaced/v1alpha1"
)

// ── stringifyEAValue ─────────────────────────────────────────────────────

func TestStringifyEAValue(t *testing.T) {
	cases := map[string]struct {
		reason string
		in     interface{}
		want   string
	}{
		"Nil": {
			reason: "a nil EA value renders as an empty string",
			in:     nil,
			want:   "",
		},
		"String": {
			reason: "a plain string value passes through unchanged",
			in:     "prod",
			want:   "prod",
		},
		"BoolTrue": {
			reason: "an ibclient.Bool(true) value renders as the WAPI-conventional \"True\"",
			in:     ibclient.Bool(true),
			want:   "True",
		},
		"BoolFalse": {
			reason: "an ibclient.Bool(false) value renders as the WAPI-conventional \"False\"",
			in:     ibclient.Bool(false),
			want:   "False",
		},
		"StringSlice": {
			reason: "a []string value (multi-value EA) is comma-joined",
			in:     []string{"a", "b", "c"},
			want:   "a,b,c",
		},
		"EmptyStringSlice": {
			reason: "an empty []string joins to an empty string",
			in:     []string{},
			want:   "",
		},
		"Fallback": {
			reason: "any other type falls back to fmt.Sprintf(\"%v\", ...)",
			in:     42,
			want:   "42",
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			got := stringifyEAValue(tc.in)
			if got != tc.want {
				t.Errorf("%s: stringifyEAValue(%#v) = %q, want %q", tc.reason, tc.in, got, tc.want)
			}
		})
	}
}

// ── optionsEqual ─────────────────────────────────────────────────────────

func TestOptionsEqual(t *testing.T) {
	base := []templateOption{{
		Name:        stringPtr("routers"),
		Num:         uint32Ptr(3),
		VendorClass: stringPtr("DHCP"),
		Value:       stringPtr("10.0.0.1"),
		UseOption:   boolPtr(true),
	}}

	cases := map[string]struct {
		reason string
		a, b   []templateOption
		want   bool
	}{
		"BothNil": {
			reason: "two nil lists are equal",
			a:      nil,
			b:      nil,
			want:   true,
		},
		"DifferentLengths": {
			reason: "differing list lengths are never equal",
			a:      base,
			b:      nil,
			want:   false,
		},
		"IdenticalSingleEntry": {
			reason: "two lists with the same single entry are equal",
			a:      base,
			b: []templateOption{{
				Name:        stringPtr("routers"),
				Num:         uint32Ptr(3),
				VendorClass: stringPtr("DHCP"),
				Value:       stringPtr("10.0.0.1"),
				UseOption:   boolPtr(true),
			}},
			want: true,
		},
		"NameDiffers": {
			reason: "a changed Name is detected as drift",
			a:      base,
			b: []templateOption{{
				Name:        stringPtr("routers-changed"),
				Num:         uint32Ptr(3),
				VendorClass: stringPtr("DHCP"),
				Value:       stringPtr("10.0.0.1"),
				UseOption:   boolPtr(true),
			}},
			want: false,
		},
		"NumDiffers": {
			reason: "a changed Num is detected as drift",
			a:      base,
			b: []templateOption{{
				Name:        stringPtr("routers"),
				Num:         uint32Ptr(9),
				VendorClass: stringPtr("DHCP"),
				Value:       stringPtr("10.0.0.1"),
				UseOption:   boolPtr(true),
			}},
			want: false,
		},
		"VendorClassDiffers": {
			reason: "a changed VendorClass is detected as drift",
			a:      base,
			b: []templateOption{{
				Name:        stringPtr("routers"),
				Num:         uint32Ptr(3),
				VendorClass: stringPtr("other"),
				Value:       stringPtr("10.0.0.1"),
				UseOption:   boolPtr(true),
			}},
			want: false,
		},
		"ValueDiffers": {
			reason: "a changed Value is detected as drift",
			a:      base,
			b: []templateOption{{
				Name:        stringPtr("routers"),
				Num:         uint32Ptr(3),
				VendorClass: stringPtr("DHCP"),
				Value:       stringPtr("10.0.0.2"),
				UseOption:   boolPtr(true),
			}},
			want: false,
		},
		"UseOptionDiffers": {
			reason: "a changed UseOption is detected as drift",
			a:      base,
			b: []templateOption{{
				Name:        stringPtr("routers"),
				Num:         uint32Ptr(3),
				VendorClass: stringPtr("DHCP"),
				Value:       stringPtr("10.0.0.1"),
				UseOption:   boolPtr(false),
			}},
			want: false,
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			got := optionsEqual(tc.a, tc.b)
			if got != tc.want {
				t.Errorf("%s: optionsEqual = %v, want %v", tc.reason, got, tc.want)
			}
		})
	}
}

// ── cluster/namespaced <-> scope-neutral option/member conversion ───────

func TestClusterOptionsRoundTrip(t *testing.T) {
	opts := []*clusterv1alpha1.RangeTemplateOption{
		{Name: stringPtr("routers"), Num: uint32Ptr(3), VendorClass: stringPtr("DHCP"), Value: stringPtr("10.0.0.1"), UseOption: boolPtr(true)},
		nil, // a nil entry must be skipped, not panic
	}

	common := clusterOptionsToCommon(opts)
	if len(common) != 1 {
		t.Fatalf("clusterOptionsToCommon: got %d entries, want 1 (nil entry skipped)", len(common))
	}
	if got := *common[0].Name; got != "routers" {
		t.Errorf("clusterOptionsToCommon: Name = %q, want routers", got)
	}

	back := commonToClusterOptions(common)
	if len(back) != 1 {
		t.Fatalf("commonToClusterOptions: got %d entries, want 1", len(back))
	}
	if got := *back[0].Value; got != "10.0.0.1" {
		t.Errorf("commonToClusterOptions: Value = %q, want 10.0.0.1", got)
	}

	if got := clusterOptionsToCommon(nil); got != nil {
		t.Errorf("clusterOptionsToCommon(nil) = %v, want nil", got)
	}
	if got := commonToClusterOptions(nil); got != nil {
		t.Errorf("commonToClusterOptions(nil) = %v, want nil", got)
	}
}

func TestNamespacedOptionsRoundTrip(t *testing.T) {
	opts := []*namespacedv1alpha1.RangeTemplateOption{
		{Name: stringPtr("routers"), Num: uint32Ptr(3), VendorClass: stringPtr("DHCP"), Value: stringPtr("10.0.0.1"), UseOption: boolPtr(true)},
		nil, // a nil entry must be skipped, not panic
	}

	common := namespacedOptionsToCommon(opts)
	if len(common) != 1 {
		t.Fatalf("namespacedOptionsToCommon: got %d entries, want 1 (nil entry skipped)", len(common))
	}
	if got := *common[0].Name; got != "routers" {
		t.Errorf("namespacedOptionsToCommon: Name = %q, want routers", got)
	}

	back := commonToNamespacedOptions(common)
	if len(back) != 1 {
		t.Fatalf("commonToNamespacedOptions: got %d entries, want 1", len(back))
	}
	if got := *back[0].Value; got != "10.0.0.1" {
		t.Errorf("commonToNamespacedOptions: Value = %q, want 10.0.0.1", got)
	}

	if got := namespacedOptionsToCommon(nil); got != nil {
		t.Errorf("namespacedOptionsToCommon(nil) = %v, want nil", got)
	}
	if got := commonToNamespacedOptions(nil); got != nil {
		t.Errorf("commonToNamespacedOptions(nil) = %v, want nil", got)
	}
}

func TestClusterMemberRoundTrip(t *testing.T) {
	m := &clusterv1alpha1.RangeTemplateMember{
		Ipv4Addr: stringPtr("10.0.0.5"),
		Ipv6Addr: stringPtr("::1"),
		Name:     stringPtr("member1.example.com"),
	}

	common := clusterMemberToCommon(m)
	if common == nil || *common.Name != "member1.example.com" {
		t.Fatalf("clusterMemberToCommon: got %+v, want Name=member1.example.com", common)
	}

	back := commonToClusterMember(common)
	if back == nil || *back.Ipv4Addr != "10.0.0.5" {
		t.Fatalf("commonToClusterMember: got %+v, want Ipv4Addr=10.0.0.5", back)
	}

	if got := clusterMemberToCommon(nil); got != nil {
		t.Errorf("clusterMemberToCommon(nil) = %v, want nil", got)
	}
	if got := commonToClusterMember(nil); got != nil {
		t.Errorf("commonToClusterMember(nil) = %v, want nil", got)
	}
}

func TestNamespacedMemberRoundTrip(t *testing.T) {
	m := &namespacedv1alpha1.RangeTemplateMember{
		Ipv4Addr: stringPtr("10.0.0.5"),
		Ipv6Addr: stringPtr("::1"),
		Name:     stringPtr("member1.example.com"),
	}

	common := namespacedMemberToCommon(m)
	if common == nil || *common.Name != "member1.example.com" {
		t.Fatalf("namespacedMemberToCommon: got %+v, want Name=member1.example.com", common)
	}

	back := commonToNamespacedMember(common)
	if back == nil || *back.Ipv4Addr != "10.0.0.5" {
		t.Fatalf("commonToNamespacedMember: got %+v, want Ipv4Addr=10.0.0.5", back)
	}

	if got := namespacedMemberToCommon(nil); got != nil {
		t.Errorf("namespacedMemberToCommon(nil) = %v, want nil", got)
	}
	if got := commonToNamespacedMember(nil); got != nil {
		t.Errorf("commonToNamespacedMember(nil) = %v, want nil", got)
	}
}

// ── observeFromRangeTemplate: fully-populated response ───────────────────

// TestObserveFromRangeTemplateFullResponse calls the mapping helper
// directly with every optional field set, covering the "value present"
// branch of each field's conditional (the WAPI-response-to-CRD mapping
// only carries a field into the observation when it is non-empty/non-nil
// — see observeFromRangeTemplate's doc comment).
func TestObserveFromRangeTemplateFullResponse(t *testing.T) {
	useOptions := true
	cloudCompat := true
	rec := &ibclient.Rangetemplate{
		Ref:                   "rangetemplate/xyz:template1",
		Name:                  stringPtr("template1"),
		NumberOfAddresses:     uint32Ptr(10),
		Offset:                uint32Ptr(5),
		Comment:               stringPtr("hello"),
		Ea:                    ibclient.EA{"env": "prod"},
		Options:               []*ibclient.Dhcpoption{{Name: "routers", Value: "10.0.0.1"}},
		UseOptions:            &useOptions,
		ServerAssociationType: "MEMBER",
		FailoverAssociation:   stringPtr("failover1"),
		Member:                &ibclient.Dhcpmember{Name: "member1.example.com"},
		CloudApiCompatible:    &cloudCompat,
	}

	o := observeFromRangeTemplate("rangetemplate/xyz:template1", rec)

	if o.ID != "rangetemplate/xyz:template1" {
		t.Errorf("ID = %q, want rangetemplate/xyz:template1", o.ID)
	}
	if o.Comment == nil || *o.Comment != "hello" {
		t.Errorf("Comment = %v, want hello", o.Comment)
	}
	if o.UseOptions == nil || !*o.UseOptions {
		t.Errorf("UseOptions = %v, want true", o.UseOptions)
	}
	if o.ServerAssociationType == nil || *o.ServerAssociationType != "MEMBER" {
		t.Errorf("ServerAssociationType = %v, want MEMBER", o.ServerAssociationType)
	}
	if o.FailoverAssociation == nil || *o.FailoverAssociation != "failover1" {
		t.Errorf("FailoverAssociation = %v, want failover1", o.FailoverAssociation)
	}
	if o.CloudApiCompatible == nil || !*o.CloudApiCompatible {
		t.Errorf("CloudApiCompatible = %v, want true", o.CloudApiCompatible)
	}
	if o.Ref == nil || *o.Ref != "rangetemplate/xyz:template1" {
		t.Errorf("Ref = %v, want rangetemplate/xyz:template1", o.Ref)
	}
	if o.Member == nil || *o.Member.Name != "member1.example.com" {
		t.Errorf("Member = %v, want Name=member1.example.com", o.Member)
	}
	if len(o.Options) != 1 || *o.Options[0].Name != "routers" {
		t.Errorf("Options = %v, want one entry named routers", o.Options)
	}
}

// TestObserveFromRangeTemplateEmptyStringFieldsOmitted verifies that a
// Comment/FailoverAssociation/ServerAssociationType that comes back as an
// explicit empty string (as opposed to nil) is still treated as "absent"
// — the WAPI SDK's plain-string fields have no way to distinguish unset
// from empty, so this provider's convention is to omit the empty case
// from the observation, matching lateInitialize's expectations.
func TestObserveFromRangeTemplateEmptyStringFieldsOmitted(t *testing.T) {
	rec := &ibclient.Rangetemplate{
		Name:                  stringPtr("template1"),
		NumberOfAddresses:     uint32Ptr(10),
		Offset:                uint32Ptr(5),
		Comment:               stringPtr(""),
		FailoverAssociation:   stringPtr(""),
		ServerAssociationType: "",
	}

	o := observeFromRangeTemplate("rangetemplate/xyz:template1", rec)

	if o.Comment != nil {
		t.Errorf("Comment = %v, want nil for empty-string response", o.Comment)
	}
	if o.FailoverAssociation != nil {
		t.Errorf("FailoverAssociation = %v, want nil for empty-string response", o.FailoverAssociation)
	}
	if o.ServerAssociationType != nil {
		t.Errorf("ServerAssociationType = %v, want nil for empty-string response", o.ServerAssociationType)
	}
}

// ── extractCredentials ────────────────────────────────────────────────────

func TestExtractCredentials(t *testing.T) {
	ns := "crossplane-system"
	secretName := "infobloxnios-api-key"

	cases := map[string]struct {
		reason    string
		source    xpv1.CredentialsSource
		secretRef *xpv1.SecretKeySelector
		secret    *corev1SecretBuilder
		wantErr   bool
	}{
		"UnsupportedSource": {
			reason:  "only Secret is a supported credentials source",
			source:  xpv1.CredentialsSourceInjectedIdentity,
			wantErr: true,
		},
		"NilSecretRef": {
			reason:  "a Secret source with no secretRef is an error",
			source:  xpv1.CredentialsSourceSecret,
			wantErr: true,
		},
		"SecretNotFound": {
			reason: "a secretRef pointing at a Secret that doesn't exist is an error",
			source: xpv1.CredentialsSourceSecret,
			secretRef: &xpv1.SecretKeySelector{
				SecretReference: xpv1.SecretReference{Name: "does-not-exist", Namespace: ns},
				Key:             "unused",
			},
			wantErr: true,
		},
		"MissingHostKey": {
			reason: "a Secret missing the host key is an error",
			source: xpv1.CredentialsSourceSecret,
			secretRef: &xpv1.SecretKeySelector{
				SecretReference: xpv1.SecretReference{Name: secretName, Namespace: ns},
				Key:             "unused",
			},
			secret:  &corev1SecretBuilder{name: secretName, ns: ns, username: "admin", password: "s3cr3t"},
			wantErr: true,
		},
		"MissingUsernameKey": {
			reason: "a Secret missing the username key is an error",
			source: xpv1.CredentialsSourceSecret,
			secretRef: &xpv1.SecretKeySelector{
				SecretReference: xpv1.SecretReference{Name: secretName, Namespace: ns},
				Key:             "unused",
			},
			secret:  &corev1SecretBuilder{name: secretName, ns: ns, host: "grid.example.com", password: "s3cr3t"},
			wantErr: true,
		},
		"MissingPasswordKey": {
			reason: "a Secret missing the password key is an error",
			source: xpv1.CredentialsSourceSecret,
			secretRef: &xpv1.SecretKeySelector{
				SecretReference: xpv1.SecretReference{Name: secretName, Namespace: ns},
				Key:             "unused",
			},
			secret:  &corev1SecretBuilder{name: secretName, ns: ns, host: "grid.example.com", username: "admin"},
			wantErr: true,
		},
		"Success": {
			reason: "a fully-populated Secret resolves without error",
			source: xpv1.CredentialsSourceSecret,
			secretRef: &xpv1.SecretKeySelector{
				SecretReference: xpv1.SecretReference{Name: secretName, Namespace: ns},
				Key:             "unused",
			},
			secret:  &corev1SecretBuilder{name: secretName, ns: ns, host: "grid.example.com", username: "admin", password: "s3cr3t"},
			wantErr: false,
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			scheme := newTestScheme(t)
			builder := fake.NewClientBuilder().WithScheme(scheme)
			if tc.secret != nil {
				builder = builder.WithObjects(credentialsSecret(tc.secret.ns, tc.secret.name, tc.secret.host, tc.secret.username, tc.secret.password))
			}
			kube := builder.Build()

			creds, err := extractCredentials(context.Background(), kube, tc.source, tc.secretRef, ns)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("%s: extractCredentials: expected error, got nil (creds=%+v)", tc.reason, creds)
				}
				return
			}
			if err != nil {
				t.Fatalf("%s: extractCredentials: unexpected error: %v", tc.reason, err)
			}
			if creds.Host != "grid.example.com" || creds.Username != "admin" || creds.Password != "s3cr3t" {
				t.Errorf("%s: extractCredentials = %+v, want Host/Username/Password populated", tc.reason, creds)
			}
		})
	}
}

// corev1SecretBuilder is a small literal holder for the fields
// credentialsSecret needs — used only to keep the extractCredentials table
// test's per-case declarations compact.
type corev1SecretBuilder struct {
	name, ns, host, username, password string
}

// ── Update: external-name updated when the WAPI rotates the _ref ────────
//
// UpdateRangeTemplate's SDK contract (see object_manager_range_template.go)
// is PUT-then-refetch: the PUT response is the object's (possibly new)
// _ref, and the SDK immediately re-fetches by that new ref. Renaming a
// range template can rotate its _ref, so the controller must update the
// external-name annotation when the returned ref differs from the one it
// called Update with. renamingMockHandler emulates that PUT/GET pair
// directly (rather than reusing mockWapiServer, whose PUT handler always
// echoes the same ref back) so this rename path can be exercised.
func renamingMockHandler(oldRef, newRef string, newRec *ibclient.Rangetemplate) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("PUT /wapi/v"+wapiVersion+"/{ref...}", func(w http.ResponseWriter, r *http.Request) {
		if r.PathValue("ref") != oldRef {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		writeJSON(w, http.StatusOK, newRef)
	})
	mux.HandleFunc("GET /wapi/v"+wapiVersion+"/{ref...}", func(w http.ResponseWriter, r *http.Request) {
		if r.PathValue("ref") != newRef {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		writeJSON(w, http.StatusOK, newRec)
	})
	return mux
}

func TestClusterUpdateChangesExternalNameOnRefChange(t *testing.T) {
	const (
		oldRef = "rangetemplate/old123:template1"
		newRef = "rangetemplate/new456:renamed-template"
	)
	srv := httptest.NewServer(renamingMockHandler(oldRef, newRef, &ibclient.Rangetemplate{
		Ref:               newRef,
		Name:              stringPtr("renamed-template"),
		NumberOfAddresses: uint32Ptr(10),
		Offset:            uint32Ptr(5),
	}))
	defer srv.Close()

	e := &clusterExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newClusterRangeTemplate("my-rangetemplate", oldRef)
	cr.Spec.ForProvider.Name = stringPtr("renamed-template")

	if _, err := e.Update(context.Background(), cr); err != nil {
		t.Fatalf("Update: unexpected error: %v", err)
	}
	if got := meta.GetExternalName(cr); got != newRef {
		t.Errorf("external-name after Update = %q, want %q (WAPI rotated the _ref)", got, newRef)
	}
}

func TestNamespacedUpdateChangesExternalNameOnRefChange(t *testing.T) {
	const (
		oldRef = "rangetemplate/old123:template1"
		newRef = "rangetemplate/new456:renamed-template"
	)
	srv := httptest.NewServer(renamingMockHandler(oldRef, newRef, &ibclient.Rangetemplate{
		Ref:               newRef,
		Name:              stringPtr("renamed-template"),
		NumberOfAddresses: uint32Ptr(10),
		Offset:            uint32Ptr(5),
	}))
	defer srv.Close()

	e := &namespacedExternal{objMgr: newTestObjectManager(t, srv)}
	cr := newNamespacedRangeTemplate("app-ns", "my-rangetemplate", oldRef, "ProviderConfig")
	cr.Spec.ForProvider.Name = stringPtr("renamed-template")

	if _, err := e.Update(context.Background(), cr); err != nil {
		t.Fatalf("Update: unexpected error: %v", err)
	}
	if got := meta.GetExternalName(cr); got != newRef {
		t.Errorf("external-name after Update = %q, want %q (WAPI rotated the _ref)", got, newRef)
	}
}

// ── Connect: no ProviderConfigReference set ──────────────────────────────

func TestClusterConnectNoProviderConfigReference(t *testing.T) {
	scheme := newTestScheme(t)
	kube := fake.NewClientBuilder().WithScheme(scheme).Build()

	conn := &clusterConnector{
		kube:  kube,
		usage: resource.NewLegacyProviderConfigUsageTracker(kube, &clusterpcv1alpha1.ProviderConfigUsage{}),
	}

	cr := newClusterRangeTemplate("my-rangetemplate", "")
	cr.Spec.ProviderConfigReference = nil

	if _, err := conn.Connect(context.Background(), cr); err == nil {
		t.Fatal("Connect: expected error for nil ProviderConfigReference, got nil")
	}
}

func TestNamespacedConnectNoProviderConfigReference(t *testing.T) {
	scheme := newTestScheme(t)
	kube := fake.NewClientBuilder().WithScheme(scheme).Build()

	conn := &namespacedConnector{
		kube:  kube,
		usage: resource.NewProviderConfigUsageTracker(kube, &namespacedpcv1alpha1.ProviderConfigUsage{}),
	}

	cr := newNamespacedRangeTemplate("app-ns", "my-rangetemplate", "", "ProviderConfig")
	cr.Spec.ProviderConfigReference = nil

	if _, err := conn.Connect(context.Background(), cr); err == nil {
		t.Fatal("Connect: expected error for nil ProviderConfigReference, got nil")
	}
}

// ── isNotFound: unwraps a generically-formatted (non-typed) 404 ─────────

// TestIsNotFoundReturnsFalseForUnrelatedError verifies isNotFound does not
// misclassify an ordinary Go error that carries no WAPI status code.
func TestIsNotFoundReturnsFalseForUnrelatedError(t *testing.T) {
	if isNotFound(nil) {
		t.Error("isNotFound(nil) = true, want false")
	}
	if isNotFound(errors.New("boom")) {
		t.Error("isNotFound(unrelated error) = true, want false")
	}
}
