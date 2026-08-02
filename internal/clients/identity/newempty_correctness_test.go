package identity

import (
	"testing"

	ibclient "github.com/infobloxopen/infoblox-go-client/v2"
)

// TestNewEmptyCorrectness is the newEmpty correctness gate for this
// provider's UID-in-EA identity ladder: every resource controller wired
// to Resolve supplies a newEmpty constructor, and that constructor's
// ObjectType must match the WAPI object type the resource controller
// actually talks to, and its ReturnFields must include "extattrs" —
// Resolve reads the identity stamp out of the Ea field on every object
// it resolves (both the direct-ref fetch and the identity-EA search), so
// a constructor that omits extattrs from its return-fields list would
// silently defeat the entire ladder: the search step would never see the
// stamp to match against, and every resolution would either 404 or land
// on OutcomeAdopted forever.
//
// This test exercises the SDK's own constructors directly. Two wired
// packages (dtcpool, dtclbdn) do not pass these exact constructors to
// Resolve — they pass a local wrapper (newEmptyDtcPool, newEmptyDtcLbdn)
// that starts from the same object type but requests a trimmed
// return-fields list, dropping auto_consolidated_monitors because it
// 400s on this deployment's WAPI schema. Those wrappers still request
// extattrs (see their own doc comments), so the correctness property
// this gate checks holds for them by construction; testing the raw SDK
// constructors here documents the baseline contract those wrappers
// deviate from, and would catch a future SDK upgrade that changes it.
func TestNewEmptyCorrectness(t *testing.T) {
	cases := []struct {
		resource string
		obj      ibclient.IBObject
		wantType string
	}{
		// ZoneAuth has no ibclient.NewEmptyZoneAuth in the SDK; the
		// package's own newZoneAuthForGet wraps this same constructor.
		{"zoneauth", ibclient.NewZoneAuth(ibclient.ZoneAuth{}), "zone_auth"},
		{"zoneforward", ibclient.NewEmptyZoneForward(), "zone_forward"},
		{"zonedelegated", ibclient.NewEmptyZoneDelegated(), "zone_delegated"},
		{"dtcpool", ibclient.NewEmptyDtcPool(), "dtc:pool"},
		{"dtcserver", ibclient.NewEmptyDtcServer(), "dtc:server"},
		{"dtclbdn", ibclient.NewEmptyDtcLbdn(), "dtc:lbdn"},
		// DNSView's WAPI object type is "view", not "dnsview" — the
		// resource's Kubernetes Kind and its WAPI object type diverge
		// here, and asserting the real value (rather than assuming it
		// matches the Kind) is the entire point of this gate.
		{"dnsview", ibclient.NewEmptyDNSView(), "view"},
	}

	for _, tc := range cases {
		t.Run(tc.resource, func(t *testing.T) {
			if got := tc.obj.ObjectType(); got != tc.wantType {
				t.Errorf("ObjectType() = %q, want %q", got, tc.wantType)
			}

			fields := tc.obj.ReturnFields()
			found := false
			for _, f := range fields {
				if f == "extattrs" {
					found = true
					break
				}
			}
			if !found {
				t.Errorf("ReturnFields() = %v, want it to contain %q (identity.Resolve reads the identity stamp from the Ea field, which this field populates on GET)", fields, "extattrs")
			}
		})
	}
}
