// Package identitycatalog holds a single cross-cutting test: a
// table-driven correctness gate over every DNS record type's newEmpty
// constructor, the seed value the UID-in-EA object-identity ladder
// (internal/clients/identity) uses to build both its by-ref GET and its
// EA-filtered search request.
//
// Each per-resource controller package (recorda, recordaaaa, recordcname,
// recordptr, recordmx, recordsrv, recordtxt, recordalias, hostrecord)
// already wires its own newEmpty constructor into identity.Resolve
// individually — this package does not duplicate that wiring. What it
// checks is the property every one of those constructors must hold for
// the ladder to work at all: the returned object must self-identify as
// the right WAPI object type, and must request the "extattrs" field so
// the identity stamp is visible in every response the ladder resolves.
// A newEmpty constructor that silently drops either property compiles
// fine and only fails at runtime against a live Grid — this test catches
// that class of defect at build time instead.
package identitycatalog

import (
	"testing"

	ibclient "github.com/infobloxopen/infoblox-go-client/v2"
)

// TestNewEmptyConstructorsSupportIdentityLadder is a table-driven gate
// covering all 8 identity-wired DNS record types (every EA-capable
// record type this provider manages except NSRecord, which record:ns
// itself does not support — see recordns's package doc). For each type
// it asserts:
//
//  1. ObjectType() is non-empty and equals the WAPI object type the
//     owning controller package already targets (a sanity check that
//     the newEmpty constructor used here is actually the one wired into
//     that package's identity.Resolve call, not a stale or unrelated
//     constructor).
//  2. ReturnFields() contains "extattrs" — without it, a resolved
//     object's Ea field is never populated, and identity.Strip/
//     identity.Stamp compare and rewrite an empty map on every
//     reconcile, silently discarding the identity stamp.
//
// RecordAlias is the one entry in this table whose constructor breaks
// the SDK's otherwise-consistent NewEmptyRecord<Type> naming pattern:
// it is ibclient.NewEmptyAliasRecord, not ibclient.NewEmptyRecordAlias
// (which does not exist — a build against that name fails to compile).
// A future maintainer copy-pasting a new table row from a sibling entry
// is exactly the mistake this test exists to catch, so this row is
// deliberately kept spelled out rather than generated.
func TestNewEmptyConstructorsSupportIdentityLadder(t *testing.T) {
	cases := map[string]struct {
		newEmpty   func() ibclient.IBObject
		objectType string
	}{
		"AAAARecord": {
			newEmpty:   func() ibclient.IBObject { return ibclient.NewEmptyRecordAAAA() },
			objectType: "record:aaaa",
		},
		"CNAMERecord": {
			newEmpty:   func() ibclient.IBObject { return ibclient.NewEmptyRecordCNAME() },
			objectType: "record:cname",
		},
		"PTRRecord": {
			newEmpty:   func() ibclient.IBObject { return ibclient.NewEmptyRecordPTR() },
			objectType: "record:ptr",
		},
		"MXRecord": {
			newEmpty:   func() ibclient.IBObject { return ibclient.NewEmptyRecordMX() },
			objectType: "record:mx",
		},
		"SRVRecord": {
			newEmpty:   func() ibclient.IBObject { return ibclient.NewEmptyRecordSRV() },
			objectType: "record:srv",
		},
		"TXTRecord": {
			newEmpty:   func() ibclient.IBObject { return ibclient.NewEmptyRecordTXT() },
			objectType: "record:txt",
		},
		// AliasRecord: the constructor is NewEmptyAliasRecord, NOT
		// NewEmptyRecordAlias — see the function doc comment above.
		"AliasRecord": {
			newEmpty:   func() ibclient.IBObject { return ibclient.NewEmptyAliasRecord() },
			objectType: "record:alias",
		},
		"HostRecord": {
			newEmpty:   func() ibclient.IBObject { return ibclient.NewEmptyHostRecord() },
			objectType: "record:host",
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			obj := tc.newEmpty()

			gotType := obj.ObjectType()
			if gotType == "" {
				t.Fatalf("%s: newEmpty().ObjectType() is empty, want %q", name, tc.objectType)
			}
			if gotType != tc.objectType {
				t.Errorf("%s: newEmpty().ObjectType() = %q, want %q", name, gotType, tc.objectType)
			}

			fields := obj.ReturnFields()
			found := false
			for _, f := range fields {
				if f == "extattrs" {
					found = true
					break
				}
			}
			if !found {
				t.Errorf("%s: newEmpty().ReturnFields() = %v, want it to contain %q — without it the identity ladder can never see the stamped uid in a response", name, fields, "extattrs")
			}
		})
	}
}

// TestNewEmptyRecordNSDoesNotSupportIdentityLadder pins the negative case
// documented in recordns's package doc: record:ns's constructor requests
// no extattrs, which is exactly why NSRecord is not in the table above
// and is not wired to the identity ladder. If a future SDK update adds
// extattrs support to record:ns, this test starts failing — which is the
// correct signal that NSRecord should be migrated onto the ladder like
// its siblings (see recordns's package doc for the migration note).
func TestNewEmptyRecordNSDoesNotSupportIdentityLadder(t *testing.T) {
	obj := ibclient.NewEmptyRecordNS()

	for _, f := range obj.ReturnFields() {
		if f == "extattrs" {
			t.Fatal("ibclient.NewEmptyRecordNS().ReturnFields() now contains \"extattrs\" — " +
				"NSRecord's package doc explains this was the reason it was excluded from the " +
				"UID-in-EA identity ladder; if the SDK now supports it, NSRecord should be " +
				"migrated onto the ladder like the other eight DNS record types")
		}
	}
}
