// Package catalog is the hand-curated resource catalog for Infoblox NIOS.
//
// NIOS does not publish an OpenAPI/Swagger specification (input format
// rest-nospec-sdk). This catalog is derived from:
//
//   - The official Go SDK, github.com/infobloxopen/infoblox-go-client/v2,
//     pinned under tools/openapi/specs/infobloxopen/infoblox-go-client/ (see
//     that directory's README.md for the exact commit and checksums). The
//     SDK's IBObjectManager interface (object_manager.go) is the definitive
//     list of which WAPI object types are reachable through typed
//     Create/Get/Update/Delete methods, and which fields each method
//     accepts. The generated struct definitions (objects_generated.go) are
//     the definitive field list, JSON tags, and doc comments (including
//     "Valid values are ..." enum constraints).
//   - Live WAPI probing against a real NIOS Grid Manager appliance (GET on
//     representative endpoints; a scoped create+rename+delete lifecycle
//     test against a throwaway A-record) — see the "Live API Probing"
//     section of inventory.md for what was verified and how.
//   - The reference Terraform provider,
//     github.com/infobloxopen/terraform-provider-infoblox, for resource
//     naming conventions. Note: unlike many Terraform providers, this one
//     does NOT use ForceNew schema tags anywhere — immutability below is
//     derived instead by diffing each SDK Create<Resource> method's
//     parameter list against its Update<Resource> method's parameter list.
//     A parameter present in Create but absent from Update means the SDK
//     provides no way to change that value after creation.
//
// Only resource types with SDK ObjectManager CRUD support are cataloged.
// The full WAPI schema (objects_generated.go) defines several hundred
// additional object types (grid configuration, member management, RPZ
// feeds, etc.) that the SDK does not wrap with typed convenience methods;
// those are out of scope for this provider (they would require raw
// Connector.CreateObject/GetObject/UpdateObject/DeleteObject calls with
// hand-built field maps, a materially different implementation shape).
package catalog

import "github.com/crossplane-contrib/provider-infoblox-nios/tools/openapi/pkg/model"

// f builds a model.Field with no enum constraint.
func f(json, goType string, scope model.FieldScope, required, immutable bool, desc string) model.Field {
	return model.Field{
		JSONName:    json,
		GoType:      goType,
		Scope:       scope,
		Required:    required,
		Immutable:   immutable,
		Description: desc,
	}
}

// fEnum builds a model.Field with an enum constraint sourced from the SDK's
// struct field doc comment.
func fEnum(json, goType string, scope model.FieldScope, required, immutable bool, desc string, enum ...string) model.Field {
	ff := f(json, goType, scope, required, immutable, desc)
	ff.Enum = enum
	ff.EnumSource = "SDK doc comment (objects_generated.go field comment)"
	return ff
}

// Resources returns the complete, hand-curated catalog of Infoblox NIOS
// resources reachable through the SDK's ObjectManager wrapper. This is the
// single source of truth rendered into inventory.md.
func Resources() []model.Resource {
	return []model.Resource{
		// DNS records.
		aRecord(),
		aaaaRecord(),
		cnameRecord(),
		ptrRecord(),
		mxRecord(),
		nsRecord(),
		srvRecord(),
		txtRecord(),
		aliasRecord(),
		httpsRecord(),
		svcbRecord(),
		hostRecord(),
		// Networking.
		networkView(),
		network(),
		networkContainer(),
		sharedNetwork(),
		networkRange(),
		rangeTemplate(),
		fixedAddress(),
		// Zones.
		zoneAuth(),
		zoneForward(),
		zoneDelegated(),
		// DTC (DNS Traffic Control / load balancing).
		dtcPool(),
		dtcServer(),
		dtcLbdn(),
		// Grid configuration.
		eaDefinition(),
	}
}

// FindResource returns the Resource for the given slug, or (Resource{},
// false) if no resource with that slug exists.
func FindResource(slug string) (model.Resource, bool) {
	for _, r := range Resources() {
		if r.Slug == slug {
			return r, true
		}
	}
	return model.Resource{}, false
}

// ActionEndpoints lists SDK methods that perform imperative operations
// rather than CRUD on a single object type. These are NOT modeled as
// managed resources.
func ActionEndpoints() []model.ActionEndpoint {
	return []model.ActionEndpoint{
		{
			Method: "AllocateNextAvailableIp",
			Notes:  "Allocates the next free IP from a network/range and creates a Host/A/AAAA record or fixed address in one call. A composite convenience wrapper, not a standalone resource — modeled (if needed) as an option on the owning record/fixed-address resource, not its own Kind.",
		},
		{
			Method: "ReleaseIP",
			Notes:  "Releases a leased/fixed IP address without deleting a specific object by ref (searches by netview+cidr+ipAddr+mac). Overlaps with DeleteFixedAddress; not modeled separately.",
		},
		{
			Method: "AllocateNetwork / AllocateNetworkByEA / AllocateNetworkContainer / AllocateNetworkContainerByEA",
			Notes:  "Dynamic CIDR allocation from a parent network/container by prefix length rather than an explicit CIDR. An alternate creation path for Network/NetworkContainer, not a separate resource.",
		},
		{
			Method: "CreateDefaultNetviews",
			Notes:  "Grid bootstrap helper that creates the global and local default network views. Not applicable to per-resource MR lifecycle (network views are managed individually via NetworkView CRUD).",
		},
		{
			Method: "SearchObjectByAltId / SearchHostRecordByAltId / validateObjByInternalId",
			Notes:  "Alternate-ID lookup via extensible attributes (used by the Terraform provider for import-by-internal-id). Candidate for controller Observe()-time lookup when external-name is not yet known, not a resource of its own.",
		},
		{
			Method: "GetCapacityReport / GetUpgradeStatus / GetAllMembers / GetGridInfo / GetGridLicense",
			Notes:  "Read-only grid/report endpoints. No create/update/delete — out of scope for managed resources (informational only).",
		},
		{
			Method: "GetDnsMember / UpdateDnsStatus / GetDhcpMember / UpdateDhcpStatus",
			Notes:  "Per-member service enable/disable toggles (grid-wide DNS/DHCP service state), not a per-object resource.",
		},
	}
}
