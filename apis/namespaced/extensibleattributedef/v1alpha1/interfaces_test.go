package v1alpha1

import (
	"github.com/crossplane/crossplane-runtime/v2/pkg/resource"
)

// Compile-time assertions that ExtensibleAttributeDef satisfies the crossplane-runtime
// interfaces the managed reconciler requires. This is a namespaced-scoped MR:
// it embeds xpv2.ManagedResourceSpec and must satisfy ModernManaged, not
// LegacyManaged. A silent regression here (a dropped angryjet marker, an
// accidental swap to ClusterManagedResourceSpec, or a missing object-root
// marker) still compiles cleanly — the break only surfaces later when the
// reconciler is wired up, far from its cause.
var (
	_ resource.Managed       = &ExtensibleAttributeDef{}
	_ resource.ModernManaged = &ExtensibleAttributeDef{}
)
