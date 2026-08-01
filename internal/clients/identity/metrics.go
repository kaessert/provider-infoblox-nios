package identity

import (
	"github.com/prometheus/client_golang/prometheus"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
)

// refMissRecoveries counts OutcomeRotated resolutions: the stored
// reference 404'd and the object was relocated by its stamped identity
// attribute. Labelled by object type so a spike against one resource
// (e.g. a record type whose identity fields change often) is visible
// without a per-resource dashboard.
var refMissRecoveries = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "infobloxnios_identity_ref_miss_recoveries_total",
	Help: "Number of times a NIOS object's stored reference 404'd and the object was relocated by searching its stamped identity extensible attribute (handle rotation).",
}, []string{"object_type"})

// adoptRestamps counts OutcomeAdopted resolutions: the stored reference
// resolved but the object carried no identity extensible attribute, so
// the caller adopted it and will re-stamp the attribute on its next
// mutating call.
var adoptRestamps = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "infobloxnios_identity_adopt_restamps_total",
	Help: "Number of times a NIOS object resolved by reference but carried no stamped identity extensible attribute and was adopted (the attribute is re-stamped on the resource's next mutating call).",
}, []string{"object_type"})

// foundByUID counts OutcomeFoundByUID resolutions: no reference was
// supplied at all and the object was located purely from its stamped
// identity attribute. This is the create-crash-window recovery path.
// Tracked alongside the two required counters above since it shares the
// same underlying search and is otherwise invisible.
var foundByUID = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "infobloxnios_identity_found_by_uid_total",
	Help: "Number of times a NIOS object was located with no prior reference at all, purely by searching its stamped identity extensible attribute (create-crash-window recovery).",
}, []string{"object_type"})

// probeCreated counts successful creations of the identity extensible
// attribute definition by Prober.Ensure — the install-prerequisite path.
// Labelled by endpoint so a create against one Grid is distinguishable
// from another.
var probeCreated = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "infobloxnios_identity_probe_created_total",
	Help: "Number of times Prober.Ensure created the identity extensible attribute definition because it was absent from the Grid.",
}, []string{"endpoint"})

// probeRefused counts refusals: the definition was absent and the
// configured credential could not create it (401/403).
var probeRefused = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "infobloxnios_identity_probe_refused_total",
	Help: "Number of times Prober.Ensure refused because the identity extensible attribute definition is absent from the Grid and the configured credential cannot create one.",
}, []string{"endpoint"})

// probeRaceWon counts the "lost the race, treat as success" outcome: a
// concurrent creator (another provider replica, or an admin) created the
// definition between Prober's existence check and its own create call.
var probeRaceWon = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "infobloxnios_identity_probe_race_won_total",
	Help: "Number of times Prober.Ensure's attempt to create the identity extensible attribute definition lost a race to a concurrent creator and was treated as success.",
}, []string{"endpoint"})

func init() {
	ctrlmetrics.Registry.MustRegister(refMissRecoveries, adoptRestamps, foundByUID,
		probeCreated, probeRefused, probeRaceWon)
}

// recordRotated increments the ref-miss-recovery counter for objType.
func recordRotated(objType string) {
	refMissRecoveries.WithLabelValues(objType).Inc()
}

// recordAdopted increments the adopt-and-re-stamp counter for objType.
func recordAdopted(objType string) {
	adoptRestamps.WithLabelValues(objType).Inc()
}

// recordFoundByUID increments the found-by-uid counter for objType.
func recordFoundByUID(objType string) {
	foundByUID.WithLabelValues(objType).Inc()
}

// recordProbeCreated increments the probe-created counter for endpoint.
func recordProbeCreated(endpoint string) {
	probeCreated.WithLabelValues(endpoint).Inc()
}

// recordProbeRefused increments the probe-refused counter for endpoint.
func recordProbeRefused(endpoint string) {
	probeRefused.WithLabelValues(endpoint).Inc()
}

// recordProbeRaceWon increments the probe-race-won counter for endpoint.
func recordProbeRaceWon(endpoint string) {
	probeRaceWon.WithLabelValues(endpoint).Inc()
}
