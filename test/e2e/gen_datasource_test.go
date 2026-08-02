// Package e2e contains tests for the E2E test infrastructure under
// test/e2e/ — currently just gen-datasource.sh, the script that derives
// per-run backend identities (a name token and a set of disjoint address
// sub-blocks) for isolating concurrent E2E runs against the shared NIOS
// Grid Manager. See gen-datasource.sh's own header comment for the full
// design rationale; these tests pin down its observable behavior: output
// format, determinism, and — most importantly — that every derived
// sub-block is mutually disjoint from every other one within a single
// run, which is the property the whole mechanism depends on.
package e2e

import (
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// scriptPath resolves gen-datasource.sh relative to this test file, so the
// test works regardless of the caller's working directory.
func scriptPath(t *testing.T) string {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("os.Getwd(): %v", err)
	}
	return filepath.Join(wd, "gen-datasource.sh")
}

// runGenDatasource invokes gen-datasource.sh with the given seed and
// returns the parsed key/value pairs from the generated datasource file.
func runGenDatasource(t *testing.T, seed string) map[string]string {
	t.Helper()
	out := filepath.Join(t.TempDir(), "datasource.yaml")
	cmd := exec.CommandContext(t.Context(), scriptPath(t), seed, out)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("gen-datasource.sh %q %q failed: %v\noutput:\n%s", seed, out, err, output)
	}
	raw, err := os.ReadFile(out)
	if err != nil {
		t.Fatalf("reading generated datasource file: %v", err)
	}
	return parseDatasource(t, string(raw))
}

// parseDatasource parses the flat `key: "value"` YAML gen-datasource.sh
// emits. It deliberately does not pull in a YAML library — the format is a
// fixed, simple subset the script fully controls.
func parseDatasource(t *testing.T, content string) map[string]string {
	t.Helper()
	values := map[string]string{}
	for _, line := range strings.Split(content, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		parts := strings.SplitN(line, ":", 2)
		if len(parts) != 2 {
			t.Fatalf("malformed datasource line %q", line)
		}
		key := strings.TrimSpace(parts[0])
		val := strings.TrimSpace(parts[1])
		val = strings.Trim(val, `"`)
		values[key] = val
	}
	return values
}

// Datasource key names referenced from more than one test — pulled into
// constants so goconst doesn't flag the repetition and so a rename only
// needs to happen in one place.
const (
	keyRunToken                     = "runToken"
	keyNetPrefix                    = "netPrefix"
	keyNetNetworkCluster            = "netNetworkCluster"
	keyNetNetworkNamespaced         = "netNetworkNamespaced"
	keyNetContainerCluster          = "netContainerCluster"
	keyNetContainerNamespaced       = "netContainerNamespaced"
	keyNetSharedMemberCluster       = "netSharedMemberCluster"
	keyNetSharedMemberNamespaced    = "netSharedMemberNamespaced"
	keyNetFixedAddrParentCluster    = "netFixedAddrParentCluster"
	keyNetFixedAddrHostCluster      = "netFixedAddrHostCluster"
	keyNetFixedAddrParentNamespaced = "netFixedAddrParentNamespaced"
	keyNetFixedAddrHostNamespaced   = "netFixedAddrHostNamespaced"
	keyNetHostRecordCluster         = "netHostRecordCluster"
	keyNetHostRecordNamespaced      = "netHostRecordNamespaced"
	keyNetRangeParentCluster        = "netRangeParentCluster"
	keyNetRangeStartCluster         = "netRangeStartCluster"
	keyNetRangeEndCluster           = "netRangeEndCluster"
	keyNetRangeParentNamespaced     = "netRangeParentNamespaced"
	keyNetRangeStartNamespaced      = "netRangeStartNamespaced"
	keyNetRangeEndNamespaced        = "netRangeEndNamespaced"
	keyNetAllocParentCluster        = "netAllocParentCluster"
	keyNetPtrHostCluster            = "netPtrHostCluster"
	keyNetPtrHostNamespaced         = "netPtrHostNamespaced"
	keyNetV6NetworkCluster          = "netV6NetworkCluster"
	keyNetV6NetworkNamespaced       = "netV6NetworkNamespaced"
)

// requiredKeys is every key gen-datasource.sh's sub-allocation map
// promises to emit — kept as a single list so a future consumer ticket
// that expects one of these can rely on this test to catch it disappearing.
var requiredKeys = []string{
	keyRunToken,
	keyNetPrefix,
	keyNetNetworkCluster,
	keyNetNetworkNamespaced,
	keyNetContainerCluster,
	keyNetContainerNamespaced,
	keyNetSharedMemberCluster,
	keyNetSharedMemberNamespaced,
	keyNetFixedAddrParentCluster,
	keyNetFixedAddrHostCluster,
	keyNetFixedAddrParentNamespaced,
	keyNetFixedAddrHostNamespaced,
	keyNetHostRecordCluster,
	keyNetHostRecordNamespaced,
	keyNetRangeParentCluster,
	keyNetRangeStartCluster,
	keyNetRangeEndCluster,
	keyNetRangeParentNamespaced,
	keyNetRangeStartNamespaced,
	keyNetRangeEndNamespaced,
	keyNetAllocParentCluster,
	keyNetPtrHostCluster,
	keyNetPtrHostNamespaced,
	keyNetV6NetworkCluster,
	keyNetV6NetworkNamespaced,
}

func TestGenDatasourceEmitsAllKeys(t *testing.T) {
	values := runGenDatasource(t, "TestGenDatasourceEmitsAllKeys-seed")
	for _, key := range requiredKeys {
		if _, ok := values[key]; !ok {
			t.Errorf("datasource missing key %q", key)
		}
	}
}

func TestGenDatasourceRunTokenFormat(t *testing.T) {
	values := runGenDatasource(t, "TestGenDatasourceRunTokenFormat-seed")
	re := regexp.MustCompile(`^[0-9a-f]{10}$`)
	if !re.MatchString(values[keyRunToken]) {
		t.Errorf("runToken %q does not match ^[0-9a-f]{10}$", values[keyRunToken])
	}
}

func TestGenDatasourceNetPrefixFormat(t *testing.T) {
	values := runGenDatasource(t, "TestGenDatasourceNetPrefixFormat-seed")
	_, ipNet, err := net.ParseCIDR(values[keyNetPrefix])
	if err != nil {
		t.Fatalf("netPrefix %q is not a valid CIDR: %v", values[keyNetPrefix], err)
	}
	ones, bits := ipNet.Mask.Size()
	if ones != 24 || bits != 32 {
		t.Errorf("netPrefix %q is not a /24: mask is /%d", values[keyNetPrefix], ones)
	}
	if !strings.HasPrefix(ipNet.IP.String(), "100.64.") {
		t.Errorf("netPrefix %q is not carved from 100.64.0.0/16", values[keyNetPrefix])
	}
}

func TestGenDatasourceDeterministic(t *testing.T) {
	seed := "TestGenDatasourceDeterministic-seed"
	first := runGenDatasource(t, seed)
	second := runGenDatasource(t, seed)
	for _, key := range requiredKeys {
		if first[key] != second[key] {
			t.Errorf("key %q not deterministic across runs with the same seed: %q vs %q", key, first[key], second[key])
		}
	}
}

func TestGenDatasourceDifferentSeedsDifferOnRunToken(t *testing.T) {
	a := runGenDatasource(t, "seed-a-TestGenDatasourceDifferentSeeds")
	b := runGenDatasource(t, "seed-b-TestGenDatasourceDifferentSeeds")
	if a[keyRunToken] == b[keyRunToken] {
		t.Errorf("two different seeds produced the same runToken %q — sha256 collision or a broken derivation", a[keyRunToken])
	}
}

func TestGenDatasourceUsageError(t *testing.T) {
	cmd := exec.CommandContext(t.Context(), scriptPath(t))
	if err := cmd.Run(); err == nil {
		t.Fatal("gen-datasource.sh with no arguments should fail, but exited 0")
	}
}

// TestGenDatasourceSubBlocksShareBlockIndex asserts every netXxx sub-block
// key is carved from the SAME third octet (BLOCK_INDEX) as netPrefix — the
// whole point of sub-blocking is that every consumer draws from the one
// block reserved for this run, not a second independent draw.
func TestGenDatasourceSubBlocksShareBlockIndex(t *testing.T) {
	values := runGenDatasource(t, "TestGenDatasourceSubBlocksShareBlockIndex-seed")
	_, prefixNet, err := net.ParseCIDR(values[keyNetPrefix])
	if err != nil {
		t.Fatalf("netPrefix %q invalid: %v", values[keyNetPrefix], err)
	}
	blockIndex := prefixNet.IP.To4()[2]

	cidrKeys := []string{
		keyNetNetworkCluster, keyNetNetworkNamespaced,
		keyNetContainerCluster, keyNetContainerNamespaced,
		keyNetSharedMemberCluster, keyNetSharedMemberNamespaced,
		keyNetFixedAddrParentCluster, keyNetFixedAddrParentNamespaced,
		keyNetRangeParentCluster, keyNetRangeParentNamespaced,
		keyNetAllocParentCluster,
	}
	for _, key := range cidrKeys {
		ip, _, err := net.ParseCIDR(values[key])
		if err != nil {
			t.Errorf("%s=%q is not a valid CIDR: %v", key, values[key], err)
			continue
		}
		if got := ip.To4()[2]; got != blockIndex {
			t.Errorf("%s=%q has third octet %d, want %d (netPrefix's block index)", key, values[key], got, blockIndex)
		}
	}

	hostKeys := []string{
		keyNetFixedAddrHostCluster, keyNetFixedAddrHostNamespaced,
		keyNetHostRecordCluster, keyNetHostRecordNamespaced,
		keyNetRangeStartCluster, keyNetRangeEndCluster,
		keyNetRangeStartNamespaced, keyNetRangeEndNamespaced,
	}
	for _, key := range hostKeys {
		ip := net.ParseIP(values[key])
		if ip == nil {
			t.Errorf("%s=%q is not a valid IP", key, values[key])
			continue
		}
		if got := ip.To4()[2]; got != blockIndex {
			t.Errorf("%s=%q has third octet %d, want %d (netPrefix's block index)", key, values[key], got, blockIndex)
		}
	}
}

// TestGenDatasourceSubBlocksAreDisjoint is the load-bearing test: `make
// e2e` (CORE) applies every addressed family in ONE run with ONE
// netPrefix, and WAPI rejects overlapping Network objects, so every
// (resource, scope) sub-block MUST occupy non-overlapping address space
// within that single /24. This walks every CIDR-shaped and host-shaped
// consumer and asserts pairwise disjointness by expanding each into its
// concrete address set and checking for intersection.
func TestGenDatasourceSubBlocksAreDisjoint(t *testing.T) {
	values := runGenDatasource(t, "TestGenDatasourceSubBlocksAreDisjoint-seed")

	type span struct {
		name string
		ips  map[string]bool
	}

	cidrKeys := []string{
		keyNetNetworkCluster, keyNetNetworkNamespaced,
		keyNetContainerCluster, keyNetContainerNamespaced,
		keyNetSharedMemberCluster, keyNetSharedMemberNamespaced,
		keyNetFixedAddrParentCluster, keyNetFixedAddrParentNamespaced,
		keyNetRangeParentCluster, keyNetRangeParentNamespaced,
		keyNetAllocParentCluster,
	}
	// Range is a contiguous [start, end] address span, not a CIDR.
	rangeSpans := [][2]string{
		{keyNetRangeStartCluster, keyNetRangeEndCluster},
		{keyNetRangeStartNamespaced, keyNetRangeEndNamespaced},
	}
	// Single-host consumers.
	hostKeys := []string{keyNetFixedAddrHostCluster, keyNetFixedAddrHostNamespaced, keyNetHostRecordCluster, keyNetHostRecordNamespaced}

	spans := make([]span, 0, len(cidrKeys)+len(rangeSpans)+len(hostKeys))
	for _, key := range cidrKeys {
		ips, err := expandCIDR(values[key])
		if err != nil {
			t.Fatalf("%s=%q: %v", key, values[key], err)
		}
		spans = append(spans, span{name: key, ips: ips})
	}

	for _, rs := range rangeSpans {
		ips, err := expandRange(values[rs[0]], values[rs[1]])
		if err != nil {
			t.Fatalf("range %s..%s: %v", rs[0], rs[1], err)
		}
		if len(ips) != 21 {
			t.Errorf("range %s..%s spans %d addresses, want 21 (matching range.yaml's existing 21-address example span)", rs[0], rs[1], len(ips))
		}
		spans = append(spans, span{name: rs[0] + ".." + rs[1], ips: ips})
	}

	for _, key := range hostKeys {
		ip := net.ParseIP(values[key])
		if ip == nil {
			t.Fatalf("%s=%q is not a valid IP", key, values[key])
		}
		spans = append(spans, span{name: key, ips: map[string]bool{ip.String(): true}})
	}

	// FixedAddress's host and Range's [start,end] span are DELIBERATELY
	// nested inside their own parent blocks (see the assertions below) —
	// those relationships are excluded from the blanket pairwise-disjoint
	// sweep, everything else must not overlap with anything else.
	allowedOverlap := map[[2]string]bool{
		{keyNetFixedAddrParentCluster, keyNetFixedAddrHostCluster}:                                  true,
		{keyNetFixedAddrParentNamespaced, keyNetFixedAddrHostNamespaced}:                            true,
		{keyNetRangeParentCluster, keyNetRangeStartCluster + ".." + keyNetRangeEndCluster}:          true,
		{keyNetRangeParentNamespaced, keyNetRangeStartNamespaced + ".." + keyNetRangeEndNamespaced}: true,
	}
	for i := range spans {
		for j := i + 1; j < len(spans); j++ {
			if allowedOverlap[[2]string{spans[i].name, spans[j].name}] || allowedOverlap[[2]string{spans[j].name, spans[i].name}] {
				continue
			}
			for ip := range spans[i].ips {
				if spans[j].ips[ip] {
					t.Errorf("%s and %s overlap at %s — sub-allocation map is broken", spans[i].name, spans[j].name, ip)
				}
			}
		}
	}

	// FixedAddress's host MUST fall inside its own parent block — that is
	// the whole point of nesting it there (a per-run parent Network
	// covering the per-run allocated address).
	assertHostInsideCIDR(t, values, keyNetFixedAddrHostCluster, keyNetFixedAddrParentCluster)
	assertHostInsideCIDR(t, values, keyNetFixedAddrHostNamespaced, keyNetFixedAddrParentNamespaced)

	// Range's [start,end] span MUST fall entirely inside its own parent
	// block — CreateNetworkRange requires an existing parent Network
	// covering the address span (live-verified on the Grid: WAPI 400
	// IBDataConflictError "Cannot find the parent network for the DHCP
	// range ..." without one).
	assertHostInsideCIDR(t, values, keyNetRangeStartCluster, keyNetRangeParentCluster)
	assertHostInsideCIDR(t, values, keyNetRangeEndCluster, keyNetRangeParentCluster)
	assertHostInsideCIDR(t, values, keyNetRangeStartNamespaced, keyNetRangeParentNamespaced)
	assertHostInsideCIDR(t, values, keyNetRangeEndNamespaced, keyNetRangeParentNamespaced)
}

func assertHostInsideCIDR(t *testing.T, values map[string]string, hostKey, cidrKey string) {
	t.Helper()
	ip := net.ParseIP(values[hostKey])
	if ip == nil {
		t.Fatalf("%s=%q is not a valid IP", hostKey, values[hostKey])
	}
	_, ipNet, err := net.ParseCIDR(values[cidrKey])
	if err != nil {
		t.Fatalf("%s=%q is not a valid CIDR: %v", cidrKey, values[cidrKey], err)
	}
	if !ipNet.Contains(ip) {
		t.Errorf("%s=%s is not inside %s=%s", hostKey, ip, cidrKey, ipNet)
	}
}

// TestGenDatasourcePtrHostWithinReverseZone asserts record-ptr's
// documented exception: its host offsets are drawn from the pre-existing
// shared 10.1.1.0/24 reverse zone (NOT netPrefix, which has no reverse
// zone), split into two fixed, non-overlapping halves so the cluster and
// namespaced examples of the same run never collide with each other.
func TestGenDatasourcePtrHostWithinReverseZone(t *testing.T) {
	values := runGenDatasource(t, "TestGenDatasourcePtrHostWithinReverseZone-seed")
	_, reverseZone, err := net.ParseCIDR("10.1.1.0/24")
	if err != nil {
		t.Fatalf("net.ParseCIDR(10.1.1.0/24): %v", err)
	}

	cluster := net.ParseIP(values[keyNetPtrHostCluster])
	namespaced := net.ParseIP(values[keyNetPtrHostNamespaced])
	if cluster == nil || namespaced == nil {
		t.Fatalf("netPtrHostCluster=%q netPtrHostNamespaced=%q must both be valid IPs", values[keyNetPtrHostCluster], values[keyNetPtrHostNamespaced])
	}
	if !reverseZone.Contains(cluster) {
		t.Errorf("netPtrHostCluster=%s is not inside the 10.1.1.0/24 reverse zone", cluster)
	}
	if !reverseZone.Contains(namespaced) {
		t.Errorf("netPtrHostNamespaced=%s is not inside the 10.1.1.0/24 reverse zone", namespaced)
	}
	if cluster.Equal(namespaced) {
		t.Errorf("netPtrHostCluster and netPtrHostNamespaced must never be equal: both are %s", cluster)
	}

	clusterLast := cluster.To4()[3]
	namespacedLast := namespaced.To4()[3]
	if clusterLast < 2 || clusterLast > 127 {
		t.Errorf("netPtrHostCluster last octet %d out of documented range [2,127]", clusterLast)
	}
	if namespacedLast < 129 || namespacedLast > 254 {
		t.Errorf("netPtrHostNamespaced last octet %d out of documented range [129,254]", namespacedLast)
	}
}

// blockIndexFromDatasource re-derives the run's BLOCK_INDEX byte from
// netPrefix's third octet — the same value both the IPv4 sub-blocks and
// the IPv6 network keys are supposed to share.
func blockIndexFromDatasource(t *testing.T, values map[string]string) byte {
	t.Helper()
	_, prefixNet, err := net.ParseCIDR(values[keyNetPrefix])
	if err != nil {
		t.Fatalf("netPrefix %q invalid: %v", values[keyNetPrefix], err)
	}
	return prefixNet.IP.To4()[2]
}

// TestGenDatasourceV6NetworkFormat asserts netV6NetworkCluster and
// netV6NetworkNamespaced are both valid /64 CIDRs carved from the RFC 3849
// IPv6 documentation prefix (2001:db8::/32) — the IPv6 analogue of
// TestGenDatasourceNetPrefixFormat.
func TestGenDatasourceV6NetworkFormat(t *testing.T) {
	values := runGenDatasource(t, "TestGenDatasourceV6NetworkFormat-seed")
	_, docPrefix, err := net.ParseCIDR("2001:db8::/32")
	if err != nil {
		t.Fatalf("net.ParseCIDR(2001:db8::/32): %v", err)
	}

	for _, key := range []string{keyNetV6NetworkCluster, keyNetV6NetworkNamespaced} {
		ip, ipNet, err := net.ParseCIDR(values[key])
		if err != nil {
			t.Fatalf("%s=%q is not a valid CIDR: %v", key, values[key], err)
		}
		ones, bits := ipNet.Mask.Size()
		if ones != 64 || bits != 128 {
			t.Errorf("%s=%q is not a /64: mask is /%d (bits=%d)", key, values[key], ones, bits)
		}
		if !docPrefix.Contains(ip) {
			t.Errorf("%s=%q is not carved from 2001:db8::/32", key, values[key])
		}
	}
}

// TestGenDatasourceV6NetworkSharesBlockIndex asserts the IPv6 network
// keys are derived from the SAME BLOCK_INDEX byte as netPrefix (rendered
// as 2-digit lowercase hex in the third hextet) — the whole point of
// reusing the draw instead of hashing a second byte.
func TestGenDatasourceV6NetworkSharesBlockIndex(t *testing.T) {
	values := runGenDatasource(t, "TestGenDatasourceV6NetworkSharesBlockIndex-seed")
	blockIndex := blockIndexFromDatasource(t, values)
	wantHex := fmt.Sprintf("%02x", blockIndex)

	for _, key := range []string{keyNetV6NetworkCluster, keyNetV6NetworkNamespaced} {
		ip, _, err := net.ParseCIDR(values[key])
		if err != nil {
			t.Fatalf("%s=%q invalid: %v", key, values[key], err)
		}
		// ip.String() renders the canonical, zero-compressed form
		// (e.g. "2001:db8:7:1::"); split on ":" to pull out the third
		// hextet directly rather than assuming a fixed string offset.
		parts := strings.Split(ip.String(), ":")
		if len(parts) < 3 {
			t.Fatalf("%s=%q did not parse into enough hextets: %v", key, values[key], parts)
		}
		if got := parts[2]; got != wantHex {
			t.Errorf("%s=%q has third hextet %q, want %q (netPrefix's BLOCK_INDEX in hex)", key, values[key], got, wantHex)
		}
	}
}

// TestGenDatasourceV6NetworkDisjoint asserts the cluster and namespaced
// IPv6 network blocks never overlap each other, and neither ever lands in
// the 2001:db8::/64 zero subnet record-aaaa's example payload addresses
// already occupy (2001:db8::10, ::11, ::20, ::21).
func TestGenDatasourceV6NetworkDisjoint(t *testing.T) {
	for _, seed := range []string{"a", "b", "c", "some-longer-seed-value", "e2e-abc123", "TestGenDatasourceV6NetworkDisjoint-seed"} {
		values := runGenDatasource(t, seed)

		_, cluster, err := net.ParseCIDR(values[keyNetV6NetworkCluster])
		if err != nil {
			t.Fatalf("seed %q: %s=%q invalid: %v", seed, keyNetV6NetworkCluster, values[keyNetV6NetworkCluster], err)
		}
		_, namespaced, err := net.ParseCIDR(values[keyNetV6NetworkNamespaced])
		if err != nil {
			t.Fatalf("seed %q: %s=%q invalid: %v", seed, keyNetV6NetworkNamespaced, values[keyNetV6NetworkNamespaced], err)
		}
		if cluster.String() == namespaced.String() {
			t.Errorf("seed %q: netV6NetworkCluster and netV6NetworkNamespaced must never be equal: both are %s", seed, cluster)
		}
		if cluster.Contains(namespaced.IP) || namespaced.Contains(cluster.IP) {
			t.Errorf("seed %q: netV6NetworkCluster=%s and netV6NetworkNamespaced=%s overlap", seed, cluster, namespaced)
		}

		_, zeroSubnet, err := net.ParseCIDR("2001:db8::/64")
		if err != nil {
			t.Fatalf("net.ParseCIDR(2001:db8::/64): %v", err)
		}
		if zeroSubnet.Contains(cluster.IP) {
			t.Errorf("seed %q: netV6NetworkCluster=%s lands in the 2001:db8::/64 zero subnet record-aaaa's examples already occupy", seed, cluster)
		}
		if zeroSubnet.Contains(namespaced.IP) {
			t.Errorf("seed %q: netV6NetworkNamespaced=%s lands in the 2001:db8::/64 zero subnet record-aaaa's examples already occupy", seed, namespaced)
		}
	}
}

// expandCIDR returns the set of usable host addresses inside a CIDR
// (excluding network and broadcast addresses for anything wider than a
// /31), as a set keyed by dotted-decimal string.
func expandCIDR(cidr string) (map[string]bool, error) {
	ip, ipNet, err := net.ParseCIDR(cidr)
	if err != nil {
		return nil, fmt.Errorf("invalid CIDR %q: %w", cidr, err)
	}
	ones, bits := ipNet.Mask.Size()
	if bits != 32 {
		return nil, fmt.Errorf("only IPv4 is supported, got %q", cidr)
	}
	ip = ip.Mask(ipNet.Mask).To4()
	size := 1 << uint(32-ones)
	result := make(map[string]bool, size)
	base := toUint32(ip)
	for i := 0; i < size; i++ {
		result[fromUint32(base+uint32(i)).String()] = true
	}
	return result, nil
}

// expandRange returns the inclusive set of addresses between start and end
// (both IPv4, start <= end, same /24 assumed since these are all within a
// single run's block).
func expandRange(start, end string) (map[string]bool, error) {
	s := net.ParseIP(start).To4()
	e := net.ParseIP(end).To4()
	if s == nil || e == nil {
		return nil, fmt.Errorf("invalid range %q..%q", start, end)
	}
	sv, ev := toUint32(s), toUint32(e)
	if sv > ev {
		return nil, fmt.Errorf("range start %q is after end %q", start, end)
	}
	result := make(map[string]bool, ev-sv+1)
	for v := sv; v <= ev; v++ {
		result[fromUint32(v).String()] = true
	}
	return result, nil
}

func toUint32(ip net.IP) uint32 {
	ip4 := ip.To4()
	return uint32(ip4[0])<<24 | uint32(ip4[1])<<16 | uint32(ip4[2])<<8 | uint32(ip4[3])
}

func fromUint32(v uint32) net.IP {
	return net.IPv4(byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
}

// TestGenDatasourceFixedAddressHostNotNetworkOrBroadcast guards against an
// off-by-one in the chosen host offsets: FixedAddress's host (and, by the
// same reasoning, Range's start/end) must be real usable addresses inside
// their parent CIDR, not the network or broadcast address of that block.
func TestGenDatasourceFixedAddressHostNotNetworkOrBroadcast(t *testing.T) {
	values := runGenDatasource(t, "TestGenDatasourceFixedAddressHostNotNetworkOrBroadcast-seed")
	for _, pair := range [][2]string{
		{keyNetFixedAddrHostCluster, keyNetFixedAddrParentCluster},
		{keyNetFixedAddrHostNamespaced, keyNetFixedAddrParentNamespaced},
		{keyNetRangeStartCluster, keyNetRangeParentCluster},
		{keyNetRangeEndCluster, keyNetRangeParentCluster},
		{keyNetRangeStartNamespaced, keyNetRangeParentNamespaced},
		{keyNetRangeEndNamespaced, keyNetRangeParentNamespaced},
	} {
		host := net.ParseIP(values[pair[0]])
		_, cidr, err := net.ParseCIDR(values[pair[1]])
		if err != nil {
			t.Fatalf("%s=%q invalid: %v", pair[1], values[pair[1]], err)
		}
		network := cidr.IP.Mask(cidr.Mask)
		ones, bits := cidr.Mask.Size()
		broadcastVal := toUint32(network.To4()) | (1<<uint(bits-ones) - 1)
		broadcast := fromUint32(broadcastVal)
		if host.Equal(network) {
			t.Errorf("%s=%s equals the network address of %s", pair[0], host, values[pair[1]])
		}
		if host.Equal(broadcast) {
			t.Errorf("%s=%s equals the broadcast address of %s", pair[0], host, values[pair[1]])
		}
	}
}

// TestGenDatasourceMainProviderFileParses is a light sanity check that this
// test file's own parseDatasource helper agrees with strconv-friendly
// expectations for the header comment line format gen-datasource.sh emits,
// guarding against a future edit to the script silently changing the
// output shape in a way none of the other tests would catch (e.g. dropping
// the quotes, which downstream YAML consumers rely on for string typing).
func TestGenDatasourceMainProviderFileParses(t *testing.T) {
	seed := "TestGenDatasourceMainProviderFileParses-seed"
	out := filepath.Join(t.TempDir(), "datasource.yaml")
	cmd := exec.CommandContext(t.Context(), scriptPath(t), seed, out)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("gen-datasource.sh failed: %v\n%s", err, output)
	}
	raw, err := os.ReadFile(out)
	if err != nil {
		t.Fatalf("reading generated datasource file: %v", err)
	}
	content := string(raw)
	if !strings.Contains(content, "# Generated by test/e2e/gen-datasource.sh") {
		t.Errorf("missing generated-file header comment")
	}
	if !strings.Contains(content, fmt.Sprintf("# seed: %s", seed)) {
		t.Errorf("missing seed comment line for seed %q", seed)
	}
	for _, key := range requiredKeys {
		if !strings.Contains(content, key+`: "`) {
			t.Errorf("key %q not emitted in quoted key: \"value\" form", key)
		}
	}
}

// TestGenDatasourceBlockIndexRange asserts BLOCK_INDEX (netPrefix's third
// octet) is always a valid byte value — guards the ceiling arithmetic
// documented in gen-datasource.sh and ADR-IN-0007 (256 possible blocks).
func TestGenDatasourceBlockIndexRange(t *testing.T) {
	for _, seed := range []string{"a", "b", "c", "some-longer-seed-value", "e2e-abc123"} {
		values := runGenDatasource(t, seed)
		_, ipNet, err := net.ParseCIDR(values[keyNetPrefix])
		if err != nil {
			t.Fatalf("seed %q: netPrefix %q invalid: %v", seed, values[keyNetPrefix], err)
		}
		// A Go byte can never be out of [0,255] by construction; what this
		// actually guards is the STRING form gen-datasource.sh emits — a
		// stringification bug (e.g. a signed/negative render) would show up
		// here as a parse failure or an out-of-range parsed value.
		parts := strings.Split(ipNet.IP.String(), ".")
		if len(parts) != 4 {
			t.Fatalf("seed %q: unexpected IP format %q", seed, ipNet.IP.String())
		}
		n, err := strconv.Atoi(parts[2])
		if err != nil || n < 0 || n > 255 {
			t.Errorf("seed %q: third octet %q not a valid byte", seed, parts[2])
		}
	}
}
