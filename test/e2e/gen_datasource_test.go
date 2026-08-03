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
	"sort"
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
	keyNetV6ContainerCluster        = "netV6ContainerCluster"
	keyNetV6ContainerNamespaced     = "netV6ContainerNamespaced"
	keyNetV6FixedAddrParent         = "netV6FixedAddrParent"
	keyNetV6FixedAddrHostCluster    = "netV6FixedAddrHostCluster"
	keyNetV6FixedAddrHostNamespaced = "netV6FixedAddrHostNamespaced"
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
	keyNetV6ContainerCluster,
	keyNetV6ContainerNamespaced,
	keyNetV6FixedAddrParent,
	keyNetV6FixedAddrHostCluster,
	keyNetV6FixedAddrHostNamespaced,
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

// TestGenDatasourceScriptIsExecutable guards against the owner-execute bit
// silently getting stripped from gen-datasource.sh — e.g. by an editor or a
// git operation that rewrites the index entry as 100644 instead of 100755.
// Every other test in this file already fails loudly when that happens
// (exec.CommandContext returns "permission denied"), but those failures are
// easy to misdiagnose as a script bug. This test names the actual cause
// directly so the fix is obvious: chmod +x and `git update-index
// --chmod=+x`.
func TestGenDatasourceScriptIsExecutable(t *testing.T) {
	info, err := os.Stat(scriptPath(t))
	if err != nil {
		t.Fatalf("stat gen-datasource.sh: %v", err)
	}
	if info.Mode()&0o111 == 0 {
		t.Fatalf("gen-datasource.sh is not executable (mode %s) — run: chmod +x test/e2e/gen-datasource.sh && git update-index --chmod=+x test/e2e/gen-datasource.sh", info.Mode())
	}
}

// TestReapScriptIsExecutable is TestGenDatasourceScriptIsExecutable's
// counterpart for reap.sh — every test in this file that shells out to it
// (TestReapScopeCoversDatasourceV6Keys) fails just as opaquely if the
// exec bit is lost.
func TestReapScriptIsExecutable(t *testing.T) {
	info, err := os.Stat(reapScriptPath(t))
	if err != nil {
		t.Fatalf("stat reap.sh: %v", err)
	}
	if info.Mode()&0o111 == 0 {
		t.Fatalf("reap.sh is not executable (mode %s) — run: chmod +x test/e2e/reap.sh && git update-index --chmod=+x test/e2e/reap.sh", info.Mode())
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
// as UNPADDED lowercase hex in the third hextet — WAPI stores IPv6
// addresses in RFC 5952 canonical form, which strips a hextet's leading
// zero, so ip.String() below never has one either) — the whole point of
// reusing the draw instead of hashing a second byte.
func TestGenDatasourceV6NetworkSharesBlockIndex(t *testing.T) {
	values := runGenDatasource(t, "TestGenDatasourceV6NetworkSharesBlockIndex-seed")
	blockIndex := blockIndexFromDatasource(t, values)
	wantHex := fmt.Sprintf("%x", blockIndex)

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

// TestGenDatasourceV6ContainerFixedAddrFormat asserts
// netV6ContainerCluster, netV6ContainerNamespaced, and
// netV6FixedAddrParent are all valid /64 CIDRs carved from the RFC 3849
// IPv6 documentation prefix (2001:db8::/32), and
// netV6FixedAddrHostCluster/Namespaced are valid IPv6 addresses within it
// — the same shape assertions TestGenDatasourceV6NetworkFormat makes for
// Network's own IPv6 keys, extended to the NetworkContainer and
// FixedAddress dual-object-type gates.
func TestGenDatasourceV6ContainerFixedAddrFormat(t *testing.T) {
	values := runGenDatasource(t, "TestGenDatasourceV6ContainerFixedAddrFormat-seed")
	_, docPrefix, err := net.ParseCIDR("2001:db8::/32")
	if err != nil {
		t.Fatalf("net.ParseCIDR(2001:db8::/32): %v", err)
	}

	for _, key := range []string{keyNetV6ContainerCluster, keyNetV6ContainerNamespaced, keyNetV6FixedAddrParent} {
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

	for _, key := range []string{keyNetV6FixedAddrHostCluster, keyNetV6FixedAddrHostNamespaced} {
		ip := net.ParseIP(values[key])
		if ip == nil {
			t.Fatalf("%s=%q is not a valid IP", key, values[key])
		}
		if !docPrefix.Contains(ip) {
			t.Errorf("%s=%q is not carved from 2001:db8::/32", key, values[key])
		}
	}
}

// TestGenDatasourceV6ContainerFixedAddrSharesBlockIndex asserts
// netV6ContainerCluster, netV6ContainerNamespaced, netV6FixedAddrParent,
// and the two netV6FixedAddrHost* keys all carry the SAME BLOCK_INDEX
// byte (rendered as UNPADDED lowercase hex, matching WAPI's RFC 5952
// canonical storage form) in their third hextet as netPrefix and
// netV6NetworkCluster/Namespaced — the whole point of reusing netV6Hex
// instead of drawing a fresh hash byte per resource.
func TestGenDatasourceV6ContainerFixedAddrSharesBlockIndex(t *testing.T) {
	values := runGenDatasource(t, "TestGenDatasourceV6ContainerFixedAddrSharesBlockIndex-seed")
	blockIndex := blockIndexFromDatasource(t, values)
	wantHex := fmt.Sprintf("%x", blockIndex)

	thirdHextet := func(key string) string {
		t.Helper()
		ip := net.ParseIP(values[key])
		if ip == nil {
			// Try as a CIDR — netV6ContainerCluster/Namespaced and
			// netV6FixedAddrParent are CIDRs, not bare IPs.
			var err error
			ip, _, err = net.ParseCIDR(values[key])
			if err != nil {
				t.Fatalf("%s=%q is neither a valid IP nor CIDR: %v", key, values[key], err)
			}
		}
		parts := strings.Split(ip.String(), ":")
		if len(parts) < 3 {
			t.Fatalf("%s=%q did not parse into enough hextets: %v", key, values[key], parts)
		}
		return parts[2]
	}

	for _, key := range []string{
		keyNetV6ContainerCluster, keyNetV6ContainerNamespaced,
		keyNetV6FixedAddrParent,
		keyNetV6FixedAddrHostCluster, keyNetV6FixedAddrHostNamespaced,
	} {
		if got := thirdHextet(key); got != wantHex {
			t.Errorf("%s=%q has third hextet %q, want %q (netPrefix's BLOCK_INDEX in hex)", key, values[key], got, wantHex)
		}
	}
}

// TestGenDatasourceV6SubBlocksAreDisjoint is the IPv6 analogue of
// TestGenDatasourceSubBlocksAreDisjoint: every /64 the script hands out
// (Network cluster/namespaced, NetworkContainer cluster/namespaced,
// FixedAddress's shared parent) must be mutually disjoint, the
// FixedAddress host offsets must nest inside their own shared parent
// (the one deliberate overlap), and none of the fourth hextets (1-5) may
// ever be 0 — the zero subnet record-aaaa's payload addresses occupy.
func TestGenDatasourceV6SubBlocksAreDisjoint(t *testing.T) {
	values := runGenDatasource(t, "TestGenDatasourceV6SubBlocksAreDisjoint-seed")

	blockKeys := []string{
		keyNetV6NetworkCluster, keyNetV6NetworkNamespaced,
		keyNetV6ContainerCluster, keyNetV6ContainerNamespaced,
		keyNetV6FixedAddrParent,
	}
	blocks := make(map[string]*net.IPNet, len(blockKeys))
	for _, key := range blockKeys {
		_, ipNet, err := net.ParseCIDR(values[key])
		if err != nil {
			t.Fatalf("%s=%q invalid: %v", key, values[key], err)
		}
		blocks[key] = ipNet
	}

	for i, ki := range blockKeys {
		for j := i + 1; j < len(blockKeys); j++ {
			kj := blockKeys[j]
			bi, bj := blocks[ki], blocks[kj]
			if bi.String() == bj.String() {
				t.Errorf("%s and %s must never be equal: both are %s", ki, kj, bi)
				continue
			}
			if bi.Contains(bj.IP) || bj.Contains(bi.IP) {
				t.Errorf("%s=%s and %s=%s overlap — sub-allocation map is broken", ki, bi, kj, bj)
			}
		}
	}

	// The fixed-address host offsets are deliberately nested inside the
	// shared parent block, not disjoint from it.
	for _, hostKey := range []string{keyNetV6FixedAddrHostCluster, keyNetV6FixedAddrHostNamespaced} {
		host := net.ParseIP(values[hostKey])
		if host == nil {
			t.Fatalf("%s=%q is not a valid IP", hostKey, values[hostKey])
		}
		if !blocks[keyNetV6FixedAddrParent].Contains(host) {
			t.Errorf("%s=%s is not inside %s=%s", hostKey, host, keyNetV6FixedAddrParent, blocks[keyNetV6FixedAddrParent])
		}
	}
	hostCluster := net.ParseIP(values[keyNetV6FixedAddrHostCluster])
	hostNamespaced := net.ParseIP(values[keyNetV6FixedAddrHostNamespaced])
	if hostCluster != nil && hostNamespaced != nil && hostCluster.Equal(hostNamespaced) {
		t.Errorf("netV6FixedAddrHostCluster and netV6FixedAddrHostNamespaced must never be equal: both are %s", hostCluster)
	}

	_, zeroSubnet, err := net.ParseCIDR("2001:db8::/64")
	if err != nil {
		t.Fatalf("net.ParseCIDR(2001:db8::/64): %v", err)
	}
	for _, key := range blockKeys {
		if zeroSubnet.Contains(blocks[key].IP) {
			t.Errorf("%s=%s lands in the 2001:db8::/64 zero subnet record-aaaa's examples already occupy", key, blocks[key])
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

// reapScriptPath resolves reap.sh relative to this test file, the same
// way scriptPath resolves gen-datasource.sh.
func reapScriptPath(t *testing.T) string {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("os.Getwd(): %v", err)
	}
	return filepath.Join(wd, "reap.sh")
}

// reapScopeEntry is one line of `reap.sh --print-scope` output: the WAPI
// object type, the field it searches, and the compiled regex it matches
// against that field's canonical value.
type reapScopeEntry struct {
	otype string
	field string
	regex *regexp.Regexp
}

// envWithoutInfobloxCreds strips INFOBLOX_HOST/USER/PASS from the current
// environment, so reapPrintScope actually proves --print-scope needs none
// rather than merely happening to run in an environment that lacks them.
func envWithoutInfobloxCreds(t *testing.T) []string {
	t.Helper()
	var filtered []string
	for _, kv := range os.Environ() {
		switch {
		case strings.HasPrefix(kv, "INFOBLOX_HOST="),
			strings.HasPrefix(kv, "INFOBLOX_USER="),
			strings.HasPrefix(kv, "INFOBLOX_PASS="):
			continue
		default:
			filtered = append(filtered, kv)
		}
	}
	return filtered
}

// reapPrintScope invokes `reap.sh --net-prefix=<netPrefix> --print-scope`
// with no INFOBLOX_* credentials in its environment and parses the
// object-type/field/regex triples it reports.
func reapPrintScope(t *testing.T, netPrefix string) []reapScopeEntry {
	t.Helper()
	cmd := exec.CommandContext(t.Context(), reapScriptPath(t), "--net-prefix="+netPrefix, "--print-scope")
	cmd.Env = envWithoutInfobloxCreds(t)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("reap.sh --net-prefix=%s --print-scope failed: %v\noutput:\n%s", netPrefix, err, output)
	}
	trimmed := strings.TrimRight(string(output), "\n")
	if trimmed == "" {
		return nil
	}
	var entries []reapScopeEntry
	for _, line := range strings.Split(trimmed, "\n") {
		fields := strings.SplitN(line, " ", 3)
		if len(fields) != 3 {
			t.Fatalf("reap.sh --print-scope emitted an unparseable line: %q", line)
		}
		re, err := regexp.Compile(fields[2])
		if err != nil {
			t.Fatalf("reap.sh --print-scope emitted an invalid regex %q: %v", fields[2], err)
		}
		entries = append(entries, reapScopeEntry{otype: fields[0], field: fields[1], regex: re})
	}
	return entries
}

// canonicalIPv6String renders value (a bare IPv6 address or a CIDR) in the
// same canonical, zero-compressed form WAPI stores it in (RFC 5952) — the
// form reap.sh's PREFIX_V6_REGEX is built to match and reapPrintScope's
// regexes are matched against below.
func canonicalIPv6String(t *testing.T, value string) string {
	t.Helper()
	if ip := net.ParseIP(value); ip != nil {
		return ip.String()
	}
	ip, _, err := net.ParseCIDR(value)
	if err != nil {
		t.Fatalf("%q is neither a valid IPv6 address nor CIDR", value)
	}
	return ip.String()
}

// netV6KeyExpectedOType maps each netV6* key gen-datasource.sh emits to the
// WAPI object type reap.sh's IPv6 sweep must cover it as. Matching the
// regex alone is not sufficient: reap.sh's PREFIX_V6_REGEX is the exact
// same regex string for every IPv6 object-type row in IPV6_ADDRESSED_TYPES,
// so ANY single scope entry vacuously "covers" every other type's address
// too — a sweep narrowed to one object type (dropping the other two
// entirely) still passes a check that only compares regexes. Requiring the
// otype to match as well is what makes object-type coverage drift visible.
var netV6KeyExpectedOType = map[string]string{
	keyNetV6NetworkCluster:          "ipv6network",
	keyNetV6NetworkNamespaced:       "ipv6network",
	keyNetV6ContainerCluster:        "ipv6networkcontainer",
	keyNetV6ContainerNamespaced:     "ipv6networkcontainer",
	keyNetV6FixedAddrParent:         "ipv6network",
	keyNetV6FixedAddrHostCluster:    "ipv6fixedaddress",
	keyNetV6FixedAddrHostNamespaced: "ipv6fixedaddress",
}

// netV6KeyExpectedField maps each netV6* key to the WAPI field the scope
// entry for its object type must search. ipv6network and
// ipv6networkcontainer objects are keyed on their "network" field;
// ipv6fixedaddress objects are keyed on "ipv6addr". An entry with the
// right otype but the wrong field would still never find the object it
// claims to cover.
var netV6KeyExpectedField = map[string]string{
	keyNetV6NetworkCluster:          "network",
	keyNetV6NetworkNamespaced:       "network",
	keyNetV6ContainerCluster:        "network",
	keyNetV6ContainerNamespaced:     "network",
	keyNetV6FixedAddrParent:         "network",
	keyNetV6FixedAddrHostCluster:    "ipv6addr",
	keyNetV6FixedAddrHostNamespaced: "ipv6addr",
}

// knownV6Keys is the floor list of netV6* keys gen-datasource.sh is known
// to emit today. datasourceV6Keys derives the actual set fresh on every
// run so a newly ADDED key is picked up automatically with no test edit
// required; this floor guards the opposite failure — the datasource
// silently emitting FEWER netV6* keys than it should, which would let the
// per-key loop below iterate a shrunken (or empty) set and pass vacuously.
var knownV6Keys = []string{
	keyNetV6NetworkCluster, keyNetV6NetworkNamespaced,
	keyNetV6ContainerCluster, keyNetV6ContainerNamespaced,
	keyNetV6FixedAddrParent,
	keyNetV6FixedAddrHostCluster, keyNetV6FixedAddrHostNamespaced,
}

// datasourceV6Keys returns every key gen-datasource.sh actually emitted
// whose name starts with "netV6", derived from the datasource output
// itself rather than a hand-maintained list. This is what lets
// TestReapScopeCoversDatasourceV6Keys catch a new IPv6 sub-allocation
// added to gen-datasource.sh with no corresponding reap.sh sweep entry: the
// key shows up here automatically and then fails the otype lookup below.
// Sorted for a deterministic iteration order and deterministic failure
// messages (map iteration order is randomized).
func datasourceV6Keys(values map[string]string) []string {
	var keys []string
	for key := range values {
		if strings.HasPrefix(key, "netV6") {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	return keys
}

// TestReapScopeCoversDatasourceV6Keys is the drift guard between
// gen-datasource.sh's netV6* keys and reap.sh's IPv6 sweep scope. The two
// evolved independently more than once, and each time they silently fell
// out of sync in one of two ways: gen-datasource.sh started emitting an
// address shape reap.sh's regex no longer matched, or reap.sh's sweep
// stopped covering one of the three IPv6 WAPI object types
// (ipv6network / ipv6networkcontainer / ipv6fixedaddress) while its regex
// kept matching addresses of the type it still did cover — a coverage gap
// invisible to any check that only compares regexes, because
// PREFIX_V6_REGEX is identical across all three rows in
// IPV6_ADDRESSED_TYPES. A dry run with nothing to reap prints the exact
// same "Nothing to reap" in both failure modes — the gap is silent by
// construction. This test closes both gaps mechanically: for every
// netV6* key gen-datasource.sh actually emits (derived fresh each run, not
// a hardcoded list — a floor guards against the set shrinking to nothing),
// at least one `reap.sh --print-scope` entry for that SAME run's
// --net-prefix must have the object type and field that key's WAPI object
// is actually created as/on, and its regex must match the key's CANONICAL
// (WAPI-stored) rendering. A scope entry that matches only on regex, or
// only on otype, does not count.
func TestReapScopeCoversDatasourceV6Keys(t *testing.T) {
	// Seeds below were chosen by running gen-datasource.sh and reading the
	// BLOCK_INDEX each one derives, not guessed — an unverified guess is
	// exactly how earlier rounds of this mechanism ended up never
	// exercising the low band at all. BLOCK_INDEX 5 (seed "...-seed-f")
	// is the round-2 regression case: WAPI's RFC 5952 canonical storage
	// strips a hextet's leading zero, so a reap regex built from the
	// zero-padded spelling can never match a low-index run's actual
	// object. The other seeds cover the high band where padded and
	// canonical already agree, so a regression that only breaks the low
	// band can't hide behind them.
	seeds := []string{
		"TestReapScopeCoversDatasourceV6Keys-seed-a", // BLOCK_INDEX 32
		"TestReapScopeCoversDatasourceV6Keys-seed-b", // BLOCK_INDEX 91
		"TestReapScopeCoversDatasourceV6Keys-seed-c", // BLOCK_INDEX 153
		"TestReapScopeCoversDatasourceV6Keys-seed-f", // BLOCK_INDEX 5 (< 16)
	}

	sawLowBlockIndex := false
	for _, seed := range seeds {
		values := runGenDatasource(t, seed)
		blockIndex := blockIndexFromDatasource(t, values)
		if blockIndex < 16 {
			sawLowBlockIndex = true
		}
		scope := reapPrintScope(t, values[keyNetPrefix])
		if len(scope) == 0 {
			t.Fatalf("seed %q: reap.sh --print-scope reported an empty scope for --net-prefix=%s", seed, values[keyNetPrefix])
		}

		v6Keys := datasourceV6Keys(values)
		if len(v6Keys) < len(knownV6Keys) {
			t.Fatalf("seed %q: gen-datasource.sh emitted only %d netV6* keys %v, expected at least the %d known keys %v — the datasource's IPv6 sub-allocation may have regressed",
				seed, len(v6Keys), v6Keys, len(knownV6Keys), knownV6Keys)
		}

		for _, key := range v6Keys {
			wantOType, ok := netV6KeyExpectedOType[key]
			if !ok {
				t.Errorf("seed %q: netV6* key %q has no entry in netV6KeyExpectedOType — a new IPv6 sub-allocation was added to gen-datasource.sh without declaring which WAPI object type reap.sh's sweep must cover it as",
					seed, key)
				continue
			}
			wantField := netV6KeyExpectedField[key]
			canonical := canonicalIPv6String(t, values[key])
			matched := false
			for _, entry := range scope {
				if entry.otype == wantOType && entry.field == wantField && entry.regex.MatchString(canonical) {
					matched = true
					break
				}
			}
			if !matched {
				t.Errorf("seed %q (BLOCK_INDEX=%d, netPrefix=%s): %s=%q (canonical %q) is not covered by any reap.sh --print-scope entry with otype=%q field=%q: %v",
					seed, blockIndex, values[keyNetPrefix], key, values[key], canonical, wantOType, wantField, scope)
			}
		}
	}

	if !sawLowBlockIndex {
		t.Fatalf("none of the seeds %v produced a BLOCK_INDEX < 16 — this test needs at least one low-index run to guard the RFC 5952 leading-zero regression; re-derive a low-index seed with gen-datasource.sh and update the list", seeds)
	}
}
