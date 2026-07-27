/*
Copyright 2021 Upbound Inc.
*/

package split

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

// This file implements a minimal, offline Infoblox WAPI mock used by the
// per-verb read/write routing integration test. It is a _test.go file, so it
// never ships in the provider binary.
//
// Two mock servers are started per test: a "primary" (write / server) and a
// "candidate" (read / read_server). Each server records every request it
// receives as (method, path) so the test can assert which verb was routed to
// which endpoint. Both servers share a single in-memory "grid" so a record
// created via a POST to the primary is visible to a read-back GET on the
// candidate (mirroring a replicated NIOS grid) — this is what lets upjet's
// Observe succeed against the candidate after a Create against the primary.
//
// The servers are HTTPS (httptest.NewTLSServer): the real infoblox provider
// always talks WAPI over https (the go-client only switches to http when
// HostConfig.Scheme=="http", which providerConfigure never sets), and the
// credential sslmode=false maps to TransportConfig.SslVerify=false ->
// InsecureSkipVerify=true, so the self-signed httptest cert is accepted.

// wapiReq is a single recorded request.
type wapiReq struct {
	method string
	path   string
}

// grid is the shared, replicated backing store for the two mock servers. It is
// keyed by the object _ref.
type grid struct {
	mu      sync.Mutex
	records map[string]map[string]any
	refSeq  int64
}

func newGrid() *grid { return &grid{records: map[string]map[string]any{}} }

func (g *grid) put(ref string, rec map[string]any) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.records[ref] = rec
}

func (g *grid) get(ref string) (map[string]any, bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	r, ok := g.records[ref]
	return r, ok
}

func (g *grid) del(ref string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	delete(g.records, ref)
}

func (g *grid) nextRef() string {
	n := atomic.AddInt64(&g.refSeq, 1)
	// A realistic-looking _ref: record:a/<opaque>:<fqdn>/<view>. The opaque and
	// view segments contain slashes/colons, exercising the path handling.
	return fmt.Sprintf("record:a/ZG5zLmJpbmRfYSix%d:test%d.example.com/default", n, n)
}

// wapiMock is one mock WAPI endpoint.
type wapiMock struct {
	name   string
	ver    string
	server *httptest.Server
	grid   *grid

	mu       sync.Mutex
	reqs     []wapiReq
	unexpect []string // paths that hit the permissive fallback (surfaces real WAPI surface gaps)
}

// newWAPIMock starts an HTTPS mock WAPI server backed by the given shared grid.
func newWAPIMock(t *testing.T, name, ver string, g *grid) *wapiMock {
	t.Helper()
	m := &wapiMock{name: name, ver: ver, grid: g}
	m.server = httptest.NewTLSServer(http.HandlerFunc(m.handle))
	t.Cleanup(m.server.Close)
	return m
}

// hostPort returns the bare host and port of the mock server (the httptest URL
// is https://127.0.0.1:PORT). They map to the "server"/"read_server" and
// "port"/"read_port" credential keys respectively; they must be provided
// separately because the go-client builds the request host as host+":"+port.
func (m *wapiMock) hostPort() (host, port string) {
	hp := strings.TrimPrefix(m.server.URL, "https://")
	if i := strings.LastIndex(hp, ":"); i >= 0 {
		return hp[:i], hp[i+1:]
	}
	return hp, ""
}

func (m *wapiMock) record(method, path string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.reqs = append(m.reqs, wapiReq{method: method, path: path})
}

// requests returns a copy of the recorded requests.
func (m *wapiMock) requests() []wapiReq {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]wapiReq, len(m.reqs))
	copy(out, m.reqs)
	return out
}

// countRecordA returns how many requests with the given HTTP method targeted a
// record:a path (POST record:a, or GET/PUT/DELETE record:a/<ref>).
func (m *wapiMock) countRecordA(method string) int {
	n := 0
	for _, r := range m.requests() {
		if r.method == method && strings.Contains(r.path, "record:a") {
			n++
		}
	}
	return n
}

func (m *wapiMock) reset() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.reqs = nil
	m.unexpect = nil
}

func (m *wapiMock) handle(w http.ResponseWriter, r *http.Request) {
	m.record(r.Method, r.URL.Path)

	prefix := "/wapi/v" + m.ver + "/"
	rest := strings.TrimPrefix(r.URL.Path, prefix)

	// Connect prerequisite: the extensible-attribute-definition probe for the
	// "Terraform Internal ID" EA. A non-empty array makes the provider skip the
	// create-POST.
	if r.Method == http.MethodGet && strings.HasPrefix(rest, "extensibleattributedef") {
		writeJSON(w, http.StatusOK, []map[string]any{
			{"name": "Terraform Internal ID", "type": "STRING"},
		})
		return
	}

	switch {
	case strings.HasPrefix(rest, "record:a/"): // operate on a specific record by ref
		ref := rest
		switch r.Method {
		case http.MethodGet:
			rec, ok := m.grid.get(ref)
			if !ok {
				// go-client treats an empty JSON array as "not found".
				writeJSON(w, http.StatusOK, []any{})
				return
			}
			writeJSON(w, http.StatusOK, rec)
		case http.MethodPut: // update
			body := readBody(r)
			if rec, ok := m.grid.get(ref); ok {
				mergeRecord(rec, body)
				m.grid.put(ref, rec)
			}
			writeQuoted(w, http.StatusOK, ref)
		case http.MethodDelete:
			m.grid.del(ref)
			writeQuoted(w, http.StatusOK, ref)
		default:
			writeJSON(w, http.StatusOK, map[string]any{})
		}
	case rest == "record:a": // create
		if r.Method == http.MethodPost {
			body := readBody(r)
			ref := m.grid.nextRef()
			rec := newRecordFromCreate(ref, body)
			m.grid.put(ref, rec)
			writeQuoted(w, http.StatusCreated, ref)
			return
		}
		writeJSON(w, http.StatusOK, []any{})
	default:
		// Permissive fallback: record the unexpected path so the test can report
		// any additional WAPI surface the real client needs, and answer in the
		// least-disruptive way (empty array for GET).
		m.mu.Lock()
		m.unexpect = append(m.unexpect, r.Method+" "+r.URL.Path)
		m.mu.Unlock()
		if r.Method == http.MethodGet {
			writeJSON(w, http.StatusOK, []any{})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{})
	}
}

// newRecordFromCreate builds the stored record object from a create POST body,
// mirroring the ibclient.RecordA JSON shape the go-client reads back.
func newRecordFromCreate(ref string, body map[string]any) map[string]any {
	rec := map[string]any{
		"_ref":     ref,
		"use_ttl":  false,
		"comment":  "",
		"extattrs": map[string]any{},
	}
	if v, ok := body["name"]; ok {
		rec["name"] = v
	}
	if v, ok := body["ipv4addr"]; ok {
		rec["ipv4addr"] = v
	}
	if v, ok := body["view"]; ok {
		rec["view"] = v
	}
	if v, ok := body["comment"]; ok {
		rec["comment"] = v
	}
	if v, ok := body["use_ttl"]; ok {
		rec["use_ttl"] = v
	}
	if v, ok := body["ttl"]; ok {
		rec["ttl"] = v
	}
	// Preserve the extensible attributes, including the "Terraform Internal ID"
	// EA that the provider generated and sent on create, so the read-back's
	// internal-ID validation matches.
	if ea, ok := body["extattrs"].(map[string]any); ok {
		rec["extattrs"] = ea
	}
	return rec
}

func mergeRecord(rec, body map[string]any) {
	for _, k := range []string{"name", "ipv4addr", "view", "comment", "use_ttl", "ttl"} {
		if v, ok := body[k]; ok {
			rec[k] = v
		}
	}
	if ea, ok := body["extattrs"].(map[string]any); ok {
		rec["extattrs"] = ea
	}
}

func readBody(r *http.Request) map[string]any {
	b, _ := io.ReadAll(r.Body)
	m := map[string]any{}
	_ = json.Unmarshal(b, &m)
	return m
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

// writeQuoted writes a JSON string (a quoted _ref), as WAPI returns for
// create/update/delete.
func writeQuoted(w http.ResponseWriter, code int, ref string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	b, _ := json.Marshal(ref)
	_, _ = w.Write(b)
}
