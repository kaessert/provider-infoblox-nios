// Package convergence unit tests. Tests use inline httptest.NewServer
// mocks that emulate the zone_auth WAPI object queries this package makes
// (the SDK doesn't cover zone_auth, so the client under test issues raw
// HTTP requests — these tests exercise exactly that wire format).
package convergence

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

// mockZoneAuthServer serves GET /wapi/<version>/zone_auth?fqdn=...&view=...
// requests, returning a configurable JSON array response.
type mockZoneAuthServer struct {
	// respond is called per-request; return the array body to serve (nil
	// serializes to "[]") and the HTTP status.
	respond func(r *http.Request) (body interface{}, status int)
}

func (m *mockZoneAuthServer) handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, status := m.respond(r)
		if body == nil {
			body = []zoneAuthObject{}
		}
		raw, err := json.Marshal(body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write(raw)
	})
}

func newTestClient(t *testing.T, srv *httptest.Server) *Client {
	t.Helper()
	u, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatalf("cannot parse test server URL: %v", err)
	}
	return NewClientWithScheme("http", u.Hostname(), u.Port(), "2.9.7", "admin", "s3cr3t", true)
}

func uint32Ptr(v uint32) *uint32 { return &v }

// Shared literals reused across client_test.go and gate_test.go (same
// package) to satisfy the goconst lint check.
const (
	testZoneAuthRef  = "zone_auth/xyz"
	testCandidateFQN = "gmc.example.com"
)

func TestReadZoneSerialFound(t *testing.T) {
	m := &mockZoneAuthServer{respond: func(r *http.Request) (interface{}, int) {
		if got := r.URL.Query().Get("fqdn"); got != "example.com" {
			t.Fatalf("fqdn query param = %q, want example.com", got)
		}
		return []zoneAuthObject{{Ref: testZoneAuthRef, SOASerialNumber: uint32Ptr(5)}}, http.StatusOK
	}}
	srv := httptest.NewServer(m.handler())
	defer srv.Close()
	c := newTestClient(t, srv)

	serial, found, err := c.ReadZoneSerial(context.Background(), "example.com", "Internal")
	if err != nil {
		t.Fatalf("ReadZoneSerial: unexpected error: %v", err)
	}
	if !found || serial != 5 {
		t.Fatalf("ReadZoneSerial = (%d, %v), want (5, true)", serial, found)
	}
}

func TestReadZoneSerialAbsentSOA(t *testing.T) {
	// Zone with no grid_primary assigned: soa_serial_number key is
	// entirely absent from the response.
	m := &mockZoneAuthServer{respond: func(r *http.Request) (interface{}, int) {
		return []zoneAuthObject{{Ref: testZoneAuthRef}}, http.StatusOK
	}}
	srv := httptest.NewServer(m.handler())
	defer srv.Close()
	c := newTestClient(t, srv)

	_, found, err := c.ReadZoneSerial(context.Background(), "example.com", "Internal")
	if err != nil {
		t.Fatalf("ReadZoneSerial: unexpected error: %v", err)
	}
	if found {
		t.Fatal("ReadZoneSerial must report found=false when soa_serial_number is absent")
	}
}

func TestReadZoneSerialZoneNotFound(t *testing.T) {
	m := &mockZoneAuthServer{respond: func(r *http.Request) (interface{}, int) {
		return []zoneAuthObject{}, http.StatusOK
	}}
	srv := httptest.NewServer(m.handler())
	defer srv.Close()
	c := newTestClient(t, srv)

	_, found, err := c.ReadZoneSerial(context.Background(), "nope.example.com", "Internal")
	if err != nil {
		t.Fatalf("ReadZoneSerial: unexpected error: %v", err)
	}
	if found {
		t.Fatal("ReadZoneSerial must report found=false for an empty zone_auth response")
	}
}

func TestReadMemberSerialsFound(t *testing.T) {
	m := &mockZoneAuthServer{respond: func(r *http.Request) (interface{}, int) {
		return []zoneAuthObject{{
			Ref: testZoneAuthRef,
			MemberSOASerials: []MemberSerial{
				{GridPrimary: "gm.example.com", Serial: 5},
				{GridPrimary: testCandidateFQN, Serial: 4},
			},
		}}, http.StatusOK
	}}
	srv := httptest.NewServer(m.handler())
	defer srv.Close()
	c := newTestClient(t, srv)

	members, err := c.ReadMemberSerials(context.Background(), "example.com", "Internal")
	if err != nil {
		t.Fatalf("ReadMemberSerials: unexpected error: %v", err)
	}
	if len(members) != 2 || members[1].GridPrimary != testCandidateFQN || members[1].Serial != 4 {
		t.Fatalf("ReadMemberSerials returned unexpected members: %+v", members)
	}
}

func TestReadMemberSerialsEmptyArray(t *testing.T) {
	// Zone exists but has no grid_primary assigned: member_soa_serials is
	// an empty (not absent) array.
	m := &mockZoneAuthServer{respond: func(r *http.Request) (interface{}, int) {
		return []zoneAuthObject{{Ref: testZoneAuthRef, MemberSOASerials: []MemberSerial{}}}, http.StatusOK
	}}
	srv := httptest.NewServer(m.handler())
	defer srv.Close()
	c := newTestClient(t, srv)

	members, err := c.ReadMemberSerials(context.Background(), "example.com", "Internal")
	if err != nil {
		t.Fatalf("ReadMemberSerials: unexpected error: %v", err)
	}
	if len(members) != 0 {
		t.Fatalf("expected zero members, got %+v", members)
	}
}

func TestReadMemberSerialsZoneNotFound(t *testing.T) {
	m := &mockZoneAuthServer{respond: func(r *http.Request) (interface{}, int) {
		return []zoneAuthObject{}, http.StatusOK
	}}
	srv := httptest.NewServer(m.handler())
	defer srv.Close()
	c := newTestClient(t, srv)

	members, err := c.ReadMemberSerials(context.Background(), "nope.example.com", "Internal")
	if err != nil {
		t.Fatalf("ReadMemberSerials: unexpected error: %v", err)
	}
	if members != nil {
		t.Fatalf("expected a nil slice for a zone-not-found response, got %+v", members)
	}
}

func TestReadMemberSerialsHTTPError(t *testing.T) {
	m := &mockZoneAuthServer{respond: func(r *http.Request) (interface{}, int) {
		return map[string]string{"Error": "authentication failed"}, http.StatusUnauthorized
	}}
	srv := httptest.NewServer(m.handler())
	defer srv.Close()
	c := newTestClient(t, srv)

	if _, err := c.ReadMemberSerials(context.Background(), "example.com", "Internal"); err == nil {
		t.Fatal("expected an error for a non-200 zone_auth response")
	}
}

func TestReadMemberSerialsNetworkError(t *testing.T) {
	// Point the client at a closed server to simulate the candidate WAPI
	// being unreachable.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	u, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatalf("cannot parse test server URL: %v", err)
	}
	srv.Close() // close immediately so requests fail to connect

	c := NewClientWithScheme("http", u.Hostname(), u.Port(), "2.9.7", "admin", "s3cr3t", true)
	if _, err := c.ReadMemberSerials(context.Background(), "example.com", "Internal"); err == nil {
		t.Fatal("expected a network error against a closed server")
	}
}

func TestZoneFQDNFromRecordName(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{"simple record", "www.example.com", "example.com"},
		{"subdomain record", "host.dev.example.com", "dev.example.com"},
		{"reverse zone PTR name", "4.3.2.1.in-addr.arpa", "3.2.1.in-addr.arpa"},
		{"bare name, no dot", "example", "example"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := ZoneFQDNFromRecordName(tc.in); got != tc.want {
				t.Fatalf("ZoneFQDNFromRecordName(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestNewClientBuildsHTTPSBaseURL(t *testing.T) {
	c := NewClient("grid.example.com", "443", "2.9.7", "admin", "s3cr3t", true)
	if c.baseURL != "https://grid.example.com:443/wapi/2.9.7" {
		t.Fatalf("baseURL = %q, want %q", c.baseURL, "https://grid.example.com:443/wapi/2.9.7")
	}
	if c.username != "admin" || c.password != "s3cr3t" {
		t.Fatalf("unexpected client credentials: username=%q password=%q", c.username, c.password)
	}
	if c.httpClient.Transport.(*http.Transport).TLSClientConfig != nil {
		t.Fatal("sslVerify=true must not install a custom TLSClientConfig")
	}
}

func TestNewClientSslVerifyFalseSkipsCertValidation(t *testing.T) {
	c := NewClient("grid.example.com", "443", "2.9.7", "admin", "s3cr3t", false)
	tlsCfg := c.httpClient.Transport.(*http.Transport).TLSClientConfig
	if tlsCfg == nil || !tlsCfg.InsecureSkipVerify {
		t.Fatal("sslVerify=false must install a TLSClientConfig with InsecureSkipVerify=true")
	}
}

func TestReadZoneSerialBuildRequestErrorPropagates(t *testing.T) {
	c := NewClientWithScheme("http", "127.0.0.1", "0", "2.9.7", "admin", "s3cr3t", true)

	//nolint:staticcheck // intentionally passing a nil context to exercise the request-build error path
	if _, _, err := c.ReadZoneSerial(nil, "example.com", "Internal"); err == nil {
		t.Fatal("expected an error when the underlying request cannot be built (nil context)")
	}
}

// Regression guard: query strings must escape/URL-encode values correctly
// (e.g. views containing spaces) rather than being string-concatenated.
func TestQueryZoneAuthEncodesParams(t *testing.T) {
	var gotRawQuery string
	m := &mockZoneAuthServer{respond: func(r *http.Request) (interface{}, int) {
		gotRawQuery = r.URL.RawQuery
		return []zoneAuthObject{}, http.StatusOK
	}}
	srv := httptest.NewServer(m.handler())
	defer srv.Close()
	c := newTestClient(t, srv)

	if _, _, err := c.ReadZoneSerial(context.Background(), "example.com", "My View"); err != nil {
		t.Fatalf("ReadZoneSerial: unexpected error: %v", err)
	}
	if !strings.Contains(gotRawQuery, "view=My+View") && !strings.Contains(gotRawQuery, "view=My%20View") {
		t.Fatalf("expected the view query param to be URL-encoded, got %q", gotRawQuery)
	}
}
