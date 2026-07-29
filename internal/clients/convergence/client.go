// Package convergence implements the SOA-serial convergence gate for the
// Infoblox NIOS read/write endpoint split. The infoblox-go-client SDK
// does not cover the zone_auth WAPI object, so this package makes raw
// HTTP calls for the two zone_auth queries the gate needs: reading the
// primary's freshly-written serial, and reading the candidate's per-member
// serials to check whether it has caught up.
//
// Every exported function that touches soa_serial_number or
// member_soa_serials guards against the absent/empty-response edge cases
// (a zone with no grid_primary omits soa_serial_number entirely; a zone
// that doesn't exist returns an empty zone_auth array) — callers never
// see a panic or a distinguishing error for these expected states, only a
// "not found" boolean.
package convergence

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/crossplane/crossplane-runtime/v2/pkg/errors"
)

const (
	errBuildRequest   = "cannot build zone_auth WAPI request"
	errDoRequest      = "cannot reach candidate/primary WAPI endpoint for zone_auth query"
	errReadBody       = "cannot read zone_auth WAPI response body"
	errDecodeBody     = "cannot decode zone_auth WAPI response"
	errUnexpectedHTTP = "zone_auth WAPI query returned an unexpected HTTP status"
)

// MemberSerial is one entry of a zone_auth object's member_soa_serials
// array: the SOA serial NIOS currently reports for one grid member serving
// that zone.
type MemberSerial struct {
	GridPrimary string `json:"grid_primary"`
	Serial      uint32 `json:"serial"`
}

// zoneAuthObject is the subset of the WAPI zone_auth object shape this
// package requests via _return_fields. soa_serial_number uses a pointer
// because NIOS omits the key entirely (not zero, not null) when the zone
// has no grid_primary assigned.
type zoneAuthObject struct {
	Ref              string         `json:"_ref,omitempty"`
	SOASerialNumber  *uint32        `json:"soa_serial_number,omitempty"`
	MemberSOASerials []MemberSerial `json:"member_soa_serials,omitempty"`
}

// Client is a minimal raw WAPI HTTP client scoped to the single object
// type (zone_auth) the SDK doesn't cover. It authenticates with HTTP
// Basic Auth on every request — NIOS accepts this for reads without
// needing the SDK's session-cookie dance.
type Client struct {
	httpClient *http.Client
	baseURL    string
	username   string
	password   string
}

// NewClient constructs a Client pointed at a NIOS WAPI endpoint.
// sslVerify mirrors the same credentials-Secret option the SDK connector
// honors; set it false only for self-signed Grid Manager certificates.
func NewClient(host, port, version, username, password string, sslVerify bool) *Client {
	return newClient("https", host, port, version, username, password, sslVerify)
}

// NewClientWithScheme is the scheme-parameterized variant of NewClient
// used by tests to point at a plain-HTTP httptest.Server.
func NewClientWithScheme(scheme, host, port, version, username, password string, sslVerify bool) *Client {
	return newClient(scheme, host, port, version, username, password, sslVerify)
}

func newClient(scheme, host, port, version, username, password string, sslVerify bool) *Client {
	tr := &http.Transport{}
	if !sslVerify {
		// Mirrors the SDK connector's own ssl_verify=false option — used
		// only when the Grid Manager presents a self-signed certificate.
		tr.TLSClientConfig = &tls.Config{InsecureSkipVerify: true} //nolint:gosec // opt-in via credentials Secret, matches SDK behavior
	}
	return &Client{
		httpClient: &http.Client{Transport: tr},
		baseURL:    fmt.Sprintf("%s://%s:%s/wapi/%s", scheme, host, port, version),
		username:   username,
		password:   password,
	}
}

// ReadZoneSerial reads the primary's soa_serial_number for the zone
// identified by fqdn+view, immediately after a DNS write. It returns
// found=false (not an error) when the zone doesn't exist or has no
// grid_primary assigned — either case means there is no convergence
// signal to gate on for this write.
func (c *Client) ReadZoneSerial(ctx context.Context, fqdn, view string) (serial uint32, found bool, err error) {
	objs, err := c.queryZoneAuth(ctx, fqdn, view, "soa_serial_number")
	if err != nil {
		return 0, false, err
	}
	if len(objs) == 0 {
		return 0, false, nil
	}
	if objs[0].SOASerialNumber == nil {
		return 0, false, nil
	}
	return *objs[0].SOASerialNumber, true, nil
}

// ReadMemberSerials reads the zone's member_soa_serials array — the
// per-grid-member serial comparison the convergence gate checks the
// candidate against. Returns a nil slice (not an error) when the zone
// doesn't exist; returns an empty (non-nil) slice when the zone exists but
// has no grid_primary assigned, matching NIOS's own empty-array behavior
// for that case.
func (c *Client) ReadMemberSerials(ctx context.Context, fqdn, view string) ([]MemberSerial, error) {
	objs, err := c.queryZoneAuth(ctx, fqdn, view, "member_soa_serials")
	if err != nil {
		return nil, err
	}
	if len(objs) == 0 {
		return nil, nil
	}
	return objs[0].MemberSOASerials, nil
}

// queryZoneAuth issues GET /wapi/<version>/zone_auth?fqdn=<fqdn>&view=<view>&_return_fields=<fields>
// and decodes the (possibly empty) JSON array WAPI returns for list
// queries.
func (c *Client) queryZoneAuth(ctx context.Context, fqdn, view, returnFields string) ([]zoneAuthObject, error) {
	q := url.Values{}
	q.Set("fqdn", fqdn)
	if view != "" {
		q.Set("view", view)
	}
	q.Set("_return_fields", returnFields)

	reqURL := c.baseURL + "/zone_auth?" + q.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return nil, errors.Wrap(err, errBuildRequest)
	}
	req.SetBasicAuth(c.username, c.password)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, errors.Wrap(err, errDoRequest)
	}
	defer resp.Body.Close() //nolint:errcheck // best-effort close on a read-only response

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, errors.Wrap(err, errReadBody)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, errors.Errorf("%s: %d: %s", errUnexpectedHTTP, resp.StatusCode, strings.TrimSpace(string(body)))
	}

	var objs []zoneAuthObject
	if err := json.Unmarshal(body, &objs); err != nil {
		return nil, errors.Wrap(err, errDecodeBody)
	}
	return objs, nil
}

// ZoneFQDNFromRecordName derives a record's parent zone FQDN by stripping
// its leftmost (hostname) label — e.g. "www.example.com" -> "example.com".
// This is the only zone-derivation signal available before a record has
// been read back from WAPI (which returns the authoritative "zone" field
// directly); it assumes the record is not itself a zone apex. Records with
// no dot (a bare hostname, or an already-bare zone name) return the input
// unchanged.
func ZoneFQDNFromRecordName(name string) string {
	idx := strings.Index(name, ".")
	if idx < 0 {
		return name
	}
	return name[idx+1:]
}
