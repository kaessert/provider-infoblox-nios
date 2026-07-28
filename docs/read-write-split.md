# Infoblox gridmaster read/write split

> Status: **built, compiles, and wired — UNVALIDATED against a real NIOS grid.**
> This feature has not been exercised end-to-end. Validate against a real
> gridmaster primary + candidate before relying on it. See "What is unvalidated"
> below.

## What it does

The provider can route Crossplane's **read** traffic to an Infoblox gridmaster
**candidate** and its **write** traffic to the **primary** gridmaster, mirroring
customer tooling that uses a `read_url` (candidate) and `write_url` (primary):

| Crossplane call                | Endpoint                       |
|--------------------------------|--------------------------------|
| `Observe` (DNS records)        | candidate — `read_server`      |
| `Observe` (IPAM resources)     | primary — `server`             |
| `Create` / `Update` / `Delete` | primary — `server`             |

Only **DNS records** (API group `dns.infoblox-nios.crossplane.io`: the A, AAAA,
CNAME, MX, PTR, SRV and TXT record kinds) are offloaded to the read candidate.
**IPAM resources** (networks, ranges, allocations, …) always `Observe` against
the primary — IPAM is far more sensitive to replication lag (next-available-IP
allocation must read the authoritative primary), so it is deliberately excluded
from the split.

It is **opt-in** and **fully backward compatible**: if no read endpoint is
configured the provider behaves exactly as the single-endpoint build.

## Why it is only possible under the no-fork runtime

In the previous Terraform-CLI runtime, upjet's `WorkspaceStore` keyed an
on-disk Terraform workspace by managed-resource UID (`/tmp/<UID>`). Two endpoint
configurations for the same MR would have collided on that path, so a split was
impossible.

Under the no-fork (Terraform Plugin SDKv2, in-process) runtime there is no
on-disk workspace. The SDKv2 external client uses the provider meta stored on
`terraform.Setup.Meta`, which the setup builder produces by calling
`(*schema.Provider).Configure` per endpoint. Two in-process clients
(read-configured meta and write-configured meta) can therefore coexist for the
same MR. The same `*schema.Provider` is Configured twice — once per endpoint —
by copying it **by value** in `configureNoForkClient`, so each endpoint gets an
independent meta with no shared/mutated `ProviderConfig`.

## How to enable

Add `read_server` to the credentials secret JSON. Optionally override the
read-endpoint port / sslmode / WAPI version; each falls back to its write value
when omitted:

```json
{
  "server": "gm-primary.infoblox.example.com",
  "read_server": "gm-candidate.infoblox.example.com",
  "username": "admin",
  "password": "…",
  "sslmode": "false",
  "port": "443",
  "connection_timeout": "60",
  "pool_connections": "10",
  "wapi_version": "2.12"
}
```

Auth and connection-pool settings (`username`, `password`,
`connection_timeout`, `pool_connections`) are always shared between endpoints.

If `read_server` is absent, or equal to `server`, the read setup is identical to
the write setup and the split is a functional no-op. See
`examples/providerconfig/providerconfig-rw-split.yaml`.

## Read-after-write hazard and the convergence gate

Grid replication from primary to candidate is not instantaneous. A read routed
to the candidate immediately after a write to the primary may observe stale data
— a missing or out-of-date resource — which would make `Observe` report spurious
drift and trigger reconcile churn (repeated `Update`/`Create`).

Two mechanisms guard against this, and **neither is a timer** — there is no
replication-lag value to tune:

1. **Shared operation-tracker store.** The read and write connecters share a
   single upjet `OperationTrackerStore`, so they resolve to the *same*
   `AsyncTracker` per MR. While an asynchronous write is in flight the tracker's
   `LastOperation` reports running, and the decorator serves `Observe` from the
   **primary** instead of probing the candidate mid-flight. This coordination
   only works with a shared store — a separate read store would not see the
   write's in-flight operation.

2. **Candidate-observe convergence gate.** After a write completes, the
   decorator marks the MR "post-write" and keeps routing `Observe` to the
   **primary** while probing the candidate on each reconcile. Only once the
   candidate itself reports the resource exists and is up-to-date does the marker
   clear and steady-state reads return to the candidate. The window ends when
   replication has **demonstrably converged** — observed, not timed.

**Safe degradation.** If the candidate never catches up (a broken or lagging
replica), the post-write marker never clears and reads for that MR stay on the
primary indefinitely. This is intentional: the split degrades to no-offload
rather than ever serving stale data. There is deliberately no timeout and no
max-attempts.

**Convergence seam.** The "has the candidate caught up?" decision is factored
into `candidateCaughtUp` so the convergence signal can be swapped without
touching the routing state machine. Today it trusts the candidate's own
`Observe` result; a future implementation could instead compare the NIOS SOA
serial watermark (`zone_auth.soa_serial_number`) of primary vs. candidate behind
that same seam.

## What is unvalidated / needs e2e

- No run against a real NIOS grid (primary + candidate). Routing correctness,
  the shared-tracker early-return, and the convergence gate are reasoned about
  from the upjet source, not observed.
- The candidate-observe convergence signal has not been validated against real
  grid replication behavior; whether the candidate's own `Observe` is a reliable
  caught-up watermark (vs. an SOA-serial comparison) is unconfirmed.
- Read-side management-policy parity is set from the provider's
  `--enable-management-policies` flag (`split.ManagementPolicies`), but init-
  parameter merging parity between the two connecters has not been observed in
  practice.
- Behavior of `Observe` against a candidate that is itself briefly unreachable
  (the decorator falls back to the primary for that reconcile) is untested.
