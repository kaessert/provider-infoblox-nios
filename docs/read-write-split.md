# Infoblox gridmaster read/write split

> Status: **built, compiles, and wired — UNVALIDATED against a real NIOS grid.**
> This feature has not been exercised end-to-end. Validate against a real
> gridmaster primary + candidate before relying on it. See "What is unvalidated"
> below.

## What it does

The provider can route Crossplane's **read** traffic to an Infoblox gridmaster
**candidate** and its **write** traffic to the **primary** gridmaster, mirroring
customer tooling that uses a `read_url` (candidate) and `write_url` (primary):

| Crossplane call            | Endpoint                       |
|----------------------------|--------------------------------|
| `Observe`                  | candidate — `read_server`      |
| `Create` / `Update` / `Delete` | primary — `server`         |

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

## Replication-lag caveat (read-after-write hazard)

Grid replication from primary to candidate is not instantaneous. A read routed
to the candidate immediately after a write to the primary may observe stale data
— a missing or out-of-date resource — which would make `Observe` report spurious
drift and trigger reconcile churn (repeated `Update`/`Create`).

Two mechanisms guard against this:

1. **Shared operation-tracker store.** The read and write connecters share a
   single upjet `OperationTrackerStore`, so they resolve to the *same*
   `AsyncTracker` per MR. While an asynchronous write is in flight the tracker's
   `LastOperation` reports running, and the read-side `Observe` returns early
   (`ResourceExists && ResourceUpToDate`) instead of querying the candidate
   mid-flight. This coordination only works with a shared store — a separate
   read store would not see the write's in-flight operation.

2. **Post-write grace window.** After the async write is observed to complete,
   the decorator keeps routing `Observe` to the **primary** for
   `split.GraceWindow` (default 30s), giving the grid time to replicate before
   reads move back to the candidate. The window is anchored to the *observed
   completion* of the async operation, not to when the async call returned, so a
   slow write does not consume the window early.

`split.GraceWindow` is a package-level knob. Tune it to the customer's measured
worst-case replication lag during validation. Too small → residual churn risk;
too large → reads stay on the primary longer than necessary (still correct, just
less benefit from the split).

## What is unvalidated / needs e2e

- No run against a real NIOS grid (primary + candidate). Routing correctness,
  the shared-tracker early-return, and the grace window are reasoned about from
  the upjet source, not observed.
- The correct `GraceWindow` for real grid replication lag is unknown; the 30s
  default is a placeholder.
- Read-side management-policy parity is set from the provider's
  `--enable-management-policies` flag (`split.ManagementPolicies`), but init-
  parameter merging parity between the two connecters has not been observed in
  practice.
- Behavior of `Observe` against a candidate that is itself briefly unreachable
  (the decorator falls back to the primary for that reconcile) is untested.
