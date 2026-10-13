# Multi-App Workflow Management

**Status:** Approved
**Date:** 2026-07-14
**Branch:** `rebase/upstream`

## Problem

Dapr workflow instances are partitioned per app-id: a sidecar for app-id X can
only see and manage X's workflow instances, and no cross-app "list all
workflows" API exists in the Dapr runtime. On top of that, this server holds
exactly one Dapr client connection (its own sidecar), so the single-app
workflow tools can only manage workflows of the single app-id the server
happens to run under.

Operators with multiple workflow applications (e.g. `orders` and
`billing`) currently need one MCP server instance per app to inspect
their workflows. The goal: **one MCP server process, one MCP endpoint, all
configured workflow apps.**

The first limitation (one connection per process) lives in this codebase and
is what this change removes. The second (per-app partitioning) is fundamental
to Dapr; we work with it by holding one connection per configured app.

## Design

### Configuration

A new optional environment variable maps app-ids to sidecar gRPC endpoints:

```
DAPR_MCP_SERVER_WORKFLOW_APPS=orders=localhost:50002,billing=localhost:50003
```

- Format: comma-separated `app-id=host:port` pairs. Whitespace around entries
  is trimmed. Duplicate app-ids and malformed entries are startup errors.
- Unset or empty: behavior is identical to before this change (single app,
  own sidecar).
- Note: `dapr run` assigns dynamic gRPC ports per run; this configuration is
  therefore most useful with fixed ports (`--dapr-grpc-port`), Kubernetes, or
  Catalyst where endpoints are stable.

### Connection pool

At startup, `cmd/dapr-mcp-server/workflow_apps.go` builds one workflow client
per configured app via `dapr.NewClientWithAddressContext(ctx, addr)` +
`dtworkflow.NewClient(client.GrpcClientConn())`,
reusing the same retry policy as the primary Dapr client (5 attempts, 2s
apart), and exits fatally if an app remains unreachable. Rationale: the
configuration is explicit, so an unreachable entry is a deployment error;
silently skipping it would make `list_workflows` quietly incomplete.
Operators can remove an app from the mapping if it is intentionally down.

The server's own sidecar remains the **default client**. Its app-id is
resolved once at startup via the metadata API (`GetMetadata().ID`, fallback
label `"default"`) so it can be labeled in multi-app listings.

### Tool interface changes

Every workflow tool gains an `appID` argument:

- No apps configured: `appID` may be omitted → the default client (own
  sidecar). Fully backwards compatible with single-app setups.
- Apps configured: `appID` is **required** on all per-instance tools; the
  omission returns a tool error listing the configured app-ids. The server's
  own app-id is accepted as an explicit `appID` for its sidecar.
- An unknown `appID` returns a tool error listing the configured app-ids
  (the agent can self-correct).

The required-`appID` rule in multi-app mode is a hardening guard, added
after field testing: routing a per-instance call (history/status) to a
sidecar that does not own the instance crashed daprd itself with a nil
dereference in `wfengine/state.LoadWorkflowState` (observed on Dapr 1.18.1)
— an agent forgetting `appID` could take down the MCP server's own `dapr
run`. Requiring the argument turns a fatal sidecar crash into a
self-correcting hint. This is a Dapr runtime bug (crash instead of a clean
error); it was reported upstream as
[dapr/dapr#10217](https://github.com/dapr/dapr/issues/10217) and fixed by
[dapr/dapr#10218](https://github.com/dapr/dapr/pull/10218), which is merged.
The guard stays, because the server cannot know which runtime version a
configured sidecar runs.

`list_workflows` is the exception with richer semantics:

- `appID` set → list only that app (current behavior).
- `appID` omitted **and** apps are configured → fan out over the default
  client and all pool clients, merge the results, and report `app_id` per
  instance plus `counts_by_app` alongside the existing per-workflow and
  per-status counts.
- Per-app failures during fan-out do not fail the whole call; they are
  reported as warnings per app so partial results remain usable.
- Pagination in fan-out mode: `limit` applies per app, and continuation
  tokens are returned per app (`continuation_tokens: {app-id: token}`).
  A follow-up call with `appID` + `continuationToken` fetches that app's next
  page. (Cross-app merged pagination is intentionally out of scope.)

Mutating tools (`start_workflow`, `raise_workflow_event`,
`terminate_workflow`, ...) do not fan out — they always target exactly one
client. Their descriptions instruct agents to pass `appID` when more than one
app is configured.

### Package changes (`pkg/workflow`)

- The handler's single `WorkflowClient` is replaced by a small registry:
  default client + `map[appID]WorkflowClient`, set via
  `RegisterTools(server, workflow.Clients{…}, metrics)`. `workflow.Clients`
  carries `Default`, `DefaultAppID` and `ByAppID`.
- `clientFor(appID)` resolves the registry and produces the
  unknown-app error message.
- `ParseAppsConfig(string) (map[string]string, error)` parses the environment
  variable; it lives in this package so it is unit-testable.

## Alternatives considered

1. **Read the state store directly** (enumerate `<app-id>||...` keys in
   Redis): covers all apps in one read, but couples the server to the
   workflow engine's internal key format (unstable across Dapr versions) and
   requires store credentials outside the sidecar model. Rejected as a
   feature; fine as a manual audit trick.
2. **One server instance per app** (status quo): zero code, but N endpoints,
   N MCP registrations, and no cross-app overview in one call.

## Testing

- Unit tests for `ParseAppsConfig` (valid, empty, malformed, duplicates).
- Registry resolution tests (default, known app, unknown app error).
- Fan-out tests with multiple mock clients, including one failing app
  (partial results + warning) and per-app continuation tokens.
- All existing single-app tests keep passing unchanged semantics with an
  empty pool.

## Out of scope

- Cross-app merged pagination.
- Dynamic discovery of workflow apps (would require a registry Dapr does not
  provide).
- Auto-reconnect/health-checking of pool connections beyond gRPC's built-in
  reconnect behavior.
