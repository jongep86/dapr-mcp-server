# Workflow Notifications (server → client)

**Status:** Draft
**Date:** 2026-07-22
**Branch:** _(unstarted; builds on `rebase/upstream`)_

## Problem

Today the workflow tools are strictly request/response: an agent only learns
that a workflow advanced, failed, or is waiting for input by **polling**
(`get_workflow_status` / `list_workflows`). Many production workflows
are long-running, human-in-the-loop instances that park on
`wait_for_external_event` (e.g. "waiting for an approval before the next step")
sometimes for days. An agent connected to this MCP server has no way to be
told *"instance X is waiting for input"* or *"instance Y reached its approval
gate"* without repeatedly re-listing.

The goal: let the server **push** to the client when a workflow changes state
or needs input, so an agent (or the human behind Claude Desktop / Codex) can
react — e.g. by gathering the requested input and calling
`raise_workflow_event` back.

This is the reason the transport keeps a **stateful** mode (see the
`--stateless` flag on `rebase/upstream`): stateless drops the
server→client channel, so notifications require the default (stateful) session.

## Design (proposed)

### Two client-facing shapes

1. **Progress / status notification** — the workflow advanced or changed
   `custom_status` (e.g. `validating → awaiting-approval`), completed, or
   failed. Maps to an MCP
   server-initiated **notification** (`notifications/message`, or a namespaced
   custom notification). Fire-and-forget; the client surfaces it.
2. **Input request (elicitation)** — the workflow is blocked on an external
   event that needs data from the user ("upload the signed approval
   form"). Maps to the MCP **elicitation** capability
   (`elicitation/create`): the server asks the client for a typed payload,
   and on reply the server calls `raise_workflow_event` on the owning app.
   This closes the human-in-the-loop cycle inside one MCP session.

### Where the events come from

Dapr Workflow does not emit progress events natively, so the server needs a
source. Two options (decision open):

- **A. Pub/sub subscription (preferred).** The workflow apps publish a small
  event (`{app_id, instance_id, workflow_name, runtime_status, custom_status,
  needs_input?}`) on `custom_status` transitions / gate waits. This server
  subscribes and translates each event into shape (1) or (2). The hook already
  exists: `cmd/dapr-mcp-server/main.go` serves `/dapr/subscribe` (currently returns `[]`) — this
  feature fills it in. Clean and event-driven; cost is that each workflow app
  must publish (a small activity or middleware).
- **B. Internal polling.** The server polls `list_workflows` per configured
  app on an interval, diffs `runtime_status` / `custom_status` against a
  last-seen cache, and emits notifications on change. No app changes, but adds
  load, latency, and it cannot see "needs input" unless that is encoded in
  `custom_status`.

### Multi-app

Notifications must carry `app_id` (the pool already keys clients by app-id).
A subscription/poller runs per configured app; elicitation replies route
`raise_workflow_event` to the correct pool client via the existing
`clientFor(appID)`.

### Transport constraint

Only available in **stateful** mode. In stateless mode the server has no
session to push on; the tools degrade to request/response and this feature is
simply absent (documented, not an error). `list_workflows` polling by the
client remains the fallback.

## Open questions

- Which MCP notification primitive do Claude Desktop / Codex actually surface
  today (`notifications/message` vs. custom)? Verify before committing to a
  shape.
- Elicitation support in the target clients — is `elicitation/create`
  honored, or should "needs input" also degrade to a plain notification the
  agent acts on with `raise_workflow_event`?
- Event contract: exact fields the workflow apps publish, and which
  transitions warrant a push (every `custom_status` change vs. only
  gate-waits and terminal states).
- Delivery semantics on reconnect: a stateful session lost on server restart
  drops in-flight notifications — do we need any replay, or is
  at-most-once + client re-list acceptable? (Likely acceptable.)

## Alternatives considered

1. **Client-side polling only** (status quo): zero server work, but no "needs
   input" push and constant re-listing for long-parked instances.
2. **Webhooks / out-of-band channel** (server calls an external URL): breaks
   the single-MCP-session model; the agent would need a second channel to
   correlate. Rejected — elicitation keeps it in-session.

## Out of scope

- Guaranteed/replayed delivery across server restarts.
- Notifications in stateless mode.
- Driving the workflow apps' publishing side (that is an app-repo change;
  this spec only defines what this server consumes and emits).
