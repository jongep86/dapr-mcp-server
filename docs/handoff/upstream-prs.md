# Upstream PR and issue drafts

**Opened so far on `dapr/dapr-mcp-server`: issues #90 and #91. No pull request and no comment yet.** The branches are pushed to the fork `jongep86/dapr-mcp-server`. The texts below are meant to be copied into GitHub by hand.

| Draft | Local branch | Base | Commits |
| --- | --- | --- | --- |
| Issue for PR A: dapr/dapr-mcp-server#90 (opened 2026-10-16) | none | | |
| PR A: workflow tools | `upstream-pr/workflow-tools` | `upstream/main` at `f672ba8` | `9441caf`, `5c8914f` |
| Issue for PR B: dapr/dapr-mcp-server#91 (opened 2026-10-16) | none | | |
| PR B: `get_bulk_state` | `upstream-pr/get-bulk-state` | `upstream/main` at `f672ba8` | `bbf2f37`, `bc49bde` |
| Issue: multi-app workflows and stateless HTTP | none (implementation lives on `rebase/upstream`: `65b34ef`, `9e448a1`) | | |

Order: the issue for PR A, then PR A, then the issue for PR B and PR B. The two are independent in code, but the issue refers to PR A, and PR B is small enough to rebase once A has merged. They touch four of the same files, so PR B will need that rebase; see "Before submitting". PR A is single-app: it has no `appID` argument and only talks to the server's own sidecar. The multi-app feature and `--stateless` are not part of either PR; they are what the issue proposes.

The PR bodies follow `.github/pull_request_template.md`. The checklists record what was actually run; see "Before submitting" at the end for what is still open.

---

## Issue for PR A: `[Feature]: workflow management tools`

Template: Feature request. Opened as dapr/dapr-mcp-server#90.

**Area:** workflow

**Describe the feature**

```markdown
The server exposes most Dapr building blocks as tools, but not Dapr Workflow. An agent connected to the server cannot see which workflow instances exist, why one failed or is stuck, or act on it, even though these are common operator questions for long-running processes ("which orders are waiting for approval", "why did this one fail", "retry it from the failed step").

I would like the server to offer tools to inspect and steer the workflow instances of the app its sidecar runs as: list instances, get the status and the event history of one, start one, raise an external event, pause and resume, rerun from an event in the history, terminate, and purge.
```

**Proposed approach**

```markdown
A `pkg/workflow` package in the same shape as the other tool packages (narrow client interface, `handler` with `toolkit.Instrumentation`, safety classes from the `AGENTS.md` table), using the `durabletask-go` workflow client over the gRPC connection the server already holds. Workflows are part of the runtime, not a component, so the tools are always registered, like `invoke` and `actors`. The terminate and purge tools are classed destructive and tell the agent to confirm with the user first.

Scope is the server's own app: Dapr partitions workflows per app-id, so these tools see only the instances of the app the server's sidecar runs as. Reaching other apps' workflows is a separate question that I would raise on its own.

I have an implementation ready and tested against Dapr 1.18.2, and will open it as a PR referencing this issue.
```

**Alternatives considered**

```markdown
- Calling the workflow HTTP API through `invoke_service`: the agent would have to know the Dapr API paths and has no structured history, and destructive calls would not carry the destructive annotations.
- Read-only tools only: covers diagnosis, but not the common follow-up of retrying or unblocking an instance.
```

**Release note**

```markdown
Add workflow tools: list_workflows, get_workflow_status, get_workflow_history, start_workflow, raise_workflow_event, pause_workflow, resume_workflow, rerun_workflow, terminate_workflow and purge_workflow.
```

---

## PR A: `feat(workflow): add workflow management tools`

Branch: `upstream-pr/workflow-tools`

```markdown
# Description

Adds a `pkg/workflow` package with ten tools to inspect and steer Dapr Workflow instances.

| Tool | What it does | Safety class | `AGENTS.md` row |
| --- | --- | --- | --- |
| `list_workflows` | Lists the app's instances with workflow name, status and creation time, plus counts per workflow and per status. Does not fetch inputs, outputs or custom status. `limit` defaults to 100 (max 500); pages with a continuation token. | `ReadOnly` | `read-only`, `idempotent` |
| `get_workflow_status` | Runtime status of one instance with output, custom status and failure details. | `ReadOnly` | `read-only`, `idempotent` |
| `get_workflow_history` | Event history of one instance. An `event_id` is reported only for events that have a sequence ID, which are mainly the scheduling events (`taskScheduled`, `timerCreated`, `childWorkflowInstanceCreated`); that is the ID `rerun_workflow` takes. Outcome events (`taskCompleted`, `taskFailed`, `timerFired` and the child workflow equivalents) have none and report the ID of their scheduling event as `task_scheduled_id` or `timer_id`. | `ReadOnly` | `read-only`, `idempotent` |
| `start_workflow` | Starts an instance, immediately or at an RFC 3339 `startTime`. | `AdditiveWrite` | `not-idempotent`, `side-effect` |
| `raise_workflow_event` | Delivers an external event to a waiting instance. | `AdditiveWrite` | `not-idempotent`, `side-effect` |
| `rerun_workflow` | Reruns an instance from a scheduling event in its history as a new instance. | `AdditiveWrite` | `not-idempotent`, `side-effect` |
| `pause_workflow` | Requests suspension of a running instance. | `IdempotentWrite` | `idempotent`, `side-effect` |
| `resume_workflow` | Requests resumption of a suspended instance. | `IdempotentWrite` | `idempotent`, `side-effect` |
| `terminate_workflow` | Terminates an instance without running its remaining steps. Recursive: the child workflows the instance started are terminated too (the `durabletask-go` client default). | `DestructiveIdempotent` | `destructive`, `idempotent` |
| `purge_workflow` | Deletes the state and history of a finished instance. Not recursive: child workflows keep their state (the client leaves `Recursive` unset on the purge request). | `DestructiveWrite` | `destructive`, `not-idempotent`, `side-effect` |

Design notes:

- **Always registered.** Workflows are part of the Dapr runtime, not a component, so there is no component type to gate on. The tools are registered next to `metadata`, `invoke` and `actors` in `registerTools` (`toolsync.go`), outside the component sync, so they are never removed on a component reload. The package has no `ToolNames()` for the same reason, and the README lists the tools as `Core`.
- **Client.** The tools use the `durabletask-go` workflow client over the gRPC connection the server already holds: `dtworkflow.NewClient(client.GrpcClientConn())`. No second connection is opened.
- **Dependencies.** `github.com/dapr/durabletask-go` moves from the indirect to the direct `require` block at the version that was already resolved (`v0.12.2-0.20260720132056-79042da5fe61`). `google.golang.org/protobuf` moves to the direct block for the same reason, also at its existing version. No versions change and `go.sum` is untouched.
- **Package shape.** Same as the other tool packages: a narrow `WorkflowClient` interface, a `handler` struct with `toolkit.Instrumentation`, `RegisterTools(server, client, metrics)`. The mock is `test/mocks/workflow_client_mock.go`.
- **Payloads.** `input` and `eventData` are forwarded verbatim, because they are already JSON strings and the SDK helpers would encode them a second time. Payloads are not logged; the tests assert this for the `start_workflow` input, the `rerun_workflow` input and the `raise_workflow_event` data, on the success path and when Dapr returns an error.
- **Event IDs.** In the `durabletask-go` history, most events carry event ID -1; a real sequence ID is mainly given to the events that schedule work. `get_workflow_history` therefore leaves a negative ID out instead of printing `#-1`, and shows the back-reference an outcome event carries (`taskScheduledId`, `timerId`), so a `taskFailed` can be traced to the activity that failed. `rerun_workflow` rejects a negative `eventID` with a hint to use the `event_id` of a scheduling event. Against Dapr 1.18.2, a rerun from `taskScheduled` and from `timerCreated` was accepted, and a rerun from `executionCompleted` was rejected by the sidecar with `NotFound: ... is not an event that can be rerun`, which the tool passes on. I did not test every event type, and the descriptions do not claim more than that.
- **No status without a read.** `pause_workflow`, `resume_workflow` and `terminate_workflow` return `"requested": true` in the structured result. They do not report a status, because the tools do not read the state back.
- **Optional IDs.** A whitespace-only `instanceID` on `start_workflow` or `newInstanceID` on `rerun_workflow` counts as omitted, so Dapr generates one.
- **Listing.** `list_workflows` fetches the instance IDs and then the metadata of each instance, so a page of N instances costs N+1 sidecar calls. The metadata is fetched without payloads (`WithFetchPayloads(false)`), because the listing only reports name, status and creation time. An instance whose metadata cannot be fetched is left out and counted in a `warning` field. The tool does not filter; its description says so.
- **Agent guidance.** The server instructions gain a line telling agents to take instance IDs from `start_workflow` or `list_workflows` and to confirm with the user before `terminate_workflow` or `purge_workflow` unless the user explicitly asked for it. The two destructive tool descriptions and the `AGENTS.md` decision tree state the same rule.
- **Docs.** The `AGENTS.md` safety table and decision tree, `internal/toolkit/safety_table_test.go` and the README tool table are updated. The README rows link to anchors in the tool reference on docs.dapr.io that do not exist yet; I will open a follow-up PR against `dapr/docs`.

Scope: only the server's own app is visible. Dapr partitions workflows per app-id, so these tools see and manage the instances of the app the server's sidecar runs as, and `start_workflow` can only start workflows registered by that app. The `list_workflows` description states this. Reaching the workflows of other apps is a separate question that I will raise as a proposal once this PR has landed.

Known runtime bug: a workflow history query (`GetInstanceHistory`) against a sidecar that has no actor state store configured made daprd hit a nil pointer dereference and exit, instead of returning an error. I reported this as dapr/dapr#10217, seen on Dapr 1.18.1, and it is fixed by dapr/dapr#10218, which is merged and shipped in Dapr 1.18.2 (backport dapr/dapr#10229). On a runtime without that fix, `get_workflow_history` can therefore take the sidecar down when the server's sidecar has no actor state store. This PR does not work around it.

The PR has two commits so they can be reviewed separately:

1. `feat(workflow): add read-only workflow tools`: the package, the client wiring, the mock and the three read-only tools.
2. `feat(workflow): add tools to start, steer and purge workflows`: the seven mutating tools and the server-instruction line.

## Issue reference

Closes #90

## Checklist

Please make sure you've completed the relevant tasks for this PR, out of the following list:

* [x] Code compiles correctly (`go build ./...`)
* [x] Created / updated unit tests
* [ ] Created / updated integration tests (under `test/`)
* [x] Linter passes (`golangci-lint run ./...`)
* [x] Commits are signed off per the [DCO](../CONTRIBUTING.md#developer-certificate-of-origin-signing-your-work)
* [x] Updated documentation (README, code comments, or examples)

Notes on the checklist:

- Unit tests: `go test -race ./...` passes. `go test -cover ./pkg/workflow/`, measured locally on this branch, reports 92.5% of statements (CI has no coverage gate). The unit tests run against a mock client; the manual run against a sidecar is in the next note.
- Integration tests: I did not add any, and I did not run the Python harness under `test/` against this branch. I did run the branch by hand against Dapr 1.18.2 (`daprd` in slim mode, an in-memory actor state store, and a small Go workflow app with two activities and an external event). All ten tools behaved as described: start, status, pause and resume, raise event, history, rerun from a `taskScheduled` event with new activity input (the new instance completed), terminate, purge (rejected while the instance was running, accepted after terminate), and `list_workflows` paging with `limit` and `continuationToken`. The history showed IDs only on scheduling events and `executionCompleted`, with `task_scheduled_id` on `taskCompleted` and `taskFailed`.
- `./scripts/check-gofmt.sh` is clean, and `CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath ./cmd/dapr-mcp-server` (the CI build, without its version flag) succeeds.
```

---

## Issue for PR B: `[Feature]: get_bulk_state tool`

Template: Feature request. Opened as dapr/dapr-mcp-server#91.

**Area:** state

**Describe the feature**

```markdown
Reading several keys from a state store takes one `get_state` call per key. Agents that need a handful of related keys (for example an order and its line items) make many sequential tool calls for what Dapr supports as one bulk read. The secrets block already has this pair (`get_secret` and `get_bulk_secrets`); the state block does not.
```

**Proposed approach**

```markdown
A `get_bulk_state` tool next to `get_state`, backed by the SDK's `GetBulkState`. Arguments: `storeName`, `keys` and an optional `parallelism`. Each key gets its own result item with `found`, `value`, `etag` and, when that key failed, `error`, so one bad key does not fail the whole call. Classed `ReadOnly` like `get_state`, and removed together with the other state tools when the last state store goes.

I have an implementation ready and will open it as a PR referencing this issue.
```

**Alternatives considered**

```markdown
- Keep one `get_state` call per key: works, but costs a round trip and a tool call per key.
- Let `get_state` accept a list: changes the shape of an existing tool and its result.
```

**Release note**

```markdown
Add a get_bulk_state tool that reads several keys from a state store in one call.
```

---

## PR B: `feat(state): add get_bulk_state tool`

Branch: `upstream-pr/get-bulk-state`

```markdown
# Description

Adds a `get_bulk_state` tool that reads several keys from a state store in one call. It sits next to `get_state` the same way `get_bulk_secrets` sits next to `get_secret`, so an agent that needs several keys does not have to issue one `get_state` call per key.

What the tool does:

- Arguments: `storeName`, `keys` (a non-empty list of non-empty keys) and an optional `parallelism`, which is passed through to the Dapr SDK's `GetBulkState`. Omitted or `0` lets Dapr choose; a negative value is rejected.
- Each returned item has `key`, `found`, `value` and `etag`. A key that failed carries `error` on its own item, so one bad key does not fail the whole call. Only a failure of the bulk call itself is reported as a tool error.
- The text content starts with a summary line that separates what was asked from what was found, for example `Read 3 key(s) from state store 'statestore' (2 found):`, followed by the items as JSON.
- Values are returned in the text content and in the structured result. They are never logged; `TestGetBulkStateValuesNotLogged` asserts this.
- `get_bulk_state` is added to `state.ToolNames()`, so it is removed together with the other state tools when the last state store is unloaded.
- As with `get_state`, `found` is derived from a non-empty value, so a key stored with an empty value reads as not found.

Classification and docs:

- Safety class `ReadOnly` (read-only, idempotent), open-world like `get_state`. The row is added to the "Safety defaults" table in `AGENTS.md` and to `internal/toolkit/safety_table_test.go`, which checks the annotations against that table.
- `AGENTS.md` decision tree and the README tool table are updated.
- The README row links to a `#get_bulk_state` anchor in the tool reference on docs.dapr.io. That entry does not exist yet. I will open a follow-up PR against `dapr/docs` once the tool's shape is agreed here.

The change is 6 files, +220/-6: `pkg/state/tools.go`, `pkg/state/tools_test.go`, the safety table test, the expected tool list in `cmd/dapr-mcp-server/components_test.go`, `AGENTS.md` and `README.md`. No new dependencies.

## Issue reference

Closes #91

## Checklist

Please make sure you've completed the relevant tasks for this PR, out of the following list:

* [x] Code compiles correctly (`go build ./...`)
* [x] Created / updated unit tests
* [ ] Created / updated integration tests (under `test/`)
* [x] Linter passes (`golangci-lint run ./...`)
* [x] Commits are signed off per the [DCO](../CONTRIBUTING.md#developer-certificate-of-origin-signing-your-work)
* [x] Updated documentation (README, code comments, or examples)

Notes on the checklist:

- Unit tests: `go test -race ./...` passes. `TestGetBulkStateTool` covers the happy path with a key that is not found, a per-key error, a Dapr failure, a missing `storeName`, empty `keys`, a blank key and negative `parallelism`. It asserts the summary line and the structured result (`store_name`, and `key`, `found`, `value`, `etag` and `error` per item).
- Integration tests: I did not add any, and I did not run the Python harness under `test/` against this branch.
- `./scripts/check-gofmt.sh` is clean, and `CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath ./cmd/dapr-mcp-server` (the CI build, without its version flag) succeeds.
```

---

## Issue draft: `Proposal: manage workflows of multiple apps, and a stateless HTTP mode`

`CONTRIBUTING.md` calls this issue type a Proposal. Blank issues are disabled in `.github/ISSUE_TEMPLATE/config.yml`, and the form that fits is "Feature request" (`feature_request.yml`), which prefixes the title with `[Feature]: `. The text below is laid out along that form's fields.

**Area:** `workflow`, `transport (stdio / SSE / HTTP)`

**Describe the feature**

```markdown
This proposal has two parts. They are separable, and I would be glad to hear about either one on its own. Both build on the workflow tools proposed in <link to PR A>.

### 1. Manage the workflows of several apps from one server

Dapr partitions workflow instances per app-id: a sidecar sees and manages only the instances of its own app, and the runtime has no API that lists workflows across apps. The MCP server holds one Dapr client, for its own sidecar. Together this means the workflow tools only ever reach one app.

Operators running several workflow applications, say `orders` and `billing`, therefore need one MCP server per app: N deployments, N endpoints registered with the MCP client, and no single call that answers "what is running, and where is it stuck?".

The first limitation, per-app partitioning, belongs to Dapr and I am not proposing to change it. The second, one connection per server process, lives in this repository. I propose to let one server hold a connection to each configured app's sidecar.

### 2. A stateless mode for streamable HTTP

The streamable HTTP handler keeps sessions in server memory. After the server restarts, a client that sends its old `Mcp-Session-Id` gets a 404 and has to initialize again. For a server that only does request/response tool calls, that session carries nothing the server needs.

I propose an opt-in `--stateless` flag (also `DAPR_MCP_SERVER_STATELESS=true`) that sets `Stateless` on the go-sdk's `StreamableHTTPOptions`. The default stays stateful.
```

**Proposed approach**

```markdown
### 1. Multi-app workflows

**Configuration.** One optional environment variable maps app-ids to sidecar gRPC addresses:

    DAPR_MCP_SERVER_WORKFLOW_APPS=orders=localhost:50002,billing=localhost:50003

Comma-separated `app-id=host:port` pairs. Malformed entries and duplicate app-ids are startup errors. Unset or empty, the server behaves exactly as it does without this feature.

**Connections.** At startup the server connects to every configured sidecar with the same retry policy as its own (5 attempts, 2 seconds apart) and exits if one stays unreachable. The mapping is explicit, so an unreachable entry is treated as a deployment error; skipping it would make `list_workflows` quietly incomplete. The server's own sidecar stays the default, and its app-id is read once from the metadata API so it can be labelled in listings.

**Tool interface.** Every workflow tool gains an `appID` argument.

- No apps configured: `appID` may be omitted and the server's own sidecar is used. Existing single-app setups are unaffected.
- Apps configured: `appID` is required on every workflow tool except `list_workflows`. Omitting it, or passing an unknown one, returns a tool error that lists the valid app-ids, so the agent can correct itself. The server's own app-id is accepted.
- `list_workflows` without `appID` fans out over all apps, merges the results, and reports `app_id` per instance and `counts_by_app`. A failing app does not fail the call; it is reported as a per-app warning and the other apps' results are returned. `limit` applies per app, and continuation tokens are returned per app (`continuation_tokens: {"orders": "..."}`); the next page of one app is fetched by passing its `appID` together with its token.
- Mutating tools never fan out. They always target exactly one app.

**Why `appID` is required in multi-app mode.** It is a guard. Defaulting a per-instance call to the server's own sidecar sends it to an app that most likely does not own the instance. A workflow history query (`GetInstanceHistory`) against a sidecar that has no actor state store configured made daprd hit a nil pointer dereference and exit, instead of returning an error. I reported this as dapr/dapr#10217, seen on Dapr 1.18.1, and it is fixed by dapr/dapr#10218, which is merged and shipped in Dapr 1.18.2 (backport dapr/dapr#10229). I hit it in a multi-app setup, where a per-instance call was routed to a sidecar that did not own the instance and had no actor state store. The server cannot know which runtime version each configured sidecar runs, and even with the fix a silent default would report an error for an instance that exists in another app. Requiring the argument turns both cases into a hint the agent can act on.

**Out of scope.** Merged pagination across apps, discovering workflow apps dynamically (Dapr has no registry to ask), and health-checking the extra connections beyond what gRPC does on its own.

**Open point I would like input on.** `dapr run` assigns a new gRPC port on every run, so the mapping needs fixed ports (`--dapr-grpc-port`) or stable endpoints such as Kubernetes services. If there is a more idiomatic way to address another app's sidecar for workflow management, I would rather use that.

### 2. Stateless HTTP

With `--stateless`, the go-sdk handler does not keep or validate sessions. What I verified against the SDK version this repository uses (`github.com/modelcontextprotocol/go-sdk` v1.6.0):

- The server still returns an `Mcp-Session-Id` header on `initialize`. It just does not remember it.
- A request carrying a session id the server never issued succeeds. In stateful mode the same request is answered with 404. This is the behaviour that lets a client carry on after a server restart.
- The standalone GET stream is not offered (the handler answers 405), so the server cannot push notifications to the client outside of a request.

That last point is the trade-off, and the reason I would keep stateful as the default: a possible later feature that pushes workflow state changes to the client ("instance X is waiting for input") needs the stateful session. I have only a draft for that and am not proposing it here.

I expect stateless mode to also help when running more than one replica without session affinity, but I have not tested that setup.

### Status

A working implementation of both parts exists on a fork branch, with unit tests, and can be opened as a PR, or as two, if the direction is welcome. It builds on the workflow tools PR, so it would follow that one.
```

**Alternatives considered**

```markdown
For multi-app workflows:

1. **One MCP server per app.** This is what works today. No code, but N endpoints to deploy and register, and no cross-app overview in one call.
2. **Read the workflow state store directly** to enumerate instances of all apps. It covers every app in one read, but it couples the server to the workflow engine's internal key format and needs state store credentials outside the sidecar model. I rejected it.
3. **Default to the server's own sidecar when `appID` is omitted in multi-app mode.** Simpler for the agent, but see the guard above: it routes calls to an app that does not own the instance.

For stateless HTTP:

1. **Keep stateful only** and rely on clients to initialize again after a 404. This works for clients that do so.
2. **Make stateless the default.** I did not, because it removes the server-to-client channel for everyone.
```

**Release note**

```markdown
**ADD** `DAPR_MCP_SERVER_WORKFLOW_APPS` to manage workflows of several apps from one server; **ADD** `--stateless` streamable HTTP mode.
```

---

## Before submitting

- [x] `upstream-pr/get-bulk-state`, `upstream-pr/workflow-tools` and `rebase/upstream` are pushed to the fork `jongep86/dapr-mcp-server` (2026-10-13).
- [x] Both branches rebased on `upstream/main` at `f672ba8` (dynamic component tools, #85) on 2026-10-16; `go test -race ./...`, `golangci-lint run ./...` and the linux/amd64 build pass. Rebase again if `upstream/main` moves before opening.
- [ ] Expect a rebase between the two PRs. They touch four shared files: the `AGENTS.md` safety table row, `internal/toolkit/safety_table_test.go`, the README tool table and `cmd/dapr-mcp-server/components_test.go`. Whichever PR merges second has to be rebased on the first. A trial merge of the two branches (`git merge-tree`) reports conflicts in `AGENTS.md` and `components_test.go`, where both add entries to the same lines; the README table and the safety table test merge cleanly. The resolution is to keep the additions of both sides.
- [x] Manual smoke run of PR A against Dapr 1.18.2 (2026-10-16): all ten tools, see the notes on PR A's checklist. Not covered: `terminate_workflow` on an instance with child workflows (the recursive default), and the multi-app layer on `rebase/upstream`.
- [x] Issues opened first, as `CONTRIBUTING.md` asks: #90 for PR A and #91 for PR B (2026-10-16). The PR bodies say `Closes #90` and `Closes #91`.
- [ ] Decide whether to run the Python harness (`cd test && uv sync && uv run python app.py`, needs a running server and sidecar) before opening the PRs. It was not run for either branch, and the "integration tests" box is unchecked in both.
- [ ] The proposal issue waits until PR A has merged. When opening it, replace the placeholder link to PR A in its text with the PR number.
- [ ] Follow-up, not part of these PRs: tool-reference entries in `dapr/docs` for `get_bulk_state` and the ten workflow tools. The README rows already link to those anchors.
- [ ] If the proposal is welcomed: the implementation is `65b34ef` (multi-app) and `9e448a1` (`--stateless`) on `rebase/upstream`. Both sit on top of the workflow tools, so they need a branch cut after PR A.
