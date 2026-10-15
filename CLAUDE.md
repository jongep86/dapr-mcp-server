# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

The contributor playbook (commands, Go conventions, how to add a tool, the safety table, PR flow) is [AGENTS.md](./AGENTS.md). Read it first; this file only adds the orientation AGENTS.md does not give, plus what is specific to this fork.

## Project

A Model Context Protocol (MCP) server in Go that exposes Dapr's building blocks (state, pub/sub, service invocation, secrets, locks, bindings, conversation/LLM, crypto, actors, workflows) as MCP tools for AI agents. Module: `github.com/dapr/dapr-mcp-server`.

This checkout is a fork of `dapr/dapr-mcp-server`. Branch `rebase/upstream` is upstream `main` plus four additions: `get_bulk_state`, the workflow tools, multi-app workflows, and `--stateless`. The first two are also cut as stand-alone branches for upstream PRs; see `docs/handoff/upstream-prs.md`.

## Commands

```bash
# Build
go build -o dapr-mcp-server ./cmd/dapr-mcp-server

# Unit tests (CI runs `go test -v -race ./...`; there is no coverage gate)
go test -race ./...

# Single test
go test -race -run TestName ./pkg/state/

# Go integration tests (behind a build tag, not run by CI)
go test -v -tags integration ./test/integration/

# Lint (config in .golangci.yml; gofmt + goimports with local prefix github.com/dapr/dapr-mcp-server)
golangci-lint run ./...

# gofmt check over the whole tree
./scripts/check-gofmt.sh

# Run locally (requires the Dapr CLI and a one-time `dapr init`)
dapr run --app-id dapr-mcp-server --resources-path resources -- ./dapr-mcp-server --http localhost:8080
```

Every commit needs a DCO sign-off: `git commit -s`.

The `dapr-mcp-server` binary in the repo root is a build artifact; never commit it.

## Architecture

**Entry point (`cmd/dapr-mcp-server/`):** `main.go` parses flags, sets up telemetry and the Dapr client (5 attempts, 2s apart), builds the MCP server and picks the transport: stdio by default, or Streamable HTTP with `--http <addr>`. Logs go to stderr as JSON because stdout carries the JSON-RPC stream in stdio mode. `buildHTTPHandler(ctx, server, stateless, healthChecker, httpMetrics, logger)` wires the health probes (`/livez`, `/readyz`, `/startupz`, always outside auth), a stub `/dapr/subscribe`, and the MCP handler behind the auth and telemetry middleware. `healthcheck.go` implements `--health-check`.

**Stateless mode (fork-only):** `--stateless` or `DAPR_MCP_SERVER_STATELESS=true` sets `mcp.StreamableHTTPOptions.Stateless`. The server then keeps no sessions, so clients survive a server restart, but server-to-client notifications are unavailable. The go-sdk still returns an `Mcp-Session-Id` on `initialize`; the difference is that later requests are not validated against it.

**Tool registration (`cmd/dapr-mcp-server/toolsync.go`, `components.go`):** `registerTools(ctx, server, client, wfClients, toolMetrics, logger)` always registers `metadata`, `invoke`, `actors` and `workflow`, and returns a `toolSyncer`. The syncer registers a building block's tools only while the sidecar reports a component with the matching type prefix (`state.`, `pubsub.`, `bindings.`, `secretstores.`, `lock.`, `conversation.`, `crypto.`, mapped in `componentPrefixes` in `components.go`). Dapr hot-reloads components, so the syncer re-checks before every `tools/list` and after every `get_components`, and removes a block's tools by the names its package's `ToolNames()` returns. A new conditional building block needs an entry in `componentPrefixes`, an entry in `toolsByBlock` and a `ToolNames()`; a new tool in an existing block must be added to that block's `ToolNames()`.

**Tool packages (`pkg/<building-block>/tools.go`):** each building block is its own package with the same shape:

- A narrow, package-local client interface (e.g. `state.StateClient`) with only the Dapr SDK methods the package uses, so tests can mock it.
- An unexported `handler` struct holding that client and a `toolkit.Instrumentation`. There is no package-level client variable.
- `RegisterTools(server *mcp.Server, client <Interface>, metrics *telemetry.ToolMetrics)` builds the handler and calls `mcp.AddTool` per tool. `metrics` may be nil.
- Args structs use `json` + `jsonschema` struct tags; the `jsonschema` text is the argument description the agent sees. Structs with slices, maps or pointers set `InputSchema: toolkit.InputSchema[T]()` to drop `null` from the inferred types.
- Handlers do not open spans by hand. `h.inst.Start(ctx, tool, packageName, attrs...)` returns a `*toolkit.Call`; use `call.Require(...)` for required arguments, `call.Fail(err)` for failures, `call.Succeed(logAttrs...)` on success, and always `defer call.End()`. Failures are returned as a `CallToolResult` with `IsError`, not as a Go error. Never pass payloads or secret values to `Succeed`; it logs them.

**Shared plumbing (`internal/toolkit/`):** `Instrumentation`/`Call` (span, metrics, logging), result helpers (`TextResult`, `ErrorResult`, `MarshalIndent`), and the `Safety` classes (`ReadOnly`, `IdempotentWrite`, `DestructiveIdempotent`, `DestructiveWrite`, `AdditiveWrite`) that produce the MCP tool annotations.

**Safety classes are enforced:** `internal/toolkit/safety_table_test.go` registers every package's tools and checks their annotations against a copy of the "Safety defaults" table in AGENTS.md. A new or reclassified tool needs the `Annotations:` line in its registration, the row in that test, the AGENTS.md table, the README tool table, and the expected tool lists in `cmd/dapr-mcp-server/components_test.go`.

**Workflows (`pkg/workflow/`):** not a component, so always registered. The tools use the durabletask-go workflow client over a sidecar's gRPC connection (`dtworkflow.NewClient(client.GrpcClientConn())`). The signature differs from the other packages: `RegisterTools(server *mcp.Server, clients workflow.Clients, metrics *telemetry.ToolMetrics)`, where `Clients` carries the server's own sidecar (`Default`, `DefaultAppID`) and an optional `ByAppID` pool.

**Multi-app workflows (fork-only):** `DAPR_MCP_SERVER_WORKFLOW_APPS=orders=localhost:50002,billing=localhost:50003` maps additional app-ids to sidecar gRPC addresses. `workflow.ParseAppsConfig` (`pkg/workflow/config.go`) parses it, `cmd/dapr-mcp-server/workflow_apps.go` connects the pool at startup and fails if an app stays unreachable, and `pkg/workflow/registry.go` resolves the `appID` tool argument. With a pool configured, `appID` is required on every workflow tool except `list_workflows`, which fans out over all apps when `appID` is omitted. Design: `docs/specs/2026-07-14-multi-app-workflows.md`. An unstarted follow-up idea is in `docs/specs/2026-07-22-workflow-notifications.md`.

**Server instructions:** `buildInstructions()` in `main.go` produces the safety rules sent to connecting agents as `mcp.ServerOptions.Instructions` (e.g. "run get_components before guessing component names"); `workflowAppsInstruction` appends a line when a workflow pool is configured. Keep these in sync when adding tools.

**Auth (`pkg/auth/`):** an `Authenticator` interface with OIDC (`oidc.go`), SPIFFE (`spiffe.go`) and Dapr Sentry JWT (`sentry.go`) implementations. `AUTH_MODE=disabled|oidc|spiffe|dapr-sentry|hybrid` selects them (`config.go`); in hybrid mode the first authenticator that accepts the token wins (`middleware.go`). Health endpoints never require auth and every other path always does; the old `AUTH_ENABLED` and `AUTH_SKIP_PATHS` variables are rejected at startup.

**Telemetry (`pkg/telemetry/`):** OpenTelemetry traces, metrics and logs. `telemetry.HTTPMiddleware` wraps the MCP handler; `NewToolMetrics` defines the `dapr-mcp-server.tool.*` instruments that `toolkit.Instrumentation` records to.

**Tests:**

- Unit tests live next to the code and are table-driven. `test/mocks/` has the testify mocks (`MockDaprClient`, the workflow client mock, auth mocks).
- `test/integration/` holds `//go:build integration` tests (Dapr Sentry auth against a mock JWKS server).
- `test/app.py` + `test/components/` are a manual end-to-end harness in Python (`cd test && uv sync && uv run python app.py`); it needs a running server and sidecar.

**Dapr components:** `resources/` holds the component YAMLs for local runs via `--resources-path resources`.
