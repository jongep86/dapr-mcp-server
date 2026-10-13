package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sort"
	"strings"
	"time"

	dtworkflow "github.com/dapr/durabletask-go/workflow"
	dapr "github.com/dapr/go-sdk/client"

	"github.com/dapr/dapr-mcp-server/pkg/workflow"
)

const fallbackWorkflowAppID = "default"

// workflowApps holds the Dapr clients of the additional workflow apps
// configured in DAPR_MCP_SERVER_WORKFLOW_APPS.
type workflowApps struct {
	clients map[string]dapr.Client
}

// daprAddressClientFactory creates a Dapr client for a sidecar address.
// It is a parameter so tests can avoid real sidecars.
type daprAddressClientFactory func(ctx context.Context, addr string) (dapr.Client, error)

func newDaprClientWithAddress(ctx context.Context, addr string) (dapr.Client, error) {
	return dapr.NewClientWithAddressContext(ctx, addr)
}

// connectWorkflowApps connects to the sidecar of every configured workflow app,
// with the same retry policy as the primary client.
// An unreachable app is an error: the mapping is explicit,
// so skipping it would make list_workflows silently incomplete.
func connectWorkflowApps(ctx context.Context, raw string, newClient daprAddressClientFactory, retryDelay time.Duration, logger *slog.Logger) (*workflowApps, error) {
	addrs, err := workflow.ParseAppsConfig(raw)
	if err != nil {
		return nil, err
	}
	apps := &workflowApps{clients: make(map[string]dapr.Client, len(addrs))}
	for appID, addr := range addrs {
		client, err := initializeDaprClient(ctx, func() (dapr.Client, error) { return newClient(ctx, addr) }, retryDelay, logger)
		if err != nil {
			apps.Close()
			return nil, fmt.Errorf("workflow app %q at %s: %w", appID, addr, err)
		}
		apps.clients[appID] = client
	}
	return apps, nil
}

// Close closes every pool connection.
func (a *workflowApps) Close() {
	for _, client := range a.clients {
		client.Close()
	}
}

// appIDs returns the configured app-ids plus own, sorted.
func (a *workflowApps) appIDs(own string) []string {
	ids := []string{own}
	for id := range a.clients {
		if id != own {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	return ids
}

// workflowClients builds the clients the workflow tools target:
// the server's own sidecar, labeled ownID, plus one per configured app.
func (a *workflowApps) workflowClients(own dapr.Client, ownID string) workflow.Clients {
	byAppID := make(map[string]workflow.WorkflowClient, len(a.clients))
	for id, client := range a.clients {
		byAppID[id] = dtworkflow.NewClient(client.GrpcClientConn())
	}
	return workflow.Clients{
		Default:      dtworkflow.NewClient(own.GrpcClientConn()),
		DefaultAppID: ownID,
		ByAppID:      byAppID,
	}
}

// ownAppID resolves the app-id of the server's own sidecar,
// falling back to "default" when the metadata API does not report one.
func ownAppID(ctx context.Context, client dapr.Client) string {
	meta, err := client.GetMetadata(ctx)
	if err != nil || meta == nil || meta.ID == "" {
		return fallbackWorkflowAppID
	}
	return meta.ID
}

func workflowAppsInstruction(appIDs []string) string {
	return fmt.Sprintf("- **Workflow Apps**: Multiple workflow apps are configured (%s). Every workflow tool except `list_workflows` REQUIRES `appID`; run `list_workflows` without `appID` to see which app owns which instance.\n", strings.Join(appIDs, ", "))
}

func workflowAppsFromEnv(ctx context.Context, logger *slog.Logger) (*workflowApps, error) {
	return connectWorkflowApps(ctx, os.Getenv(workflow.AppsEnvVar), newDaprClientWithAddress, daprClientRetryDelay, logger)
}
