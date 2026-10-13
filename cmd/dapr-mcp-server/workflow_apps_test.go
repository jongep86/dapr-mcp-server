package main

import (
	"context"
	"errors"
	"testing"
	"time"

	dapr "github.com/dapr/go-sdk/client"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConnectWorkflowApps(t *testing.T) {
	t.Parallel()

	t.Run("empty config yields an empty pool", func(t *testing.T) {
		t.Parallel()
		apps, err := connectWorkflowApps(context.Background(), "", nil, time.Millisecond, discardLogger())
		require.NoError(t, err)
		assert.Empty(t, apps.clients)
		assert.Equal(t, []string{"mcp-server"}, apps.appIDs("mcp-server"))
	})

	t.Run("connects every configured app", func(t *testing.T) {
		t.Parallel()
		var dialed []string
		factory := func(_ context.Context, addr string) (dapr.Client, error) {
			dialed = append(dialed, addr)
			return newTestDaprClient(), nil
		}
		apps, err := connectWorkflowApps(context.Background(), "orders=localhost:50002, billing=localhost:50003", factory, time.Millisecond, discardLogger())
		require.NoError(t, err)
		assert.ElementsMatch(t, []string{"localhost:50002", "localhost:50003"}, dialed)
		assert.Equal(t, []string{"billing", "mcp-server", "orders"}, apps.appIDs("mcp-server"))
	})

	t.Run("malformed config is an error before any dial", func(t *testing.T) {
		t.Parallel()
		_, err := connectWorkflowApps(context.Background(), "orders", nil, time.Millisecond, discardLogger())
		require.ErrorContains(t, err, "expected app-id=host:port")
	})

	t.Run("unreachable app is fatal and names the app", func(t *testing.T) {
		t.Parallel()
		factory := func(context.Context, string) (dapr.Client, error) { return nil, errors.New("connection refused") }
		_, err := connectWorkflowApps(context.Background(), "orders=localhost:50002", factory, time.Millisecond, discardLogger())
		require.ErrorContains(t, err, `workflow app "orders" at localhost:50002`)
		require.ErrorContains(t, err, "connection refused")
	})
}

func TestOwnAppID(t *testing.T) {
	t.Parallel()
	withID := newTestDaprClient()
	withID.On("GetMetadata", context.Background()).Return(&dapr.GetMetadataResponse{ID: "mcp-server"}, nil)
	assert.Equal(t, "mcp-server", ownAppID(context.Background(), withID))

	failing := newTestDaprClient()
	failing.On("GetMetadata", context.Background()).Return(nil, errors.New("down"))
	assert.Equal(t, "default", ownAppID(context.Background(), failing))
}

func TestWorkflowAppsInstruction(t *testing.T) {
	t.Parallel()
	assert.Contains(t, workflowAppsInstruction([]string{"billing", "mcp-server"}), "(billing, mcp-server)")
}

func TestWorkflowClients(t *testing.T) {
	t.Parallel()
	factory := func(context.Context, string) (dapr.Client, error) { return newTestDaprClient(), nil }
	apps, err := connectWorkflowApps(context.Background(), "orders=localhost:50002,billing=localhost:50003", factory, time.Millisecond, discardLogger())
	require.NoError(t, err)
	defer apps.Close()

	clients := apps.workflowClients(newTestDaprClient(), "mcp-server")

	assert.NotNil(t, clients.Default)
	assert.Equal(t, "mcp-server", clients.DefaultAppID)
	require.Len(t, clients.ByAppID, 2)
	assert.NotNil(t, clients.ByAppID["orders"])
	assert.NotNil(t, clients.ByAppID["billing"])
}
