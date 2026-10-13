package workflow

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/dapr-mcp-server/test/mocks"
)

func TestNewRegistryFallbackAppID(t *testing.T) {
	t.Parallel()
	r := newRegistry(new(mocks.MockWorkflowClient), "", nil)
	assert.Equal(t, "default", r.defaultAppID)
}

func TestClientForSingleApp(t *testing.T) {
	t.Parallel()
	def := new(mocks.MockWorkflowClient)
	r := newRegistry(def, "mcp-server", nil)

	t.Run("empty appID selects the default client", func(t *testing.T) {
		t.Parallel()
		got, err := r.clientFor("")
		require.NoError(t, err)
		assert.Same(t, def, got.client)
		assert.Equal(t, "mcp-server", got.appID)
	})

	t.Run("own app-id selects the default client", func(t *testing.T) {
		t.Parallel()
		got, err := r.clientFor("mcp-server")
		require.NoError(t, err)
		assert.Same(t, def, got.client)
	})

	t.Run("unknown appID lists the configured app-ids", func(t *testing.T) {
		t.Parallel()
		_, err := r.clientFor("other")
		require.Error(t, err)
		assert.Equal(t, "unknown appID 'other'; configured app-ids: mcp-server", err.Error())
	})
}

func TestClientForMultiApp(t *testing.T) {
	t.Parallel()
	def := new(mocks.MockWorkflowClient)
	appA := new(mocks.MockWorkflowClient)
	appB := new(mocks.MockWorkflowClient)
	r := newRegistry(def, "mcp-server", map[string]WorkflowClient{"app-b": appB, "app-a": appA})

	t.Run("empty appID is rejected", func(t *testing.T) {
		t.Parallel()
		_, err := r.clientFor("")
		require.Error(t, err)
		assert.Equal(t, "appID is required because multiple workflow apps are configured; pass one of: app-a, app-b, mcp-server", err.Error())
	})

	t.Run("known app", func(t *testing.T) {
		t.Parallel()
		got, err := r.clientFor("app-b")
		require.NoError(t, err)
		assert.Same(t, appB, got.client)
		assert.Equal(t, "app-b", got.appID)
	})

	t.Run("own app-id selects the default client", func(t *testing.T) {
		t.Parallel()
		got, err := r.clientFor("mcp-server")
		require.NoError(t, err)
		assert.Same(t, def, got.client)
	})

	t.Run("unknown app", func(t *testing.T) {
		t.Parallel()
		_, err := r.clientFor("app-c")
		require.Error(t, err)
		assert.Equal(t, "unknown appID 'app-c'; configured app-ids: app-a, app-b, mcp-server", err.Error())
	})
}

func TestTargets(t *testing.T) {
	t.Parallel()
	def := new(mocks.MockWorkflowClient)
	appA := new(mocks.MockWorkflowClient)
	appB := new(mocks.MockWorkflowClient)

	t.Run("default first, then apps sorted", func(t *testing.T) {
		t.Parallel()
		r := newRegistry(def, "mcp-server", map[string]WorkflowClient{"app-b": appB, "app-a": appA})
		targets := r.targets()
		require.Len(t, targets, 3)
		assert.Equal(t, "mcp-server", targets[0].appID)
		assert.Same(t, def, targets[0].client)
		assert.Equal(t, "app-a", targets[1].appID)
		assert.Equal(t, "app-b", targets[2].appID)
	})

	t.Run("pool entry for the own app-id replaces the default client", func(t *testing.T) {
		t.Parallel()
		r := newRegistry(def, "app-a", map[string]WorkflowClient{"app-a": appA, "app-b": appB})
		targets := r.targets()
		require.Len(t, targets, 2)
		assert.Same(t, appA, targets[0].client)
		assert.Equal(t, []string{"app-a", "app-b"}, r.appIDs())

		got, err := r.clientFor("app-a")
		require.NoError(t, err)
		assert.Same(t, appA, got.client)
	})
}
