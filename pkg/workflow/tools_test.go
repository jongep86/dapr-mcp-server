package workflow

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/dapr/durabletask-go/api/protos"
	wf "github.com/dapr/durabletask-go/workflow"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/dapr/dapr-mcp-server/internal/toolkit"
	"github.com/dapr/dapr-mcp-server/test/mocks"
)

var createdAt = time.Date(2026, 7, 14, 6, 0, 0, 0, time.UTC)

func newTestHandler(t *testing.T) (*handler, *mocks.MockWorkflowClient, *bytes.Buffer) {
	t.Helper()
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	client := new(mocks.MockWorkflowClient)
	t.Cleanup(func() { client.AssertExpectations(t) })
	return &handler{client: client, inst: toolkit.Instrumentation{Logger: logger}}, client, &buf
}

func text(t *testing.T, result *mcp.CallToolResult) string {
	t.Helper()
	require.Len(t, result.Content, 1)
	tc, ok := result.Content[0].(*mcp.TextContent)
	require.True(t, ok)
	return tc.Text
}

func metadata(id, name string, status protos.OrchestrationStatus) *wf.WorkflowMetadata {
	return (*wf.WorkflowMetadata)(&protos.WorkflowMetadata{
		InstanceId:    id,
		Name:          name,
		RuntimeStatus: status,
		CreatedAt:     timestamppb.New(createdAt),
	})
}

func idList(token string, ids ...string) *wf.ListInstanceIDsResponse {
	resp := &protos.ListInstanceIDsResponse{InstanceIds: ids}
	if token != "" {
		resp.ContinuationToken = &token
	}
	return (*wf.ListInstanceIDsResponse)(resp)
}

func TestGetWorkflowStatus(t *testing.T) {
	t.Parallel()

	t.Run("reports status, custom status, output and failure", func(t *testing.T) {
		t.Parallel()
		h, client, _ := newTestHandler(t)
		meta := (*wf.WorkflowMetadata)(&protos.WorkflowMetadata{
			InstanceId:     "order-1",
			Name:           "order",
			RuntimeStatus:  protos.OrchestrationStatus_ORCHESTRATION_STATUS_FAILED,
			CreatedAt:      timestamppb.New(createdAt),
			CustomStatus:   wrapperspb.String("step 3/3"),
			Output:         wrapperspb.String(`{"shipped":false}`),
			FailureDetails: &protos.TaskFailureDetails{ErrorMessage: "boom"},
		})
		client.On("FetchWorkflowMetadata", mock.Anything, "order-1", mock.Anything).Return(meta, nil)

		result, structured, err := h.getWorkflowStatus(context.Background(), &mcp.CallToolRequest{}, InstanceArgs{InstanceID: "order-1"})

		require.NoError(t, err)
		assert.False(t, result.IsError)
		got := text(t, result)
		assert.Contains(t, got, "Workflow instance 'order-1' (workflow 'order') is FAILED.")
		assert.Contains(t, got, "Custom status: step 3/3")
		assert.Contains(t, got, "Failure: boom")
		s, ok := structured.(map[string]any)
		require.True(t, ok)
		assert.Equal(t, "FAILED", s["status"])
		assert.Equal(t, "2026-07-14T06:00:00Z", s["created_at"])
		assert.NotContains(t, s, "last_updated_at", "a nil timestamp is omitted, not rendered as the zero time")
	})

	t.Run("dapr failure", func(t *testing.T) {
		t.Parallel()
		h, client, _ := newTestHandler(t)
		client.On("FetchWorkflowMetadata", mock.Anything, "order-1", mock.Anything).Return(nil, errors.New("not found"))

		result, _, err := h.getWorkflowStatus(context.Background(), &mcp.CallToolRequest{}, InstanceArgs{InstanceID: "order-1"})

		require.NoError(t, err)
		assert.True(t, result.IsError)
		assert.Contains(t, text(t, result), `get status of workflow instance "order-1": not found`)
	})
}

func TestGetWorkflowHistory(t *testing.T) {
	t.Parallel()
	h, client, _ := newTestHandler(t)
	resp := (*wf.GetInstanceHistoryResponse)(&protos.GetInstanceHistoryResponse{Events: []*protos.HistoryEvent{
		{EventId: 0, Timestamp: timestamppb.New(createdAt), EventType: &protos.HistoryEvent_ExecutionStarted{ExecutionStarted: &protos.ExecutionStartedEvent{Name: "order"}}},
		{EventId: 1, EventType: &protos.HistoryEvent_TaskScheduled{TaskScheduled: &protos.TaskScheduledEvent{Name: "reserve"}}},
		{EventId: 2, EventType: &protos.HistoryEvent_TaskFailed{TaskFailed: &protos.TaskFailedEvent{FailureDetails: &protos.TaskFailureDetails{ErrorMessage: "out of stock"}}}},
	}})
	client.On("GetInstanceHistory", mock.Anything, "order-1", mock.Anything).Return(resp, nil)

	result, structured, err := h.getWorkflowHistory(context.Background(), &mcp.CallToolRequest{}, InstanceArgs{InstanceID: "order-1"})

	require.NoError(t, err)
	assert.False(t, result.IsError)
	got := text(t, result)
	assert.Contains(t, got, "has 3 history event(s)")
	assert.Contains(t, got, "#0 executionStarted (workflow: order)")
	assert.Contains(t, got, "#1 taskScheduled (activity: reserve)")
	assert.Contains(t, got, "#2 taskFailed (error: out of stock)")
	s, ok := structured.(map[string]any)
	require.True(t, ok)
	assert.Equal(t, 3, s["count"])
}

func TestInstanceToolsRequireInstanceID(t *testing.T) {
	t.Parallel()
	for _, id := range []string{"", "   "} {
		h, _, _ := newTestHandler(t) // no expectations: the client must not be called
		calls := map[string]func() (*mcp.CallToolResult, any, error){
			"get_workflow_status": func() (*mcp.CallToolResult, any, error) {
				return h.getWorkflowStatus(context.Background(), &mcp.CallToolRequest{}, InstanceArgs{InstanceID: id})
			},
			"get_workflow_history": func() (*mcp.CallToolResult, any, error) {
				return h.getWorkflowHistory(context.Background(), &mcp.CallToolRequest{}, InstanceArgs{InstanceID: id})
			},
		}
		for name, call := range calls {
			result, _, err := call()
			require.NoError(t, err, name)
			assert.True(t, result.IsError, name)
			assert.Contains(t, text(t, result), "missing required argument: instanceID", name)
		}
	}
}

func TestListPageSize(t *testing.T) {
	t.Parallel()
	tests := map[int]uint32{0: 100, -5: 100, 1: 1, 500: 500, 501: 500, 1 << 40: 500}
	for limit, want := range tests {
		assert.Equal(t, want, listPageSize(limit), "limit %d", limit)
	}
}

func TestListWorkflows(t *testing.T) {
	t.Parallel()

	t.Run("lists with counts and continuation token", func(t *testing.T) {
		t.Parallel()
		h, client, _ := newTestHandler(t)
		client.On("ListInstanceIDs", mock.Anything, mock.MatchedBy(func(opts []wf.ListInstanceIDsOptions) bool {
			req := &protos.ListInstanceIDsRequest{}
			for _, opt := range opts {
				require.NoError(t, opt(req))
			}
			return req.GetPageSize() == 2 && req.GetContinuationToken() == "page-1"
		})).Return(idList("page-2", "o-1", "o-2"), nil)
		client.On("FetchWorkflowMetadata", mock.Anything, "o-1", mock.Anything).
			Return(metadata("o-1", "order", protos.OrchestrationStatus_ORCHESTRATION_STATUS_RUNNING), nil)
		client.On("FetchWorkflowMetadata", mock.Anything, "o-2", mock.Anything).
			Return(metadata("o-2", "order", protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED), nil)

		result, structured, err := h.listWorkflows(context.Background(), &mcp.CallToolRequest{}, ListWorkflowsArgs{Limit: 2, ContinuationToken: "page-1"})

		require.NoError(t, err)
		assert.False(t, result.IsError)
		got := text(t, result)
		assert.Contains(t, got, "Found 2 workflow instance(s).")
		assert.Contains(t, got, "Counts by status: COMPLETED=1 RUNNING=1")
		assert.Contains(t, got, "- o-1 (workflow: order, status: RUNNING)")
		assert.Contains(t, got, "continuationToken 'page-2'")
		s, ok := structured.(map[string]any)
		require.True(t, ok)
		assert.Equal(t, "page-2", s["continuation_token"])
		assert.Equal(t, map[string]int{"order": 2}, s["counts_by_workflow"])
	})

	t.Run("empty app returns an empty list, not an error", func(t *testing.T) {
		t.Parallel()
		h, client, _ := newTestHandler(t)
		client.On("ListInstanceIDs", mock.Anything, mock.Anything).Return(idList(""), nil)

		result, structured, err := h.listWorkflows(context.Background(), &mcp.CallToolRequest{}, ListWorkflowsArgs{})

		require.NoError(t, err)
		assert.False(t, result.IsError)
		assert.Contains(t, text(t, result), "Found 0 workflow instance(s).")
		s, ok := structured.(map[string]any)
		require.True(t, ok)
		assert.NotNil(t, s["instances"], "instances is serialized as [], not null")
	})

	t.Run("dapr failure", func(t *testing.T) {
		t.Parallel()
		h, client, _ := newTestHandler(t)
		client.On("ListInstanceIDs", mock.Anything, mock.Anything).Return(nil, errors.New("down"))

		result, _, err := h.listWorkflows(context.Background(), &mcp.CallToolRequest{}, ListWorkflowsArgs{})

		require.NoError(t, err)
		assert.True(t, result.IsError)
		assert.Contains(t, text(t, result), "list workflow instances: down")
	})
}

func TestListWorkflowsSkipsUnfetchableInstances(t *testing.T) {
	t.Parallel()
	h, client, _ := newTestHandler(t)
	client.On("ListInstanceIDs", mock.Anything, mock.Anything).Return(idList("", "o-1", "o-2"), nil)
	client.On("FetchWorkflowMetadata", mock.Anything, "o-1", mock.Anything).
		Return(metadata("o-1", "order", protos.OrchestrationStatus_ORCHESTRATION_STATUS_RUNNING), nil)
	client.On("FetchWorkflowMetadata", mock.Anything, "o-2", mock.Anything).Return(nil, errors.New("gone"))

	result, structured, err := h.listWorkflows(context.Background(), &mcp.CallToolRequest{}, ListWorkflowsArgs{})

	require.NoError(t, err)
	assert.False(t, result.IsError)
	got := text(t, result)
	assert.Contains(t, got, "Found 1 workflow instance(s).")
	assert.Contains(t, got, "WARNING: metadata of 1 instance(s) could not be fetched; they are missing from this list.")
	s, ok := structured.(map[string]any)
	require.True(t, ok)
	assert.Contains(t, s, "warning")
}
