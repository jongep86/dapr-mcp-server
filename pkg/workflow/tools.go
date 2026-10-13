// Package workflow exposes Dapr Workflow management operations as MCP tools.
package workflow

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/dapr/durabletask-go/api/protos"
	wf "github.com/dapr/durabletask-go/workflow"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"go.opentelemetry.io/otel/attribute"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/dapr/dapr-mcp-server/internal/toolkit"
	"github.com/dapr/dapr-mcp-server/pkg/telemetry"
)

const (
	packageName = "workflow"

	toolGetWorkflowStatus  = "get_workflow_status"
	toolGetWorkflowHistory = "get_workflow_history"
	toolListWorkflows      = "list_workflows"
	toolStartWorkflow      = "start_workflow"
	toolRerunWorkflow      = "rerun_workflow"
	toolPauseWorkflow      = "pause_workflow"
	toolResumeWorkflow     = "resume_workflow"
	toolTerminateWorkflow  = "terminate_workflow"
	toolRaiseWorkflowEvent = "raise_workflow_event"
	toolPurgeWorkflow      = "purge_workflow"

	attrAppID        = "dapr.workflow.app_id"
	attrInstanceID   = "dapr.workflow.instance_id"
	attrWorkflowName = "dapr.workflow.name"
	attrEventName    = "dapr.workflow.event_name"
	attrEventID      = "dapr.workflow.event_id"

	defaultListLimit = 100
	maxListLimit     = 500

	statusPrefix = "ORCHESTRATION_STATUS_"
)

// WorkflowClient defines the workflow operations used by the tools.
// *workflow.Client from durabletask-go satisfies this interface.
type WorkflowClient interface {
	ScheduleWorkflow(ctx context.Context, workflow string, opts ...wf.NewWorkflowOptions) (string, error)
	FetchWorkflowMetadata(ctx context.Context, id string, opts ...wf.FetchWorkflowMetadataOptions) (*wf.WorkflowMetadata, error)
	GetInstanceHistory(ctx context.Context, id string, opts ...wf.GetInstanceHistoryOptions) (*wf.GetInstanceHistoryResponse, error)
	RerunWorkflowFromEvent(ctx context.Context, id string, eventID uint32, opts ...wf.RerunOptions) (string, error)
	ListInstanceIDs(ctx context.Context, opts ...wf.ListInstanceIDsOptions) (*wf.ListInstanceIDsResponse, error)
	SuspendWorkflow(ctx context.Context, id, reason string) error
	ResumeWorkflow(ctx context.Context, id, reason string) error
	TerminateWorkflow(ctx context.Context, id string, opts ...wf.TerminateOptions) error
	RaiseEvent(ctx context.Context, id, eventName string, opts ...wf.RaiseEventOptions) error
	PurgeWorkflowState(ctx context.Context, id string, opts ...wf.PurgeOptions) error
}

// InstanceArgs are the arguments of the tools that take only an instance ID.
type InstanceArgs struct {
	AppID      string `json:"appID,omitempty" jsonschema:"App ID of the workflow application to target. REQUIRED when multiple workflow apps are configured (the server's own app-id is accepted too); may be omitted in single-app setups."`
	InstanceID string `json:"instanceID" jsonschema:"The workflow instance ID."`
}

// ListWorkflowsArgs are the arguments of the list_workflows tool.
type ListWorkflowsArgs struct {
	AppID             string `json:"appID,omitempty" jsonschema:"Optional app ID to list. When multiple workflow apps are configured and this is omitted, all apps are listed."`
	Limit             int    `json:"limit,omitempty" jsonschema:"Maximum number of instances to return per app (default 100, max 500)."`
	ContinuationToken string `json:"continuationToken,omitempty" jsonschema:"Optional continuation token from a previous list_workflows call. In multi-app setups, pass it together with the appID it was returned for."`
}

type handler struct {
	clients registry
	inst    toolkit.Instrumentation
}

// startInstanceCall starts a call for a tool that targets one instance,
// validates the instance ID and resolves the app's client.
// A non-nil result is the validation or resolution failure.
func (h *handler) startInstanceCall(ctx context.Context, tool, appID, instanceID string, attrs ...attribute.KeyValue) (context.Context, *toolkit.Call, target, *mcp.CallToolResult) {
	attrs = append([]attribute.KeyValue{
		attribute.String(attrAppID, appID),
		attribute.String(attrInstanceID, instanceID),
	}, attrs...)
	ctx, call := h.inst.Start(ctx, tool, packageName, attrs...)
	if res := call.Require(toolkit.Field{Name: "instanceID", Value: instanceID}); res != nil {
		return ctx, call, target{}, res
	}
	t, err := h.clients.clientFor(appID)
	if err != nil {
		return ctx, call, target{}, call.Fail(err)
	}
	return ctx, call, t, nil
}

// statusName turns ORCHESTRATION_STATUS_RUNNING into RUNNING.
func statusName(status protos.OrchestrationStatus) string {
	return strings.TrimPrefix(status.String(), statusPrefix)
}

func formatTime(ts *timestamppb.Timestamp) string {
	return ts.AsTime().UTC().Format(time.RFC3339)
}

func (h *handler) getWorkflowStatus(ctx context.Context, _ *mcp.CallToolRequest, args InstanceArgs) (*mcp.CallToolResult, any, error) {
	ctx, call, t, res := h.startInstanceCall(ctx, toolGetWorkflowStatus, args.AppID, args.InstanceID)
	defer call.End()
	if res != nil {
		return res, nil, nil
	}

	meta, err := t.client.FetchWorkflowMetadata(ctx, args.InstanceID, wf.WithFetchPayloads(true))
	if err != nil {
		return call.Fail(fmt.Errorf("get status of workflow instance %q on app %q: %w", args.InstanceID, t.appID, err)), nil, nil
	}

	// wf.WorkflowMetadata is a defined type over the proto message;
	// convert back to use its nil-safe getters.
	pm := (*protos.WorkflowMetadata)(meta)
	status := statusName(pm.GetRuntimeStatus())

	var sb strings.Builder
	fmt.Fprintf(&sb, "Workflow instance '%s' (workflow '%s', app '%s') is %s.", args.InstanceID, pm.GetName(), t.appID, status)
	structured := map[string]any{
		"app_id":        t.appID,
		"instance_id":   args.InstanceID,
		"workflow_name": pm.GetName(),
		"status":        status,
	}
	if pm.GetCreatedAt() != nil {
		structured["created_at"] = formatTime(pm.GetCreatedAt())
	}
	if pm.GetLastUpdatedAt() != nil {
		structured["last_updated_at"] = formatTime(pm.GetLastUpdatedAt())
	}
	if custom := pm.GetCustomStatus().GetValue(); custom != "" {
		structured["custom_status"] = custom
		fmt.Fprintf(&sb, "\nCustom status: %s", custom)
	}
	if output := pm.GetOutput().GetValue(); output != "" {
		structured["output"] = output
		fmt.Fprintf(&sb, "\nOutput: %s", output)
	}
	if failure := pm.GetFailureDetails(); failure != nil {
		structured["failure"] = failure.GetErrorMessage()
		fmt.Fprintf(&sb, "\nFailure: %s", failure.GetErrorMessage())
	}

	call.Succeed("app_id", t.appID, "instance_id", args.InstanceID, "status", status)
	return toolkit.TextResult(sb.String()), structured, nil
}

// describeEvent returns the history event's type (the name of the set
// eventType oneof field, e.g. taskScheduled) and a short detail for the event
// types most useful when diagnosing a workflow.
func describeEvent(ev *protos.HistoryEvent) (string, string) {
	eventType := "unknown"
	msg := ev.ProtoReflect()
	if oneof := msg.Descriptor().Oneofs().ByName("eventType"); oneof != nil {
		if field := msg.WhichOneof(oneof); field != nil {
			eventType = string(field.Name())
		}
	}

	switch {
	case ev.GetExecutionStarted() != nil:
		return eventType, "workflow: " + ev.GetExecutionStarted().GetName()
	case ev.GetExecutionCompleted() != nil:
		completed := ev.GetExecutionCompleted()
		detail := statusName(completed.GetWorkflowStatus())
		if failure := completed.GetFailureDetails(); failure != nil {
			detail += ": " + failure.GetErrorMessage()
		}
		return eventType, detail
	case ev.GetTaskScheduled() != nil:
		return eventType, "activity: " + ev.GetTaskScheduled().GetName()
	case ev.GetTaskFailed() != nil:
		return eventType, "error: " + ev.GetTaskFailed().GetFailureDetails().GetErrorMessage()
	case ev.GetChildWorkflowInstanceCreated() != nil:
		return eventType, "child workflow: " + ev.GetChildWorkflowInstanceCreated().GetName()
	case ev.GetEventRaised() != nil:
		return eventType, "event: " + ev.GetEventRaised().GetName()
	case ev.GetTimerCreated() != nil && ev.GetTimerCreated().GetFireAt() != nil:
		return eventType, "fires at: " + formatTime(ev.GetTimerCreated().GetFireAt())
	}
	return eventType, ""
}

func (h *handler) getWorkflowHistory(ctx context.Context, _ *mcp.CallToolRequest, args InstanceArgs) (*mcp.CallToolResult, any, error) {
	ctx, call, t, res := h.startInstanceCall(ctx, toolGetWorkflowHistory, args.AppID, args.InstanceID)
	defer call.End()
	if res != nil {
		return res, nil, nil
	}

	resp, err := t.client.GetInstanceHistory(ctx, args.InstanceID)
	if err != nil {
		return call.Fail(fmt.Errorf("get history of workflow instance %q on app %q: %w", args.InstanceID, t.appID, err)), nil, nil
	}

	history := (*protos.GetInstanceHistoryResponse)(resp).GetEvents()
	events := make([]map[string]any, 0, len(history))

	var sb strings.Builder
	fmt.Fprintf(&sb, "Workflow instance '%s' on app '%s' has %d history event(s):", args.InstanceID, t.appID, len(history))
	for _, ev := range history {
		eventType, detail := describeEvent(ev)
		event := map[string]any{"event_id": ev.GetEventId(), "type": eventType}
		fmt.Fprintf(&sb, "\n- #%d %s", ev.GetEventId(), eventType)
		if ev.GetTimestamp() != nil {
			event["timestamp"] = formatTime(ev.GetTimestamp())
		}
		if detail != "" {
			event["detail"] = detail
			fmt.Fprintf(&sb, " (%s)", detail)
		}
		events = append(events, event)
	}

	call.Succeed("app_id", t.appID, "instance_id", args.InstanceID, "events", len(events))
	return toolkit.TextResult(sb.String()), map[string]any{
		"app_id":      t.appID,
		"instance_id": args.InstanceID,
		"count":       len(events),
		"events":      events,
	}, nil
}

// instanceSummary is one workflow instance in a list_workflows result.
type instanceSummary struct {
	AppID        string `json:"app_id"`
	InstanceID   string `json:"instance_id"`
	WorkflowName string `json:"workflow_name"`
	Status       string `json:"status"`
	CreatedAt    string `json:"created_at,omitempty"`
}

// listing is one page of an app's workflow instances.
type listing struct {
	appID             string
	instances         []instanceSummary
	continuationToken string
	// skipped counts instances whose metadata could not be fetched.
	skipped int
	// err is set when the app's instances could not be listed at all.
	err error
}

// listPageSize clamps the requested limit to [1, maxListLimit],
// using defaultListLimit when none was given.
func listPageSize(limit int) uint32 {
	switch {
	case limit <= 0:
		return defaultListLimit
	case limit > maxListLimit:
		return maxListLimit
	}
	return uint32(limit) //nolint:gosec // bounded to [1, maxListLimit] by the cases above
}

// listInstances lists one page of t's workflow instances.
// A failure is reported in the listing's err,
// so a fan-out over several apps keeps the results of the others.
func listInstances(ctx context.Context, t target, pageSize uint32, continuationToken string) listing {
	l := listing{appID: t.appID}

	opts := []wf.ListInstanceIDsOptions{wf.WithListInstanceIDsPageSize(pageSize)}
	if continuationToken != "" {
		opts = append(opts, wf.WithListInstanceIDsContinuationToken(continuationToken))
	}
	resp, err := t.client.ListInstanceIDs(ctx, opts...)
	if err != nil {
		l.err = err
		return l
	}

	pr := (*protos.ListInstanceIDsResponse)(resp)
	l.instances = make([]instanceSummary, 0, len(pr.GetInstanceIds()))
	l.continuationToken = pr.GetContinuationToken()
	for _, id := range pr.GetInstanceIds() {
		meta, err := t.client.FetchWorkflowMetadata(ctx, id)
		if err != nil {
			l.skipped++
			continue
		}
		pm := (*protos.WorkflowMetadata)(meta)
		instance := instanceSummary{
			AppID:        t.appID,
			InstanceID:   id,
			WorkflowName: pm.GetName(),
			Status:       statusName(pm.GetRuntimeStatus()),
		}
		if pm.GetCreatedAt() != nil {
			instance.CreatedAt = formatTime(pm.GetCreatedAt())
		}
		l.instances = append(l.instances, instance)
	}
	return l
}

func writeCounts(sb *strings.Builder, label string, counts map[string]int) {
	keys := make([]string, 0, len(counts))
	for k := range counts {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	fmt.Fprintf(sb, "\n%s:", label)
	for _, k := range keys {
		fmt.Fprintf(sb, " %s=%d", k, counts[k])
	}
}

func (h *handler) listWorkflows(ctx context.Context, _ *mcp.CallToolRequest, args ListWorkflowsArgs) (*mcp.CallToolResult, any, error) {
	ctx, call := h.inst.Start(ctx, toolListWorkflows, packageName, attribute.String(attrAppID, args.AppID))
	defer call.End()

	fanOut := args.AppID == "" && h.clients.multiApp()
	var targets []target
	if fanOut {
		if args.ContinuationToken != "" {
			return call.Fail(errors.New("continuationToken requires appID when multiple workflow apps are configured; pass the appID the token was returned for")), nil, nil
		}
		targets = h.clients.targets()
	} else {
		t, err := h.clients.clientFor(args.AppID)
		if err != nil {
			return call.Fail(err), nil, nil
		}
		targets = []target{t}
	}

	// Each goroutine writes only its own index, so no lock is needed.
	pageSize := listPageSize(args.Limit)
	listings := make([]listing, len(targets))
	var wg sync.WaitGroup
	for i, t := range targets {
		wg.Add(1)
		go func() {
			defer wg.Done()
			listings[i] = listInstances(ctx, t, pageSize, args.ContinuationToken)
		}()
	}
	wg.Wait()

	if !fanOut && listings[0].err != nil {
		return call.Fail(fmt.Errorf("list workflow instances on app %q: %w", listings[0].appID, listings[0].err)), nil, nil
	}

	instances := make([]instanceSummary, 0)
	countsByWorkflow := make(map[string]int)
	countsByStatus := make(map[string]int)
	countsByApp := make(map[string]int)
	continuationTokens := make(map[string]string)
	warnings := make(map[string]string)
	for _, l := range listings {
		if l.err != nil {
			warnings[l.appID] = fmt.Sprintf("listing failed, instances of this app are missing: %v", l.err)
			continue
		}
		if l.skipped > 0 {
			warnings[l.appID] = fmt.Sprintf("metadata of %d instance(s) could not be fetched; they are missing", l.skipped)
		}
		if l.continuationToken != "" {
			continuationTokens[l.appID] = l.continuationToken
		}
		countsByApp[l.appID] = len(l.instances)
		for _, instance := range l.instances {
			countsByWorkflow[instance.WorkflowName]++
			countsByStatus[instance.Status]++
			instances = append(instances, instance)
		}
	}

	var sb strings.Builder
	if fanOut {
		fmt.Fprintf(&sb, "Found %d workflow instance(s) across %d app(s).", len(instances), len(targets))
	} else {
		fmt.Fprintf(&sb, "Found %d workflow instance(s) on app '%s'.", len(instances), targets[0].appID)
	}
	if len(instances) > 0 {
		writeCounts(&sb, "Counts by workflow", countsByWorkflow)
		writeCounts(&sb, "Counts by status", countsByStatus)
	}
	if fanOut {
		writeCounts(&sb, "Counts by app", countsByApp)
	}
	for _, instance := range instances {
		fmt.Fprintf(&sb, "\n- %s (app: %s, workflow: %s, status: %s)", instance.InstanceID, instance.AppID, instance.WorkflowName, instance.Status)
	}
	for _, l := range listings {
		if warning, ok := warnings[l.appID]; ok {
			fmt.Fprintf(&sb, "\nWARNING (app '%s'): %s", l.appID, warning)
		}
		if token, ok := continuationTokens[l.appID]; ok {
			fmt.Fprintf(&sb, "\nMore instances are available on app '%s': call list_workflows again with appID '%s' and continuationToken '%s'.", l.appID, l.appID, token)
		}
	}

	structured := map[string]any{
		"instances":          instances,
		"count":              len(instances),
		"counts_by_workflow": countsByWorkflow,
		"counts_by_status":   countsByStatus,
	}
	if fanOut {
		structured["counts_by_app"] = countsByApp
		structured["continuation_tokens"] = continuationTokens
	} else {
		structured["app_id"] = targets[0].appID
		if token := listings[0].continuationToken; token != "" {
			structured["continuation_token"] = token
		}
	}
	if len(warnings) > 0 {
		structured["warnings"] = warnings
	}

	call.Succeed("instances", len(instances), "apps", len(targets), "warnings", len(warnings))
	return toolkit.TextResult(sb.String()), structured, nil
}

// StartWorkflowArgs are the arguments of the start_workflow tool.
type StartWorkflowArgs struct {
	AppID        string `json:"appID,omitempty" jsonschema:"App ID of the workflow application to target. REQUIRED when multiple workflow apps are configured (the server's own app-id is accepted too); may be omitted in single-app setups."`
	WorkflowName string `json:"workflowName" jsonschema:"The name of the workflow to start, as registered by the workflow application (e.g., 'order_processing_workflow')."`
	InstanceID   string `json:"instanceID,omitempty" jsonschema:"Optional unique instance ID for the new workflow. If omitted, Dapr generates one."`
	Input        string `json:"input,omitempty" jsonschema:"Optional input for the workflow, typically a JSON string."`
	StartTime    string `json:"startTime,omitempty" jsonschema:"Optional scheduled start time in RFC 3339 format (e.g., '2026-07-15T06:00:00Z'). If omitted, the workflow starts immediately."`
}

// ReasonArgs are the arguments of the pause_workflow and resume_workflow tools.
type ReasonArgs struct {
	AppID      string `json:"appID,omitempty" jsonschema:"App ID of the workflow application to target. REQUIRED when multiple workflow apps are configured (the server's own app-id is accepted too); may be omitted in single-app setups."`
	InstanceID string `json:"instanceID" jsonschema:"The workflow instance ID."`
	Reason     string `json:"reason,omitempty" jsonschema:"Optional short reason, recorded for operators."`
}

// RerunWorkflowArgs are the arguments of the rerun_workflow tool.
type RerunWorkflowArgs struct {
	AppID         string `json:"appID,omitempty" jsonschema:"App ID of the workflow application to target. REQUIRED when multiple workflow apps are configured (the server's own app-id is accepted too); may be omitted in single-app setups."`
	InstanceID    string `json:"instanceID" jsonschema:"The instance ID of the (typically failed) workflow to rerun."`
	EventID       int64  `json:"eventID" jsonschema:"The event ID from get_workflow_history to rerun from."`
	NewInstanceID string `json:"newInstanceID,omitempty" jsonschema:"Optional instance ID for the new instance. If omitted, Dapr generates one."`
	Input         string `json:"input,omitempty" jsonschema:"Optional replacement input for the rerun, typically a JSON string. If omitted, the original input is reused."`
}

// RaiseWorkflowEventArgs are the arguments of the raise_workflow_event tool.
type RaiseWorkflowEventArgs struct {
	AppID      string `json:"appID,omitempty" jsonschema:"App ID of the workflow application to target. REQUIRED when multiple workflow apps are configured (the server's own app-id is accepted too); may be omitted in single-app setups."`
	InstanceID string `json:"instanceID" jsonschema:"The instance ID of the workflow waiting for the event."`
	EventName  string `json:"eventName" jsonschema:"The event name the workflow is waiting for (must match the name used in the workflow code)."`
	EventData  string `json:"eventData,omitempty" jsonschema:"Optional event payload, typically a JSON string."`
}

func (h *handler) startWorkflow(ctx context.Context, _ *mcp.CallToolRequest, args StartWorkflowArgs) (*mcp.CallToolResult, any, error) {
	ctx, call := h.inst.Start(ctx, toolStartWorkflow, packageName,
		attribute.String(attrAppID, args.AppID),
		attribute.String(attrWorkflowName, args.WorkflowName),
	)
	defer call.End()

	if res := call.Require(toolkit.Field{Name: "workflowName", Value: args.WorkflowName}); res != nil {
		return res, nil, nil
	}

	t, err := h.clients.clientFor(args.AppID)
	if err != nil {
		return call.Fail(err), nil, nil
	}

	var opts []wf.NewWorkflowOptions
	if args.InstanceID != "" {
		opts = append(opts, wf.WithInstanceID(args.InstanceID))
	}
	if args.Input != "" {
		// Raw, because the input already is a JSON string; WithInput would encode it again.
		opts = append(opts, wf.WithRawInput(wrapperspb.String(args.Input)))
	}
	if args.StartTime != "" {
		startTime, parseErr := time.Parse(time.RFC3339, args.StartTime)
		if parseErr != nil {
			return call.Fail(fmt.Errorf("invalid startTime %q: expected RFC 3339 (e.g., '2026-07-15T06:00:00Z'): %w", args.StartTime, parseErr)), nil, nil
		}
		opts = append(opts, wf.WithStartTime(startTime))
	}

	id, err := t.client.ScheduleWorkflow(ctx, args.WorkflowName, opts...)
	if err != nil {
		return call.Fail(fmt.Errorf("start workflow %q on app %q: %w", args.WorkflowName, t.appID, err)), nil, nil
	}

	call.Succeed("app_id", t.appID, "workflow", args.WorkflowName, "instance_id", id)
	structured := map[string]any{"app_id": t.appID, "workflow_name": args.WorkflowName, "instance_id": id}
	if args.StartTime != "" {
		structured["start_time"] = args.StartTime
		text := fmt.Sprintf("Successfully scheduled workflow '%s' with instance ID '%s' to start at %s on app '%s'.", args.WorkflowName, id, args.StartTime, t.appID)
		return toolkit.TextResult(text), structured, nil
	}
	text := fmt.Sprintf("Successfully started workflow '%s' with instance ID '%s' on app '%s'.", args.WorkflowName, id, t.appID)
	return toolkit.TextResult(text), structured, nil
}

// withRawRerunInput sets the rerun input verbatim.
// wf.WithRerunInput would JSON-encode it, double-encoding an input that already is a JSON string.
func withRawRerunInput(input string) wf.RerunOptions {
	return func(req *protos.RerunWorkflowFromEventRequest) error {
		req.Input = wrapperspb.String(input)
		req.OverwriteInput = true
		return nil
	}
}

func (h *handler) rerunWorkflow(ctx context.Context, _ *mcp.CallToolRequest, args RerunWorkflowArgs) (*mcp.CallToolResult, any, error) {
	ctx, call, t, res := h.startInstanceCall(ctx, toolRerunWorkflow, args.AppID, args.InstanceID, attribute.Int64(attrEventID, args.EventID))
	defer call.End()
	if res != nil {
		return res, nil, nil
	}
	if args.EventID < 0 || args.EventID > math.MaxUint32 {
		return call.Fail(fmt.Errorf("invalid eventID %d: use an event ID from get_workflow_history", args.EventID)), nil, nil
	}

	var opts []wf.RerunOptions
	if args.NewInstanceID != "" {
		opts = append(opts, wf.WithRerunNewInstanceID(args.NewInstanceID))
	}
	if args.Input != "" {
		opts = append(opts, withRawRerunInput(args.Input))
	}

	newID, err := t.client.RerunWorkflowFromEvent(ctx, args.InstanceID, uint32(args.EventID), opts...) //nolint:gosec // range-checked above
	if err != nil {
		return call.Fail(fmt.Errorf("rerun workflow instance %q from event %d on app %q: %w", args.InstanceID, args.EventID, t.appID, err)), nil, nil
	}

	call.Succeed("app_id", t.appID, "instance_id", args.InstanceID, "event_id", args.EventID, "new_instance_id", newID)
	text := fmt.Sprintf("Successfully reran workflow instance '%s' from event %d as new instance '%s' on app '%s'.", args.InstanceID, args.EventID, newID, t.appID)
	return toolkit.TextResult(text), map[string]any{"app_id": t.appID, "source_instance_id": args.InstanceID, "event_id": args.EventID, "new_instance_id": newID}, nil
}

func (h *handler) pauseWorkflow(ctx context.Context, _ *mcp.CallToolRequest, args ReasonArgs) (*mcp.CallToolResult, any, error) {
	ctx, call, t, res := h.startInstanceCall(ctx, toolPauseWorkflow, args.AppID, args.InstanceID)
	defer call.End()
	if res != nil {
		return res, nil, nil
	}
	if err := t.client.SuspendWorkflow(ctx, args.InstanceID, args.Reason); err != nil {
		return call.Fail(fmt.Errorf("pause workflow instance %q on app %q: %w", args.InstanceID, t.appID, err)), nil, nil
	}
	call.Succeed("app_id", t.appID, "instance_id", args.InstanceID)
	text := fmt.Sprintf("Successfully paused workflow instance '%s' on app '%s'.", args.InstanceID, t.appID)
	return toolkit.TextResult(text), map[string]any{"app_id": t.appID, "instance_id": args.InstanceID, "status": "SUSPENDED"}, nil
}

func (h *handler) resumeWorkflow(ctx context.Context, _ *mcp.CallToolRequest, args ReasonArgs) (*mcp.CallToolResult, any, error) {
	ctx, call, t, res := h.startInstanceCall(ctx, toolResumeWorkflow, args.AppID, args.InstanceID)
	defer call.End()
	if res != nil {
		return res, nil, nil
	}
	if err := t.client.ResumeWorkflow(ctx, args.InstanceID, args.Reason); err != nil {
		return call.Fail(fmt.Errorf("resume workflow instance %q on app %q: %w", args.InstanceID, t.appID, err)), nil, nil
	}
	call.Succeed("app_id", t.appID, "instance_id", args.InstanceID)
	text := fmt.Sprintf("Successfully resumed workflow instance '%s' on app '%s'.", args.InstanceID, t.appID)
	return toolkit.TextResult(text), map[string]any{"app_id": t.appID, "instance_id": args.InstanceID, "status": "RUNNING"}, nil
}

func (h *handler) terminateWorkflow(ctx context.Context, _ *mcp.CallToolRequest, args InstanceArgs) (*mcp.CallToolResult, any, error) {
	ctx, call, t, res := h.startInstanceCall(ctx, toolTerminateWorkflow, args.AppID, args.InstanceID)
	defer call.End()
	if res != nil {
		return res, nil, nil
	}
	if err := t.client.TerminateWorkflow(ctx, args.InstanceID); err != nil {
		return call.Fail(fmt.Errorf("terminate workflow instance %q on app %q: %w", args.InstanceID, t.appID, err)), nil, nil
	}
	call.Succeed("app_id", t.appID, "instance_id", args.InstanceID)
	text := fmt.Sprintf("Successfully requested termination of workflow instance '%s' on app '%s'.", args.InstanceID, t.appID)
	return toolkit.TextResult(text), map[string]any{"app_id": t.appID, "instance_id": args.InstanceID, "status": "TERMINATED"}, nil
}

func (h *handler) raiseWorkflowEvent(ctx context.Context, _ *mcp.CallToolRequest, args RaiseWorkflowEventArgs) (*mcp.CallToolResult, any, error) {
	ctx, call := h.inst.Start(ctx, toolRaiseWorkflowEvent, packageName,
		attribute.String(attrAppID, args.AppID),
		attribute.String(attrInstanceID, args.InstanceID),
		attribute.String(attrEventName, args.EventName),
	)
	defer call.End()

	if res := call.Require(
		toolkit.Field{Name: "instanceID", Value: args.InstanceID},
		toolkit.Field{Name: "eventName", Value: args.EventName},
	); res != nil {
		return res, nil, nil
	}

	t, err := h.clients.clientFor(args.AppID)
	if err != nil {
		return call.Fail(err), nil, nil
	}

	var opts []wf.RaiseEventOptions
	if args.EventData != "" {
		opts = append(opts, wf.WithRawEventData(wrapperspb.String(args.EventData)))
	}
	if err := t.client.RaiseEvent(ctx, args.InstanceID, args.EventName, opts...); err != nil {
		return call.Fail(fmt.Errorf("raise event %q for workflow instance %q on app %q: %w", args.EventName, args.InstanceID, t.appID, err)), nil, nil
	}
	call.Succeed("app_id", t.appID, "instance_id", args.InstanceID, "event", args.EventName)
	text := fmt.Sprintf("Successfully raised event '%s' for workflow instance '%s' on app '%s'.", args.EventName, args.InstanceID, t.appID)
	return toolkit.TextResult(text), map[string]any{"app_id": t.appID, "instance_id": args.InstanceID, "event_name": args.EventName}, nil
}

func (h *handler) purgeWorkflow(ctx context.Context, _ *mcp.CallToolRequest, args InstanceArgs) (*mcp.CallToolResult, any, error) {
	ctx, call, t, res := h.startInstanceCall(ctx, toolPurgeWorkflow, args.AppID, args.InstanceID)
	defer call.End()
	if res != nil {
		return res, nil, nil
	}
	if err := t.client.PurgeWorkflowState(ctx, args.InstanceID); err != nil {
		return call.Fail(fmt.Errorf("purge workflow instance %q on app %q: %w", args.InstanceID, t.appID, err)), nil, nil
	}
	call.Succeed("app_id", t.appID, "instance_id", args.InstanceID)
	text := fmt.Sprintf("Successfully purged the state of workflow instance '%s' on app '%s'.", args.InstanceID, t.appID)
	return toolkit.TextResult(text), map[string]any{"app_id": t.appID, "instance_id": args.InstanceID, "purged": true}, nil
}

// Clients holds the workflow clients the tools can target.
type Clients struct {
	// Default targets the server's own sidecar.
	Default WorkflowClient
	// DefaultAppID labels Default in results; "default" is used when empty.
	DefaultAppID string
	// ByAppID holds one client per additional workflow app and may be empty.
	ByAppID map[string]WorkflowClient
}

// RegisterTools registers the workflow tools on server.
// metrics may be nil.
func RegisterTools(server *mcp.Server, clients Clients, metrics *telemetry.ToolMetrics) {
	h := &handler{
		clients: newRegistry(clients.Default, clients.DefaultAppID, clients.ByAppID),
		inst:    toolkit.NewInstrumentation(metrics),
	}

	const appIDRule = "**MULTI-APP**: When multiple workflow apps are configured, you MUST pass the `appID` of the app that owns the workflow (the tool error lists the valid app-ids). Use the `app_id` reported by `list_workflows`."

	mcp.AddTool(server, &mcp.Tool{
		Name:  toolGetWorkflowStatus,
		Title: "Get Workflow Instance Status",
		Description: "Fetches the runtime status (RUNNING, COMPLETED, FAILED, SUSPENDED, TERMINATED, PENDING) of a workflow instance with its output, custom status, and failure details. **This is a READ-ONLY action and IS IDEMPOTENT.**\n\n" +
			"**ARGUMENT RULES:**\n" +
			"1. **REQUIRED INPUTS**: You MUST provide a non-empty `instanceID`.\n" +
			"2. **NEVER INVENT**: Take `instanceID` from the user, from `start_workflow`, or from `list_workflows`.\n" +
			"3. " + appIDRule,
		Annotations: toolkit.ReadOnly.Annotations(true),
	}, h.getWorkflowStatus)
	mcp.AddTool(server, &mcp.Tool{
		Name:  toolGetWorkflowHistory,
		Title: "Get Workflow Instance History",
		Description: "Retrieves the event history of a workflow instance: scheduled and completed activities, timers, raised events, and failures. **This is a READ-ONLY action and IS IDEMPOTENT.**\n\n" +
			"**GUIDANCE:**\n" +
			"1. Use this to diagnose WHY a workflow failed or is stuck.\n" +
			"2. The returned `event_id` values can be passed to `rerun_workflow`.\n\n" +
			"**ARGUMENT RULES:**\n" +
			"1. **REQUIRED INPUTS**: You MUST provide a non-empty `instanceID`.\n" +
			"2. " + appIDRule,
		Annotations: toolkit.ReadOnly.Annotations(true),
	}, h.getWorkflowHistory)
	mcp.AddTool(server, &mcp.Tool{
		Name:  toolListWorkflows,
		Title: "List Workflow Instances",
		Description: "Lists workflow instances with their app, workflow name, runtime status, and creation time, plus counts per workflow and per status. **This is a READ-ONLY action and IS IDEMPOTENT.** The tool does NOT filter; filter the returned list yourself.\n\n" +
			"**GUIDANCE:**\n" +
			"1. When multiple workflow apps are configured and `appID` is omitted, all apps are listed with counts per app; one failing app is reported as a warning instead of failing the call.\n" +
			"2. `limit` applies per app. If continuation tokens are returned, call again with the matching `appID` and `continuationToken` before drawing conclusions about totals.\n" +
			"3. Pass the `app_id` of a listed instance to the other workflow tools.",
		Annotations: toolkit.ReadOnly.Annotations(true),
	}, h.listWorkflows)
	mcp.AddTool(server, &mcp.Tool{
		Name:  toolStartWorkflow,
		Title: "Start Workflow Instance",
		Description: "Starts a new Dapr Workflow instance, immediately or at a scheduled `startTime`. **This is a SIDE-EFFECT action that is NOT IDEMPOTENT** unless an explicit `instanceID` is provided.\n\n" +
			"**GUIDANCE:**\n" +
			"1. The workflow must be registered by the targeted app: the app this server's sidecar runs as, or the app named by `appID`.\n" +
			"2. Provide `input` as a JSON string when the workflow expects input.\n" +
			"3. Workflows run asynchronously: use `get_workflow_status` to track progress.\n\n" +
			"**ARGUMENT RULES:**\n" +
			"1. **REQUIRED INPUTS**: You MUST provide a non-empty `workflowName`.\n" +
			"2. **NEVER INVENT**: Do NOT invent workflow names; ask the user if unknown.\n" +
			"3. " + appIDRule,
		Annotations: toolkit.AdditiveWrite.Annotations(true),
	}, h.startWorkflow)
	mcp.AddTool(server, &mcp.Tool{
		Name:  toolRerunWorkflow,
		Title: "Rerun Workflow From Event",
		Description: "Reruns a workflow instance from an event in its history as a NEW instance, reusing the results recorded before that event. **This is a SIDE-EFFECT action that is NOT IDEMPOTENT** unless an explicit `newInstanceID` is provided.\n\n" +
			"**ARGUMENT RULES:**\n" +
			"1. **REQUIRED INPUTS**: You MUST provide a non-empty `instanceID` and an `eventID`.\n" +
			"2. **NEVER INVENT**: Take `eventID` from `get_workflow_history`.\n" +
			"3. " + appIDRule,
		Annotations: toolkit.AdditiveWrite.Annotations(true),
	}, h.rerunWorkflow)
	mcp.AddTool(server, &mcp.Tool{
		Name:  toolPauseWorkflow,
		Title: "Pause Workflow Instance",
		Description: "Pauses (suspends) a running workflow instance until it is resumed with `resume_workflow`. **This is a SIDE-EFFECT action that IS IDEMPOTENT.**\n\n" +
			"**ARGUMENT RULES:**\n" +
			"1. **REQUIRED INPUTS**: You MUST provide a non-empty `instanceID`.\n" +
			"2. " + appIDRule,
		Annotations: toolkit.IdempotentWrite.Annotations(true),
	}, h.pauseWorkflow)
	mcp.AddTool(server, &mcp.Tool{
		Name:  toolResumeWorkflow,
		Title: "Resume Workflow Instance",
		Description: "Resumes a paused (suspended) workflow instance. **This is a SIDE-EFFECT action that IS IDEMPOTENT.**\n\n" +
			"**ARGUMENT RULES:**\n" +
			"1. **REQUIRED INPUTS**: You MUST provide a non-empty `instanceID`.\n" +
			"2. " + appIDRule,
		Annotations: toolkit.IdempotentWrite.Annotations(true),
	}, h.resumeWorkflow)
	mcp.AddTool(server, &mcp.Tool{
		Name:  toolTerminateWorkflow,
		Title: "Terminate Workflow Instance",
		Description: "Forcefully terminates a running workflow instance without executing its remaining steps. **This is a DESTRUCTIVE action that cannot be undone and IS IDEMPOTENT.**\n\n" +
			"**ARGUMENT RULES:**\n" +
			"1. **REQUIRED INPUTS**: You MUST provide a non-empty `instanceID`.\n" +
			"2. **CONFIRMATION**: Unless the user explicitly asked for termination, confirm before calling this tool.\n" +
			"3. " + appIDRule,
		Annotations: toolkit.DestructiveIdempotent.Annotations(true),
	}, h.terminateWorkflow)
	mcp.AddTool(server, &mcp.Tool{
		Name:  toolRaiseWorkflowEvent,
		Title: "Raise Workflow Event",
		Description: "Delivers an external event (e.g., an approval) to a workflow instance waiting for it. **This is a SIDE-EFFECT action that is NOT IDEMPOTENT.**\n\n" +
			"**ARGUMENT RULES:**\n" +
			"1. **REQUIRED INPUTS**: You MUST provide non-empty `instanceID` and `eventName` values.\n" +
			"2. **NEVER INVENT**: `eventName` must exactly match the name the workflow waits for.\n" +
			"3. Provide `eventData` as a JSON string when the workflow expects a payload.\n" +
			"4. " + appIDRule,
		Annotations: toolkit.AdditiveWrite.Annotations(true),
	}, h.raiseWorkflowEvent)
	mcp.AddTool(server, &mcp.Tool{
		Name:  toolPurgeWorkflow,
		Title: "Purge Workflow Instance State",
		Description: "Permanently deletes the state and history of a COMPLETED, FAILED, or TERMINATED workflow instance. **This is a DESTRUCTIVE action that cannot be undone and is NOT IDEMPOTENT.**\n\n" +
			"**ARGUMENT RULES:**\n" +
			"1. **REQUIRED INPUTS**: You MUST provide a non-empty `instanceID`.\n" +
			"2. **CONFIRMATION**: Unless the user explicitly asked for purging, confirm before calling this tool.\n" +
			"3. " + appIDRule,
		Annotations: toolkit.DestructiveWrite.Annotations(true),
	}, h.purgeWorkflow)
}
