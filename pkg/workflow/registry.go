package workflow

import (
	"fmt"
	"sort"
	"strings"
)

// fallbackAppID labels the server's own sidecar when its app-id could not be
// resolved from the metadata API.
const fallbackAppID = "default"

// registry holds the workflow clients: the default client (the server's own
// sidecar) and one client per additional app configured via
// DAPR_MCP_SERVER_WORKFLOW_APPS.
type registry struct {
	defaultClient WorkflowClient
	defaultAppID  string
	byAppID       map[string]WorkflowClient
}

// target is a resolved workflow client together with the app-id it serves.
type target struct {
	appID  string
	client WorkflowClient
}

func newRegistry(defaultClient WorkflowClient, defaultAppID string, byAppID map[string]WorkflowClient) registry {
	if defaultAppID == "" {
		defaultAppID = fallbackAppID
	}
	return registry{
		defaultClient: defaultClient,
		defaultAppID:  defaultAppID,
		byAppID:       byAppID,
	}
}

// multiApp reports whether additional workflow apps are configured.
func (r registry) multiApp() bool {
	return len(r.byAppID) > 0
}

// appIDs returns every app-id accepted as an explicit appID, sorted: the
// server's own app-id plus all configured apps.
func (r registry) appIDs() []string {
	ids := []string{r.defaultAppID}
	for id := range r.byAppID {
		if id != r.defaultAppID {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	return ids
}

// targets returns the clients to query when fanning out over all apps: the
// default client first, then the configured apps in app-id order. A pool
// entry for the server's own app-id replaces the default client so the same
// app is never listed twice.
func (r registry) targets() []target {
	var targets []target
	if _, inPool := r.byAppID[r.defaultAppID]; !inPool {
		targets = append(targets, target{appID: r.defaultAppID, client: r.defaultClient})
	}
	ids := make([]string, 0, len(r.byAppID))
	for id := range r.byAppID {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		targets = append(targets, target{appID: id, client: r.byAppID[id]})
	}
	return targets
}

// clientFor resolves appID to a single workflow client.
//
// Without configured apps, an empty appID selects the default client. With
// configured apps, appID is required, so the caller must name the app
// explicitly: a default would route a per-instance call to a sidecar that
// most likely does not own the instance. A workflow history query
// (GetInstanceHistory) against a sidecar that has no actor state store
// configured made daprd hit a nil pointer dereference and exit
// (dapr/dapr#10217, fixed by the merged dapr/dapr#10218); in a multi-app
// setup that was hit by exactly such a misrouted call. The server's own
// app-id is always accepted. Unknown app-ids produce an error listing the
// valid ones so the agent can self-correct.
func (r registry) clientFor(appID string) (target, error) {
	if appID == "" {
		if r.multiApp() {
			return target{}, fmt.Errorf("appID is required because multiple workflow apps are configured; pass one of: %s", strings.Join(r.appIDs(), ", "))
		}
		return target{appID: r.defaultAppID, client: r.defaultClient}, nil
	}
	if client, ok := r.byAppID[appID]; ok {
		return target{appID: appID, client: client}, nil
	}
	if appID == r.defaultAppID {
		return target{appID: appID, client: r.defaultClient}, nil
	}
	return target{}, fmt.Errorf("unknown appID '%s'; configured app-ids: %s", appID, strings.Join(r.appIDs(), ", "))
}
