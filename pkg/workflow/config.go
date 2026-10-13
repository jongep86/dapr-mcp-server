package workflow

import (
	"fmt"
	"strings"
)

// AppsEnvVar names the environment variable that maps additional workflow
// app-ids to their sidecar gRPC endpoints.
const AppsEnvVar = "DAPR_MCP_SERVER_WORKFLOW_APPS"

// ParseAppsConfig parses the value of DAPR_MCP_SERVER_WORKFLOW_APPS:
// comma-separated app-id=host:port pairs. Whitespace around entries, app-ids
// and addresses is trimmed and empty entries (e.g. a trailing comma) are
// ignored. Malformed entries and duplicate app-ids are errors. An empty value
// yields an empty map (single-app mode).
func ParseAppsConfig(raw string) (map[string]string, error) {
	apps := make(map[string]string)
	for _, entry := range strings.Split(raw, ",") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		appID, addr, found := strings.Cut(entry, "=")
		appID = strings.TrimSpace(appID)
		addr = strings.TrimSpace(addr)
		if !found || appID == "" || addr == "" {
			return nil, fmt.Errorf("invalid %s entry '%s': expected app-id=host:port", AppsEnvVar, entry)
		}
		if _, exists := apps[appID]; exists {
			return nil, fmt.Errorf("duplicate app-id '%s' in %s", appID, AppsEnvVar)
		}
		apps[appID] = addr
	}
	return apps, nil
}
