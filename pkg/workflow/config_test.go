package workflow

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseAppsConfig(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		raw     string
		want    map[string]string
		wantErr string
	}{
		{
			name: "empty",
			raw:  "",
			want: map[string]string{},
		},
		{
			name: "whitespace only",
			raw:  "  ",
			want: map[string]string{},
		},
		{
			name: "single app",
			raw:  "orders=localhost:50002",
			want: map[string]string{"orders": "localhost:50002"},
		},
		{
			name: "multiple apps with whitespace and trailing comma",
			raw:  " orders = localhost:50002 , billing=localhost:50003, ",
			want: map[string]string{
				"orders":  "localhost:50002",
				"billing": "localhost:50003",
			},
		},
		{
			name:    "missing separator",
			raw:     "orders",
			wantErr: "invalid DAPR_MCP_SERVER_WORKFLOW_APPS entry 'orders'",
		},
		{
			name:    "missing app-id",
			raw:     "=localhost:50002",
			wantErr: "expected app-id=host:port",
		},
		{
			name:    "missing address",
			raw:     "orders=",
			wantErr: "expected app-id=host:port",
		},
		{
			name:    "duplicate app-id",
			raw:     "a=localhost:1,b=localhost:2, a=localhost:3",
			wantErr: "duplicate app-id 'a'",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := ParseAppsConfig(tt.raw)
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				assert.Nil(t, got)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}
