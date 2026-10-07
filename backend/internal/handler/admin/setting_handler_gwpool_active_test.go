package admin

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func TestSettingHandlerGatewayPoolActiveAccounts(t *testing.T) {
	for _, tc := range []struct {
		name, initial, body, stored string
		status                      int
	}{
		{"default", "", `{}`, "", 200},
		{"preserve", "2", `{}`, "2", 200},
		{"set", "", `{"openai_gwpool_active_accounts":2}`, "", 400},
		{"zero", "", `{"openai_gwpool_active_accounts":0}`, "", 400},
		{"negative", "", `{"openai_gwpool_active_accounts":-1}`, "", 400},
		{"excess", "", `{"openai_gwpool_active_accounts":65}`, "", 400},
		{"fraction", "", `{"openai_gwpool_active_accounts":1.5}`, "", 400},
	} {
		t.Run(tc.name, func(t *testing.T) {
			repo := &settingHandlerRepoStub{values: map[string]string{service.SettingKeyOpenAIGatewayPoolActiveAccounts: tc.initial}}
			handler := NewSettingHandler(service.NewSettingService(repo, &config.Config{}), nil, nil, nil, nil, nil, nil)
			rec := httptest.NewRecorder()
			ctx, _ := gin.CreateTestContext(rec)
			ctx.Request = httptest.NewRequest(http.MethodPut, "/api/v1/admin/settings", bytes.NewBufferString(tc.body))
			ctx.Request.Header.Set("Content-Type", "application/json")
			handler.UpdateSettings(ctx)
			require.Equal(t, tc.status, rec.Code, rec.Body.String())
			if tc.status != 200 {
				require.Empty(t, repo.lastUpdates)
				return
			}
			require.Equal(t, tc.stored, repo.values[service.SettingKeyOpenAIGatewayPoolActiveAccounts])
			var body struct {
				Data map[string]any `json:"data"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
			require.NotContains(t, body.Data, service.SettingKeyOpenAIGatewayPoolActiveAccounts)
		})
	}
}
