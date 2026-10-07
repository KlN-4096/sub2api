package admin

import (
	"encoding/json"
	"testing"

	"github.com/gin-gonic/gin/binding"
	"github.com/stretchr/testify/require"
)

func TestGroupGatewayPoolActiveAccountRequestBounds(t *testing.T) {
	for _, payload := range []string{
		`{"name":"g","openai_gwpool_active_accounts":0}`,
		`{"name":"g","openai_gwpool_active_accounts":-1}`,
		`{"name":"g","openai_gwpool_active_accounts":65}`,
		`{"name":"g","openai_gwpool_active_accounts":1.5}`,
	} {
		for _, request := range []any{&CreateGroupRequest{}, &UpdateGroupRequest{}} {
			err := json.Unmarshal([]byte(payload), request)
			if err == nil {
				err = binding.Validator.ValidateStruct(request)
			}
			require.Error(t, err, payload)
		}
	}
	var create CreateGroupRequest
	require.NoError(t, json.Unmarshal([]byte(`{"name":"g"}`), &create))
	require.Nil(t, create.OpenAIGatewayPoolActiveAccounts)
	require.NoError(t, json.Unmarshal([]byte(`{"name":"g","openai_gwpool_active_accounts":64}`), &create))
	require.NoError(t, binding.Validator.ValidateStruct(&create))
	require.Equal(t, 64, *create.OpenAIGatewayPoolActiveAccounts)
	var update UpdateGroupRequest
	require.NoError(t, json.Unmarshal([]byte(`{}`), &update))
	require.Nil(t, update.OpenAIGatewayPoolActiveAccounts)
}
