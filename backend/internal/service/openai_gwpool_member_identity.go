package service

import (
	"encoding/base64"
	"encoding/json"
	"strings"
)

const (
	openAIGatewayPoolMemberIsolationKey = "openai_gwpool_member_isolation"
	gatewayPoolMemberIdentityPrefix     = "gwpool-member:"
	gatewayPoolMemberReferenceMax       = 128
	gatewayPoolIdentityTokenMax         = 64 << 10
)

// Partitioning only, not token authentication. This opt-in never changes
// ChatGPT headers or the protocol session namespace.
func gatewayPoolMemberIdentity(source, settings *Account) string {
	if source == nil || settings == nil || !settings.getExtraBool(openAIGatewayPoolMemberIsolationKey) {
		return ""
	}
	token := source.GetOpenAIAccessToken()
	if len(token) > gatewayPoolIdentityTokenMax {
		return ""
	}
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return ""
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return ""
	}
	var claims struct {
		Auth struct {
			AccountID string `json:"chatgpt_account_id"`
			UserID    string `json:"chatgpt_user_id"`
		} `json:"https://api.openai.com/auth"`
	}
	if json.Unmarshal(payload, &claims) != nil {
		return ""
	}
	validID := func(value string) bool {
		if value == "" {
			return false
		}
		for _, char := range value {
			if (char >= 'a' && char <= 'z') || (char >= 'A' && char <= 'Z') ||
				(char >= '0' && char <= '9') || char == '-' || char == '_' || char == '.' {
				continue
			}
			return false
		}
		return true
	}
	accountID, userID := claims.Auth.AccountID, claims.Auth.UserID
	if !validID(accountID) || !validID(userID) || len(accountID)+len(userID)+1 > gatewayPoolMemberReferenceMax {
		return ""
	}
	if configured := strings.TrimSpace(source.GetChatGPTAccountID()); configured != "" && configured != accountID {
		return "" // The token does not establish membership in this configured workspace.
	}
	return gatewayPoolMemberIdentityPrefix + accountID + "/" + userID
}
