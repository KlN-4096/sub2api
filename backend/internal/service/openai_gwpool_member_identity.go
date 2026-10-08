package service

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/Wei-Shaw/sub2api/internal/pkg/gwpool"
)

const (
	gatewayPoolMemberIdentityPrefix = "gwpool-member:"
	gatewayPoolMemberReferenceMax   = 128
	gatewayPoolIdentityTokenMax     = 64 << 10
)

var errGatewayPoolMemberIdentity = fmt.Errorf("%w: gateway pool requires a consistent upstream account and member identity", gwpool.ErrPool)

// This partitions pool state, not protocol sessions or token authentication.
// Persisted OAuth/PAT metadata survives bearer refresh and opaque token formats.
// JWT claims may complete it, but may not silently replace a conflicting member.
func gatewayPoolMemberIdentity(source *Account) (string, error) {
	if source == nil {
		return "", errGatewayPoolMemberIdentity
	}
	accountID := strings.TrimSpace(source.GetChatGPTAccountID())
	userID := strings.TrimSpace(source.GetChatGPTUserID())
	token := source.GetOpenAIAccessToken()
	if len(token) > gatewayPoolIdentityTokenMax {
		return "", errGatewayPoolMemberIdentity
	}
	parts := strings.Split(token, ".")
	if len(parts) == 3 {
		payload, err := base64.RawURLEncoding.DecodeString(parts[1])
		var claims struct {
			Auth struct {
				AccountID string `json:"chatgpt_account_id"`
				UserID    string `json:"chatgpt_user_id"`
			} `json:"https://api.openai.com/auth"`
		}
		if err == nil && json.Unmarshal(payload, &claims) == nil {
			for _, field := range []struct {
				saved *string
				claim string
			}{{&accountID, claims.Auth.AccountID}, {&userID, claims.Auth.UserID}} {
				if field.claim == "" {
					continue
				}
				if *field.saved != "" && *field.saved != field.claim {
					return "", errGatewayPoolMemberIdentity
				}
				*field.saved = field.claim
			}
		}
	}
	if !validGatewayPoolMemberID(accountID) || !validGatewayPoolMemberID(userID) ||
		len(accountID)+len(userID)+1 > gatewayPoolMemberReferenceMax {
		return "", errGatewayPoolMemberIdentity
	}
	return gatewayPoolMemberIdentityPrefix + accountID + "/" + userID, nil
}

func validGatewayPoolMemberID(value string) bool {
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
