package service

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
)

const (
	cprAdminMaxBody      = 1 << 20
	cprAdminMaxPages     = 100
	cprManagedGroupColor = "#3B82F6FF"
)

// Serializes provisioning within this process. CPR has no cross-process
// idempotency contract; stable names let retries recover partial provisioning.
var cprProvisionMu sync.Mutex

type CPRConnectAccount struct {
	ID               string `json:"id"`
	Name             string `json:"name"`
	Email            string `json:"email"`
	PlanType         string `json:"plan_type"`
	Status           string `json:"status"`
	Enabled          bool   `json:"enabled"`
	ConcurrencyLimit *int   `json:"concurrency_limit"`
}

type cprGroupRef struct {
	ID      string `json:"id"`
	Enabled bool   `json:"enabled"`
}

type cprConnectAccountView struct {
	cprAccountView
	Provider         string          `json:"provider"`
	ConcurrencyLimit *int            `json:"concurrencyLimit"`
	Weight           int             `json:"weight"`
	Groups           []cprGroupRef   `json:"groups"`
	Notes            *string         `json:"notes"`
	ModelAccess      json.RawMessage `json:"modelAccess"`
	OutboundProxyID  *string         `json:"outboundProxyId"`
}

type cprConnectGroup struct {
	cprGroupRef
	Name        string `json:"name"`
	Description string `json:"description"`
	MemberCount int    `json:"memberCount"`
}

type cprConnectKey struct {
	ID           string        `json:"id"`
	Enabled      bool          `json:"enabled"`
	RoutingScope string        `json:"routingScope"`
	Groups       []cprGroupRef `json:"groups"`
}

// adminJSON never includes upstream messages or transport errors in its errors:
// a peer can reflect a key, URL credentials, or response secrets.
func (s *CPRQuotaService) adminJSON(ctx context.Context, base, key, method, path string, query url.Values, input, output any) error {
	base, err := validateOutboundURLWithConfig(s.cfg, base)
	if err != nil || strings.TrimSpace(key) == "" {
		return ErrCPRNotConfigured
	}
	var body bytes.Buffer
	if input != nil {
		if err := json.NewEncoder(&body).Encode(input); err != nil {
			return errors.New("encode cpr admin request failed")
		}
	}
	endpoint := buildOpenAIEndpointURL(strings.TrimRight(base, "/"), path)
	if len(query) > 0 {
		endpoint += "?" + query.Encode()
	}
	req, err := http.NewRequestWithContext(ctx, method, endpoint, &body)
	if err != nil {
		return ErrCPRNotConfigured
	}
	req.Header.Set(cprAdminAPIKeyHeader, strings.TrimSpace(key))
	req.Header.Set("Accept", "application/json")
	if input != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return errors.New("cpr admin request failed")
	}
	defer func() { _ = resp.Body.Close() }()
	data, err := io.ReadAll(io.LimitReader(resp.Body, cprAdminMaxBody+1))
	if err != nil || len(data) > cprAdminMaxBody {
		return errors.New("invalid cpr admin response size")
	}
	var envelope cprAdminEnvelope
	if json.Unmarshal(data, &envelope) != nil {
		return errors.New("invalid cpr admin response")
	}
	if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden || envelope.Code == 40101 || envelope.Code == 40103 {
		return ErrCPRAdminKeyInvalid
	}
	if resp.StatusCode == http.StatusNotFound || envelope.Code == 40401 {
		return ErrCPRAccountMissing
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 || envelope.Code != http.StatusOK {
		return fmt.Errorf("cpr admin operation failed (HTTP %d, code %d)", resp.StatusCode, envelope.Code)
	}
	if output != nil && json.Unmarshal(envelope.Data, output) != nil {
		return errors.New("invalid cpr admin data")
	}
	return nil
}

func cprListPages[T any](ctx context.Context, s *CPRQuotaService, base, key, path string, filters url.Values) ([]T, error) {
	var items []T
	for page := 1; page <= cprAdminMaxPages; page++ {
		var data struct {
			Items []T `json:"items"`
			Page  struct {
				TotalPages int `json:"totalPages"`
			} `json:"page"`
		}
		query := url.Values{"page": {strconv.Itoa(page)}, "pageSize": {"200"}}
		for name, values := range filters {
			query[name] = values
		}
		err := s.adminJSON(ctx, base, key, http.MethodGet, path, query, nil, &data)
		if err != nil {
			return nil, err
		}
		items = append(items, data.Items...)
		if data.Page.TotalPages == 0 && len(data.Items) == 0 {
			return items, nil
		}
		if data.Page.TotalPages < 1 {
			return nil, errors.New("cpr pagination metadata missing")
		}
		if page >= data.Page.TotalPages {
			return items, nil
		}
	}
	return nil, errors.New("cpr account directory too large")
}

func (s *CPRQuotaService) DiscoverAccounts(ctx context.Context, base, key string) ([]CPRConnectAccount, error) {
	accounts, err := cprListPages[cprConnectAccountView](ctx, s, base, key, "/api/admin/accounts", url.Values{"provider": {"openai"}})
	if err != nil {
		return nil, err
	}
	result := make([]CPRConnectAccount, 0, len(accounts))
	for _, a := range accounts {
		if a.Provider == "openai" {
			result = append(result, CPRConnectAccount{a.ID, a.Name, a.Email, a.PlanType, a.Status, a.Enabled, a.ConcurrencyLimit})
		}
	}
	return result, nil
}

func (s *CPRQuotaService) connectAccount(ctx context.Context, base, key, id string) (*cprConnectAccountView, error) {
	var data struct {
		Account cprConnectAccountView `json:"account"`
	}
	if err := s.adminJSON(ctx, base, key, http.MethodGet, "/api/admin/accounts/detail", url.Values{"accountId": {id}}, nil, &data); err != nil {
		return nil, err
	}
	if data.Account.ID != id || data.Account.Provider != "openai" {
		return nil, errors.New("selected cpr account is not an OpenAI account")
	}
	return &data.Account, nil
}

func cprHasGroup(account *cprConnectAccountView, id string) bool {
	for _, group := range account.Groups {
		if group.ID == id {
			return true
		}
	}
	return false
}

func (s *CPRQuotaService) connectKeys(ctx context.Context, base, key string) ([]cprConnectKey, error) {
	var result []cprConnectKey
	cursor := ""
	for page := 0; page < cprAdminMaxPages; page++ {
		query := url.Values{"limit": {"200"}}
		if cursor != "" {
			query.Set("cursor", cursor)
		}
		var data struct {
			Items      []cprConnectKey `json:"items"`
			NextCursor string          `json:"nextCursor"`
		}
		if err := s.adminJSON(ctx, base, key, http.MethodGet, "/api/admin/client-keys", query, nil, &data); err != nil {
			return nil, err
		}
		result = append(result, data.Items...)
		if data.NextCursor == "" {
			return result, nil
		}
		if data.NextCursor == cursor {
			return nil, errors.New("invalid cpr client key cursor")
		}
		cursor = data.NextCursor
	}
	return nil, errors.New("cpr client key directory too large")
}

// EnsureClientKey only reuses a single-account group and a key scoped solely to
// that group. It never widens existing keys, removes groups, or changes policies.
func (s *CPRQuotaService) EnsureClientKey(ctx context.Context, account *Account) (string, error) {
	if account == nil || !account.IsCPR() || account.Platform != PlatformOpenAI || account.GetCPRAccountID() == "" {
		return "", ErrCPRNotConfigured
	}
	if _, err := validateOutboundURLWithConfig(s.cfg, account.GetCPRGatewayBaseURL()); err != nil {
		return "", ErrCPRNotConfigured
	}
	cprProvisionMu.Lock()
	defer cprProvisionMu.Unlock()
	if err := ctx.Err(); err != nil {
		return "", err
	}
	base, key, id := account.GetCPRAdminBaseURL(), account.GetCPRAdminAPIKey(), account.GetCPRAccountID()
	selected, err := s.connectAccount(ctx, base, key, id)
	if err != nil {
		return "", err
	}
	groups, err := cprListPages[cprConnectGroup](ctx, s, base, key, "/api/admin/account-groups", nil)
	if err != nil {
		return "", err
	}
	groupID := ""
	for _, group := range groups {
		if group.Enabled && group.MemberCount == 1 && cprHasGroup(selected, group.ID) {
			groupID = group.ID
			break
		}
	}
	if groupID == "" {
		digest := sha256.Sum256([]byte(id))
		name := "sub2-" + hex.EncodeToString(digest[:12])
		description := "sub2api account " + id
		for _, group := range groups {
			// Recover only our own empty group after an interrupted save.
			if group.Enabled && group.MemberCount == 0 && group.Name == name && group.Description == description {
				groupID = group.ID
				break
			}
		}
		if groupID == "" {
			var created struct {
				ID string `json:"id"`
			}
			err = s.adminJSON(ctx, base, key, http.MethodPost, "/api/admin/account-groups/create", nil,
				map[string]any{"name": name, "description": description, "color": cprManagedGroupColor}, &created)
			if err != nil {
				return "", err
			}
			groupID = created.ID
		}
		if groupID == "" {
			return "", errors.New("cpr group creation returned no id")
		}
		// Read immediately before the read/modify/write; preserve all non-group settings.
		selected, err = s.connectAccount(ctx, base, key, id)
		if err != nil {
			return "", err
		}
		groupIDs := make([]string, 0, len(selected.Groups)+1)
		for _, group := range selected.Groups {
			groupIDs = append(groupIDs, group.ID)
		}
		if !cprHasGroup(selected, groupID) {
			groupIDs = append(groupIDs, groupID)
		}
		err = s.adminJSON(ctx, base, key, http.MethodPost, "/api/admin/accounts/update", nil, map[string]any{
			"accountId": id, "enabled": selected.Enabled, "concurrencyLimit": selected.ConcurrencyLimit,
			"weight": selected.Weight, "groupIds": groupIDs, "notes": selected.Notes,
			"modelAccess": selected.ModelAccess, "outboundProxyId": selected.OutboundProxyID,
		}, nil)
		if err != nil {
			return "", err
		}
	}
	// Recheck the scope before revealing/creating a secret, including retry recovery.
	groups, err = cprListPages[cprConnectGroup](ctx, s, base, key, "/api/admin/account-groups", nil)
	if err != nil {
		return "", err
	}
	selected, err = s.connectAccount(ctx, base, key, id)
	if err != nil {
		return "", err
	}
	safe := false
	for _, group := range groups {
		if group.ID == groupID && group.Enabled && group.MemberCount == 1 && cprHasGroup(selected, groupID) {
			safe = true
		}
	}
	if !safe {
		return "", errors.New("cpr group must contain only the selected account")
	}
	keys, err := s.connectKeys(ctx, base, key)
	if err != nil {
		return "", err
	}
	var secret struct {
		PlaintextKey string `json:"plaintextKey"`
	}
	for _, candidate := range keys {
		if candidate.Enabled && candidate.RoutingScope == "groups" && len(candidate.Groups) == 1 && candidate.Groups[0].ID == groupID {
			err = s.adminJSON(ctx, base, key, http.MethodGet, "/api/admin/client-keys/reveal", url.Values{"id": {candidate.ID}}, nil, &secret)
			if err != nil {
				return "", err
			}
			if strings.TrimSpace(secret.PlaintextKey) == "" {
				return "", errors.New("cpr key reveal returned no key")
			}
			return secret.PlaintextKey, nil
		}
	}
	err = s.adminJSON(ctx, base, key, http.MethodPost, "/api/admin/client-keys/create", nil, map[string]any{
		"name": "sub2-" + groupID, "groupIds": []string{groupID}, "maxConcurrency": 0, "requestsPerMinute": 0,
	}, &secret)
	if err != nil {
		return "", err
	}
	if strings.TrimSpace(secret.PlaintextKey) == "" {
		return "", errors.New("cpr key creation returned no key")
	}
	return secret.PlaintextKey, nil
}
