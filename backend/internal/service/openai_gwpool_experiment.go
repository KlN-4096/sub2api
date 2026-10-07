//go:build gwpoolexperiment

package service

import (
	"context"
	"encoding/json"
	"net/http"
)

type GatewayPoolExperimentSession struct{ ids openAITurnStateProbeIdentity }

func NewGatewayPoolExperimentSession(account *Account) *GatewayPoolExperimentSession {
	return &GatewayPoolExperimentSession{ids: newOpenAITurnStateProbeIdentity(account)}
}

// BuildGatewayPoolExperimentRequest reuses the actual production probe builder.
// This adapter is excluded from production builds and has no refresh provider,
// account repository, background workers, or route selection side effects.
func BuildGatewayPoolExperimentRequest(ctx context.Context, account *Account, model, text string, session *GatewayPoolExperimentSession) (*http.Request, error) {
	service := &OpenAIGatewayService{}
	if session == nil {
		_, request, err := service.buildOpenAITurnStateProbe(ctx, account, model, gatewayPoolWarmProbeEffort, text)
		return request, err
	}
	// The only experimental variable is reusing the pre-transform identity;
	// never splice scoped headers onto a differently scoped body.
	ids := newOpenAITurnStateProbeIdentity(account)
	ids.session, ids.thread, ids.window, ids.installation =
		session.ids.session, session.ids.thread, session.ids.window, session.ids.installation
	c := newOpenAITurnStateProbeContext(ids)
	if _, err := service.prepareCodexAccountIdentitySource(ctx, c, account); err != nil {
		return nil, err
	}
	decoded := openAITurnStateProbeBody(model, gatewayPoolWarmProbeEffort, ids, text)
	result := applyCodexOAuthTransformWithOptions(decoded, codexOAuthTransformOptions{IsCodexCLI: true})
	if result.Error != nil {
		return nil, result.Error
	}
	stageCodexOAuthIdentity(c, account, decoded, false)
	upstreamModel := model
	if result.NormalizedModel != "" {
		upstreamModel = result.NormalizedModel
	}
	SetOpsUpstreamModel(c, upstreamModel)
	body, err := json.Marshal(decoded)
	if err != nil {
		return nil, err
	}
	token, _, err := service.GetAccessToken(ctx, account)
	if err != nil {
		return nil, err
	}
	return service.buildUpstreamRequest(ctx, c, account, body, token, true, ids.session, true)
}

// Launcher must reject any enabled OAuth transport plugin. With that precondition
// this is the identical warm-probe transport, including account concurrency.
func GatewayPoolExperimentRoundTrip(upstream HTTPUpstream, request *http.Request, proxyURL string, account *Account) (*http.Response, error) {
	s := &OpenAIGatewayService{httpUpstream: upstream}
	return s.doOpenAIUpstreamRoundTrip(request, proxyURL, account)
}

func GatewayPoolExperimentGateway(cookie string) string { return openAICodexRouteGateway(cookie) }
