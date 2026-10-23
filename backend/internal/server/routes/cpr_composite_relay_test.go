//go:build unit

package routes

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/handler"
	servermiddleware "github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

type cprCompositeRepo struct {
	service.AccountRepository
	account service.Account
}

func (r cprCompositeRepo) GetByID(context.Context, int64) (*service.Account, error) {
	copy := r.account
	return &copy, nil
}
func (r cprCompositeRepo) ListSchedulableByPlatform(context.Context, string) ([]service.Account, error) {
	return []service.Account{r.account}, nil
}
func (r cprCompositeRepo) ListSchedulableByGroupIDAndPlatform(context.Context, int64, string) ([]service.Account, error) {
	return []service.Account{r.account}, nil
}
func (r cprCompositeRepo) ListSchedulableUngroupedByPlatform(context.Context, string) ([]service.Account, error) {
	return []service.Account{r.account}, nil
}

type cprCompositeTransport struct{ service.HTTPUpstream }

func (cprCompositeTransport) Do(req *http.Request, _ string, _ int64, _ int) (*http.Response, error) {
	return http.DefaultTransport.RoundTrip(req)
}

func TestCPRCompositeAliasPreservesOriginalCompressedRequestOnlyForCPR(t *testing.T) {
	for _, accountType := range []string{service.AccountTypeCPR, service.AccountTypeAPIKey} {
		t.Run(accountType, func(t *testing.T) {
			var received []byte
			var encoding string
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				received, _ = io.ReadAll(req.Body)
				encoding = req.Header.Get("Content-Encoding")
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusServiceUnavailable)
				_, _ = w.Write([]byte(`{"error":{"message":"fixture"}}`))
			}))
			defer upstream.Close()
			cfg := &config.Config{RunMode: config.RunModeSimple}
			cfg.Default.RateMultiplier = 1
			cfg.Security.URLAllowlist.AllowInsecureHTTP = true
			repo := cprCompositeRepo{account: service.Account{
				ID: 9077, Name: "alias-relay", Platform: service.PlatformOpenAI, Type: accountType,
				Status: service.StatusActive, Schedulable: true, Concurrency: 1,
				Credentials: map[string]any{"base_url": upstream.URL, "api_key": "fake-client"},
				Extra:       map[string]any{"openai_raw_relay": true},
			}}
			billing := service.NewBillingCacheService(nil, nil, nil, nil, nil, nil, cfg, nil)
			defer billing.Stop()
			gateway := service.NewOpenAIGatewayService(repo, nil, nil, nil, nil, nil, nil, cfg, nil, nil,
				service.NewBillingService(cfg, nil), nil, billing, cprCompositeTransport{}, &service.DeferredService{}, nil, nil, nil, nil, nil, nil, nil)
			h := handler.NewOpenAIGatewayHandler(gateway, service.NewConcurrencyService(nil), billing,
				service.NewAPIKeyService(nil, nil, nil, nil, nil, nil, cfg), nil, nil, nil, nil, cfg)
			resolver := service.NewCompositeRouteResolver(compositeRouteRepoStub{routes: []service.CompositeModelRoute{{
				ID: 1, GroupID: 1, PublicModel: "public-alias", MatchType: service.CompositeRouteMatchExact,
				TargetPlatform: service.PlatformOpenAI, UpstreamModel: "gpt-5.4",
				Endpoint: service.CompositeRouteEndpointAny, Priority: 100, Enabled: true,
			}}})
			router := gin.New()
			router.Use(func(c *gin.Context) {
				groupID := int64(1)
				c.Set(string(servermiddleware.ContextKeyAPIKey), &service.APIKey{ID: 9, GroupID: &groupID,
					User:  &service.User{ID: 9, Status: service.StatusActive},
					Group: &service.Group{ID: groupID, Platform: service.PlatformComposite, Status: service.StatusActive}})
				c.Set(string(servermiddleware.ContextKeyUser), servermiddleware.AuthSubject{UserID: 9, Concurrency: 0})
				c.Next()
			})
			router.Use(compositeTargetPlatformMiddleware(resolver))
			router.POST("/v1/responses", h.Responses)
			plain := []byte(`{ "model":"public-alias","input":"hi","reasoning":{"effort":"xhigh"} }`)
			enc, err := zstd.NewWriter(nil)
			require.NoError(t, err)
			wire := enc.EncodeAll(plain, nil)
			enc.Close()
			req := httptest.NewRequest(http.MethodPost, "/v1/responses", bytes.NewReader(wire))
			req.Header.Set("Content-Encoding", "zstd")
			req.Header.Set("Content-Type", "application/json")
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)
			require.NotEmpty(t, received, rec.Body.String())
			if accountType == service.AccountTypeCPR {
				require.Equal(t, wire, received)
				require.Equal(t, "zstd", encoding)
			} else {
				require.Equal(t, "gpt-5.4", gjson.GetBytes(received, "model").String())
				require.Empty(t, encoding)
			}
		})
	}
}
