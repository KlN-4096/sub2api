package service

import (
	"fmt"
	"net/url"
	"strings"

	"github.com/Wei-Shaw/sub2api/internal/pkg/httputil"
	"github.com/gin-gonic/gin"
)

const cprOriginalBodyKey = "sub2api.cpr_original_body"

// PreserveCPRRelayBody must run before an ingress model/compatibility rewrite.
// Keep the immutable preread object; other channels still see the rewritten body.
func PreserveCPRRelayBody(c *gin.Context) {
	if _, exists := c.Get(cprOriginalBodyKey); exists {
		return
	}
	if original, ok := c.Request.Body.(*httputil.PrereadBody); ok {
		c.Set(cprOriginalBodyKey, original)
	}
}

// cprRelayTargetURL preserves the inbound endpoint and query, not a handler's
// Responses/compact or compatibility rewrite. Only existing gateway endpoints
// can be relayed; the admin API can never be reached through this path.
func cprRelayTargetURL(base string, c *gin.Context) (string, error) {
	inbound := *c.Request.URL
	if c.Request.RequestURI != "" {
		original, err := url.ParseRequestURI(c.Request.RequestURI)
		if err != nil {
			return "", fmt.Errorf("invalid cpr request URI")
		}
		inbound = *original
	}
	path := inbound.Path
	for _, prefix := range []string{"/backend-api/codex", "/openai/v1", "/v1"} {
		if strings.HasPrefix(path, prefix+"/") {
			path = strings.TrimPrefix(path, prefix)
			break
		}
	}
	switch path {
	case "/responses", "/images/generations", "/images/edits", "/alpha/search",
		"/chat/completions", "/messages", "/messages/count_tokens":
	default:
		if !strings.HasPrefix(path, "/responses/") {
			return "", fmt.Errorf("unsupported cpr relay endpoint")
		}
		if _, ok := sanitizedUpstreamPathSuffix(strings.TrimPrefix(path, "/responses")); !ok {
			return "", fmt.Errorf("invalid cpr relay endpoint")
		}
	}
	target, err := url.Parse(buildOpenAIEndpointURL(base, "/v1"+path))
	if err != nil {
		return "", fmt.Errorf("invalid cpr gateway URL")
	}
	target.RawQuery = inbound.RawQuery
	return target.String(), nil
}
