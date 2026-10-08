package service

import (
	"encoding/base64"
	"encoding/json"
	"strings"
	"time"
)

func (p openAIGatewayPoolPair) routeExpired(now time.Time) bool {
	return !p.routeExpiresAt.IsZero() && !now.Before(p.routeExpiresAt)
}

// Only the cookie's explicit exp is a hard boundary. The pool's unproven
// route_expires_at may include an exp-300 estimate and is not credential expiry.
// Missing information stays unknown: no now+valid_for or received+remaining.
func gatewayPoolRouteExpiresAt(cookie string) time.Time {
	const maxCookiePayload = 16 << 10
	var deadline time.Time
	for _, part := range strings.Split(cookie, ";") {
		name, value, _ := strings.Cut(strings.TrimSpace(part), "=")
		if name != "__oailb" || len(value) > maxCookiePayload {
			continue
		}
		segments := strings.Split(value, ".")
		if len(segments) != 3 {
			continue
		}
		raw, err := base64.RawURLEncoding.DecodeString(strings.TrimRight(segments[1], "="))
		var claims struct {
			Exp int64 `json:"exp"`
		}
		if err != nil || json.Unmarshal(raw, &claims) != nil || claims.Exp <= 0 || claims.Exp > 253402300799 {
			continue
		}
		exp := time.Unix(claims.Exp, 0).UTC()
		if deadline.IsZero() || exp.Before(deadline) {
			deadline = exp
		}
	}
	return deadline
}
