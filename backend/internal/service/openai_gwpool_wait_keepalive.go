package service

import (
	"time"

	"github.com/gin-gonic/gin"
)

const gatewayPoolWaitKeepaliveInterval = 10 * time.Second
const gatewayPoolWaitKeepaliveCleanupKey = "gateway_pool_wait_keepalive_cleanup"

// Only opted-in streaming requests reach this entry point. Comments preserve
// the downstream connection without pretending that inference has started.
// Existing writers account for these bytes separately from semantic output and
// encode late failures in the endpoint's streaming error format.
func StartGatewayPoolWaitKeepalive(c *gin.Context, stream bool) func() {
	if !stream {
		return func() {}
	}
	return startOpenAISSEKeepalive(c, gatewayPoolWaitKeepaliveInterval)
}

// Keep heartbeat ownership across account-slot release and retry backoff.
// Each HTTP handler finishes it once, when the logical request really ends.
func MaintainGatewayPoolWaitKeepalive(c *gin.Context, stream bool) {
	if !stream || c == nil {
		return
	}
	if value, ok := c.Get(openAICompactSSEKeepaliveKey); ok {
		if current, valid := value.(*openAICompactSSEKeepalive); valid {
			current.mu.Lock()
			active := !current.stopped
			current.mu.Unlock()
			if active {
				return
			}
		}
	}
	FinishGatewayPoolWaitKeepalive(c)
	c.Set(gatewayPoolWaitKeepaliveCleanupKey, StartGatewayPoolWaitKeepalive(c, stream))
}

func FinishGatewayPoolWaitKeepalive(c *gin.Context) {
	if c == nil {
		return
	}
	if value, ok := c.Get(gatewayPoolWaitKeepaliveCleanupKey); ok {
		if stop, valid := value.(func()); valid {
			stop()
		}
		c.Set(gatewayPoolWaitKeepaliveCleanupKey, nil)
	}
}
