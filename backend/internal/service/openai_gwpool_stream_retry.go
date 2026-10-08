package service

import (
	"bufio"
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"
)

func (s *OpenAIGatewayService) gatewayPoolStreamReadFailure(
	c *gin.Context, account *Account, started, disconnected bool, requestID string, headers http.Header, err error,
) *UpstreamFailoverError {
	if !account.UsesGatewayPool() || started || disconnected || c == nil || c.Request == nil ||
		c.Request.Context().Err() != nil || errors.Is(err, bufio.ErrTooLong) {
		return nil
	}
	return s.newOpenAIStreamFailoverError(c, account, false, requestID, nil,
		"Upstream stream failed before business output", headers)
}
