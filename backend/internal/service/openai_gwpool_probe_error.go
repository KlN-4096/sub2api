package service

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"syscall"
	"time"
)

const gatewayPoolVerificationRetryGap = 5 * time.Second

var errGatewayPoolProbeMissingState = errors.New("gateway probe returned no state")

type gatewayPoolProbeHTTPError struct {
	status int
}

func (e *gatewayPoolProbeHTTPError) Error() string {
	return fmt.Sprintf("gateway probe HTTP %d", e.status)
}

// Only for the pre-business A/B loop. WarmUnverified itself is not retryable:
// the same sentinel can occur after business transmission, where replay is forbidden.
func gatewayPoolRetryableProbeError(err error) bool {
	if errors.Is(err, errGatewayPoolProbeMissingState) || errors.Is(err, context.Canceled) ||
		errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var status *gatewayPoolProbeHTTPError
	if errors.As(err, &status) {
		switch status.status {
		case http.StatusRequestTimeout, http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
			return true
		default:
			return false // authentication, quota and configuration are not supply shortages
		}
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, net.ErrClosed) ||
		errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.ECONNABORTED) || errors.Is(err, syscall.EPIPE) {
		return true
	}
	var network net.Error
	return errors.As(err, &network) && network.Timeout()
}
