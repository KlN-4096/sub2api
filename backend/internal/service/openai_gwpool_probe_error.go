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

	"github.com/Wei-Shaw/sub2api/internal/pkg/gwpool"
)

const gatewayPoolVerificationRetryGap = 5 * time.Second

var errGatewayPoolProbeMissingState = errors.New("gateway probe returned no state")

type gatewayPoolProbeHTTPError struct {
	status int
}

func gatewayPoolRetryablePreparationError(err error) bool {
	var poolErr *gwpool.PoolError
	if errors.As(err, &poolErr) {
		// Shortages use their own queue wait; authentication and 429 do not
		// acquire a second recovery budget here.
		return poolErr.Code == "" && (poolErr.Status == http.StatusBadGateway ||
			poolErr.Status == http.StatusServiceUnavailable || poolErr.Status == http.StatusGatewayTimeout)
	}
	return gatewayPoolRetryableProbeError(err) && !errors.Is(err, context.Canceled)
}

func (e *gatewayPoolProbeHTTPError) Error() string {
	return fmt.Sprintf("gateway probe HTTP %d", e.status)
}

// Classify A/B failures without treating every WarmUnverified as retryable:
// it also covers authentication/policy changes. Recoverable post-business
// failures use gatewayPoolAttemptRetryError before any downstream delivery.
func gatewayPoolRetryableProbeError(err error) bool {
	if errors.Is(err, errGatewayPoolProbeMissingState) || errors.Is(err, context.Canceled) ||
		errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var status *gatewayPoolProbeHTTPError
	if errors.As(err, &status) {
		switch status.status {
		case http.StatusRequestTimeout, http.StatusInternalServerError, http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
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
