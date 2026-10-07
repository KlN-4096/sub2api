package httpclient

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func TestValidatedTransport_CacheHostValidation(t *testing.T) {
	originalValidate := validateResolvedIP
	defer func() { validateResolvedIP = originalValidate }()

	var validateCalls int32
	validateResolvedIP = func(host string) error {
		atomic.AddInt32(&validateCalls, 1)
		require.Equal(t, "api.openai.com", host)
		return nil
	}

	var baseCalls int32
	base := roundTripFunc(func(_ *http.Request) (*http.Response, error) {
		atomic.AddInt32(&baseCalls, 1)
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader(`{}`)),
			Header:     make(http.Header),
		}, nil
	})

	now := time.Unix(1730000000, 0)
	transport := newValidatedTransport(base)
	transport.now = func() time.Time { return now }

	req, err := http.NewRequest(http.MethodGet, "https://api.openai.com/v1/responses", nil)
	require.NoError(t, err)

	_, err = transport.RoundTrip(req)
	require.NoError(t, err)
	_, err = transport.RoundTrip(req)
	require.NoError(t, err)

	require.Equal(t, int32(1), atomic.LoadInt32(&validateCalls))
	require.Equal(t, int32(2), atomic.LoadInt32(&baseCalls))
}

func TestValidatedTransport_ExpiredCacheTriggersRevalidation(t *testing.T) {
	originalValidate := validateResolvedIP
	defer func() { validateResolvedIP = originalValidate }()

	var validateCalls int32
	validateResolvedIP = func(_ string) error {
		atomic.AddInt32(&validateCalls, 1)
		return nil
	}

	base := roundTripFunc(func(_ *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader(`{}`)),
			Header:     make(http.Header),
		}, nil
	})

	now := time.Unix(1730001000, 0)
	transport := newValidatedTransport(base)
	transport.now = func() time.Time { return now }

	req, err := http.NewRequest(http.MethodGet, "https://api.openai.com/v1/responses", nil)
	require.NoError(t, err)

	_, err = transport.RoundTrip(req)
	require.NoError(t, err)

	now = now.Add(validatedHostTTL + time.Second)
	_, err = transport.RoundTrip(req)
	require.NoError(t, err)

	require.Equal(t, int32(2), atomic.LoadInt32(&validateCalls))
}

func TestValidatedTransport_ValidationErrorStopsRoundTrip(t *testing.T) {
	originalValidate := validateResolvedIP
	defer func() { validateResolvedIP = originalValidate }()

	expectedErr := errors.New("dns rebinding rejected")
	validateResolvedIP = func(_ string) error {
		return expectedErr
	}

	var baseCalls int32
	base := roundTripFunc(func(_ *http.Request) (*http.Response, error) {
		atomic.AddInt32(&baseCalls, 1)
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(`{}`))}, nil
	})

	transport := newValidatedTransport(base)
	req, err := http.NewRequest(http.MethodGet, "https://api.openai.com/v1/responses", nil)
	require.NoError(t, err)

	_, err = transport.RoundTrip(req)
	require.ErrorIs(t, err, expectedErr)
	require.Equal(t, int32(0), atomic.LoadInt32(&baseCalls))
}

// ForceHTTP2 必须真的落到 transport，并且参与客户端缓存键——否则先建的 HTTP/1.1 客户端
// 会被后来的 h2 请求复用。
func TestBuildTransport_ForceHTTP2(t *testing.T) {
	plain, err := buildTransport(Options{})
	require.NoError(t, err)
	require.False(t, plain.ForceAttemptHTTP2)

	forced, err := buildTransport(Options{ForceHTTP2: true})
	require.NoError(t, err)
	require.True(t, forced.ForceAttemptHTTP2)

	require.NotEqual(t, buildClientKey(Options{}), buildClientKey(Options{ForceHTTP2: true}))
}

// 端到端：ForceHTTP2 置真后 transport 真的按 ALPN 协商到 HTTP/2.0，置假时退回 HTTP/1.1。
// 注意：置假那一腿在本用例里是被注入的 TLSClientConfig 关掉的自动 h2（net/http 只要
// TLSClientConfig/DialContext 任一非 nil 且未 ForceAttemptHTTP2 就不自动升级），生产里关掉它的
// 是 buildTransport 的自定义 DialContext——同一条件式，本用例证明的是 ForceHTTP2 这条开关有效。
// 本地 httptest TLS 服务器，只注入它的根证书，不出网。
func TestBuildTransport_ForceHTTP2NegotiatesH2(t *testing.T) {
	var (
		mu   sync.Mutex
		seen []string
	)
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		seen = append(seen, r.Proto)
		mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	}))
	srv.EnableHTTP2 = true
	srv.StartTLS()
	defer srv.Close()
	roots := x509.NewCertPool()
	roots.AddCert(srv.Certificate())

	for _, force := range []bool{false, true} {
		tr, err := buildTransport(Options{ForceHTTP2: force})
		require.NoError(t, err)
		tr.TLSClientConfig = &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}
		resp, err := (&http.Client{Transport: tr, Timeout: 5 * time.Second}).Get(srv.URL)
		require.NoError(t, err)
		_ = resp.Body.Close()
		want := "HTTP/1.1"
		if force {
			want = "HTTP/2.0"
		}
		require.Equal(t, want, resp.Proto, "force=%v", force)
		tr.CloseIdleConnections()
	}
	require.Equal(t, []string{"HTTP/1.1", "HTTP/2.0"}, seen)
}
