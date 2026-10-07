//go:build unit

package service

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/imroc/req/v3"
	"github.com/stretchr/testify/require"
)

type wireTimezoneStubReply struct {
	status int // 0 视为 200
	body   string
}

// stubDialError 模拟"这个 URL 根本连不通"：IPv6-only 出口打纯 IPv4 的服务就是这个形态
// （生产里是 *url.Error 裹着 *net.OpError，这里只需要 Get() 返回非 nil error 这一点）。
type stubDialError struct{ url string }

func (e *stubDialError) Error() string { return "socks connect failed: " + e.url }

// wireTimezoneRoutingStubClient 按请求 URL 分别作答；名单里没有的 URL 视为连不通。
func wireTimezoneRoutingStubClient(t *testing.T, answers map[string]wireTimezoneStubReply, seen *[]string) *req.Client {
	t.Helper()
	client := req.C()
	client.GetTransport().WrapRoundTripFunc(func(http.RoundTripper) req.HttpRoundTripFunc {
		return func(r *http.Request) (*http.Response, error) {
			url := r.URL.String()
			*seen = append(*seen, url)
			reply, ok := answers[url]
			if !ok {
				return nil, &stubDialError{url: url}
			}
			status := reply.status
			if status == 0 {
				status = http.StatusOK
			}
			return &http.Response{
				StatusCode: status,
				Header:     http.Header{"Content-Type": []string{"application/json"}},
				Body:       io.NopCloser(strings.NewReader(reply.body)),
				Request:    r,
			}, nil
		}
	})
	return client
}

func wireTimezoneLookupService(t *testing.T, answers map[string]wireTimezoneStubReply, seen *[]string) *OpenAIQuotaService {
	t.Helper()
	return &OpenAIQuotaService{privacyClientFactory: func(string) (*req.Client, error) {
		return wireTimezoneRoutingStubClient(t, answers, seen), nil
	}}
}

// ipinfo.io 只有 A 记录没有 AAAA，IPv6-only 出口连不到它。此前这里是单条硬编码 URL，
// 这类账号永远解析不出时区，然后静默退化成"不改写"——等于把客户端本机时区发给上游。
// 生产实测：账号 3 的 socks5 出口打 ipinfo.io 得 curl exit=97，打 v6.ipinfo.io 得
// 200 + ip=2001:57a:f200:b900::323 / timezone=America/Los_Angeles。
func TestLookupCodexWireTimezoneFallsBackToIPv6Endpoint(t *testing.T) {
	const primary = "https://ipinfo.io/json"
	const fallback = "https://v6.ipinfo.io/json"

	require.Equal(t, primary, codexWireTimezoneLookupURLs[0],
		"第一条必须仍是 ipinfo.io：IPv4 出口的行为不能变")
	require.Contains(t, codexWireTimezoneLookupURLs, fallback)

	t.Run("ipv4_exit_uses_first_provider_only", func(t *testing.T) {
		var seen []string
		svc := wireTimezoneLookupService(t, map[string]wireTimezoneStubReply{
			primary: {body: `{"ip":"24.120.102.167","timezone":"America/Los_Angeles"}`},
		}, &seen)

		exit, err := svc.lookupCodexWireTimezone(context.Background(), "")

		require.NoError(t, err)
		require.Equal(t, "24.120.102.167", exit.ip)
		require.Equal(t, "America/Los_Angeles", exit.timezone)
		require.Equal(t, []string{primary}, seen, "第一家成功就不该再打第二家")
	})

	t.Run("ipv6_only_exit_falls_back", func(t *testing.T) {
		var seen []string
		svc := wireTimezoneLookupService(t, map[string]wireTimezoneStubReply{
			fallback: {body: `{"ip":"2001:57a:f200:b900::323","timezone":"America/Los_Angeles"}`},
		}, &seen)

		exit, err := svc.lookupCodexWireTimezone(context.Background(), "")

		require.NoError(t, err)
		require.Equal(t, "2001:57a:f200:b900::323", exit.ip)
		require.Equal(t, "America/Los_Angeles", exit.timezone)
		require.Equal(t, []string{primary, fallback}, seen)
	})

	// ipinfo 免费额度是 1000/天/IP，用完就是 429——最可能发生的真实失败形态。
	//
	// 注意这里验的是"非 2xx 会兜底到下一家"这个结果，不是 IsSuccessState 那一行本身：
	// req 的 SetSuccessResult 只在 2xx 时反序列化，非 2xx 时 payload 恒为空，因此就算
	// 去掉状态码判断，下面那条"无时区"守卫也会得到同样的结果。状态码判断留着只为让
	// 日志显示 "returned 429" 而不是一句反序列化错误——它是等价变异，杀不掉。
	// carries_timezone 这条钉住上面这个前提：非 2xx 的响应体一律不得被采信。
	t.Run("非_2xx_也要兜底", func(t *testing.T) {
		for name, reply := range map[string]wireTimezoneStubReply{
			"rate_limited":     {status: http.StatusTooManyRequests, body: `{"error":{"title":"Rate limit exceeded"}}`},
			"forbidden":        {status: http.StatusForbidden, body: `<html>blocked</html>`},
			"server_error":     {status: http.StatusBadGateway, body: ``},
			"carries_timezone": {status: http.StatusTooManyRequests, body: `{"ip":"9.9.9.9","timezone":"Asia/Shanghai"}`},
		} {
			t.Run(name, func(t *testing.T) {
				var seen []string
				svc := wireTimezoneLookupService(t, map[string]wireTimezoneStubReply{
					primary:  reply,
					fallback: {body: `{"ip":"2001:db8::1","timezone":"America/New_York"}`},
				}, &seen)

				exit, err := svc.lookupCodexWireTimezone(context.Background(), "")

				require.NoError(t, err)
				require.Equal(t, "America/New_York", exit.timezone)
				require.Equal(t, []string{primary, fallback}, seen)
			})
		}
	})

	// 200 但解析得出、没有时区：`{}`、ipinfo 对私有地址返回的 bogon 体。
	// 这两种不会在反序列化报错，只有守卫能拦，否则会吃掉兜底那一条。
	t.Run("_200_但无时区不算成功", func(t *testing.T) {
		for _, body := range []string{`{}`, `{"ip":"10.0.0.1","bogon":true}`, `{"ip":"1.2.3.4","timezone":"  "}`} {
			var seen []string
			svc := wireTimezoneLookupService(t, map[string]wireTimezoneStubReply{
				primary:  {body: body},
				fallback: {body: `{"ip":"2001:db8::1","timezone":"America/Denver"}`},
			}, &seen)

			exit, err := svc.lookupCodexWireTimezone(context.Background(), "")

			require.NoError(t, err, body)
			require.Equal(t, "America/Denver", exit.timezone, body)
			require.Equal(t, []string{primary, fallback}, seen, body)
		}
	})

	// exit IP 只写进 extra 供人工排查，不参与改写判定：缺它不该让整次解析失败。
	t.Run("缺_ip_不影响时区可用", func(t *testing.T) {
		var seen []string
		svc := wireTimezoneLookupService(t, map[string]wireTimezoneStubReply{
			primary: {body: `{"timezone":"America/Chicago"}`},
		}, &seen)

		exit, err := svc.lookupCodexWireTimezone(context.Background(), "")

		require.NoError(t, err)
		require.Empty(t, exit.ip)
		require.Equal(t, "America/Chicago", exit.timezone)
	})

	t.Run("两家都失败时错误要都带上", func(t *testing.T) {
		var seen []string
		svc := wireTimezoneLookupService(t, nil, &seen)

		_, err := svc.lookupCodexWireTimezone(context.Background(), "")

		require.Error(t, err)
		require.Contains(t, err.Error(), primary)
		require.Contains(t, err.Error(), fallback)
		require.Len(t, seen, 2)
	})

	// 名单被改空时 errors.Join 返回 nil，调用方会把"什么都没查"当成查到了空时区。
	t.Run("空名单必须报错而不是静默成功", func(t *testing.T) {
		original := codexWireTimezoneLookupURLs
		codexWireTimezoneLookupURLs = nil
		t.Cleanup(func() { codexWireTimezoneLookupURLs = original })
		var seen []string
		svc := wireTimezoneLookupService(t, nil, &seen)

		_, err := svc.lookupCodexWireTimezone(context.Background(), "")

		require.Error(t, err)
		require.Empty(t, seen)
	})
}
