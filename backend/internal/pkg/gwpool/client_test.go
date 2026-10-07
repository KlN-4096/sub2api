package gwpool

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// 测试一律打 httptest 假池子，**绝不打真实上游**。

func TestClientCookieReturnsPair(t *testing.T) {
	var gotAuth, gotPath, gotQuery string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth, gotPath, gotQuery = r.Header.Get("Authorization"), r.URL.Path, r.URL.RawQuery
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"gateway":"unified-142","cookie":"__cflb=lb; __oailb=jwt",`+
			`"expires_at":"2026-10-01T06:40:00Z","valid_for_s":150,"verified_full":true,`+
			`"ttl_is_advisory":true}`)
	}))
	defer srv.Close()

	pair, err := New(srv.URL+"/", "ck-secret").Cookie(context.Background(), "", false)
	if err != nil {
		t.Fatalf("cookie: %v", err)
	}
	if gotAuth != "Bearer ck-secret" {
		t.Fatalf("consumer key must travel in the Authorization header, got %q", gotAuth)
	}
	if gotPath != "/cookie" {
		t.Fatalf("path = %q", gotPath)
	}
	if gotQuery != "" {
		t.Fatalf("空 gateway 不该拼查询串, got %q", gotQuery)
	}
	if pair.Gateway != "unified-142" || pair.Cookie != "__cflb=lb; __oailb=jwt" {
		t.Fatalf("pair = %+v", pair)
	}
	if pair.ValidFor != 150*time.Second {
		t.Fatalf("valid_for_s 是满血窗口剩余量，要原样带出来, got %v", pair.ValidFor)
	}
	if !pair.VerifiedFull {
		t.Fatal("verified_full 丢了")
	}
	if !pair.TTLIsAdvisory {
		t.Fatal("ttl_is_advisory 丢了")
	}
}

// force=1 = 「我租着的那张不行了，给一个不同的网关」。参数要和 gateway 共存。
func TestClientCookieForcesRotation(t *testing.T) {
	queries := make(chan string, 3)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		queries <- r.URL.RawQuery
		_, _ = io.WriteString(w, `{"gateway":"unified-84","cookie":"__cflb=a","valid_for_s":150}`)
	}))
	defer srv.Close()
	client := New(srv.URL, "k")

	if _, err := client.Cookie(context.Background(), "", false); err != nil {
		t.Fatalf("cookie: %v", err)
	}
	if got := <-queries; got != "" {
		t.Fatalf("正常路径不该带 force: %q", got)
	}
	if _, err := client.Cookie(context.Background(), "", true); err != nil {
		t.Fatalf("cookie force: %v", err)
	}
	if got := <-queries; got != "force=1" {
		t.Fatalf("force 查询串 = %q", got)
	}
	if _, err := client.Cookie(context.Background(), "unified-84", true); err != nil {
		t.Fatalf("cookie force+gateway: %v", err)
	}
	if got := <-queries; got != "force=1&gateway=unified-84" {
		t.Fatalf("force+gateway 查询串 = %q", got)
	}
}

// force=1 换不出来时池子回 503，**不会把原来那张再发一遍** ⇒ 这边只认 ErrNoSlot。
func TestClientCookieForceNoSlot(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("force") == "1" {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		_, _ = io.WriteString(w, `{"gateway":"unified-84","cookie":"__cflb=a","valid_for_s":150}`)
	}))
	defer srv.Close()
	if _, err := New(srv.URL, "k").Cookie(context.Background(), "", true); !errors.Is(err, ErrNoSlot) {
		t.Fatalf("want ErrNoSlot, got %v", err)
	}
}

func TestClientCookiePassesGateway(t *testing.T) {
	var gotQuery string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotQuery = r.URL.RawQuery
		_, _ = io.WriteString(w, `{"gateway":"unified-84","cookie":"__cflb=a","valid_for_s":10}`)
	}))
	defer srv.Close()

	if _, err := New(srv.URL, "k").Cookie(context.Background(), "unified-84", false); err != nil {
		t.Fatalf("cookie: %v", err)
	}
	if gotQuery != "gateway=unified-84" {
		t.Fatalf("query = %q", gotQuery)
	}
}

// 503 = 池子没有满血槽位。调用方据此走失败路径，不能回落降级回放，所以必须是一个可识别的错误。
func TestClientCookieNoSlot(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	_, err := New(srv.URL, "k").Cookie(context.Background(), "", false)
	if !errors.Is(err, ErrNoSlot) {
		t.Fatalf("want ErrNoSlot, got %v", err)
	}
}

func TestClientCookieRejectsUnusablePayloads(t *testing.T) {
	cases := map[string]struct {
		status int
		body   string
	}{
		"非 200":    {http.StatusInternalServerError, `{}`},
		"空 cookie": {http.StatusOK, `{"gateway":"unified-1","cookie":"","valid_for_s":150}`},
		"窗口已过":     {http.StatusOK, `{"gateway":"unified-1","cookie":"__cflb=a","valid_for_s":0}`},
		"不是 JSON":  {http.StatusOK, `not json`},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tc.status)
				_, _ = io.WriteString(w, tc.body)
			}))
			defer srv.Close()
			if _, err := New(srv.URL, "k").Cookie(context.Background(), "", false); err == nil {
				t.Fatal("want error")
			}
		})
	}
}

// 端点一律 JoinPath 拼：base_url 带 query / 缺结尾斜杠时字符串拼接会把路径拼坏。
func TestClientJoinsEndpointPath(t *testing.T) {
	paths := make(chan string, 2)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths <- r.URL.Path + "?" + r.URL.RawQuery
		_, _ = io.WriteString(w, `{"gateway":"unified-1","cookie":"__cflb=a","valid_for_s":9}`)
	}))
	defer srv.Close()

	client := New(srv.URL+"/pool?x=1", "k")
	if _, err := client.Cookie(context.Background(), "", false); err != nil {
		t.Fatalf("cookie: %v", err)
	}
	if got := <-paths; got != "/pool/cookie?" {
		t.Fatalf("cookie endpoint = %q", got)
	}
}

// 控制字符会劈开出站头：这是信任边界，入站就拦。
func TestClientCookieRejectsControlCharacters(t *testing.T) {
	// CRLF 必须是 **JSON 转义**（报文里是反斜杠 + r），解码后才会变成真的控制字符。
	// 写成裸 CRLF 的话 encoding/json 先把它当非法字符拒掉，这条用例就成了空测——
	// 把产品侧的控制字符守卫整条去掉也照样绿（实测过）。
	const body = `{"gateway":"unified-1","cookie":"__cflb=a\r\nX-Injected: 1","valid_for_s":9}`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, body)
	}))
	defer srv.Close()

	// 先确认这份报文本身是合法 JSON 且 cookie 里真带控制字符，否则错误可能来自解码而不是守卫。
	var probe struct {
		Cookie string `json:"cookie"`
	}
	if err := json.Unmarshal([]byte(body), &probe); err != nil {
		t.Fatalf("报文必须是合法 JSON，否则守卫根本没被执行: %v", err)
	}
	if !strings.ContainsRune(probe.Cookie, '\r') {
		t.Fatalf("解码后的 cookie 必须带控制字符: %q", probe.Cookie)
	}

	_, err := New(srv.URL, "k").Cookie(context.Background(), "", false)
	if err == nil {
		t.Fatal("want error")
	}
	if !errors.Is(err, ErrPool) || !strings.Contains(err.Error(), "control characters") {
		t.Fatalf("错误必须来自控制字符守卫，而不是解码失败: %v", err)
	}
}

// 池子侧的每一个错误都要包着 ErrPool：消费端据此把它和真实代理故障分开，不然重启池子会把
// 一批真账号按「代理持久故障」停调度 10 分钟。
func TestClientErrorsWrapErrPool(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	client := New(srv.URL, "k")
	_, err := client.Cookie(context.Background(), "", false)
	if !errors.Is(err, ErrPool) {
		t.Fatalf("cookie HTTP 500 must wrap ErrPool: %v", err)
	}
	if !errors.Is(ErrNoSlot, ErrPool) {
		t.Fatal("ErrNoSlot must wrap ErrPool")
	}
	srv.Close()
	// 传输错误（池子没起/换了端口）同样要带标记，它正是会被误判成代理故障的那一类。
	if _, err := client.Cookie(context.Background(), "", false); !errors.Is(err, ErrPool) {
		t.Fatalf("transport error must wrap ErrPool: %v", err)
	}
}

// 错误串不得带 consumer key。
func TestNewRejectsUnusableBaseURL(t *testing.T) {
	for _, bad := range []string{"", "   ", "://nope"} {
		if New(bad, "k") != nil {
			t.Fatalf("%q should not build a client", bad)
		}
	}
}

func TestClientErrorsCarryNoConsumerKey(t *testing.T) {
	const key = "super-secret-consumer-key"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	srv.Close() // 关掉：逼出传输错误
	client := New(srv.URL, key)
	_, err := client.Cookie(context.Background(), "", false)
	if err == nil {
		t.Fatal("want transport error")
	}
	if strings.Contains(err.Error(), key) {
		t.Fatalf("error string leaked the consumer key: %v", err)
	}
}
