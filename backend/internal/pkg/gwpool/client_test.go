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

	pair, err := New(srv.URL+"/", "ck-secret", 0).Cookie(context.Background(), CookieRequest{})
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

// /gateways 只是把池子那本账读出来给消费端挑落点：字段原样带出，策略留给调用方。
func TestClientGatewaysListsCandidates(t *testing.T) {
	var gotAuth, gotPath, gotQuery string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth, gotPath, gotQuery = r.Header.Get("Authorization"), r.URL.Path, r.URL.RawQuery
		_, _ = io.WriteString(w, `{"account":"acc-a","live":2,"target":6,"gateways":[`+
			`{"name":"unified-167","pair_ready":true,"valid_for_s":2400,`+
			`"used_by_you":false,"last_used_at":"2026-10-01T12:00:00Z"},`+
			`{"name":"unified-126","pair_ready":true,"used_by_you":true},`+
			`{"name":"  ","pair_ready":true}]}`)
	}))
	defer srv.Close()

	gateways, err := New(srv.URL, "ck-secret", 0).Gateways(context.Background(), "acc-a")
	if err != nil {
		t.Fatalf("gateways: %v", err)
	}
	if gotAuth != "Bearer ck-secret" {
		t.Fatalf("consumer key must travel in the Authorization header, got %q", gotAuth)
	}
	if gotPath != "/gateways" {
		t.Fatalf("path = %q", gotPath)
	}
	// 带着 key 也要报 account：一把 key 能替多个上游账号取票，used_by_you 必须是**那个**账号的历史。
	if gotQuery != "account=acc-a" {
		t.Fatalf("query = %q", gotQuery)
	}
	if len(gateways) != 2 {
		t.Fatalf("没名字的那项点不了名，应当丢掉: %+v", gateways)
	}
	if got := gateways[0]; got.Name != "unified-167" || !got.PairReady || got.UsedByYou {
		t.Fatalf("gateways[0] = %+v", got)
	}
	if !gateways[0].LastUsedAt.Equal(time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)) {
		t.Fatalf("last_used_at = %v", gateways[0].LastUsedAt)
	}
	if got := gateways[1]; !got.UsedByYou || !got.LastUsedAt.IsZero() {
		t.Fatalf("gateways[1] = %+v（缺省 last_used_at 应当是零值 = 没碰过）", got)
	}
}

// 没配 consumer key 时只能匿名问，身份同样走 ?account=。
func TestClientGatewaysAnonymousCarriesAccount(t *testing.T) {
	var gotAuth, gotQuery string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth, gotQuery = r.Header.Get("Authorization"), r.URL.RawQuery
		_, _ = io.WriteString(w, `{"gateways":[]}`)
	}))
	defer srv.Close()

	if _, err := New(srv.URL, "", 0).Gateways(context.Background(), "acc a/b"); err != nil {
		t.Fatalf("gateways: %v", err)
	}
	if gotAuth != "" {
		t.Fatalf("没 key 就不该有 Authorization 头: %q", gotAuth)
	}
	if gotQuery != "account=acc+a%2Fb" {
		t.Fatalf("标识必须转义后进查询串: %q", gotQuery)
	}
}

// 回显的身份和报上去的不是一个 ⇒ used_by_you / last_used_at 是**别人的**历史，这张列表不能用来
// 挑落点（调用方据此退回裸取）。回显为空 = 池子还没报这个字段，不作数。
func TestClientGatewaysRejectsAnswerForAnotherAccount(t *testing.T) {
	var body string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, body)
	}))
	defer srv.Close()
	client := New(srv.URL, "k", 0)

	body = `{"account":"acc-uploader","gateways":[{"name":"unified-167","pair_ready":true}]}`
	if _, err := client.Gateways(context.Background(), "acc-a"); !errors.Is(err, ErrPool) {
		t.Fatalf("报错给别的账号也该拒: %v", err)
	}
	body = `{"gateways":[{"name":"unified-167","pair_ready":true}]}`
	if _, err := client.Gateways(context.Background(), "acc-a"); err != nil {
		t.Fatalf("没回显就不作数: %v", err)
	}
	// 自己也没报 account（没有上游标识可报）时，池子回显谁都不作数。
	body = `{"account":"acc-uploader","gateways":[]}`
	if _, err := client.Gateways(context.Background(), ""); err != nil {
		t.Fatalf("没报 account 时不做这个自检: %v", err)
	}
}

// /cookie 也要报 account：一把 key 替多个上游账号取票时，槽位必须记在**报上来的那个**账号上。
// 跨账号取到的票 verified_full 恒为 false（池子没有那个账号的凭据），但票本身照常交出来。
func TestClientCookieCarriesAccount(t *testing.T) {
	var gotQuery string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotQuery = r.URL.RawQuery
		_, _ = io.WriteString(w, `{"gateway":"unified-167","cookie":"__cflb=a","valid_for_s":150,`+
			`"verified_full":false}`)
	}))
	defer srv.Close()

	pair, err := New(srv.URL, "k", 0).Cookie(context.Background(), CookieRequest{Account: "acc-a", Gateway: "unified-167", Force: true})
	if err != nil {
		t.Fatalf("cookie: %v", err)
	}
	if gotQuery != "account=acc-a&force=1&gateway=unified-167" {
		t.Fatalf("query = %q", gotQuery)
	}
	if pair.VerifiedFull {
		t.Fatal("verified_full 要原样带出来")
	}
	if pair.Cookie == "" {
		t.Fatal("跨账号验不了满血不是丢票的理由")
	}
}

// 列表拿不到就是拿不到：原样返回错误（消费端据此退回裸取），而且照样包着 ErrPool——
// 池子重启不能把真账号当成代理持久故障停调度。
func TestClientGatewaysErrorsWrapErrPool(t *testing.T) {
	for name, body := range map[string]string{
		"池子没这个端点": "",
		"不是 JSON": "not json",
	} {
		t.Run(name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				if body == "" {
					w.WriteHeader(http.StatusNotFound)
					return
				}
				_, _ = io.WriteString(w, body)
			}))
			defer srv.Close()
			if _, err := New(srv.URL, "k", 0).Gateways(context.Background(), "acc"); !errors.Is(err, ErrPool) {
				t.Fatalf("want ErrPool, got %v", err)
			}
		})
	}
	// 传输错误（池子没起 / 换端口）同样要带标记，且不许带出 consumer key。
	const key = "super-secret-consumer-key"
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	srv.Close()
	_, err := New(srv.URL, key, 0).Gateways(context.Background(), "acc")
	if !errors.Is(err, ErrPool) {
		t.Fatalf("transport error must wrap ErrPool: %v", err)
	}
	if strings.Contains(err.Error(), key) {
		t.Fatalf("error string leaked the consumer key: %v", err)
	}
}

// last_used_at 只用来排序，格式坏了不该把**整张列表**废掉（没有列表 = 退回池子自己挑）。
func TestClientGatewaysToleratesUnparsableLastUsedAt(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, `{"gateways":[{"name":"unified-167","pair_ready":true,"last_used_at":""},`+
			`{"name":"unified-183","pair_ready":true,"last_used_at":"昨天"}]}`)
	}))
	defer srv.Close()

	gateways, err := New(srv.URL, "k", 0).Gateways(context.Background(), "acc")
	if err != nil {
		t.Fatalf("gateways: %v", err)
	}
	if len(gateways) != 2 || !gateways[0].LastUsedAt.IsZero() || !gateways[1].LastUsedAt.IsZero() {
		t.Fatalf("解不开的时刻当零值（没碰过）: %+v", gateways)
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
	client := New(srv.URL, "k", 0)

	if _, err := client.Cookie(context.Background(), CookieRequest{}); err != nil {
		t.Fatalf("cookie: %v", err)
	}
	if got := <-queries; got != "" {
		t.Fatalf("正常路径不该带 force: %q", got)
	}
	if _, err := client.Cookie(context.Background(), CookieRequest{Force: true}); err != nil {
		t.Fatalf("cookie force: %v", err)
	}
	if got := <-queries; got != "force=1" {
		t.Fatalf("force 查询串 = %q", got)
	}
	if _, err := client.Cookie(context.Background(), CookieRequest{Gateway: "unified-84", Force: true}); err != nil {
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
	if _, err := New(srv.URL, "k", 0).Cookie(context.Background(), CookieRequest{Force: true}); !errors.Is(err, ErrNoSlot) {
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

	if _, err := New(srv.URL, "k", 0).Cookie(context.Background(), CookieRequest{Gateway: "unified-84"}); err != nil {
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

	_, err := New(srv.URL, "k", 0).Cookie(context.Background(), CookieRequest{})
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
			if _, err := New(srv.URL, "k", 0).Cookie(context.Background(), CookieRequest{}); err == nil {
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

	client := New(srv.URL+"/pool?x=1", "k", 0)
	if _, err := client.Cookie(context.Background(), CookieRequest{}); err != nil {
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

	_, err := New(srv.URL, "k", 0).Cookie(context.Background(), CookieRequest{})
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
	client := New(srv.URL, "k", 0)
	_, err := client.Cookie(context.Background(), CookieRequest{})
	if !errors.Is(err, ErrPool) {
		t.Fatalf("cookie HTTP 500 must wrap ErrPool: %v", err)
	}
	if !errors.Is(ErrNoSlot, ErrPool) {
		t.Fatal("ErrNoSlot must wrap ErrPool")
	}
	srv.Close()
	// 传输错误（池子没起/换了端口）同样要带标记，它正是会被误判成代理故障的那一类。
	if _, err := client.Cookie(context.Background(), CookieRequest{}); !errors.Is(err, ErrPool) {
		t.Fatalf("transport error must wrap ErrPool: %v", err)
	}
}

// 错误串不得带 consumer key。
func TestNewRejectsUnusableBaseURL(t *testing.T) {
	for _, bad := range []string{"", "   ", "://nope"} {
		if New(bad, "k", 0) != nil {
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
	client := New(srv.URL, key, 0)
	_, err := client.Cookie(context.Background(), CookieRequest{})
	if err == nil {
		t.Fatal("want transport error")
	}
	if strings.Contains(err.Error(), key) {
		t.Fatalf("error string leaked the consumer key: %v", err)
	}
}

// 单次调用的上限必须跟着参数走，不能被包里那个写死的 5s 盖住：http.Client.Timeout 与 ctx
// deadline 取较小者，所以消费端把账号配的「取票超时」调到 5s 以上时，必须由这个参数放开。
// 白盒读 http.Timeout（省掉一个 5 秒级的等待），再用一对快测证明「它就是单次调用的闸」。
func TestNewAppliesCallTimeout(t *testing.T) {
	const url = "http://127.0.0.1:1/"
	if got := New(url, "k", 8*time.Second).http.Timeout; got != 8*time.Second {
		t.Fatalf("单次调用上限应为 8s，实际 %v", got)
	}
	for _, zero := range []time.Duration{0, -1} {
		if got := New(url, "k", zero).http.Timeout; got != defaultRequestTimeout {
			t.Fatalf("超时给 %v 时应取包默认 %v，实际 %v", zero, defaultRequestTimeout, got)
		}
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(300 * time.Millisecond)
		_, _ = io.WriteString(w, `{"gateway":"unified-1","cookie":"__cflb=a; __oailb=b","valid_for_s":150}`)
	}))
	defer srv.Close()

	if _, err := New(srv.URL, "k", 100*time.Millisecond).Cookie(context.Background(), CookieRequest{}); err == nil {
		t.Fatal("上限 100ms 时这次 300ms 的调用必须超时")
	}
	if _, err := New(srv.URL, "k", 3*time.Second).Cookie(context.Background(), CookieRequest{}); err != nil {
		t.Fatalf("上限 3s 时这次 300ms 的调用不该失败: %v", err)
	}
}
