package gwpool

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"
)

// 2026-10-02 新协议：exclude / exclude_versions / min_remaining / wait / cookie_version /
// 闭集错误码 / POST /release。测试一律打 httptest 假池子，**绝不打真实上游**。

// 这组用例要测的两个控制字节。用 rune 拼而不是写成字面量里的转义序列：它们是被测对象，
// 放在一处才看得见。
var (
	nulByte = string(rune(0))
	lfByte  = string(rune(10))
)

// 四个新参数都要原样上送，并且按契约上限自己先裁：超限池子会拒掉**整条**请求（= 取不到票）。
func TestCookieRequestCarriesNewParams(t *testing.T) {
	var gotQuery string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotQuery = r.URL.RawQuery
		_, _ = io.WriteString(w, `{"gateway":"unified-1","cookie":"__cflb=a; __oailb=b",`+
			`"valid_for_s":150,"cookie_version":"tkt-7"}`)
	}))
	defer srv.Close()

	many := make([]string, 0, MaxExcludeItems+12)
	for i := 0; i < MaxExcludeItems+10; i++ {
		many = append(many, "unified-"+strconv.Itoa(i))
	}
	many = append(many, "", strings.Repeat("x", maxExcludeItemLen+1))

	pair, err := New(srv.URL, "k", 0).Cookie(context.Background(), CookieRequest{
		Account: "acc-a", Gateway: "unified-9", Force: true,
		Exclude: many, ExcludeVersions: []string{"tkt-1", "tkt-2"},
		MinRemaining: 60 * time.Second, Wait: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("取票失败: %v", err)
	}
	if pair.Version != "tkt-7" {
		t.Fatalf("cookie_version 没收上来: %q", pair.Version)
	}
	query, err := url.ParseQuery(gotQuery)
	if err != nil {
		t.Fatalf("查询串解不开 %q: %v", gotQuery, err)
	}
	for key, want := range map[string]string{
		"account": "acc-a", "gateway": "unified-9", "force": "1",
		"exclude_versions": "tkt-1,tkt-2", "min_remaining": "60", "wait": "5",
	} {
		if got := query.Get(key); got != want {
			t.Fatalf("%s=%q，期望 %q", key, got, want)
		}
	}
	excluded := strings.Split(query.Get("exclude"), ",")
	if len(excluded) != MaxExcludeItems {
		t.Fatalf("exclude 应裁到 %d 项，实际 %d", MaxExcludeItems, len(excluded))
	}
	// 裁的是**尾部**：调用方把最近烧的放前面，那些最可能还在冷却期里。
	if excluded[0] != "unified-0" {
		t.Fatalf("exclude 头一项应是调用方给的第一项，实际 %q", excluded[0])
	}
	if last := excluded[MaxExcludeItems-1]; last != "unified-"+strconv.Itoa(MaxExcludeItems-1) {
		t.Fatalf("exclude 末项 %q 不对", last)
	}
	for _, item := range excluded {
		if item == "" || len(item) > maxExcludeItemLen {
			t.Fatalf("非法 exclude 项上了线: %q", item)
		}
	}
}

// min_remaining 与 wait 按契约区间钳位，不发一个必然被池子改写的值。
func TestCookieClampsMinRemainingAndWait(t *testing.T) {
	low := CookieRequest{MinRemaining: time.Second, Wait: -time.Second}.query(0)
	if got := low.Get("min_remaining"); got != "30" {
		t.Fatalf("低于下限应钳到 30，实际 %q", got)
	}
	if got := low.Get("wait"); got != "" {
		t.Fatalf("wait 非正数时不该带，实际 %q", got)
	}
	high := CookieRequest{MinRemaining: 10 * time.Hour, Wait: time.Hour}.query(0)
	if got := high.Get("min_remaining"); got != "3600" {
		t.Fatalf("超上限应钳到 3600，实际 %q", got)
	}
	if got := high.Get("wait"); got != "30" {
		t.Fatalf("wait 超上限应钳到 30，实际 %q", got)
	}
}

// wait 必须钳进本次调用的剩余预算：池子还在等、这边先断等于白等一场。
// 这条钳位是代码而不是注释 —— 调用方算错 wait 时不该出现「池子等 25s、客户端 8s 就断」。
func TestCookieWaitIsClampedToCallBudget(t *testing.T) {
	request := CookieRequest{Wait: 30 * time.Second}
	if got := request.query(4 * time.Second).Get("wait"); got != "3" {
		t.Fatalf("预算 4s 时 wait 应为 3（留 1s 写回响应），实际 %q", got)
	}
	if got := request.query(waitSafetyMargin).Get("wait"); got != "" {
		t.Fatalf("预算只够余量时不该带 wait，实际 %q", got)
	}

	var gotWait string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotWait = r.URL.Query().Get("wait")
		_, _ = io.WriteString(w, `{"gateway":"g","cookie":"__cflb=a; __oailb=b","valid_for_s":150}`)
	}))
	defer srv.Close()
	// 这一段只钉一件事：**钳位接在了 Cookie() 这条真实路径上**，不是只在 query() 里。
	// 精确秒数由上面两段钉（它们传的是确定的 budget，不含时序）。别再往这里塞边界值：
	// query() 里是**截断**，而「设死线」到「算 budget」之间必然过掉几微秒 ⇒ 掐着边界写的
	// 断言只在够快的机器上绿（CI 上就红过）。
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := New(srv.URL, "k", 0).Cookie(ctx, CookieRequest{Wait: 30 * time.Second}); err != nil {
		t.Fatalf("取票失败: %v", err)
	}
	w, err := strconv.Atoi(gotWait)
	if err != nil {
		t.Fatalf("出站 wait 解不开: %q", gotWait)
	}
	if w <= 0 || w > 9 {
		t.Fatalf("死线 10s 时出站 wait 应落在 (0, 9]（预算内、且留了写回余量），实际 %d", w)
	}
}

// 失败响应：闭集码与退避时长要收上来；集合外的码当没报；retry_after 钳上限（不然池子报一个
// 10 年就把这个身份永久饿死）。
func TestCookieRefusalCarriesCodeAndRetryAfter(t *testing.T) {
	cases := []struct {
		name           string
		status         int
		body           string
		retryAfter     string
		wantCode       string
		wantRetryAfter time.Duration
	}{
		{
			name: "冷却带退避", status: http.StatusServiceUnavailable,
			body:     `{"error":{"code":"all_cooling","message":"人话","retry_after_seconds":7200}}`,
			wantCode: CodeAllCooling, wantRetryAfter: MaxRetryAfter,
		},
		{
			name: "出口熔断没给时长", status: http.StatusServiceUnavailable,
			body: `{"error":{"code":"no_exit"}}`, wantCode: CodeNoExit,
		},
		{
			name: "时长只在头里", status: http.StatusTooManyRequests,
			body: `{"error":{"code":"rate_limited"}}`, retryAfter: "45",
			wantCode: CodeRateLimited, wantRetryAfter: 45 * time.Second,
		},
		{
			name: "集合外的码不作数", status: http.StatusServiceUnavailable,
			body:           `{"error":{"code":"please_back_off_forever","retry_after_seconds":60}}`,
			wantRetryAfter: time.Minute,
		},
		{
			name: "体解不开", status: http.StatusServiceUnavailable, body: `not json`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				if tc.retryAfter != "" {
					w.Header().Set("Retry-After", tc.retryAfter)
				}
				w.WriteHeader(tc.status)
				_, _ = io.WriteString(w, tc.body)
			}))
			defer srv.Close()

			_, err := New(srv.URL, "k", 0).Cookie(context.Background(), CookieRequest{})
			if !errors.Is(err, ErrNoSlot) {
				t.Fatalf("池子拒票必须 Unwrap 到 ErrNoSlot（消费端据此 fail-closed）: %v", err)
			}
			var refused *PoolError
			if !errors.As(err, &refused) {
				t.Fatalf("应是 *PoolError: %v", err)
			}
			if refused.Code != tc.wantCode {
				t.Fatalf("code=%q，期望 %q", refused.Code, tc.wantCode)
			}
			if refused.RetryAfter != tc.wantRetryAfter {
				t.Fatalf("retry_after=%v，期望 %v", refused.RetryAfter, tc.wantRetryAfter)
			}
			if refused.Status != tc.status {
				t.Fatalf("status=%d，期望 %d", refused.Status, tc.status)
			}
			// 池子的 message 是给人看的自由文本，一个判据都不从它取，也不进错误串。
			if strings.Contains(err.Error(), "人话") {
				t.Fatal("池子的 message 不该进错误串")
			}
		})
	}
}

// cookie_version 是外部输入：控制字符与超长一律判废（当池子没报），**不截断**——截出来的
// version 还回去是另一张票。畸形 version 不是丢弃这张 pair 的理由。
func TestCookieVersionIsSanitized(t *testing.T) {
	for _, version := range []string{"tkt\u00001", "tkt\u000a1", strings.Repeat("v", maxVersionLen+1), "   "} {
		body, err := json.Marshal(map[string]any{
			"gateway": "g", "cookie": "__cflb=a; __oailb=b", "valid_for_s": 150, "cookie_version": version,
		})
		if err != nil {
			t.Fatal(err)
		}
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write(body)
		}))
		pair, err := New(srv.URL, "k", 0).Cookie(context.Background(), CookieRequest{})
		srv.Close()
		if err != nil {
			t.Fatalf("畸形 version 不该废掉整张票: %v", err)
		}
		if pair.Version != "" {
			t.Fatalf("version %q 应判废，实际收上来 %q", version, pair.Version)
		}
	}
}

// 还票：204 成功、409（太晚 / 已还过 / 找不到）当失败报给调用方（它只记日志）、
// 没有票号时一个请求都不发。
func TestReleaseReturnsTicket(t *testing.T) {
	var hits int
	var gotPath, gotAuth, gotBody, gotContentType string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits++
		gotPath, gotAuth = r.URL.Path, r.Header.Get("Authorization")
		gotContentType = r.Header.Get("Content-Type")
		body, _ := io.ReadAll(io.LimitReader(r.Body, 1<<10))
		gotBody = string(body)
		if hits == 1 {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		w.WriteHeader(http.StatusConflict)
	}))
	defer srv.Close()

	client := New(srv.URL, "ck-secret", 0)
	if err := client.Release(context.Background(), "tkt-7"); err != nil {
		t.Fatalf("204 应算成功: %v", err)
	}
	if gotPath != "/release" {
		t.Fatalf("path=%q", gotPath)
	}
	if gotAuth == "" {
		t.Fatal("还票的认证必须与 /cookie 一致（带 consumer key）")
	}
	if gotContentType != "application/json" {
		t.Fatalf("content-type=%q", gotContentType)
	}
	if gotBody != `{"cookie_version":"tkt-7"}` {
		t.Fatalf("body=%q", gotBody)
	}
	if err := client.Release(context.Background(), "tkt-7"); !errors.Is(err, ErrPool) {
		t.Fatalf("409 应是包着 ErrPool 的错误: %v", err)
	}
	if err := client.Release(context.Background(), "   "); err != nil {
		t.Fatalf("没有票号就还不了，这不算错误: %v", err)
	}
	if hits != 2 {
		t.Fatalf("空票号不该发请求，实际打了 %d 次", hits)
	}
}

// 响应里的 valid_for_s 必须钳到物理上限（__oailb 的 exp-iat 恒 3900s）。不钳的后果是整个功能
// 被**静默**抵消：池子报 1e9（或把单位写成毫秒）⇒ 消费端的缓存永远 live ⇒ 满血窗口过了还在拿
// 烧掉的路由跑业务，而且池子再也不被调用（连日志都不再出现）。
func TestCookieClampsValidFor(t *testing.T) {
	for _, validForS := range []int{1_000_000_000, 3_900_000, 3901} {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			_, _ = io.WriteString(w, `{"gateway":"g","cookie":"__cflb=a; __oailb=b","valid_for_s":`+
				strconv.Itoa(validForS)+`}`)
		}))
		pair, err := New(srv.URL, "k", 0).Cookie(context.Background(), CookieRequest{})
		srv.Close()
		if err != nil {
			t.Fatalf("valid_for_s=%d 不该整票作废: %v", validForS, err)
		}
		if pair.ValidFor != maxValidFor {
			t.Fatalf("valid_for_s=%d 应钳到 %v，实际 %v", validForS, maxValidFor, pair.ValidFor)
		}
	}
	// 正常值原样通过。
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, `{"gateway":"g","cookie":"__cflb=a; __oailb=b","valid_for_s":150}`)
	}))
	defer srv.Close()
	pair, err := New(srv.URL, "k", 0).Cookie(context.Background(), CookieRequest{})
	if err != nil || pair.ValidFor != 150*time.Second {
		t.Fatalf("150s 应原样通过，实际 %v err=%v", pair.ValidFor, err)
	}
}

// 网关名同样是外部输入：它进本地账本的复合键、进日志、进 usage_logs 的一列，而 NUL 是合法
// UTF-8（ToValidUTF8 剔不掉）、Postgres 的 TEXT 直接拒收 ⇒ 一条脏行能连带让整批用量插入失败。
// 判废（返回空串）而不是截断：消费端对空网关名本来就有兜底（自己从 __oailb 解）。
func TestCookieSanitizesGateway(t *testing.T) {
	for _, gateway := range []string{
		"unified-1" + nulByte, "unified" + lfByte + "1", strings.Repeat("g", maxGatewayLen+1),
	} {
		body, err := json.Marshal(map[string]any{
			"gateway": gateway, "cookie": "__cflb=a; __oailb=b", "valid_for_s": 150,
		})
		if err != nil {
			t.Fatal(err)
		}
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write(body)
		}))
		pair, err := New(srv.URL, "k", 0).Cookie(context.Background(), CookieRequest{})
		srv.Close()
		if err != nil {
			t.Fatalf("畸形 gateway 不该废掉整张票: %v", err)
		}
		if pair.Gateway != "" {
			t.Fatalf("gateway %q 应判废，实际收上来 %q", gateway, pair.Gateway)
		}
	}
}

// 传输错误里的 URL 不许带 query：那一条会随消费端的 Ops 错误日志落库，而 query 里有
// account=<上游 account_id> 与 exclude / exclude_versions。
func TestTransportErrorDropsQuery(t *testing.T) {
	// 连一个必然拒绝的地址：错误是 *url.Error，URL 字段就是我们发出去的那条。
	client := New("http://127.0.0.1:1", "k", 0)
	_, err := client.Cookie(context.Background(), CookieRequest{
		Account: "acc-secret", Exclude: []string{"unified-7"}, ExcludeVersions: []string{"tkt-9"},
	})
	if err == nil {
		t.Fatal("打不通应当报错")
	}
	for _, leak := range []string{"acc-secret", "account=", "exclude", "tkt-9", "?"} {
		if strings.Contains(err.Error(), leak) {
			t.Fatalf("错误串里不许出现 %q: %s", leak, err.Error())
		}
	}
	// ErrPool 的字面特征与 *url.Error 的类型特征都要留着（调用方靠它们分类）。
	if !errors.Is(err, ErrPool) {
		t.Fatalf("应仍是 ErrPool: %v", err)
	}
	var transport *url.Error
	if !errors.As(err, &transport) {
		t.Fatalf("应仍是 *url.Error: %v", err)
	}
	if transport.URL != "http://127.0.0.1:1/cookie" {
		t.Fatalf("URL 应只剩路径: %q", transport.URL)
	}
}

// pair_remaining_s 是续期闸的唯一判据（消费端的 openAIGatewayPoolRenewBelow），所以缺失必须是 0
// （= 不续），畸形值要么是 0 要么远在阈值之上，不能变成一个「刚好低于阈值」的值。
func TestCookieReadsPairRemaining(t *testing.T) {
	for _, tc := range []struct {
		field string
		want  time.Duration
	}{
		{field: `,"pair_remaining_s":3000`, want: 3000 * time.Second},
		{field: `,"pair_remaining_s":600`, want: 600 * time.Second},
		{field: "", want: 0},                                     // 老池子没这个字段
		{field: `,"pair_remaining_s":-5`, want: 0},               // 负数 ⇒ 不续
		{field: `,"pair_remaining_s":999999`, want: maxValidFor}, // 钳到物理上限，仍远在阈值之上
	} {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			_, _ = io.WriteString(w, `{"gateway":"g","cookie":"__cflb=a; __oailb=b","valid_for_s":150`+
				tc.field+`}`)
		}))
		pair, err := New(srv.URL, "k", 0).Cookie(context.Background(), CookieRequest{})
		srv.Close()
		if err != nil {
			t.Fatalf("%q 不该整票作废: %v", tc.field, err)
		}
		if pair.PairRemaining != tc.want {
			t.Fatalf("%q ⇒ PairRemaining=%v，想要 %v", tc.field, pair.PairRemaining, tc.want)
		}
		if pair.ValidFor != 150*time.Second {
			t.Fatalf("%q 不该动满血窗口: %v", tc.field, pair.ValidFor)
		}
	}
}

// region 是消费端把落点按大区归档的唯一来源（账号卡片那九个格子）。缺失必须是空串而不是
// 猜一个：老版本池子不报它，猜出来的大区会让卡片把一个还能用的大区标成已烧过。
// 它和网关名一样进账号 extra 和前端 ⇒ 同样要过 sanitizeOpaque。
func TestCookieReadsRegion(t *testing.T) {
	for _, tc := range []struct{ field, want string }{
		{`,"region":"east-asia"`, "east-asia"},
		{``, ""},
		{`,"region":""`, ""},
		// 过长判废而不是截断 —— 这一格同时证明 region 真的过了 sanitizeOpaque
		//（控制字符那一支由 TestCookieSanitizesGateway 盯着，两者同一个函数）。
		{`,"region":"` + strings.Repeat("r", maxGatewayLen+1) + `"`, ""},
	} {
		body := `{"gateway":"unified-1","cookie":"__cflb=a; __oailb=b","valid_for_s":150` + tc.field + `}`
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte(body))
		}))
		pair, err := New(srv.URL, "k", 0).Cookie(context.Background(), CookieRequest{})
		srv.Close()
		if err != nil {
			t.Fatalf("%s: %v", tc.field, err)
		}
		if pair.Region != tc.want {
			t.Fatalf("%s: region = %q，应为 %q", tc.field, pair.Region, tc.want)
		}
	}
}
