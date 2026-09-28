//go:build unit

package service

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/imroc/req/v3"
	"github.com/stretchr/testify/require"
)

// 侧信道必须与它陪跑的 /responses 走同一条传输栈。装了 OAuth 能力插件时推理面由插件接管出站，
// 侧信道若直接调 httpUpstream.Do，同一账号的两条请求会从不同出口发出——正是这个功能要消除的。
// 这里用一个"已路由但运行时不可用"的插件：走插件路径时 doOpenAIUpstream 直接返回错误，
// httpUpstream 一条都收不到；改回 httpUpstream.Do 就会收到，用例变红。
func TestCodexSideCallsGoThroughPluginTransport(t *testing.T) {
	account := wireProfileTestAccount(true)
	require.Equal(t, PlatformOpenAI, account.Platform)
	require.Equal(t, AccountTypeOAuth, account.Type)

	t.Run("plugin_routed_account_never_touches_http_upstream", func(t *testing.T) {
		svc, up := codexSideCallTestService()
		manager := &PluginManager{}
		manager.route.Store(&pluginRoute{rolloutPercent: 100, unavailable: "offline test"})
		svc.pluginManager = manager
		c := newConvTestContext(t, wireProfileTestBody(t))

		svc.scheduleCodexSideCalls(c, account, codexSideCallTestRequest("thread-plugin"))

		requireNoCodexSideCall(t, up)
	})

	t.Run("without_plugin_it_falls_through_to_http_upstream", func(t *testing.T) {
		svc, up := codexSideCallTestService()
		c := newConvTestContext(t, wireProfileTestBody(t))

		svc.scheduleCodexSideCalls(c, account, codexSideCallTestRequest("thread-plain"))

		require.NotNil(t, collectCodexSideCalls(t, up, 1)[chatGPTSettingsUserURL],
			"没有插件时才落到 httpUpstream，上一条子用例的零捕获才有意义")
	})
}

type wireTimezoneRefreshRepo struct {
	AccountRepository
	byID      map[int64]*Account
	written   chan map[string]any
	writtenTo chan int64
	deadline  chan bool
}

func (r *wireTimezoneRefreshRepo) GetByID(ctx context.Context, id int64) (*Account, error) {
	if err := ctx.Err(); err != nil {
		return nil, err // 真 repo 同样会这样：ctx 已取消就不查库
	}
	_, ok := ctx.Deadline()
	select {
	case r.deadline <- ok:
	default:
	}
	return r.byID[id], nil
}

func (r *wireTimezoneRefreshRepo) UpdateExtra(_ context.Context, id int64, updates map[string]any) error {
	r.writtenTo <- id
	r.written <- updates
	return nil
}

// wireTimezoneStubClient 返回一个不出网的 req.Client：ipinfo 的响应由 RoundTripper 直接给出。
func wireTimezoneStubClient(body string) *req.Client {
	client := req.C()
	client.GetTransport().WrapRoundTripFunc(func(http.RoundTripper) req.HttpRoundTripFunc {
		return func(r *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"application/json"}},
				Body:       io.NopCloser(strings.NewReader(body)),
				Request:    r,
			}, nil
		}
	})
	return client
}

// refreshCodexWireTimezone 的接线：影子行要按凭证账号判闸门（收敛开关挂在母账号上），
// 查出口要走被转发行自己的代理（与转发侧同一个表达式），写回的代理标签也要是被转发行的。
// 这三处此前只有纯函数被测，接线改坏了没人变红。
func TestRefreshCodexWireTimezoneResolvesShadowRow(t *testing.T) {
	parent := wireProfileTestAccount(true)
	parent.ID = 9101
	shadow := wireProfileTestAccount(true)
	shadow.ID = 9102
	shadow.ParentAccountID = &parent.ID
	// 影子行自己不带收敛开关：闸门必须去看母账号。
	delete(shadow.Extra, codexFingerprintConvergenceExtraKey)
	proxyID := int64(31)
	shadow.ProxyID = &proxyID
	shadow.Proxy = &Proxy{ID: proxyID, Protocol: "socks5", Host: "10.0.0.9", Port: 1080}

	repo := &wireTimezoneRefreshRepo{
		byID:      map[int64]*Account{parent.ID: parent, shadow.ID: shadow},
		written:   make(chan map[string]any, 4),
		writtenTo: make(chan int64, 4),
		deadline:  make(chan bool, 4),
	}
	proxyURLs := make(chan string, 4)
	svc := &OpenAIQuotaService{
		accountRepo: repo,
		privacyClientFactory: func(proxyURL string) (*req.Client, error) {
			proxyURLs <- proxyURL
			return wireTimezoneStubClient(`{"ip":"24.120.102.167","timezone":"America/Los_Angeles"}`), nil
		},
	}

	// 传一个已取消的 ctx：额度查询返回后请求 ctx 就结束了，后台这段必须脱离它（WithoutCancel）
	// 才跑得完；同时又必须自带 deadline，否则 DB / 外部查询无上限，卡住就泄一个 goroutine。
	reqCtx, cancelReq := context.WithCancel(context.Background())
	cancelReq()
	svc.refreshCodexWireTimezone(reqCtx, shadow.ID)

	var updates map[string]any
	select {
	case updates = <-repo.written:
	case <-time.After(5 * time.Second):
		t.Fatal("没有写回解析结果：影子行的闸门、代理取值或后台 ctx 脱离坏了")
	}
	require.True(t, <-repo.deadline, "后台 ctx 必须自带超时")
	require.Equal(t, shadow.ID, <-repo.writtenTo, "解析结果属于被转发的那一行，不能写到母账号上")
	require.Equal(t, "socks5://10.0.0.9:1080", <-proxyURLs, "必须走被转发行自己的代理")
	require.Equal(t, "America/Los_Angeles", updates[codexWireTimezoneResolvedExtraKey])
	require.Equal(t, "24.120.102.167", updates[codexWireTimezoneResolvedIPExtraKey])
	require.Equal(t, codexWireTimezoneProxyTag(shadow), updates[codexWireTimezoneResolvedProxyExtraKey])

	// 同一账号并发/连续再来一次不重复查（包级 5 分钟窗口）。
	svc.refreshCodexWireTimezone(context.Background(), shadow.ID)
	select {
	case <-repo.written:
		t.Fatal("同一账号在窗口内被解析了第二次")
	case <-time.After(300 * time.Millisecond):
	}
}
