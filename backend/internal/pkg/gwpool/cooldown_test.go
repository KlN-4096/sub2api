package gwpool

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestCooldownReportWireAndRecommendationValidation(t *testing.T) {
	tag := strings.Repeat("a", 64)
	report := CooldownReport{ID: strings.Repeat("b", 64), AccountTag: tag, Gateway: "unified-142",
		WindowSeconds: 7200, ElapsedSeconds: 7201, Result: "full"}
	hits := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits++
		if r.Header.Get("Authorization") != "Bearer test-consumer" || r.URL.Path != "/cooldown/report" {
			t.Error("上报认证或路径错误")
		}
		if r.Header.Get(cooldownMaxHeader) != "86400" {
			t.Error("client must negotiate the 24h recommendation ceiling")
		}
		var got CooldownReport
		if err := json.NewDecoder(r.Body).Decode(&got); err != nil || got != report {
			t.Errorf("上报协议字段不一致：%v", err)
		}
		_, _ = io.WriteString(w, `{"ok":true,"recommendation":{"seconds":7200,"samples":2,"source":"account"}}`)
	}))
	defer server.Close()
	client := New(server.URL, "test-consumer", time.Second)
	rec, err := client.ReportCooldown(context.Background(), report)
	if err != nil || rec == nil || rec.Seconds != 7200 {
		t.Fatalf("合法上报未收到推荐：%+v %v", rec, err)
	}
	report.Result = "network_error"
	if _, err := client.ReportCooldown(context.Background(), report); err == nil || hits != 1 {
		t.Fatal("未知/网络错误结果不应发送")
	}
	for _, rec := range []CooldownRecommendation{
		{Seconds: 100000, Samples: 999, Source: "pool"},
		{Seconds: 3600, Samples: 1, Source: "account"},
		{Seconds: 3600, Samples: 2, Source: "pool"},
		{Seconds: 3600, Samples: 99, Source: "unknown"},
		{Seconds: 3700, Samples: 99, Source: "account"},
	} {
		if rec.Valid() {
			t.Fatalf("外部非法建议被当成决策依据：%+v", rec)
		}
	}
	for _, seconds := range []int{43200, 57600, 72000, 86400} {
		if !(CooldownRecommendation{Seconds: seconds, Samples: 2, Source: "account"}).Valid() {
			t.Fatalf("new standard cooldown step rejected: %d", seconds)
		}
	}
}

func TestGatewayListCarriesAnonymousTagAndFiltersBadRecommendation(t *testing.T) {
	tag := strings.Repeat("a", 64)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get(cooldownAccountHeader) != tag || strings.Contains(r.URL.String(), tag) {
			t.Error("匿名标识应放请求头，不进入 URL")
		}
		_, _ = io.WriteString(w, `{"gateways":[
			{"name":"unified-142","pair_ready":true,"cooldown":{"seconds":7200,"samples":2,"source":"account"}},
			{"name":"unified-143","pair_ready":true,"cooldown":{"seconds":99999999,"samples":99,"source":"account"}}]}`)
	}))
	defer server.Close()
	list, err := New(server.URL, "test-consumer", time.Second).Gateways(context.Background(), "", tag)
	if err != nil || len(list) != 2 || list[0].Cooldown == nil || list[1].Cooldown != nil {
		t.Fatalf("推荐边界解析错误：%+v %v", list, err)
	}
}
