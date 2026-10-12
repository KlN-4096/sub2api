package gwpool

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"
)

func TestCooldownReportWireAndRecommendationValidation(t *testing.T) {
	tag := strings.Repeat("a", 64)
	reports := []CooldownReport{
		{ID: strings.Repeat("b", 64), AccountTag: tag, Gateway: "unified-142", WindowSeconds: 7200, ElapsedSeconds: 7201, Result: "full"},
		{ID: strings.Repeat("c", 64), AccountTag: strings.Repeat("d", 64), Gateway: "unified-143", WindowSeconds: 3600, ElapsedSeconds: 3601, Result: "full"},
	}
	hits := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits++
		if r.Header.Get("Authorization") != "Bearer test-consumer" || r.URL.Path != "/cooldown/reports" {
			t.Error("上报认证或路径错误")
		}
		if r.Header.Get(cooldownMaxHeader) != "86400" {
			t.Error("client must negotiate the 24h recommendation ceiling")
		}
		var got struct {
			Reports []CooldownReport `json:"reports"`
		}
		if err := json.NewDecoder(r.Body).Decode(&got); err != nil || !slices.Equal(got.Reports, reports) {
			t.Errorf("上报协议字段不一致：%v", err)
		}
		_, _ = fmt.Fprintf(w, `{"ok":true,"results":[{"id":%q,"status":200,"recommendation":{"seconds":7200,"samples":2,"source":"account"}},`+
			`{"id":%q,"status":200,"recommendation":{"seconds":99999999,"samples":99,"source":"account"}}]}`, reports[0].ID, reports[1].ID)
	}))
	defer server.Close()
	client := New(server.URL, "test-consumer", time.Second)
	results, err := client.ReportCooldownBatch(context.Background(), reports)
	if err != nil || len(results) != 2 || results[0].Recommendation == nil || results[0].Recommendation.Seconds != 7200 || results[1].Recommendation != nil {
		t.Fatalf("合法上报未收到推荐，或非法推荐未被丢弃：%+v %v", results, err)
	}
	reports[1].Result = "network_error"
	if _, err := client.ReportCooldownBatch(context.Background(), reports); err == nil || hits != 1 {
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
