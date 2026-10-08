package service

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/gwpool"
)

const gatewayPoolReportTimeout = 3 * time.Second

// 同一消费 key 下可跨实例对齐；不同 key 无法据标签关联上游账号。凭据和原始身份不离开本进程。
func gatewayPoolAccountTag(account *Account, identity string) string {
	key := account.gatewayPoolConsumerKey()
	if key == "" || identity == "" {
		return ""
	}
	hash := hmac.New(sha256.New, []byte(key))
	_, _ = hash.Write([]byte("gwpool-cooldown-v1\x00" + identity))
	return hex.EncodeToString(hash.Sum(nil))
}

// 只由已经确认的静置后观察触发，不产生任何上游探测。池子故障不影响正在服务的业务请求。
func (s *OpenAIGatewayService) reportGatewayPoolCooldown(account *Account, identity string, sample *gatewayPoolCooldownSample) {
	tag := gatewayPoolAccountTag(account, identity)
	if sample == nil || tag == "" {
		return
	}
	result := "degraded"
	if sample.Full {
		result = "full"
	}
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s\x00%s\x00%d\x00%s",
		tag, sample.Gateway, sample.AttemptAt.UnixNano(), result)))
	report := gwpool.CooldownReport{
		ID: hex.EncodeToString(sum[:]), AccountTag: tag, Gateway: sample.Gateway,
		WindowSeconds: sample.WindowSeconds, ElapsedSeconds: sample.ElapsedSeconds, Result: result,
		ObservedAt: sample.AttemptAt.UTC(),
	}
	s.enqueueGatewayPoolReport(account, report)
}
