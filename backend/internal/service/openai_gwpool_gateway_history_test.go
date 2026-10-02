//go:build unit

package service

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// 这条记录每发请求都可能被写一次，所以三件事得钉住：换网关立刻写、同一个网关在节流窗口里
// 不写、条目不会无限长。写爆了不是功能问题而是**每发请求一次 UPDATE + 一次调度快照同步**。
func TestNoteOpenAIGatewayUse(t *testing.T) {
	newSvc := func() (*OpenAIGatewayService, *turnStateAutoRepo) {
		repo := newTurnStateAutoRepo()
		return &OpenAIGatewayService{accountRepo: repo}, repo
	}

	t.Run("第一次落点要写，并记成当前网关", func(t *testing.T) {
		svc, repo := newSvc()
		acct := &Account{ID: 7}
		svc.noteOpenAIGatewayUse(context.Background(), acct, "unified-167")

		require.Len(t, repo.extraWrites, 1)
		require.Contains(t, repo.extraWrites[0], openAIGatewayHistoryExtraKey)
		rec, ok := readOpenAIGatewayHistory(acct)
		require.True(t, ok)
		require.Equal(t, "unified-167", rec.Current)
		require.Contains(t, rec.Seen, "unified-167")
	})

	t.Run("同一个网关在节流窗口里不再写", func(t *testing.T) {
		svc, repo := newSvc()
		acct := &Account{ID: 7}
		svc.noteOpenAIGatewayUse(context.Background(), acct, "unified-167")
		svc.noteOpenAIGatewayUse(context.Background(), acct, "unified-167")
		require.Len(t, repo.extraWrites, 1, "节流没生效：每发请求都会写一次账号行")
	})

	t.Run("换了网关立刻写", func(t *testing.T) {
		svc, repo := newSvc()
		acct := &Account{ID: 7}
		svc.noteOpenAIGatewayUse(context.Background(), acct, "unified-167")
		svc.noteOpenAIGatewayUse(context.Background(), acct, "unified-73")

		require.Len(t, repo.extraWrites, 2, "换网关被节流窗口压住了")
		rec, _ := readOpenAIGatewayHistory(acct)
		require.Equal(t, "unified-73", rec.Current)
		require.Len(t, rec.Seen, 2, "旧网关不该被挤掉")
	})

	// 切回一个刚用过的网关：节流必须按「当前网关没变」判，按「这个网关最近写过」判的话
	// 这一发会被吞掉，于是卡片上的当前网关一直停在上一个，正好把这张卡唯一要答的问题答错。
	t.Run("切回刚用过的网关也要立刻写", func(t *testing.T) {
		svc, repo := newSvc()
		acct := &Account{ID: 7}
		svc.noteOpenAIGatewayUse(context.Background(), acct, "unified-167")
		svc.noteOpenAIGatewayUse(context.Background(), acct, "unified-73")
		svc.noteOpenAIGatewayUse(context.Background(), acct, "unified-167")

		require.Len(t, repo.extraWrites, 3)
		rec, _ := readOpenAIGatewayHistory(acct)
		require.Equal(t, "unified-167", rec.Current, "切回去之后当前网关没跟上")
	})

	t.Run("节流窗口过了同一个网关也要刷新时间", func(t *testing.T) {
		svc, repo := newSvc()
		acct := &Account{ID: 7}
		svc.noteOpenAIGatewayUse(context.Background(), acct, "unified-167")

		rec, _ := readOpenAIGatewayHistory(acct)
		stale := rec
		stale.Seen = map[string]time.Time{
			"unified-167": time.Now().UTC().Add(-2 * openAIGatewayHistoryWriteInterval),
		}
		writeGatewayHistoryForTest(t, acct, stale)

		svc.noteOpenAIGatewayUse(context.Background(), acct, "unified-167")
		require.Len(t, repo.extraWrites, 2)
	})

	t.Run("读不出落点时什么都不做", func(t *testing.T) {
		svc, repo := newSvc()
		acct := &Account{ID: 7}
		svc.noteOpenAIGatewayUse(context.Background(), acct, "unified-167")
		svc.noteOpenAIGatewayUse(context.Background(), acct, "   ")

		require.Len(t, repo.extraWrites, 1)
		rec, _ := readOpenAIGatewayHistory(acct)
		require.Equal(t, "unified-167", rec.Current, "空网关把当前落点擦掉了")
	})
}

// 条目上限：超了丢最早的，当前网关不许被丢。
func TestPruneOpenAIGatewayHistory(t *testing.T) {
	now := time.Now().UTC()
	rec := openAIGatewayHistory{Seen: map[string]time.Time{}}
	for i := range openAIGatewayHistoryMax + 5 {
		rec.Seen[gatewayNameForTest(i)] = now.Add(-time.Duration(i) * time.Minute)
	}
	// 当前网关刻意挑一个**最老的**：按时间裁的话它正好会被裁掉。
	rec.Current = gatewayNameForTest(openAIGatewayHistoryMax + 4)

	pruneOpenAIGatewayHistory(&rec)

	require.Len(t, rec.Seen, openAIGatewayHistoryMax+1, "裁完该只剩上限条加上被保住的当前网关")
	require.Contains(t, rec.Seen, rec.Current, "当前网关被裁掉了")
	require.Contains(t, rec.Seen, gatewayNameForTest(0), "最新的那条被裁掉了")
}

func gatewayNameForTest(i int) string {
	return fmt.Sprintf("unified-%d", i)
}

// writeGatewayHistoryForTest 按**从库里读回来的样子**把记录塞进 extra（JSON 往返一圈），
// 而不是直接存结构体：生产里 extra 来自 JSONB，时间是字符串不是 time.Time。
func writeGatewayHistoryForTest(t *testing.T, a *Account, rec openAIGatewayHistory) {
	t.Helper()
	encoded, err := json.Marshal(rec)
	require.NoError(t, err)
	var generic map[string]any
	require.NoError(t, json.Unmarshal(encoded, &generic))
	a.Extra[openAIGatewayHistoryExtraKey] = generic
}
