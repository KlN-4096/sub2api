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
		svc.noteOpenAIGatewayUse(context.Background(), acct, "unified-167", "", "", true, 0, 0, 0)

		require.Len(t, repo.extraWrites, 1)
		require.Contains(t, repo.extraWrites[0], openAIGatewayHistoryExtraKey)
		rec, ok := readOpenAIGatewayHistory(acct)
		require.True(t, ok)
		require.Equal(t, "unified-167", rec.Current)
		require.Contains(t, rec.Seen, "unified-167")
	})

	// 池子清单的两个读数：此刻能交付几个、其中这个号还没烧过几个。live<=0 表示这一发没问到
	// 清单（关了 steering、列表打不开），**整对不覆盖** —— 覆盖成 0 的话卡片会说「池子一个
	// 落点都没有」，和「池子真的空了」长得一模一样。拆开写还会出现新 free 配旧 live 的组合。
	t.Run("池子清单的两个读数成对记下，没问到时整对不覆盖", func(t *testing.T) {
		svc, _ := newSvc()
		acct := &Account{ID: 7}
		svc.noteOpenAIGatewayUse(context.Background(), acct, "unified-167", "", "", true, 62, 5, 0)
		rec, ok := readOpenAIGatewayHistory(acct)
		require.True(t, ok)
		require.Equal(t, 62, rec.PoolLive)
		require.Equal(t, 5, rec.PoolFree)

		// 换个网关穿过节流，这一发没问到清单 ⇒ 旧的那一对留着。
		svc.noteOpenAIGatewayUse(context.Background(), acct, "unified-73", "", "", true, 0, 0, 0)
		rec, ok = readOpenAIGatewayHistory(acct)
		require.True(t, ok)
		require.Equal(t, 62, rec.PoolLive, "没问到清单的那一发把读数抹掉了")
		require.Equal(t, 5, rec.PoolFree)

		// 可交付的全烧过了：free=0 是**真的 0**，照写。
		svc.noteOpenAIGatewayUse(context.Background(), acct, "unified-99", "", "", true, 41, 0, 0)
		rec, _ = readOpenAIGatewayHistory(acct)
		require.Equal(t, 41, rec.PoolLive)
		require.Equal(t, 0, rec.PoolFree)
	})

	t.Run("同一个网关在节流窗口里不再写", func(t *testing.T) {
		svc, repo := newSvc()
		acct := &Account{ID: 7}
		svc.noteOpenAIGatewayUse(context.Background(), acct, "unified-167", "", "", true, 0, 0, 0)
		svc.noteOpenAIGatewayUse(context.Background(), acct, "unified-167", "", "", true, 0, 0, 0)
		require.Len(t, repo.extraWrites, 1, "节流没生效：每发请求都会写一次账号行")
	})

	t.Run("换了网关立刻写", func(t *testing.T) {
		svc, repo := newSvc()
		acct := &Account{ID: 7}
		svc.noteOpenAIGatewayUse(context.Background(), acct, "unified-167", "", "", true, 0, 0, 0)
		svc.noteOpenAIGatewayUse(context.Background(), acct, "unified-73", "", "", true, 0, 0, 0)

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
		svc.noteOpenAIGatewayUse(context.Background(), acct, "unified-167", "", "", true, 0, 0, 0)
		svc.noteOpenAIGatewayUse(context.Background(), acct, "unified-73", "", "", true, 0, 0, 0)
		svc.noteOpenAIGatewayUse(context.Background(), acct, "unified-167", "", "", true, 0, 0, 0)

		require.Len(t, repo.extraWrites, 3)
		rec, _ := readOpenAIGatewayHistory(acct)
		require.Equal(t, "unified-167", rec.Current, "切回去之后当前网关没跟上")
	})

	t.Run("节流窗口过了同一个网关也要刷新时间", func(t *testing.T) {
		svc, repo := newSvc()
		acct := &Account{ID: 7}
		svc.noteOpenAIGatewayUse(context.Background(), acct, "unified-167", "", "", true, 0, 0, 0)

		rec, _ := readOpenAIGatewayHistory(acct)
		stale := rec
		stale.Seen = map[string]openAIGatewaySeen{
			"unified-167": {At: time.Now().UTC().Add(-2 * openAIGatewayHistoryWriteInterval)},
		}
		writeGatewayHistoryForTest(t, acct, stale)

		svc.noteOpenAIGatewayUse(context.Background(), acct, "unified-167", "", "", true, 0, 0, 0)
		require.Len(t, repo.extraWrites, 2)
	})

	// 大区是这张卡按「九个大区」归档落点的唯一依据。
	t.Run("大区跟着落点一起记", func(t *testing.T) {
		svc, _ := newSvc()
		acct := &Account{ID: 7}
		svc.noteOpenAIGatewayUse(context.Background(), acct, "unified-167", "east-asia", "", true, 0, 0, 0)

		rec, _ := readOpenAIGatewayHistory(acct)
		require.Equal(t, "east-asia", rec.CurrentRegion)
		require.Equal(t, "east-asia", rec.Seen["unified-167"].Region)
	})

	// 同一个网关的大区不会变（网关 = 大区 × 账号）⇒ 这一发读不出大区时要留着上次记的那个，
	// 否则一发改派就把这一格的归档擦成「未归类」。
	t.Run("读不出大区时不擦掉已记的", func(t *testing.T) {
		svc, _ := newSvc()
		acct := &Account{ID: 7}
		svc.noteOpenAIGatewayUse(context.Background(), acct, "unified-167", "east-asia", "", true, 0, 0, 0)
		svc.noteOpenAIGatewayUse(context.Background(), acct, "unified-73", "", "", true, 0, 0, 0)
		svc.noteOpenAIGatewayUse(context.Background(), acct, "unified-167", "", "", true, 0, 0, 0)

		rec, _ := readOpenAIGatewayHistory(acct)
		require.Equal(t, "east-asia", rec.Seen["unified-167"].Region)
		require.Equal(t, "east-asia", rec.CurrentRegion)
	})

	// 第一次知道某个网关的大区时必须立刻落盘，哪怕当前网关没变、还在节流窗口里：
	// 被节流窗口压住的话这一格要等五分钟才归档，而满血窗口本来就只有几分钟。
	t.Run("第一次拿到大区要穿过节流窗口", func(t *testing.T) {
		svc, repo := newSvc()
		acct := &Account{ID: 7}
		svc.noteOpenAIGatewayUse(context.Background(), acct, "unified-167", "", "", true, 0, 0, 0)
		svc.noteOpenAIGatewayUse(context.Background(), acct, "unified-167", "east-asia", "", true, 0, 0, 0)

		require.Len(t, repo.extraWrites, 2, "新的大区被节流窗口吞掉了")
		rec, _ := readOpenAIGatewayHistory(acct)
		require.Equal(t, "east-asia", rec.Seen["unified-167"].Region)
	})

	// 大区没新消息时节流照旧生效（这是节流存在的理由：每发请求一次 UPDATE）。
	t.Run("大区重复上报不绕过节流", func(t *testing.T) {
		svc, repo := newSvc()
		acct := &Account{ID: 7}
		svc.noteOpenAIGatewayUse(context.Background(), acct, "unified-167", "east-asia", "", true, 0, 0, 0)
		svc.noteOpenAIGatewayUse(context.Background(), acct, "unified-167", "east-asia", "", true, 0, 0, 0)
		require.Len(t, repo.extraWrites, 1, "同一个大区重复上报也在写库")
	})

	t.Run("读不出落点时什么都不做", func(t *testing.T) {
		svc, repo := newSvc()
		acct := &Account{ID: 7}
		svc.noteOpenAIGatewayUse(context.Background(), acct, "unified-167", "", "", true, 0, 0, 0)
		svc.noteOpenAIGatewayUse(context.Background(), acct, "   ", "", "", true, 0, 0, 0)

		require.Len(t, repo.extraWrites, 1)
		rec, _ := readOpenAIGatewayHistory(acct)
		require.Equal(t, "unified-167", rec.Current, "空网关把当前落点擦掉了")
	})

	// state-echo 读数（2026-10-02 加的两个字段）。卡片上「验过是满血」和「没验过」是两回事。
	t.Run("判定写进落点，判定变了要穿过节流", func(t *testing.T) {
		svc, repo := newSvc()
		acct := &Account{ID: 7}
		seen := func() openAIGatewaySeen {
			rec, _ := readOpenAIGatewayHistory(acct)
			return rec.Seen["unified-167"]
		}

		svc.noteOpenAIGatewayUse(context.Background(), acct, "unified-167", "", openAIGatewayVerdictFull, true, 0, 0, 0)
		require.Len(t, repo.extraWrites, 1)
		require.Equal(t, openAIGatewayVerdictFull, seen().Verdict)
		fullAt := seen().FullAt
		require.False(t, fullAt.IsZero(), "判成满血必须记下时刻：它是唯一有意义的回归起点")

		// 同一个网关、同一个判定、还在节流窗口里 ⇒ 不写。
		svc.noteOpenAIGatewayUse(context.Background(), acct, "unified-167", "", openAIGatewayVerdictFull, true, 0, 0, 0)
		require.Len(t, repo.extraWrites, 1, "判定没变就该被节流压住")

		// 判定变了必须立刻写：「这个落点刚被判降智」正是这张卡要看的事。
		svc.noteOpenAIGatewayUse(context.Background(), acct, "unified-167", "", openAIGatewayVerdictDegraded, true, 0, 0, 0)
		require.Len(t, repo.extraWrites, 2)
		require.Equal(t, openAIGatewayVerdictDegraded, seen().Verdict)
		require.Equal(t, fullAt, seen().FullAt, "降智不许把上一次判成满血的时刻擦掉")

		// 这一发没判据（判据关着 / 没送票 / 非 200）⇒ 留着上一次的判定，别当成「没验过」。
		svc.noteOpenAIGatewayUse(context.Background(), acct, "unified-73", "", "", true, 0, 0, 0)
		svc.noteOpenAIGatewayUse(context.Background(), acct, "unified-167", "", "", true, 0, 0, 0)
		require.Equal(t, openAIGatewayVerdictDegraded, seen().Verdict, "没判 ≠ 判不出来")
		require.Equal(t, fullAt, seen().FullAt)
	})

	// queue 档的预热在业务请求**之前**判死一批落点。那些落点必须进 Seen（否则每轮真烧 4 个
	// (账号 × 网关) + 8 发上游配额，事后页面上一条痕迹都没有：失败路径不落用量行，
	// 本地账本 poolUsed 重启即失），但**不许推进 Current** —— 上面永远不会有业务请求。
	t.Run("只记 Seen 不推进当前网关", func(t *testing.T) {
		svc, repo := newSvc()
		acct := &Account{ID: 7}
		svc.noteOpenAIGatewayUse(context.Background(), acct, "unified-167", "east-asia", openAIGatewayVerdictFull, true, 0, 0, 0)
		svc.noteOpenAIGatewayUse(context.Background(), acct, "unified-73", "us-east", openAIGatewayVerdictDegraded, false, 0, 0, 0)

		require.Len(t, repo.extraWrites, 2, "判死的落点也要落盘")
		rec, _ := readOpenAIGatewayHistory(acct)
		require.Equal(t, "unified-167", rec.Current, "预热判死的落点不许变成「当前网关」")
		require.Equal(t, "east-asia", rec.CurrentRegion)
		require.Equal(t, openAIGatewayVerdictDegraded, rec.Seen["unified-73"].Verdict)
		require.Equal(t, "us-east", rec.Seen["unified-73"].Region)
	})

	// 持续被验成满血的落点，它的 FullAt 至少每个满血窗口刷新一次。
	//
	// 不穿过节流（5 分钟 > 满血窗口 183 秒）的话 FullAt 停在第一次那一刻 ⇒ 前端那一格
	// 183 秒后掉成琥珀，而它可能几十秒前刚验过 —— 运营方照着这一格挑落点，会跳过一个好的。
	t.Run("满血判定过了满血窗口要穿过节流刷新时刻", func(t *testing.T) {
		svc, repo := newSvc()
		acct := &Account{ID: 7}
		svc.noteOpenAIGatewayUse(context.Background(), acct, "unified-167", "", openAIGatewayVerdictFull, true, 0, 0, 0)
		require.Len(t, repo.extraWrites, 1)

		// 把这条记录往回拨一个满血窗口多一点（At 仍在节流窗口里 ⇒ 只有满血那条规则能救它）。
		rec, _ := readOpenAIGatewayHistory(acct)
		aged := time.Now().UTC().Add(-openAIGatewayFullWindow - time.Second)
		rec.Seen["unified-167"] = openAIGatewaySeen{
			At: time.Now().UTC(), Verdict: openAIGatewayVerdictFull, FullAt: aged,
		}
		writeGatewayHistoryForTest(t, acct, rec)

		svc.noteOpenAIGatewayUse(context.Background(), acct, "unified-167", "", openAIGatewayVerdictFull, true, 0, 0, 0)
		require.Len(t, repo.extraWrites, 2, "满血窗口已过的 FullAt 被节流压住了")
		refreshed, _ := readOpenAIGatewayHistory(acct)
		require.True(t, refreshed.Seen["unified-167"].FullAt.After(aged), "FullAt 没刷新")
	})
}

// 条目上限：超了丢最早的，当前网关不许被丢。
func TestPruneOpenAIGatewayHistory(t *testing.T) {
	now := time.Now().UTC()
	rec := openAIGatewayHistory{Seen: map[string]openAIGatewaySeen{}}
	for i := range openAIGatewayHistoryMax + 5 {
		rec.Seen[gatewayNameForTest(i)] = openAIGatewaySeen{At: now.Add(-time.Duration(i) * time.Minute)}
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
