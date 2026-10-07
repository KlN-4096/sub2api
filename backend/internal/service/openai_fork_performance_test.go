package service

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math/rand"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/tidwall/gjson"
)

// These benchmarks use only local fixtures. Repository counters count service
// calls, not SQL statements or database latency; the fixture deep-copies JSON to
// model the cost of loading a row without using production data or credentials.
type forkPerformanceRepo struct {
	*gatewayRuntimeRepo
	reads, scans, writes int
}

func (r *forkPerformanceRepo) GetByID(ctx context.Context, id int64) (*Account, error) {
	r.reads++
	return r.gatewayRuntimeRepo.GetByID(ctx, id)
}

func (r *forkPerformanceRepo) FindByExtraField(ctx context.Context, key string, value any) ([]Account, error) {
	r.scans++
	return r.gatewayRuntimeRepo.FindByExtraField(ctx, key, value)
}

func (r *forkPerformanceRepo) UpdateExtra(ctx context.Context, id int64, patch map[string]any) error {
	r.writes++
	return r.gatewayRuntimeRepo.UpdateExtra(ctx, id, patch)
}

func (r *forkPerformanceRepo) FindGatewayPoolStatePeers(_ context.Context, _ string, kind string) ([]Account, error) {
	r.scans++
	r.mu.Lock()
	defer r.mu.Unlock()
	extra := map[string]any{gatewayPoolUsageExtraKey: r.account.Extra[gatewayPoolUsageExtraKey],
		GatewayPoolUsageBlockedAtKey: r.account.Extra[GatewayPoolUsageBlockedAtKey]}
	if kind == "rest" {
		extra = map[string]any{gatewayPoolRestStateKey: r.account.Extra[gatewayPoolRestStateKey]}
	}
	raw, err := json.Marshal(extra)
	if err != nil {
		return nil, err
	}
	account := Account{ID: r.account.ID}
	if err := json.Unmarshal(raw, &account.Extra); err != nil {
		return nil, err
	}
	return []Account{account}, nil
}

func BenchmarkForkPerformanceUsageLifecycle(b *testing.B) {
	for _, extraBytes := range []int{0, 64 << 10} {
		b.Run(fmt.Sprintf("extra_%d", extraBytes), func(b *testing.B) {
			account := gwpoolTestAccount(1)
			account.Extra["unrelated_fixture"] = strings.Repeat("x", extraBytes)
			repo := &forkPerformanceRepo{gatewayRuntimeRepo: &gatewayRuntimeRepo{account: *account}}
			svc := &OpenAIGatewayService{accountRepo: repo}
			svc.codexCookies.accountByID = repo.GetByID
			identity := openAIGatewayPoolAccountKey(account)
			applied := OpenAIGatewayPoolApplied{AccountID: account.ID, Gateway: "offline", Version: "v"}
			svc.codexCookies.poolPairs.Store(identity, openAIGatewayPoolPair{
				gateway: applied.Gateway, version: applied.Version, cookie: "fixture",
				until: time.Now().Add(time.Hour), routeExpiresAt: time.Now().Add(time.Hour),
			})
			svc.codexCookies.gatewayPoolMarkVerifiedFull(identity, applied.Version, "gpt-6-luna")
			tag := gatewayPoolLedgerTag(identity)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				// Each iteration is a first real send, not the checkpoint fast
				// path of an already-recorded send. Do not grow the ledger.
				b.StopTimer()
				delete(repo.account.Extra, gatewayPoolUsageExtraKey)
				delete(repo.account.Extra, gatewayPoolUsageTagKey)
				delete(repo.account.Extra, gatewayPoolUsagePreviousTagKey)
				svc.codexCookies.poolUsageCache.Delete(tag)
				b.StartTimer()
				ctx, finish := svc.beginGatewayPoolUsageRequest(context.Background(), account)
				if finish == nil {
					b.Fatal("fixture did not enter gateway-pool accounting")
				}
				at := time.Now().UTC()
				if !svc.noteGatewayPoolFullUse(ctx, account, identity, applied, at) {
					svc.noteGatewayPoolUsage(ctx, account, identity, "gpt-6-luna", applied, at, false)
				}
				finish()
			}
			b.StopTimer()
			b.ReportMetric(float64(repo.reads)/float64(b.N), "repo_reads/op")
			b.ReportMetric(float64(repo.scans)/float64(b.N), "peer_scans/op")
			b.ReportMetric(float64(repo.writes)/float64(b.N), "repo_writes/op")
		})
	}
}

func forkPerformanceBody(size int) []byte {
	// Deterministic, poorly compressible image-like data, not a run of zeroes.
	raw := make([]byte, size*3/4)
	_, _ = rand.New(rand.NewSource(1)).Read(raw)
	encoded := base64.StdEncoding.EncodeToString(raw)
	return []byte(`{"model":"gpt-6-astra","input":[{"role":"user","content":"` + encoded +
		`"}],"stream":true,"prompt_cache_key":"fixture","client_metadata":{"session_id":"fixture"}}`)
}

func BenchmarkForkPerformanceBody(b *testing.B) {
	for _, size := range []int{1 << 20, 8 << 20} {
		b.Run(fmt.Sprintf("bytes_%d", size), func(b *testing.B) {
			body := forkPerformanceBody(size)
			canonical := reorderCodexTopLevelFields(body, codexResponsesFieldOrder)
			frame, err := encodeCodexZstdRequestBody(body)
			if err != nil || !json.Valid(canonical) {
				b.Fatalf("invalid fixture: %v", err)
			}
			for _, tc := range []struct {
				name string
				run  func() []byte
			}{
				{"canonical_order", func() []byte { return reorderCodexTopLevelFields(canonical, codexResponsesFieldOrder) }},
				{"legacy_canonical_order", func() []byte { return forkPerformanceLegacyReorder(canonical, codexResponsesFieldOrder) }},
				{"zstd", func() []byte {
					wire, err := encodeCodexZstdRequestBody(body)
					if err != nil {
						b.Fatal(err)
					}
					return wire
				}},
				{"legacy_zstd", func() []byte {
					enc, ok := codexRequestZstdEncoders.Get().(*zstd.Encoder)
					if !ok {
						b.Fatal("unexpected zstd encoder fixture")
					}
					defer codexRequestZstdEncoders.Put(enc)
					frame := enc.EncodeAll(body, make([]byte, 0, len(body)/3+64))
					wire, err := normalizeCodexZstdFrameHeader(frame)
					if err != nil {
						b.Fatal(err)
					}
					return wire
				}},
				{"frame_header", func() []byte {
					wire, err := normalizeCodexZstdFrameHeader(frame)
					if err != nil {
						b.Fatal(err)
					}
					return wire
				}},
			} {
				b.Run(tc.name, func(b *testing.B) {
					b.SetBytes(int64(len(body)))
					b.ReportAllocs()
					for i := 0; i < b.N; i++ {
						runtime.KeepAlive(tc.run())
					}
				})
			}
		})
	}
}

// Frozen pre-optimization implementation for paired same-process comparisons.
// It intentionally retains the complete canonical output allocation.
func forkPerformanceLegacyReorder(body []byte, order []string) []byte {
	parsed := gjson.ParseBytes(body)
	if !parsed.IsObject() {
		return body
	}
	type field struct{ key, raw string }
	fields := make([]field, 0, len(order))
	index := make(map[string]int, len(order))
	duplicate := false
	parsed.ForEach(func(key, value gjson.Result) bool {
		name := key.String()
		if _, ok := index[name]; ok {
			duplicate = true
			return false
		}
		index[name] = len(fields)
		fields = append(fields, field{key: key.Raw, raw: value.Raw})
		return true
	})
	if duplicate || len(fields) == 0 {
		return body
	}
	out := make([]byte, 0, len(body))
	out = append(out, '{')
	emitted := make([]bool, len(fields))
	emit := func(i int) {
		if len(out) > 1 {
			out = append(out, ',')
		}
		out = append(out, fields[i].key...)
		out = append(out, ':')
		out = append(out, fields[i].raw...)
		emitted[i] = true
	}
	for _, name := range order {
		if i, ok := index[name]; ok {
			emit(i)
		}
	}
	for i := range fields {
		if !emitted[i] {
			emit(i)
		}
	}
	return append(out, '}')
}

func BenchmarkForkPerformanceDisplayClones(b *testing.B) {
	identity := gwpoolTestIdentity
	tag := gatewayPoolLedgerTag(identity)
	now := time.Now().UTC()
	history := openAIGatewayHistory{LedgerTag: tag, UpdatedAt: now, Seen: map[string]openAIGatewaySeen{}}
	contacts := gatewayPoolContacts{LedgerTag: tag, Seen: map[string]gatewayPoolContactSeen{}}
	for i := 0; i < 128; i++ {
		gateway := fmt.Sprintf("offline-%d", i)
		history.Seen[gateway] = openAIGatewaySeen{At: now.Add(-time.Duration(i) * time.Minute), Region: "east-asia"}
		contacts.Seen[gateway] = gatewayPoolContactSeen{FirstAt: now.Add(-time.Hour), LastAt: now}
	}
	peers := make([]Account, 20)
	for i := range peers {
		peers[i] = *gwpoolTestAccount(int64(i + 1))
		peers[i].Extra[openAIGatewayHistoryExtraKey] = history
		peers[i].Extra[openAIGatewayPoolContactsExtraKey] = contacts
	}
	svc := &OpenAIGatewayService{}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache := gatewayPoolDisplayCache{}
		for row := range peers {
			h, c := svc.gatewayPoolDisplaySnapshot(&peers[row], identity, peers, cache)
			runtime.KeepAlive(h)
			runtime.KeepAlive(c)
		}
	}
}
