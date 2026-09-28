package bridge

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// 估算台账测试辅助
// ---------------------------------------------------------------------------

// estimateRegister 用指定 data_dir 注册一个服务，便于验证估算存档的跨进程行为。
func estimateRegister(t *testing.T, dir string) *Service {
	t.Helper()
	s := NewService()
	configYAML := fmt.Sprintf("data_dir: %q\n", filepath.ToSlash(dir))
	configYAML += "models:\n  - id: deepseek-flash\n    upstream_id: cline-pass/deepseek-v4.1-flash\n"
	if _, err := s.Handle("plugin.register", jsonBytes(map[string]any{"config_yaml": []byte(configYAML)})); err != nil {
		t.Fatalf("register plugin: %v", err)
	}
	return s
}

func estimateAddCredential(t *testing.T, s *Service, c Credential) {
	t.Helper()
	s.mu.Lock()
	s.creds[c.ID] = c
	s.mu.Unlock()
}

func estimateUsageValue(account, plan, kind string, percent float64, reset, at time.Time) credentialUsage {
	p, r, ts := percent, reset, at
	return credentialUsage{
		accountHash: account,
		planHash:    plan,
		Status:      "ok",
		UpdatedAt:   &ts,
		Limits:      []usageLimit{{Type: kind, PercentUsed: &p, ResetsAt: &r}},
	}
}

// estimateCalibrate 模拟一次成功额度查询后的标定调用。
func estimateCalibrate(s *Service, c Credential, value credentialUsage) {
	before := s.estimateActivitySnapshot()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calibrateEstimateLocked(c, value, before)
}

// estimateRecordUsage 走真实 appendLog 路径记一笔可计价请求。
func estimateRecordUsage(t *testing.T, s *Service, c Credential, upstreamModel string, prompt, completion, cached, write int64) {
	t.Helper()
	key := s.startEstimateRequest(c)
	if key == "" {
		t.Fatalf("credential %q has no estimate key", c.ID)
	}
	s.appendLog(LogEntry{
		estimateKey:        key,
		CredentialID:       c.ID,
		UsageReported:      true,
		UpstreamModel:      upstreamModel,
		PromptTokens:       prompt,
		CompletionTokens:   completion,
		CachedTokens:       cached,
		CacheWriteTokens:   write,
		CacheWriteReported: write > 0,
		Status:             200,
		Time:               time.Now().UTC(),
		Attempts:           []Attempt{{Status: 200, Mode: "stream"}},
	})
}

func estimateRecordFailure(t *testing.T, s *Service, c Credential) {
	t.Helper()
	key := s.startEstimateRequest(c)
	s.appendLog(LogEntry{
		estimateKey:  key,
		CredentialID: c.ID,
		Status:       500,
		Attempts:     []Attempt{{Status: 500, Mode: "stream", Error: "upstream failed"}},
	})
}

func estimateWindowSnapshot(t *testing.T, s *Service, account, kind string) estimateWindow {
	t.Helper()
	s.mu.RLock()
	defer s.mu.RUnlock()
	a := s.estimates.Accounts[account]
	if a == nil {
		t.Fatalf("estimate account %q 不存在", account)
	}
	w := a.Windows[kind]
	if w == nil {
		t.Fatalf("estimate window %q 不存在", kind)
	}
	return *w
}

func estimateKeySnapshot(t *testing.T, s *Service, key string) estimateKey {
	t.Helper()
	s.mu.RLock()
	defer s.mu.RUnlock()
	k := s.estimates.Keys[key]
	if k == nil {
		t.Fatalf("estimate key %q 不存在", key)
	}
	return *k
}

func estimateTotalsOf(t *testing.T, s *Service, account string) estimateTotals {
	t.Helper()
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.estimateTotalsLocked(account)
}

func estimateViewOf(t *testing.T, s *Service, c Credential, value credentialUsage) accountEstimate {
	t.Helper()
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := s.estimateViewLocked(c, value)
	if out == nil {
		t.Fatal("estimate view is nil")
	}
	return *out
}

func estimateCloseTo(t *testing.T, got, want float64, label string) {
	t.Helper()
	if math.IsNaN(got) || math.Abs(got-want) > 1e-9*math.Max(1, math.Abs(want)) {
		t.Fatalf("%s = %v, 期望 %v", label, got, want)
	}
}

func estimatePricedEntry(upstreamModel string, prompt, completion, cached, write int64, writeReported bool) LogEntry {
	return LogEntry{
		UpstreamModel:      upstreamModel,
		UsageReported:      true,
		PromptTokens:       prompt,
		CompletionTokens:   completion,
		CachedTokens:       cached,
		CacheWriteTokens:   write,
		CacheWriteReported: writeReported,
		Status:             200,
		Attempts:           []Attempt{{Status: 200}},
	}
}

// ---------------------------------------------------------------------------
// 账本身份与归并
// ---------------------------------------------------------------------------

func TestEstimateAccountsWithSameLabelStayIsolated(t *testing.T) {
	s := registeredService(t, "")
	c1 := Credential{ID: "same-1", Type: Provider, APIKey: "key-account-a", Label: "同一标签"}
	c2 := Credential{ID: "same-2", Type: Provider, APIKey: "key-account-b", Label: "同一标签"}
	estimateAddCredential(t, s, c1)
	estimateAddCredential(t, s, c2)
	if estimateKeyID(c1) == estimateKeyID(c2) {
		t.Fatal("相同标签的不同 API Key 被合并成同一估算主体")
	}
	reset := time.Now().UTC().Add(240 * time.Hour)
	base := time.Now().UTC().Add(-2 * time.Hour)
	estimateCalibrate(s, c1, estimateUsageValue("account-a", "plan-a", "five_hour", 10, reset, base))
	estimateCalibrate(s, c2, estimateUsageValue("account-b", "plan-b", "five_hour", 10, reset, base))
	if got := estimateKeySnapshot(t, s, estimateKeyID(c1)).Account; got != "account-a" {
		t.Fatalf("c1 account = %q, 期望 account-a", got)
	}
	if got := estimateKeySnapshot(t, s, estimateKeyID(c2)).Account; got != "account-b" {
		t.Fatalf("c2 account = %q, 期望 account-b", got)
	}
	estimateRecordUsage(t, s, c1, "cline-pass/deepseek-v4.1-flash", 200000, 1000, 0, 0)
	if got := estimateTotalsOf(t, s, "account-a").Requests; got != 1 {
		t.Fatalf("account-a requests = %d, 期望 1", got)
	}
	if got := estimateTotalsOf(t, s, "account-b").Requests; got != 0 {
		t.Fatalf("account-b 被同标签账号的消费污染: requests = %d", got)
	}
	if view := estimateViewOf(t, s, c1, estimateUsageValue("account-a", "plan-a", "five_hour", 10, reset, base)); view.SharedCredentials != 1 {
		t.Fatalf("标签被当成账号身份: shared_credentials = %d, 期望 1", view.SharedCredentials)
	}
}

func TestEstimateSharedAccountAggregatesKeysAndRebaselinesOnNewBinding(t *testing.T) {
	s := registeredService(t, "")
	c1 := Credential{ID: "k1", Type: Provider, APIKey: "key-one", Label: "主账号"}
	c2 := Credential{ID: "k2", Type: Provider, APIKey: "key-two", Label: "主账号"}
	c3 := Credential{ID: "k3", Type: Provider, APIKey: "key-three", Label: "新绑定"}
	for _, c := range []Credential{c1, c2, c3} {
		estimateAddCredential(t, s, c)
	}
	reset := time.Now().UTC().Add(240 * time.Hour)
	t0 := time.Now().UTC().Add(-4 * time.Hour)
	estimateCalibrate(s, c1, estimateUsageValue("account-shared", "plan-shared", "five_hour", 10, reset, t0))
	estimateCalibrate(s, c2, estimateUsageValue("account-shared", "plan-shared", "five_hour", 10, reset, t0))
	for _, c := range []Credential{c1, c2} {
		if got := estimateKeySnapshot(t, s, estimateKeyID(c)).Account; got != "account-shared" {
			t.Fatalf("%s account = %q, 期望 account-shared", c.ID, got)
		}
	}
	estimateRecordUsage(t, s, c1, "cline-pass/deepseek-v4.1-flash", 200000, 1000, 0, 0)
	estimateRecordUsage(t, s, c2, "cline-pass/deepseek-v4.1-flash", 200000, 1000, 0, 0)
	if got := estimateTotalsOf(t, s, "account-shared").Requests; got != 2 {
		t.Fatalf("同账号多 key 未归并: requests = %d, 期望 2", got)
	}
	// 先完成一个可校准区间，让窗口里存在已累积样本。
	estimateCalibrate(s, c1, estimateUsageValue("account-shared", "plan-shared", "five_hour", 12, reset, t0.Add(time.Hour)))
	if got := estimateWindowSnapshot(t, s, "account-shared", "five_hour").Samples; got != 1 {
		t.Fatalf("区间样本未累积: samples = %d, 期望 1", got)
	}
	// 新绑定的同账号 key 必须重新基线，不能继承已有窗口样本。
	estimateCalibrate(s, c3, estimateUsageValue("account-shared", "plan-shared", "five_hour", 12, reset, t0.Add(2*time.Hour)))
	w := estimateWindowSnapshot(t, s, "account-shared", "five_hour")
	if w.Samples != 0 || w.Cost.Low != 0 || w.Cost.High != 0 || w.Delta != 0 {
		t.Fatalf("新绑定未重基线: %+v", w)
	}
	if w.Base == nil || w.Base.Percent != 12 {
		t.Fatalf("新绑定基线错误: %+v", w.Base)
	}
}

// ---------------------------------------------------------------------------
// 采样区间
// ---------------------------------------------------------------------------

func TestEstimateAccumulatesConsumptionWhilePercentUnchanged(t *testing.T) {
	s := registeredService(t, "")
	c := Credential{ID: "acc-key", Type: Provider, APIKey: "key-acc", Label: "累计"}
	estimateAddCredential(t, s, c)
	reset := time.Now().UTC().Add(240 * time.Hour)
	t0 := time.Now().UTC().Add(-4 * time.Hour)
	usage := func(percent float64, at time.Time) credentialUsage {
		return estimateUsageValue("account-acc", "plan-acc", "five_hour", percent, reset, at)
	}
	perRequest := (200000*0.30 + 1000*1.20) / 1e6

	estimateCalibrate(s, c, usage(10, t0))
	estimateRecordUsage(t, s, c, "cline-pass/deepseek-v4.1-flash", 200000, 1000, 0, 0)
	estimateCalibrate(s, c, usage(10, t0.Add(time.Hour)))
	if w := estimateWindowSnapshot(t, s, "account-acc", "five_hour"); w.Samples != 0 {
		t.Fatalf("百分比未变时不应产生样本: %+v", w)
	}
	estimateRecordUsage(t, s, c, "cline-pass/deepseek-v4.1-flash", 200000, 1000, 0, 0)
	estimateCalibrate(s, c, usage(12, t0.Add(2*time.Hour)))
	w := estimateWindowSnapshot(t, s, "account-acc", "five_hour")
	estimateCloseTo(t, w.Cost.Low, 2*perRequest, "累计区间成本")
	estimateCloseTo(t, w.Cost.High, 2*perRequest, "累计区间成本上限")
	if w.Requests != 2 || w.Samples != 1 || w.Delta != 2 {
		t.Fatalf("百分比不变期间的消费被丢弃: %+v", w)
	}

	// 第二轮：百分比仍不变时继续消费，下一次上涨时也应被计入。
	estimateRecordUsage(t, s, c, "cline-pass/deepseek-v4.1-flash", 200000, 1000, 0, 0)
	estimateCalibrate(s, c, usage(12, t0.Add(3*time.Hour)))
	estimateCalibrate(s, c, usage(14, t0.Add(3*time.Hour+30*time.Minute)))
	w = estimateWindowSnapshot(t, s, "account-acc", "five_hour")
	estimateCloseTo(t, w.Cost.Low, 3*perRequest, "二次累计区间成本")
	if w.Requests != 3 || w.Samples != 2 || w.Delta != 4 {
		t.Fatalf("第二轮百分比不变期间的消费被丢弃: %+v", w)
	}
}

func TestEstimateExcludesIntervalsWithUnknownOrFailedRequests(t *testing.T) {
	cases := []struct {
		name string
		// suffix 只用于区分测试内的 API Key。
		suffix string
		record func(t *testing.T, s *Service, c Credential)
	}{
		{
			name:   "失败请求",
			suffix: "failed",
			record: func(t *testing.T, s *Service, c Credential) {
				estimateRecordFailure(t, s, c)
			},
		},
		{
			name:   "未计价模型",
			suffix: "unpriced",
			record: func(t *testing.T, s *Service, c Credential) {
				estimateRecordUsage(t, s, c, "cline-pass/mimo-v2.6", 200000, 1000, 0, 0)
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := registeredService(t, "")
			c := Credential{ID: "unknown-key", Type: Provider, APIKey: "key-unknown-" + tc.suffix, Label: "缺失"}
			estimateAddCredential(t, s, c)
			reset := time.Now().UTC().Add(240 * time.Hour)
			t0 := time.Now().UTC().Add(-2 * time.Hour)
			account := "account-unknown-" + tc.suffix
			plan := "plan-unknown-" + tc.suffix
			usage := func(percent float64, at time.Time) credentialUsage {
				return estimateUsageValue(account, plan, "five_hour", percent, reset, at)
			}

			estimateCalibrate(s, c, usage(10, t0))
			// 区间内同时有可计费消费与缺失计价，整个区间都必须排除。
			estimateRecordUsage(t, s, c, "cline-pass/deepseek-v4.1-flash", 200000, 1000, 0, 0)
			tc.record(t, s, c)
			estimateCalibrate(s, c, usage(13, t0.Add(time.Hour)))
			w := estimateWindowSnapshot(t, s, account, "five_hour")
			if w.Cost.Low != 0 || w.Cost.High != 0 || w.Samples != 0 || w.Requests != 0 {
				t.Fatalf("含缺失计价的区间被错误校准: %+v", w)
			}
			if w.Base == nil || w.Base.Totals.Unknown != 1 {
				t.Fatalf("未重新采样: %+v", w.Base)
			}
			// 排除区间之后的新消费仍要能正常累计。
			estimateRecordUsage(t, s, c, "cline-pass/deepseek-v4.1-flash", 200000, 1000, 0, 0)
			estimateCalibrate(s, c, usage(15, t0.Add(2*time.Hour)))
			w = estimateWindowSnapshot(t, s, account, "five_hour")
			perRequest := (200000*0.30 + 1000*1.20) / 1e6
			estimateCloseTo(t, w.Cost.Low, perRequest, "排除区间后的成本")
			if w.Samples != 1 || w.Requests != 1 {
				t.Fatalf("排除区间后未恢复采样: %+v", w)
			}
		})
	}
}

func TestEstimateRestartsSamplingOnResetOrPercentDrop(t *testing.T) {
	s := registeredService(t, "")
	c := Credential{ID: "reset-key", Type: Provider, APIKey: "key-reset", Label: "重置"}
	estimateAddCredential(t, s, c)
	r1 := time.Now().UTC().Add(120 * time.Hour)
	r2 := time.Now().UTC().Add(240 * time.Hour)
	t0 := time.Now().UTC().Add(-2 * time.Hour)

	estimateCalibrate(s, c, estimateUsageValue("account-reset", "plan-reset", "five_hour", 10, r1, t0))
	estimateRecordUsage(t, s, c, "cline-pass/deepseek-v4.1-flash", 200000, 1000, 0, 0)
	// reset 时间变化：窗口必须重建，旧周期消费不得算进新周期。
	estimateCalibrate(s, c, estimateUsageValue("account-reset", "plan-reset", "five_hour", 12, r2, t0.Add(time.Hour)))
	w := estimateWindowSnapshot(t, s, "account-reset", "five_hour")
	if !w.Reset.Equal(r2) || w.Base == nil || w.Base.Percent != 12 || w.Samples != 0 || w.Cost.Low != 0 || w.Cost.High != 0 {
		t.Fatalf("reset 后未重新采样: %+v", w)
	}
	// 同一 reset 窗口内百分比下降：同样视为重新采样。
	estimateCalibrate(s, c, estimateUsageValue("account-reset", "plan-reset", "five_hour", 5, r2, t0.Add(2*time.Hour)))
	w = estimateWindowSnapshot(t, s, "account-reset", "five_hour")
	if w.Base == nil || w.Base.Percent != 5 || w.Samples != 0 || w.Cost.Low != 0 || w.Delta != 0 {
		t.Fatalf("额度下降后未重新采样: %+v", w)
	}
}

func TestEstimateResetJitterKeepsPendingUntilSecondPoint(t *testing.T) {
	for _, kind := range []string{"five_hour", "weekly", "monthly"} {
		kind := kind
		t.Run(kind, func(t *testing.T) {
			s := registeredService(t, "")
			c := Credential{ID: "jitter-" + kind, Type: Provider, APIKey: "key-jitter-" + kind, Label: "抖动"}
			estimateAddCredential(t, s, c)
			account, plan := "account-jitter", "plan-jitter"

			// 同一 reset 锚点被 Cline 重算到相邻秒，±2s 内不得重建窗口。
			resetAnchor := time.Now().UTC().Add(240 * time.Hour).Truncate(time.Second).Add(500 * time.Millisecond)
			resetAt10 := resetAnchor.Add(600 * time.Millisecond)
			resetAt11 := resetAnchor.Add(-600 * time.Millisecond)
			t0 := time.Now().UTC().Add(-4 * time.Hour)
			t1 := t0.Add(time.Hour)
			t2 := t0.Add(2 * time.Hour)
			usage := func(percent float64, reset, at time.Time) credentialUsage {
				return estimateUsageValue(account, plan, kind, percent, reset, at)
			}
			findResult := func(view accountEstimate) *estimateResult {
				for i := range view.Windows {
					if view.Windows[i].Type == kind {
						return &view.Windows[i]
					}
				}
				return nil
			}
			perRequest := (200000*0.30 + 1000*1.20) / 1e6

			estimateCalibrate(s, c, usage(9, resetAnchor, t0))
			estimateRecordUsage(t, s, c, "cline-pass/deepseek-v4.1-flash", 200000, 1000, 0, 0)
			estimateCalibrate(s, c, usage(10, resetAt10, t1))
			w := estimateWindowSnapshot(t, s, account, kind)
			if w.Samples != 0 || w.Cost.Low != 0 || w.Cost.High != 0 || w.LastPercent != 10 {
				t.Fatalf("reset 抖动被误判为新窗口或提前写出样本: %+v", w)
			}
			pending := findResult(estimateViewOf(t, s, c, usage(10, resetAt10, t1)))
			if pending == nil {
				t.Fatalf("缺少 %s 估算结果", kind)
			}
			if pending.PendingRequests != 1 || pending.PendingPercent != 1 {
				t.Fatalf("10%% pending 未保持: %+v", *pending)
			}
			if pending.Total != nil || pending.Remaining != nil {
				t.Fatalf("10%% 提前输出 Total/Remaining: %+v", *pending)
			}

			estimateRecordUsage(t, s, c, "cline-pass/deepseek-v4.1-flash", 200000, 1000, 0, 0)
			estimateCalibrate(s, c, usage(11, resetAt11, t2))
			w = estimateWindowSnapshot(t, s, account, kind)
			if w.Samples != 1 || w.Delta != 2 || w.Requests != 2 {
				t.Fatalf("11%% 未形成样本: %+v", w)
			}
			estimateCloseTo(t, w.Cost.Low, 2*perRequest, "11% 样本成本")
			estimateCloseTo(t, w.Cost.High, 2*perRequest, "11% 样本成本上限")

			estimated := findResult(estimateViewOf(t, s, c, usage(11, resetAt11, t2)))
			if estimated == nil {
				t.Fatalf("缺少 %s 估算结果", kind)
			}
			if estimated.Total == nil || estimated.Remaining == nil {
				t.Fatalf("11%% 未输出 Total/Remaining: %+v", *estimated)
			}
			estimateCloseTo(t, estimated.Total.Low, 100*2*perRequest/3, "总额度下限")
			estimateCloseTo(t, estimated.Total.High, 100*2*perRequest, "总额度上限")
			estimateCloseTo(t, estimated.Remaining.Low, estimated.Total.Low*0.89, "剩余额度下限")
			estimateCloseTo(t, estimated.Remaining.High, estimated.Total.High*0.89, "剩余额度上限")
		})
	}
}

func TestEstimateResetDriftBeyondAnchorRestartsSampling(t *testing.T) {
	t.Run("reset 漂移超过容差", func(t *testing.T) {
		s := registeredService(t, "")
		c := Credential{ID: "drift-key", Type: Provider, APIKey: "key-drift", Label: "漂移"}
		estimateAddCredential(t, s, c)
		account, plan := "account-drift", "plan-drift"
		resetAnchor := time.Now().UTC().Add(240 * time.Hour).Truncate(time.Second).Add(500 * time.Millisecond)
		t0 := time.Now().UTC().Add(-4 * time.Hour)
		t1 := t0.Add(time.Hour)
		t2 := t0.Add(2 * time.Hour)
		usage := func(percent float64, reset, at time.Time) credentialUsage {
			return estimateUsageValue(account, plan, "five_hour", percent, reset, at)
		}

		estimateCalibrate(s, c, usage(9, resetAnchor, t0))
		estimateRecordUsage(t, s, c, "cline-pass/deepseek-v4.1-flash", 200000, 1000, 0, 0)
		estimateCalibrate(s, c, usage(11, resetAnchor.Add(300*time.Millisecond), t1))
		w := estimateWindowSnapshot(t, s, account, "five_hour")
		if w.Samples != 1 || w.Cost.Low <= 0 {
			t.Fatalf("重建前样本未保留: %+v", w)
		}

		// 容差只围绕原始 reset 锚点；累计漂移超过 2s 必须重建而不是继续并样本。
		drifted := resetAnchor.Add(3 * time.Second)
		estimateCalibrate(s, c, usage(12, drifted, t2))
		w = estimateWindowSnapshot(t, s, account, "five_hour")
		if !w.Reset.Equal(drifted) || w.Base == nil || w.Base.Percent != 12 || w.LastPercent != 12 || w.Samples != 0 || w.Cost.Low != 0 || w.Cost.High != 0 || w.Delta != 0 {
			t.Fatalf("reset 漂移超过 2s 后未重建或继承了旧样本: %+v", w)
		}
	})

	t.Run("过期窗口仍重建", func(t *testing.T) {
		s := registeredService(t, "")
		c := Credential{ID: "expired-key", Type: Provider, APIKey: "key-expired", Label: "过期"}
		estimateAddCredential(t, s, c)
		account, plan := "account-expired", "plan-expired"
		start := time.Now().UTC()
		anchor := start.Add(time.Hour).Truncate(time.Second).Add(500 * time.Millisecond)
		t0 := start.Add(-2 * time.Hour)
		t1 := start.Add(-time.Hour)
		usage := func(percent float64, reset, at time.Time) credentialUsage {
			return estimateUsageValue(account, plan, "five_hour", percent, reset, at)
		}

		estimateCalibrate(s, c, usage(9, anchor, t0))
		estimateRecordUsage(t, s, c, "cline-pass/deepseek-v4.1-flash", 200000, 1000, 0, 0)
		estimateCalibrate(s, c, usage(11, anchor, t1))
		w := estimateWindowSnapshot(t, s, account, "five_hour")
		if w.Samples != 1 {
			t.Fatalf("过期场景前置样本未建立: %+v", w)
		}

		expiredAt := anchor.Add(200 * time.Millisecond)
		nextReset := expiredAt.Add(time.Second)
		estimateCalibrate(s, c, usage(12, nextReset, expiredAt))
		w = estimateWindowSnapshot(t, s, account, "five_hour")
		if !w.Reset.Equal(nextReset) || w.Base == nil || w.Base.Percent != 12 || w.Samples != 0 || w.Cost.Low != 0 {
			t.Fatalf("过期窗口未按旧行为重建: %+v", w)
		}
	})

	t.Run("百分比下降仍重建", func(t *testing.T) {
		s := registeredService(t, "")
		c := Credential{ID: "drop-key", Type: Provider, APIKey: "key-drop", Label: "下降"}
		estimateAddCredential(t, s, c)
		account, plan := "account-drop", "plan-drop"
		reset := time.Now().UTC().Add(240 * time.Hour).Truncate(time.Second).Add(500 * time.Millisecond)
		t0 := time.Now().UTC().Add(-4 * time.Hour)
		t1 := t0.Add(time.Hour)
		t2 := t0.Add(2 * time.Hour)
		usage := func(percent float64, at time.Time) credentialUsage {
			return estimateUsageValue(account, plan, "five_hour", percent, reset, at)
		}

		estimateCalibrate(s, c, usage(9, t0))
		estimateRecordUsage(t, s, c, "cline-pass/deepseek-v4.1-flash", 200000, 1000, 0, 0)
		estimateCalibrate(s, c, usage(11, t1))
		w := estimateWindowSnapshot(t, s, account, "five_hour")
		if w.Samples != 1 {
			t.Fatalf("下降场景前置样本未建立: %+v", w)
		}

		estimateCalibrate(s, c, usage(5, t2))
		w = estimateWindowSnapshot(t, s, account, "five_hour")
		if !w.Reset.Equal(reset) || w.Base == nil || w.Base.Percent != 5 || w.LastPercent != 5 || w.Samples != 0 || w.Cost.Low != 0 || w.Delta != 0 {
			t.Fatalf("百分比下降未按旧行为重建: %+v", w)
		}
	})
}

func TestEstimateRestartDoesNotBridgeGap(t *testing.T) {
	dir := t.TempDir()
	c := Credential{ID: "persist-key", Type: Provider, APIKey: "key-persist", Label: "持久化"}
	reset := time.Now().UTC().Add(240 * time.Hour)
	t0 := time.Now().UTC().Add(-3 * time.Hour)

	first := estimateRegister(t, dir)
	estimateAddCredential(t, first, c)
	estimateCalibrate(first, c, estimateUsageValue("account-persist", "plan-persist", "five_hour", 10, reset, t0))
	estimateRecordUsage(t, first, c, "cline-pass/deepseek-v4.1-flash", 200000, 1000, 0, 0)
	estimateCalibrate(first, c, estimateUsageValue("account-persist", "plan-persist", "five_hour", 12, reset, t0.Add(time.Hour)))
	saved := estimateWindowSnapshot(t, first, "account-persist", "five_hour")
	if saved.Samples != 1 || saved.Base == nil || saved.Cost.Low <= 0 {
		t.Fatalf("重启前样本未建立: %+v", saved)
	}

	second := estimateRegister(t, dir)
	estimateAddCredential(t, second, c)
	loaded := estimateWindowSnapshot(t, second, "account-persist", "five_hour")
	if loaded.Base != nil {
		t.Fatalf("重启后仍保留跨进程基线: %+v", loaded.Base)
	}
	if loaded.Samples != saved.Samples || loaded.Cost.Low != saved.Cost.Low {
		t.Fatalf("重启丢失历史样本: %+v", loaded)
	}
	// 重启后的第一次校准只能重新建立基线，不能补记 gap 内消费。
	estimateCalibrate(second, c, estimateUsageValue("account-persist", "plan-persist", "five_hour", 13, reset, t0.Add(2*time.Hour)))
	after := estimateWindowSnapshot(t, second, "account-persist", "five_hour")
	if after.Base == nil || after.Base.Percent != 13 {
		t.Fatalf("重启后未重新基线: %+v", after.Base)
	}
	if after.Samples != saved.Samples || after.Cost.Low != saved.Cost.Low {
		t.Fatalf("重启 gap 被错误补记: %+v", after)
	}
}

func TestEstimateSkipsCalibrationWhileRequestsOrQueriesInFlight(t *testing.T) {
	reset := time.Now().UTC().Add(240 * time.Hour)
	t0 := time.Now().UTC().Add(-2 * time.Hour)
	boundService := func(t *testing.T) (*Service, Credential) {
		t.Helper()
		s := registeredService(t, "")
		c := Credential{ID: "busy-key", Type: Provider, APIKey: "key-busy", Label: "忙"}
		estimateAddCredential(t, s, c)
		estimateCalibrate(s, c, estimateUsageValue("account-busy", "plan-busy", "five_hour", 10, reset, t0))
		return s, c
	}

	t.Run("活动请求期间", func(t *testing.T) {
		s, c := boundService(t)
		key := s.startEstimateRequest(c)
		estimateCalibrate(s, c, estimateUsageValue("account-busy", "plan-busy", "five_hour", 30, reset, t0.Add(time.Hour)))
		w := estimateWindowSnapshot(t, s, "account-busy", "five_hour")
		if w.LastPercent != 10 || w.Samples != 0 || w.Delta != 0 {
			t.Fatalf("活动请求期间仍完成校准: %+v", w)
		}
		s.appendLog(LogEntry{
			estimateKey: key, CredentialID: c.ID, Status: 200, UsageReported: true,
			UpstreamModel: "cline-pass/deepseek-v4.1-flash", PromptTokens: 1000,
			Attempts: []Attempt{{Status: 200}},
		})
	})

	t.Run("查询期间请求完成", func(t *testing.T) {
		s, c := boundService(t)
		key := s.startEstimateRequest(c)
		before := s.estimateActivitySnapshot()
		// 请求在额度查询进行中完成：活动数归零但 revision 前进。
		s.mu.Lock()
		a := s.estimateActivity[key]
		if a.Active > 0 {
			a.Active--
		}
		a.Revision++
		s.estimateActivity[key] = a
		s.mu.Unlock()
		s.mu.Lock()
		s.calibrateEstimateLocked(c, estimateUsageValue("account-busy", "plan-busy", "five_hour", 30, reset, t0.Add(time.Hour)), before)
		s.mu.Unlock()
		w := estimateWindowSnapshot(t, s, "account-busy", "five_hour")
		if w.LastPercent != 10 || w.Samples != 0 {
			t.Fatalf("查询期间完成的请求仍被采样: %+v", w)
		}
	})

	t.Run("采样快照版本变化", func(t *testing.T) {
		s, c := boundService(t)
		before := s.estimateActivitySnapshot()
		key := s.startEstimateRequest(c)
		s.appendLog(LogEntry{
			estimateKey: key, CredentialID: c.ID, Status: 200, UsageReported: true,
			UpstreamModel: "cline-pass/deepseek-v4.1-flash", PromptTokens: 1000,
			Attempts: []Attempt{{Status: 200}},
		})
		s.mu.Lock()
		s.calibrateEstimateLocked(c, estimateUsageValue("account-busy", "plan-busy", "five_hour", 30, reset, t0.Add(time.Hour)), before)
		s.mu.Unlock()
		w := estimateWindowSnapshot(t, s, "account-busy", "five_hour")
		if w.LastPercent != 10 || w.Samples != 0 {
			t.Fatalf("活动版本变化后仍完成校准: %+v", w)
		}
	})
}

// ---------------------------------------------------------------------------
// 参考价格
// ---------------------------------------------------------------------------

func TestPricingCachedReadIsNotChargedAsInput(t *testing.T) {
	plain, ok := referenceCost(estimatePricedEntry("cline-pass/deepseek-v4.1-flash", 200000, 0, 0, 0, false))
	if !ok {
		t.Fatal("普通请求未计价")
	}
	cached, ok := referenceCost(estimatePricedEntry("cline-pass/deepseek-v4.1-flash", 200000, 0, 100000, 0, false))
	if !ok {
		t.Fatal("缓存读请求未计价")
	}
	estimateCloseTo(t, plain.Low, 200000*0.30/1e6, "输入成本")
	if cached.Low >= plain.Low {
		t.Fatalf("缓存读被按输入价重复收费: cached=%v plain=%v", cached.Low, plain.Low)
	}
	estimateCloseTo(t, cached.Low, (100000*0.30+100000*0.006)/1e6, "缓存读成本")
}

func TestPricingQwenWriteRequirementAndHighContextThreshold(t *testing.T) {
	withWrite, ok := referenceCost(estimatePricedEntry("cline-pass/qwen3.7-plus", 1000, 0, 0, 100, true))
	if !ok {
		t.Fatal("显式上报缓存写后仍未计价")
	}
	estimateCloseTo(t, withWrite.Low, (900*0.40+100*0.50)/1e6, "Qwen 缓存写价格")
	if _, ok := referenceCost(estimatePricedEntry("cline-pass/qwen3.7-plus", 1000, 0, 0, 100, false)); ok {
		t.Fatal("未显式上报缓存写却按写价计价")
	}
	if _, ok := referenceCost(estimatePricedEntry("cline-pass/qwen3.7-plus", 1000, 0, 0, 0, false)); ok {
		t.Fatal("存在独立缓存写单价的模型在未上报缓存写时仍计价")
	}

	threshold, ok := referenceCost(estimatePricedEntry("cline-pass/qwen3.7-plus", 262144, 0, 0, 0, true))
	if !ok {
		t.Fatal("阈值内的 Qwen 请求未计价")
	}
	high, ok := referenceCost(estimatePricedEntry("cline-pass/qwen3.7-plus", 262145, 0, 0, 0, true))
	if !ok {
		t.Fatal("超过阈值的 Qwen 请求未计价")
	}
	estimateCloseTo(t, threshold.Low, 262144*0.40/1e6, "阈值内单价")
	estimateCloseTo(t, high.Low, 262145*1.20/1e6, "超阈值单价")
	if high.Low <= threshold.Low {
		t.Fatalf("超过 262144 上下文未切换高价档: threshold=%v high=%v", threshold.Low, high.Low)
	}
}

func TestPricingDeepSeekPeakValleyRange(t *testing.T) {
	r, ok := referenceCost(estimatePricedEntry("cline-pass/deepseek-v4-pro", 1000000, 0, 0, 0, false))
	if !ok {
		t.Fatal("deepseek-v4-pro 未计价")
	}
	peak := 1000000 * 1.32 / 1e6
	estimateCloseTo(t, r.High, peak, "峰时价格")
	estimateCloseTo(t, r.Low, peak/2, "谷时价格")
	if r.Low >= r.High {
		t.Fatalf("峰谷价格未保留为区间: %+v", r)
	}
}

func TestPricingUnknownModelIsExcluded(t *testing.T) {
	if _, ok := referenceCost(estimatePricedEntry("cline-pass/mimo-v2.6", 1000, 0, 0, 0, false)); ok {
		t.Fatal("未知 mimo-v2.6 被计价")
	}
	if _, ok := referenceCost(estimatePricedEntry("cline-pass/mimo-v2.5", 1000, 0, 0, 0, false)); !ok {
		t.Fatal("已登记的 mimo-v2.5 未计价")
	}
	if _, ok := referenceCost(estimatePricedEntry("deepseek-v4.1-flash", 1000, 0, 0, 0, false)); ok {
		t.Fatal("缺少 cline-pass/ 前缀的模型被计价")
	}
}

// ---------------------------------------------------------------------------
// 默认配置
// ---------------------------------------------------------------------------

func TestDefaultTimeout600KeepsExplicitSetting(t *testing.T) {
	if got := defaultConfig().TimeoutSeconds; got != 600 {
		t.Fatalf("默认请求超时 = %d, 期望 600", got)
	}
	if got := registeredService(t, "").config().TimeoutSeconds; got != 600 {
		t.Fatalf("新注册实例未使用 600 秒默认值: %d", got)
	}
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "settings.json"), []byte(`{"timeout_seconds":180}`), 0600); err != nil {
		t.Fatal(err)
	}
	if got := estimateRegister(t, dir).config().TimeoutSeconds; got != 180 {
		t.Fatalf("显式 180 秒设置被默认值覆盖: %d", got)
	}
}

// ---------------------------------------------------------------------------
// 额度查询到估算总额的集成路径
// ---------------------------------------------------------------------------

func TestUsageEstimateIntegrationReturnsTotalWithoutSecrets(t *testing.T) {
	const (
		apiKey = "secret-test-key"
		userID = "private-user"
		subID  = "private-subscription"
	)
	reset := time.Now().UTC().Add(5 * time.Hour).Truncate(time.Second)
	limitsJSON := func(percent float64) string {
		return fmt.Sprintf(`{"success":true,"data":{"limits":[{"type":"five_hour","percentUsed":%g,"resetsAt":%q}]}}`, percent, reset.Format(time.RFC3339))
	}
	planJSON := fmt.Sprintf(`{"success":true,"data":{"userId":%q,"subscriptionId":%q,"currentPeriodEnd":"2026-10-22T14:08:10Z","plan":{"displayName":"Cline Pass (Monthly)"}}}`, userID, subID)
	limitsCalls := 0
	s := usageTestService(t, func(req map[string]any) (int, string) {
		if strings.HasSuffix(str(req["url"]), "/usage-limits") {
			limitsCalls++
			if limitsCalls == 1 {
				return 200, limitsJSON(5)
			}
			return 200, limitsJSON(7)
		}
		return 200, planJSON
	})
	req := usageTestRequest()

	// 1) 首次查询用 plan 响应确认账号并建立百分比基线。
	firstRaw, err := s.management(jsonBytes(req))
	if err != nil {
		t.Fatal(err)
	}
	firstBody := firstRaw.(ManagementResponse).Body
	var first credentialUsage
	if json.Unmarshal(firstBody, &first) != nil || first.Status != "ok" || first.Estimate == nil {
		t.Fatalf("首次查询未建立估算基线: %s", firstBody)
	}
	if first.Estimate.Status != "sampling" || first.Estimate.Windows[0].Type != "five_hour" || first.Estimate.Windows[0].Total != nil {
		t.Fatalf("首次查询基线状态异常: %s", firstBody)
	}

	// 2) 走真实 newLog + observeMetadata 路径模拟一次分帧上报的流式请求。
	s.mu.RLock()
	c := s.creds["one"]
	s.mu.RUnlock()
	entry := s.newLog(ExecutorRequest{Model: "deepseek-flash", HostCallbackID: "usage-callback"}, c, "cline-pass/deepseek-v4.1-flash")
	attempt := Attempt{Mode: "stream"}
	observeMetadata(map[string]any{"usage": map[string]any{
		"prompt_tokens":         float64(200000),
		"prompt_tokens_details": map[string]any{"cached_tokens": float64(100000)},
	}}, &entry, &attempt)
	observeMetadata(map[string]any{"usage": map[string]any{"completion_tokens": float64(1000)}}, &entry, &attempt)
	if !entry.UsageReported || entry.PromptTokens != 200000 || entry.CompletionTokens != 1000 || entry.CachedTokens != 100000 {
		t.Fatalf("分帧 usage 未正确合并: %+v", entry)
	}
	entry.Attempts = []Attempt{attempt}
	entry.Status = 200
	s.appendLog(entry)

	// 3) 缓存过期且百分比上升，第二次查询应给出估算总额。
	past := time.Now().Add(-2 * time.Minute)
	s.mu.Lock()
	s.usageCache["one"].value.CheckedAt = &past
	s.mu.Unlock()
	secondRaw, err := s.management(jsonBytes(req))
	if err != nil {
		t.Fatal(err)
	}
	secondBody := secondRaw.(ManagementResponse).Body
	var second credentialUsage
	if json.Unmarshal(secondBody, &second) != nil || second.Status != "ok" || second.Estimate == nil {
		t.Fatalf("第二次查询失败: %s", secondBody)
	}
	var five *estimateResult
	for i := range second.Estimate.Windows {
		if second.Estimate.Windows[i].Type == "five_hour" {
			five = &second.Estimate.Windows[i]
		}
	}
	if five == nil || five.Total == nil {
		t.Fatalf("额度上升后未返回估算总额: %s", secondBody)
	}
	cost := (100000*0.30 + 100000*0.006 + 1000*1.20) / 1e6
	estimateCloseTo(t, five.Total.Low, 100*cost/3, "估算总额下限")
	estimateCloseTo(t, five.Total.High, 100*cost, "估算总额上限")
	if five.Total.Low >= five.Total.High {
		t.Fatalf("估算总额未保留区间: %+v", five.Total)
	}

	// 4) 响应与持久化都不得出现原始 API key / userId。
	estimates, err := os.ReadFile(filepath.Join(s.config().DataDir, "estimates.json"))
	if err != nil {
		t.Fatalf("估算存档未写入: %v", err)
	}
	requests, err := os.ReadFile(filepath.Join(s.config().DataDir, "requests.json"))
	if err != nil {
		t.Fatalf("请求日志未写入: %v", err)
	}
	for name, blob := range map[string][]byte{
		"responses":      append(append([]byte{}, firstBody...), secondBody...),
		"estimates.json": estimates,
		"requests.json":  requests,
	} {
		for _, secret := range []string{apiKey, userID, subID} {
			if strings.Contains(string(blob), secret) {
				t.Fatalf("%s 泄露 %q: %s", name, secret, blob)
			}
		}
	}
}

func TestSSEPartialUsageFramesKeepPromptCompletionAndCacheWrite(t *testing.T) {
	var entry LogEntry
	var attempt Attempt
	observeMetadata(map[string]any{"usage": map[string]any{
		"prompt_tokens":         float64(200000),
		"prompt_tokens_details": map[string]any{"cached_tokens": float64(100000)},
	}}, &entry, &attempt)
	if entry.UsageReported {
		t.Fatal("仅上报 prompt 时不应标记为完整 usage")
	}
	observeMetadata(map[string]any{"usage": map[string]any{"completion_tokens": float64(1000)}}, &entry, &attempt)
	if !entry.UsageReported {
		t.Fatal("prompt 与 completion 分帧上报后未标记为完整 usage")
	}
	observeMetadata(map[string]any{"usage": map[string]any{
		"prompt_tokens_details": map[string]any{"cache_write_tokens": float64(500)},
	}}, &entry, &attempt)
	if !entry.CacheWriteReported {
		t.Fatal("缓存写分帧上报未被识别")
	}
	if entry.PromptTokens != 200000 || entry.CompletionTokens != 1000 || entry.CachedTokens != 100000 || entry.CacheWriteTokens != 500 {
		t.Fatalf("分帧 usage 互相归零: %+v", entry)
	}
	// Cline/Anthropic 形态的 cache_creation_input_tokens 只更新缓存写。
	observeMetadata(map[string]any{"usage": map[string]any{"cache_creation_input_tokens": float64(700)}}, &entry, &attempt)
	if entry.CacheWriteTokens != 700 || entry.PromptTokens != 200000 || entry.CompletionTokens != 1000 || entry.CachedTokens != 100000 {
		t.Fatalf("cache_creation_input_tokens 覆盖了其它 usage 字段: %+v", entry)
	}
}
