package bridge

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"time"
)

type estimateTotals struct {
	Cost     priceRange `json:"cost"`
	Tokens   int64      `json:"tokens"`
	Requests int64      `json:"requests"`
	Unknown  int64      `json:"unknown"`
}
type estimateKey struct {
	Account string         `json:"account"`
	Totals  estimateTotals `json:"totals"`
}
type estimateBaseline struct {
	Percent float64        `json:"percent"`
	At      time.Time      `json:"at"`
	Totals  estimateTotals `json:"totals"`
}
type estimateWindow struct {
	Reset       time.Time         `json:"reset"`
	Base        *estimateBaseline `json:"base,omitempty"`
	Cost        priceRange        `json:"cost"`
	Delta       float64           `json:"delta"`
	Tokens      int64             `json:"tokens"`
	Requests    int64             `json:"requests"`
	Samples     int64             `json:"samples"`
	LastPercent float64           `json:"last_percent"`
	Updated     time.Time         `json:"updated"`
	Note        string            `json:"note,omitempty"`
}
type estimateAccount struct {
	Plan    string                     `json:"plan"`
	Windows map[string]*estimateWindow `json:"windows"`
}
type estimateState struct {
	Revision string                      `json:"revision"`
	Keys     map[string]*estimateKey     `json:"keys"`
	Accounts map[string]*estimateAccount `json:"accounts"`
}
type estimateActivity struct {
	Revision uint64
	Active   int
}
type estimateResult struct {
	PendingRequests int64       `json:"pending_requests"`
	PendingPercent  float64     `json:"pending_percent"`
	Type            string      `json:"type"`
	Total           *priceRange `json:"total,omitempty"`
	Remaining       *priceRange `json:"remaining,omitempty"`
	Cost            priceRange  `json:"sample_cost"`
	Delta           float64     `json:"percent_change"`
	Tokens          int64       `json:"tokens"`
	Requests        int64       `json:"requests"`
	Samples         int64       `json:"samples"`
	Updated         time.Time   `json:"updated_at"`
	Note            string      `json:"note"`
}
type accountEstimate struct {
	Status            string           `json:"status"`
	Note              string           `json:"note"`
	SharedCredentials int              `json:"shared_credentials"`
	Windows           []estimateResult `json:"windows"`
	PricingRevision   string           `json:"pricing_revision"`
	PersistenceError  string           `json:"persistence_error,omitempty"`
}

func estimateHash(kind, value string) string {
	if value == "" {
		return ""
	}
	h := sha256.Sum256([]byte(kind + "\x00" + value))
	return hex.EncodeToString(h[:])
}
func estimateKeyID(c Credential) string { return estimateHash("cline-key", c.APIKey) }

// Cline reconstructs resetsAt with sub-second jitter on each quota read.
// Compare to the original window anchor, not the previous read, so tolerated
// drift cannot accumulate indefinitely and merge genuinely different cycles.
func sameEstimateReset(a, b time.Time) bool {
	d := a.Sub(b)
	return d >= -2*time.Second && d <= 2*time.Second
}

// Caller holds s.mu. This independent ledger is never trimmed with request logs.
func (s *Service) loadEstimatesLocked() {
	s.estimates = estimateState{Revision: pricingRevision, Keys: map[string]*estimateKey{}, Accounts: map[string]*estimateAccount{}}
	b, err := os.ReadFile(filepath.Join(s.cfg.DataDir, "estimates.json"))
	if err == nil {
		var saved estimateState
		if json.Unmarshal(b, &saved) == nil && saved.Revision == pricingRevision && saved.Keys != nil && saved.Accounts != nil {
			s.estimates = saved
		} else {
			s.estimateWriteError = "估算存档格式或价格版本变化，已重新开始采样"
		}
	} else if !os.IsNotExist(err) {
		s.estimateWriteError = "估算存档读取失败，已重新开始采样"
	}
	// A restart can interrupt requests or miss activity. Never bridge that gap.
	for _, a := range s.estimates.Accounts {
		if a != nil {
			for _, w := range a.Windows {
				if w != nil {
					w.Base = nil
				}
			}
		}
	}
	if s.estimateActivity == nil {
		s.estimateActivity = map[string]estimateActivity{}
	}
}
func (s *Service) saveEstimatesLocked() {
	if err := atomicJSON(filepath.Join(s.cfg.DataDir, "estimates.json"), s.estimates); err != nil {
		s.estimateWriteError = "估算数据保存失败；重启后可能丢失本次采样"
	} else {
		s.estimateWriteError = ""
	}
}
func (s *Service) startEstimateRequest(c Credential) string {
	key := estimateKeyID(c)
	if key == "" {
		return ""
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.estimateActivity == nil {
		s.estimateActivity = map[string]estimateActivity{}
	}
	a := s.estimateActivity[key]
	a.Active++
	a.Revision++
	s.estimateActivity[key] = a
	return key
}
func (s *Service) recordEstimateLocked(e LogEntry) {
	key := e.estimateKey
	if key == "" {
		return
	}
	a := s.estimateActivity[key]
	if a.Active > 0 {
		a.Active--
	}
	a.Revision++
	s.estimateActivity[key] = a
	if s.estimates.Keys == nil {
		s.estimates.Keys = map[string]*estimateKey{}
		s.estimates.Accounts = map[string]*estimateAccount{}
		s.estimates.Revision = pricingRevision
	}
	k := s.estimates.Keys[key]
	if k == nil {
		k = &estimateKey{}
		s.estimates.Keys[key] = k
	}
	cost, ok := referenceCost(e)
	if !ok || e.Status < 200 || e.Status >= 300 || len(e.Attempts) != 1 {
		// Incomplete/retried calls may consume quota without complete token usage.
		k.Totals.Unknown++
	} else {
		k.Totals.Cost.Low += cost.Low
		k.Totals.Cost.High += cost.High
		k.Totals.Tokens += e.PromptTokens + e.CompletionTokens
		k.Totals.Requests++
	}
	s.saveEstimatesLocked()
}
func (s *Service) estimateActivitySnapshot() map[string]estimateActivity {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := map[string]estimateActivity{}
	for k, a := range s.estimateActivity {
		out[k] = a
	}
	return out
}
func (s *Service) estimateTotalsLocked(account string) estimateTotals {
	var out estimateTotals
	for _, k := range s.estimates.Keys {
		if k != nil && k.Account == account {
			out.Cost.Low += k.Totals.Cost.Low
			out.Cost.High += k.Totals.Cost.High
			out.Tokens += k.Totals.Tokens
			out.Requests += k.Totals.Requests
			out.Unknown += k.Totals.Unknown
		}
	}
	return out
}

// Identity comes only from the authenticated plan response, never labels/models.
// Only successful fresh quota reads call this, under s.mu and after key validation.
func (s *Service) calibrateEstimateLocked(c Credential, value credentialUsage, before map[string]estimateActivity) {
	if value.accountHash == "" || value.UpdatedAt == nil || value.Status != "ok" {
		return
	}
	if s.estimates.Keys == nil {
		s.estimates.Keys = map[string]*estimateKey{}
		s.estimates.Accounts = map[string]*estimateAccount{}
		s.estimates.Revision = pricingRevision
	}
	key := estimateKeyID(c)
	k := s.estimates.Keys[key]
	if k == nil {
		k = &estimateKey{}
		s.estimates.Keys[key] = k
	}
	account := s.estimates.Accounts[value.accountHash]
	if account == nil {
		account = &estimateAccount{Windows: map[string]*estimateWindow{}}
		s.estimates.Accounts[value.accountHash] = account
	}
	if account.Windows == nil {
		account.Windows = map[string]*estimateWindow{}
	}
	if k.Account != value.accountHash || account.Plan != value.planHash {
		// Newly discovered keys can carry historical counters; rebase, don't
		// attribute those historical requests to the next percentage increase.
		account.Windows = map[string]*estimateWindow{}
		if old := s.estimates.Accounts[k.Account]; old != nil && k.Account != value.accountHash {
			old.Windows = map[string]*estimateWindow{}
		}
		k.Account = value.accountHash
		account.Plan = value.planHash
	}
	for key, known := range s.estimates.Keys {
		if known == nil || known.Account != value.accountHash {
			continue
		}
		current := s.estimateActivity[key]
		if current.Active > 0 || before[key].Active > 0 || current.Revision != before[key].Revision {
			s.saveEstimatesLocked()
			return
		}
	}
	totals := s.estimateTotalsLocked(value.accountHash)
	now := *value.UpdatedAt
	for _, limit := range value.Limits {
		if limit.Type != "five_hour" && limit.Type != "weekly" && limit.Type != "monthly" {
			continue
		}
		if limit.PercentUsed == nil || limit.ResetsAt == nil || !limit.ResetsAt.After(now) || *limit.PercentUsed < 0 || *limit.PercentUsed > 100 {
			continue
		}
		p := *limit.PercentUsed
		w := account.Windows[limit.Type]
		if w != nil && !w.Updated.IsZero() && !now.After(w.Updated) {
			continue
		}
		if w == nil || !sameEstimateReset(w.Reset, *limit.ResetsAt) || !w.Reset.After(now) || p < w.LastPercent {
			w = &estimateWindow{Reset: *limit.ResetsAt}
			account.Windows[limit.Type] = w
		}
		w.LastPercent, w.Updated = p, now
		base := &estimateBaseline{Percent: p, At: now, Totals: totals}
		if w.Base == nil {
			w.Base = base
			w.Note = "已记录基线，等待额度变化"
			continue
		}
		b := w.Base
		if totals.Unknown != b.Totals.Unknown || now.Sub(b.At) > 5*time.Hour && limit.Type == "five_hour" {
			w.Base = base
			w.Note = "本段含缺失计价、失败请求或滚动过期，已重新采样"
			continue
		}
		delta := p - b.Percent
		low, high := totals.Cost.Low-b.Totals.Cost.Low, totals.Cost.High-b.Totals.Cost.High
		if delta > 0 && high <= 0 {
			w.Base = base
			w.Note = "额度变化未匹配到本插件消费，已重新采样"
			continue
		}
		// Accumulate unchanged/rounded percentages instead of dropping requests.
		if delta < 2 {
			w.Note = "累计变化达到 2 个百分点后更新估算"
			continue
		}
		if p >= 100 {
			w.Base = base
			w.Note = "额度到达上限，跳过可能被截断的百分比"
			continue
		}
		w.Cost.Low += low
		w.Cost.High += high
		w.Delta += delta
		w.Tokens += totals.Tokens - b.Totals.Tokens
		w.Requests += totals.Requests - b.Totals.Requests
		w.Samples++
		w.Base = base
		w.Note = "已按本周期样本校准"
	}
	s.saveEstimatesLocked()
}

func (s *Service) estimateViewLocked(c Credential, value credentialUsage) *accountEstimate {
	out := &accountEstimate{Status: "waiting", Note: "首次成功查询套餐后开始采样", PricingRevision: pricingRevision, Windows: []estimateResult{}, PersistenceError: s.estimateWriteError}
	k := s.estimates.Keys[estimateKeyID(c)]
	if k == nil || k.Account == "" {
		return out
	}
	a := s.estimates.Accounts[k.Account]
	if a == nil {
		return out
	}
	for _, cred := range s.creds {
		if b := s.estimates.Keys[estimateKeyID(cred)]; b != nil && b.Account == k.Account && !s.revoked[cred.ID] {
			out.SharedCredentials++
		}
	}
	out.Status, out.Note = "sampling", "仅统计通过本插件的请求；账号在其他客户端的消费会使估算偏低"
	totals := s.estimateTotalsLocked(k.Account)
	for _, kind := range []string{"five_hour", "weekly", "monthly"} {
		result := estimateResult{Type: kind, Note: "等待有效用量窗口"}
		w := a.Windows[kind]
		if w != nil {
			if w.Base != nil {
				result.PendingRequests = totals.Requests - w.Base.Totals.Requests
				result.PendingPercent = w.LastPercent - w.Base.Percent
			}
			result.Note, result.Cost, result.Delta, result.Tokens, result.Requests, result.Samples, result.Updated = w.Note, w.Cost, w.Delta, w.Tokens, w.Requests, w.Samples, w.Updated
			matchingWindow := false
			for _, limit := range value.Limits {
				if limit.Type == kind && limit.ResetsAt != nil && sameEstimateReset(*limit.ResetsAt, w.Reset) {
					matchingWindow = true
				}
			}
			// Percentage rounding contributes uncertainty per sample (±1 point).
			if w.Delta > float64(w.Samples) && w.Reset.After(time.Now()) && matchingWindow {
				total := priceRange{100 * w.Cost.Low / (w.Delta + float64(w.Samples)), 100 * w.Cost.High / (w.Delta - float64(w.Samples))}
				result.Total = &total
				for _, limit := range value.Limits {
					if limit.Type == kind && limit.PercentUsed != nil && limit.ResetsAt != nil && sameEstimateReset(*limit.ResetsAt, w.Reset) && value.Status == "ok" {
						remaining := 1 - *limit.PercentUsed/100
						if remaining < 0 {
							remaining = 0
						}
						if remaining > 1 {
							remaining = 1
						}
						result.Remaining = &priceRange{total.Low * remaining, total.High * remaining}
					}
				}
				out.Status = "estimated"
			}
		}
		out.Windows = append(out.Windows, result)
	}
	return out
}
