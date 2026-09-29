package bridge

import (
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"
)

// Management registration happens after plugin configuration/host setup. The
// sampler belongs to this plugin instance and is drained on hot unload.
func (s *Service) startUsageSampler() {
	s.mu.Lock()
	if s.stopped || s.usageSamplerStarted {
		s.mu.Unlock()
		return
	}
	s.usageSamplerStarted = true
	s.active.Add(1)
	s.mu.Unlock()
	go func() {
		defer s.active.Done()
		ticker := time.NewTicker(15 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-s.stopCh:
				return
			case <-s.usageWake:
			case <-ticker.C:
			}
			s.sampleUsage()
		}
	}()
	s.wakeUsageSampler()
}

func (s *Service) wakeUsageSampler() {
	select {
	case s.usageWake <- struct{}{}:
	default:
	}
}

func backgroundUsageTTL(value credentialUsage) time.Duration {
	switch value.Status {
	case "unauthorized", "rate_limited":
		return 5 * time.Minute
	default:
		return usageTTL
	}
}

// Query each known account once, irrespective of pagination or how many API
// keys it owns. Unknown identities are resolved per key before being grouped.
func (s *Service) usageSamplingTargets(now time.Time) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	ids := make([]string, 0, len(s.creds))
	for id, c := range s.creds {
		if !c.Disabled && !s.revoked[id] && c.APIKey != "" && strings.TrimSpace(c.ProxyURL) == "" {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	groups := map[string]string{}
	for _, id := range ids {
		c := s.creds[id]
		group := "key:" + estimateKeyID(c)
		entry := s.usageCache[id]
		if entry != nil && entry.fingerprint == usageFingerprint(c) && entry.value.accountHash != "" {
			group = "account:" + entry.value.accountHash
		}
		chosen, exists := groups[group]
		if !exists {
			groups[group] = id
			continue
		}
		// A working sibling key can keep account sampling alive if another expires.
		old := s.usageCache[chosen]
		if entry != nil && entry.fingerprint == usageFingerprint(c) && entry.value.Status == "ok" && (old == nil || old.value.Status != "ok") {
			groups[group] = id
		}
	}
	var targets []string
	for _, id := range groups {
		entry := s.usageCache[id]
		if entry != nil && entry.fingerprint == usageFingerprint(s.creds[id]) {
			if entry.done != nil || entry.value.CheckedAt != nil && now.Sub(*entry.value.CheckedAt) < backgroundUsageTTL(entry.value) {
				continue
			}
		}
		targets = append(targets, id)
	}
	sort.Strings(targets)
	return targets
}

func (s *Service) sampleUsage() {
	jobs := make(chan string)
	var workers sync.WaitGroup
	for n := 0; n < cap(s.usageSlots); n++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for id := range jobs {
				// Empty callback ID deliberately uses CPA's background context,
				// global proxy and the same bounded/closeable HTTP stream path.
				_, _ = s.credentialUsage(ManagementRequest{Query: url.Values{"id": {id}}})
			}
		}()
	}
	for _, id := range s.usageSamplingTargets(time.Now()) {
		select {
		case jobs <- id:
		case <-s.stopCh:
			close(jobs)
			workers.Wait()
			return
		}
	}
	close(jobs)
	workers.Wait()
}

// Caller holds mu. Share only with identities already verified using their own
// unchanged keys. A new/replaced key must authenticate before receiving a copy.
func (s *Service) shareAccountUsageLocked(sourceID string, value credentialUsage) {
	if value.Status != "ok" || value.accountHash == "" {
		return
	}
	for id, entry := range s.usageCache {
		c, exists := s.creds[id]
		if id == sourceID || !exists || c.Disabled || s.revoked[id] || entry.done != nil || entry.fingerprint != usageFingerprint(c) || entry.value.Status != "ok" || entry.value.accountHash != value.accountHash {
			continue
		}
		if entry.value.UpdatedAt != nil && value.UpdatedAt != nil && entry.value.UpdatedAt.After(*value.UpdatedAt) {
			continue
		}
		entry.value = value
		entry.value.ID, entry.value.Estimate = id, nil
	}
}
