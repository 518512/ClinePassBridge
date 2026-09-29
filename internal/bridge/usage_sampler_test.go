package bridge

import (
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"
)

const (
	samplerLimitsPath = "/users/me/plan/usage-limits"
	samplerPlanPath   = "/users/me/plan"
)

type samplerRequest struct {
	method, path, key, callbackID string
}

// samplerHost fakes CPA's background HTTP bridge: it only accepts plain GETs to
// the two usage endpoints and records every host_callback_id it receives.
type samplerHost struct {
	mu          sync.Mutex
	requests    []samplerRequest
	accounts    map[string]string
	failures    map[string]int
	bodies      map[string]string
	blocking    map[string]bool
	closed      map[string]bool
	blockReads  bool
	next        int
	reset       time.Time
	readStarted chan struct{}
	readOnce    sync.Once
	release     chan struct{}
	releaseOnce sync.Once
}

func newSamplerHost() *samplerHost {
	return &samplerHost{
		accounts: map[string]string{}, failures: map[string]int{}, bodies: map[string]string{},
		blocking: map[string]bool{}, closed: map[string]bool{},
		reset:       time.Now().UTC().Add(5 * time.Hour),
		readStarted: make(chan struct{}), release: make(chan struct{}),
	}
}

func (h *samplerHost) call(method string, payload, out any) error {
	req, _ := payload.(map[string]any)
	switch method {
	case "host.http.do_stream":
		key := ""
		if header, ok := req["headers"].(http.Header); ok {
			key = strings.TrimPrefix(header.Get("Authorization"), "Bearer ")
		}
		path := ""
		if parsed, err := url.Parse(str(req["url"])); err == nil {
			// CPA sends the configured base path (/api/v1); match on the endpoint.
			path = parsed.Path
		}
		switch {
		case strings.HasSuffix(path, samplerLimitsPath):
			path = samplerLimitsPath
		case strings.HasSuffix(path, samplerPlanPath):
			path = samplerPlanPath
		}
		h.mu.Lock()
		h.requests = append(h.requests, samplerRequest{str(req["method"]), path, key, str(req["host_callback_id"])})
		if str(req["method"]) != "GET" || (path != samplerLimitsPath && path != samplerPlanPath) {
			h.mu.Unlock()
			return fmt.Errorf("sampler host rejected %s %s", str(req["method"]), str(req["url"]))
		}
		h.next++
		streamID := fmt.Sprintf("usage-%d", h.next)
		status := h.failures[key]
		if status == 0 {
			status = http.StatusOK
		}
		h.bodies[streamID] = h.body(path, h.accounts[key], status)
		if h.blockReads {
			h.blocking[streamID] = true
		}
		h.mu.Unlock()
		*out.(*upstreamStream) = upstreamStream{StatusCode: status, StreamID: streamID}
		return nil
	case "host.http.stream_read":
		streamID := str(req["stream_id"])
		h.mu.Lock()
		body, known := h.bodies[streamID]
		blocked := h.blocking[streamID]
		h.mu.Unlock()
		if !known {
			return fmt.Errorf("unknown sampler stream %q", streamID)
		}
		if blocked {
			h.readOnce.Do(func() { close(h.readStarted) })
			<-h.release
		}
		*out.(*readChunk) = readChunk{Payload: []byte(body), Done: true}
		return nil
	case "host.http.stream_close":
		streamID := str(req["stream_id"])
		h.mu.Lock()
		h.closed[streamID] = true
		blocked := h.blocking[streamID]
		h.mu.Unlock()
		if blocked {
			h.releaseOnce.Do(func() { close(h.release) })
		}
		return nil
	default:
		return fmt.Errorf("unexpected host callback %q", method)
	}
}

func (h *samplerHost) body(path, account string, status int) string {
	if status != http.StatusOK {
		return `{"error":{"message":"sampler upstream failure"}}`
	}
	if path == samplerLimitsPath {
		return fmt.Sprintf(`{"success":true,"data":{"limits":[{"type":"five_hour","percentUsed":3,"resetsAt":%q}]}}`, h.reset.Format(time.RFC3339Nano))
	}
	return fmt.Sprintf(`{"success":true,"data":{"userId":%q,"subscriptionId":%q,"currentPeriodEnd":"2026-10-22T14:08:10Z","plan":{"displayName":"Cline Pass"}}}`, account, "sub-"+account)
}

func (h *samplerHost) snapshot() []samplerRequest {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]samplerRequest(nil), h.requests...)
}

func (h *samplerHost) pathRequests(path string) []samplerRequest {
	var out []samplerRequest
	for _, req := range h.snapshot() {
		if req.path == path {
			out = append(out, req)
		}
	}
	return out
}

func (h *samplerHost) pathCount(path string) int { return len(h.pathRequests(path)) }

func (h *samplerHost) closedCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.closed)
}

func samplerCredential(id, key string) Credential {
	return Credential{Type: Provider, ID: id, Label: id, APIKey: key}
}

func samplerService(t *testing.T, host *samplerHost, creds ...Credential) *Service {
	t.Helper()
	s := registeredService(t, "")
	for _, c := range creds {
		s.creds[c.ID] = c
	}
	s.SetHost(host.call)
	return s
}

// startSampler registers the management routes, the only trigger the plugin
// needs to begin background sampling, and shuts the loop down after the test.
func startSampler(t *testing.T, s *Service) {
	t.Helper()
	if _, err := s.Handle("management.register", nil); err != nil {
		t.Fatalf("register management: %v", err)
	}
	t.Cleanup(func() { _, _ = s.Handle("plugin.shutdown", nil) })
}

func waitSampler(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("timed out waiting for the usage sampler")
}

func samplerLedger(s *Service, status string, ids ...string) func() bool {
	return func() bool {
		s.mu.RLock()
		defer s.mu.RUnlock()
		for _, id := range ids {
			entry := s.usageCache[id]
			if entry == nil || entry.value.CheckedAt == nil || entry.value.Status != status {
				return false
			}
			if status == "ok" && (entry.value.accountHash == "" || len(entry.value.Limits) == 0) {
				return false
			}
		}
		return true
	}
}

func ageSamplerCache(t *testing.T, s *Service, age time.Duration, ids ...string) {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	when := time.Now().Add(-age)
	for _, id := range ids {
		entry := s.usageCache[id]
		if entry == nil || entry.value.CheckedAt == nil {
			t.Fatalf("no usage ledger for %s", id)
		}
		entry.value.CheckedAt = &when
	}
}

func TestUsageSamplerBaselineWithoutManagementHandle(t *testing.T) {
	host := newSamplerHost()
	host.accounts["k1"] = "acct-a"
	s := samplerService(t, host, samplerCredential("one", "k1"))
	startSampler(t, s)
	waitSampler(t, samplerLedger(s, "ok", "one"))

	requests := host.snapshot()
	if len(requests) != 2 {
		t.Fatalf("baseline requests = %d, want usage-limits and plan: %+v", len(requests), requests)
	}
	for _, req := range requests {
		if req.callbackID != "" {
			t.Fatalf("background sampling used a management callback id: %+v", req)
		}
	}
	if host.pathCount(samplerLimitsPath) != 1 || host.pathCount(samplerPlanPath) != 1 {
		t.Fatalf("baseline paths = %+v", requests)
	}
	s.mu.RLock()
	accountHash := s.usageCache["one"].value.accountHash
	var window *estimateWindow
	if account := s.estimates.Accounts[accountHash]; account != nil {
		window = account.Windows["five_hour"]
	}
	s.mu.RUnlock()
	if window == nil || window.Base == nil {
		t.Fatalf("baseline did not open a five_hour estimate window: %+v", window)
	}
	if window.Base.Percent != 3 || !sameEstimateReset(window.Reset, host.reset) || !window.Reset.After(time.Now()) {
		t.Fatalf("baseline window = %+v, want percent 3 and reset %s", window, host.reset)
	}
	if targets := s.usageSamplingTargets(time.Now()); len(targets) != 0 {
		t.Fatalf("fresh baseline still scheduled: %v", targets)
	}
}

func TestUsageSamplerSecondRoundKeepsFreshCache(t *testing.T) {
	host := newSamplerHost()
	host.accounts["k1"] = "acct-a"
	s := samplerService(t, host, samplerCredential("one", "k1"))
	startSampler(t, s)
	waitSampler(t, samplerLedger(s, "ok", "one"))

	before := len(host.snapshot())
	s.mu.RLock()
	stamp := *s.usageCache["one"].value.CheckedAt
	s.mu.RUnlock()
	if targets := s.usageSamplingTargets(time.Now()); len(targets) != 0 {
		t.Fatalf("fresh ledger scheduled %v", targets)
	}
	s.sampleUsage()
	if got := len(host.snapshot()); got != before {
		t.Fatalf("second round re-queried a fresh cache: %d -> %d", before, got)
	}
	s.mu.RLock()
	after := *s.usageCache["one"].value.CheckedAt
	s.mu.RUnlock()
	if !after.Equal(stamp) {
		t.Fatal("fresh cache was refreshed")
	}
}

func TestUsageSamplerGroupsAccountKeysAndSkipsPaused(t *testing.T) {
	host := newSamplerHost()
	host.accounts["k1"], host.accounts["k2"], host.accounts["k3"] = "acct-a", "acct-a", "acct-b"
	paused := samplerCredential("paused", "k4")
	paused.Disabled = true
	proxied := samplerCredential("proxied", "k5")
	proxied.ProxyURL = "http://proxy.invalid:8080"
	s := samplerService(t, host, samplerCredential("one", "k1"), samplerCredential("two", "k2"), samplerCredential("three", "k3"), paused, proxied)
	startSampler(t, s)
	waitSampler(t, samplerLedger(s, "ok", "one", "two", "three"))
	if targets := s.usageSamplingTargets(time.Now()); len(targets) != 0 {
		t.Fatalf("round one left work: %v", targets)
	}

	ageSamplerCache(t, s, 2*usageTTL, "one", "two", "three")
	targets := s.usageSamplingTargets(time.Now())
	if len(targets) != 2 || targets[0] != "one" || targets[1] != "three" {
		t.Fatalf("same-account keys were not grouped: %v", targets)
	}
	limitsBefore, planBefore := host.pathCount(samplerLimitsPath), host.pathCount(samplerPlanPath)
	s.sampleUsage()
	if got := host.pathCount(samplerLimitsPath) - limitsBefore; got != 2 {
		t.Fatalf("account samples = %d, want one per account", got)
	}
	if got := host.pathCount(samplerPlanPath) - planBefore; got != 0 {
		t.Fatalf("plan re-fetched %d times inside its own TTL", got)
	}
	keys := map[string]int{}
	for _, req := range host.pathRequests(samplerLimitsPath)[limitsBefore:] {
		keys[req.key]++
	}
	if len(keys) != 2 || keys["k1"] != 1 || keys["k3"] != 1 {
		t.Fatalf("grouped account keys = %v", keys)
	}
	for _, req := range host.snapshot() {
		if req.key == "k4" || req.key == "k5" {
			t.Fatalf("paused or proxied credential was sampled: %+v", req)
		}
	}
}

func TestUsageSamplerReplacedKeyAndAuthFailureBackoff(t *testing.T) {
	host := newSamplerHost()
	host.accounts["old-key"], host.accounts["new-key"] = "acct-a", "acct-b"
	s := samplerService(t, host, samplerCredential("one", "old-key"))
	startSampler(t, s)
	waitSampler(t, samplerLedger(s, "ok", "one"))
	s.mu.RLock()
	oldIdentity := s.usageCache["one"].value.accountHash
	s.mu.RUnlock()

	s.mu.Lock()
	c := s.creds["one"]
	c.APIKey = "new-key"
	s.creds["one"] = c
	s.mu.Unlock()
	limitsBefore := host.pathCount(samplerLimitsPath)
	s.sampleUsage()
	fresh := host.pathRequests(samplerLimitsPath)[limitsBefore:]
	if len(fresh) != 1 || fresh[0].key != "new-key" {
		t.Fatalf("replaced key reused the old identity cache: %+v", fresh)
	}
	s.mu.RLock()
	newIdentity := s.usageCache["one"].value.accountHash
	s.mu.RUnlock()
	if newIdentity == oldIdentity || newIdentity == "" {
		t.Fatal("account identity was not re-resolved after the key was replaced")
	}

	host2 := newSamplerHost()
	host2.accounts["a"], host2.accounts["b"] = "acct-a", "acct-b"
	host2.failures["a"], host2.failures["b"] = 401, 429
	s2 := samplerService(t, host2, samplerCredential("a1", "a"), samplerCredential("b1", "b"))
	startSampler(t, s2)
	waitSampler(t, samplerLedger(s2, "unauthorized", "a1"))
	waitSampler(t, samplerLedger(s2, "rate_limited", "b1"))
	if got := host2.pathCount(samplerLimitsPath); got != 2 {
		t.Fatalf("initial failed samples = %d", got)
	}
	ageSamplerCache(t, s2, time.Minute, "a1", "b1")
	if targets := s2.usageSamplingTargets(time.Now()); len(targets) != 0 {
		t.Fatalf("five minute backoff ignored: %v", targets)
	}
	s2.sampleUsage()
	if got := host2.pathCount(samplerLimitsPath); got != 2 {
		t.Fatalf("retried inside the five minute backoff (%d)", got)
	}
	ageSamplerCache(t, s2, 6*time.Minute, "a1", "b1")
	if targets := s2.usageSamplingTargets(time.Now()); len(targets) != 2 {
		t.Fatalf("backoff never expired: %v", targets)
	}
	s2.sampleUsage()
	if got := host2.pathCount(samplerLimitsPath); got != 4 {
		t.Fatalf("retry after backoff = %d", got)
	}
}

func TestUsageSamplerShutdownStopsLoopAndClosesRead(t *testing.T) {
	host := newSamplerHost()
	host.accounts["k1"] = "acct-a"
	host.blockReads = true
	s := samplerService(t, host, samplerCredential("one", "k1"))
	startSampler(t, s)
	select {
	case <-host.readStarted:
	case <-time.After(3 * time.Second):
		t.Fatal("sampler never reached the blocked stream_read")
	}

	done := make(chan struct{})
	go func() {
		_, _ = s.Handle("plugin.shutdown", nil)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("plugin.shutdown did not drain the blocked sampler read")
	}
	if host.closedCount() == 0 {
		t.Fatal("shutdown left the in-flight usage stream open")
	}
	stopped := len(host.snapshot())
	s.wakeUsageSampler()
	time.Sleep(50 * time.Millisecond)
	if got := len(host.snapshot()); got != stopped {
		t.Fatalf("usage sampler kept running after shutdown: %d -> %d", stopped, got)
	}
}
