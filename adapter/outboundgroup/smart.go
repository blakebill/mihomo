package outboundgroup

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/metacubex/mihomo/adapter/outboundgroup/smartengine"
	"github.com/metacubex/mihomo/common/utils"
	C "github.com/metacubex/mihomo/constant"
	P "github.com/metacubex/mihomo/constant/provider"
)

// SmartOption is optional config for type: smart groups.
// dart-smart:options
type SmartOption struct{}

// Smart is a Dart Smart proxy-group with connection-level dial failover.
type Smart struct {
	*GroupBase
	eng        *engine.Engine
	selected   string
	disableUDP bool
	testUrl    string
}

// dart-smart:constructor
func NewSmart(option GroupCommonOption, _ SmartOption, emptyFallback C.Proxy, providers []P.ProxyProvider) (*Smart, error) {
	if emptyFallback == nil {
		return nil, errors.New("empty fallback proxy not exist")
	}
	// Tags are resolved lazily via GetProxies; seed engine after first dial.
	s := &Smart{
		GroupBase: NewGroupBase(GroupBaseOption{
			Name:           option.Name,
			Type:           C.Smart,
			Hidden:         option.Hidden,
			Icon:           option.Icon,
			Filter:         option.Filter,
			ExcludeFilter:  option.ExcludeFilter,
			ExcludeType:    option.ExcludeType,
			TestTimeout:    option.TestTimeout,
			MaxFailedTimes: option.MaxFailedTimes,
			EmptyFallback:  emptyFallback,
			Providers:      providers,
		}),
		disableUDP: option.DisableUDP,
		testUrl:    option.URL,
	}
	return s, nil
}

func (s *Smart) ensureEngine(proxies []C.Proxy) {
	tags := make([]string, 0, len(proxies))
	for _, p := range proxies {
		tags = append(tags, p.Name())
	}
	if s.eng == nil {
		s.eng = engine.New(tags, engine.Options{})
		if s.selected != "" {
			s.eng.SetPreferred(s.selected)
		}
	} else {
		// Provider membership can change; keep stats for survivors.
		s.eng.SyncMembers(tags)
	}
	// Refresh URL-test priors every dial so delay sweeps can re-rank members.
	s.refreshURLTestPriors(proxies)
}

func (s *Smart) refreshURLTestPriors(proxies []C.Proxy) {
	if s.eng == nil || s.testUrl == "" {
		return
	}
	for _, p := range proxies {
		if d := p.LastDelayForTestUrl(s.testUrl); d > 0 && p.AliveForTestUrl(s.testUrl) {
			s.eng.SetURLTestPrior(p.Name(), d)
		}
	}
}

// Set implements SelectAble so the Clash API / app can pin a preferred member.
func (s *Smart) Set(name string) error {
	for _, proxy := range s.GetProxies(false) {
		if proxy.Name() != name {
			continue
		}
		s.selected = name
		s.ensureEngine(s.GetProxies(false))
		if s.eng != nil {
			s.eng.SetPreferred(name)
		}
		return nil
	}
	return errors.New("proxy not exist")
}

// ForceSet implements SelectAble.
func (s *Smart) ForceSet(name string) {
	s.selected = name
	if s.eng != nil {
		s.eng.SetPreferred(name)
	}
}

func (s *Smart) proxyByName(proxies []C.Proxy, name string) C.Proxy {
	for _, p := range proxies {
		if p.Name() == name {
			return p
		}
	}
	return nil
}

func (s *Smart) hostKey(metadata *C.Metadata) string {
	if metadata == nil {
		return ""
	}
	if metadata.Host != "" {
		return metadata.Host
	}
	if metadata.DstIP.IsValid() {
		return metadata.DstIP.String()
	}
	return ""
}

func (s *Smart) Now() string {
	if s.selected != "" {
		return s.selected
	}
	proxies := s.GetProxies(false)
	if len(proxies) > 0 {
		return proxies[0].Name()
	}
	return ""
}

// DialContext implements C.ProxyAdapter with Smart failover.
func (s *Smart) DialContext(ctx context.Context, metadata *C.Metadata) (C.Conn, error) {
	proxies := s.GetProxies(true)
	if len(proxies) == 0 {
		return s.EmptyFallback().DialContext(ctx, metadata)
	}
	s.ensureEngine(proxies)
	host := s.hostKey(metadata)
	var lastErr error
	for _, candidate := range s.eng.Select(host) {
		proxy := s.proxyByName(proxies, candidate.Tag)
		if proxy == nil {
			continue
		}
		start := time.Now()
		c, err := proxy.DialContext(ctx, metadata)
		rttMs := float64(time.Since(start).Milliseconds())
		if err != nil {
			s.eng.Record(candidate.Tag, engine.OutcomeFailure, rttMs)
			s.onDialFailed(proxy.Type(), err, nil)
			lastErr = err
			continue
		}
		threshold := s.eng.SoftFailThresholdMs(candidate.Tag)
		if rttMs > threshold && threshold > 0 {
			_ = c.Close()
			s.eng.Record(candidate.Tag, engine.OutcomeSoftFail, rttMs)
			lastErr = errors.New("smart soft-fail: " + candidate.Tag)
			continue
		}
		c.AppendToChains(s)
		s.eng.Record(candidate.Tag, engine.OutcomeSuccess, rttMs)
		s.eng.RememberHost(host, candidate.Tag)
		s.selected = candidate.Tag
		s.onDialSuccess()
		return c, nil
	}
	if lastErr != nil {
		return nil, lastErr
	}
	return s.EmptyFallback().DialContext(ctx, metadata)
}

// ListenPacketContext implements C.ProxyAdapter.
func (s *Smart) ListenPacketContext(ctx context.Context, metadata *C.Metadata) (C.PacketConn, error) {
	if s.disableUDP {
		return nil, errors.New("UDP disabled")
	}
	proxies := s.GetProxies(true)
	if len(proxies) == 0 {
		return s.EmptyFallback().ListenPacketContext(ctx, metadata)
	}
	s.ensureEngine(proxies)
	host := s.hostKey(metadata)
	var lastErr error
	for _, candidate := range s.eng.Select(host) {
		proxy := s.proxyByName(proxies, candidate.Tag)
		if proxy == nil || !proxy.SupportUDP() {
			continue
		}
		start := time.Now()
		pc, err := proxy.ListenPacketContext(ctx, metadata)
		rttMs := float64(time.Since(start).Milliseconds())
		if err != nil {
			s.eng.Record(candidate.Tag, engine.OutcomeFailure, rttMs)
			lastErr = err
			continue
		}
		pc.AppendToChains(s)
		s.eng.Record(candidate.Tag, engine.OutcomeSuccess, rttMs)
		s.eng.RememberHost(host, candidate.Tag)
		s.selected = candidate.Tag
		return pc, nil
	}
	if lastErr != nil {
		return nil, lastErr
	}
	return s.EmptyFallback().ListenPacketContext(ctx, metadata)
}

func (s *Smart) SupportUDP() bool {
	if s.disableUDP {
		return false
	}
	proxies := s.GetProxies(false)
	if len(proxies) == 0 {
		return s.EmptyFallback().SupportUDP()
	}
	s.ensureEngine(proxies)
	cands := s.eng.Select("")
	if len(cands) == 0 {
		return false
	}
	p := s.proxyByName(proxies, cands[0].Tag)
	if p == nil {
		return false
	}
	return p.SupportUDP()
}

func (s *Smart) IsL3Protocol(metadata *C.Metadata) bool {
	proxies := s.GetProxies(false)
	if len(proxies) == 0 {
		return false
	}
	s.ensureEngine(proxies)
	cands := s.eng.Select(s.hostKey(metadata))
	if len(cands) == 0 {
		return false
	}
	p := s.proxyByName(proxies, cands[0].Tag)
	if p == nil {
		return false
	}
	return p.IsL3Protocol(metadata)
}

func (s *Smart) Unwrap(metadata *C.Metadata, touch bool) C.Proxy {
	proxies := s.GetProxies(touch)
	if len(proxies) == 0 {
		return s.EmptyFallback()
	}
	s.ensureEngine(proxies)
	cands := s.eng.Select(s.hostKey(metadata))
	if len(cands) == 0 {
		return proxies[0]
	}
	if p := s.proxyByName(proxies, cands[0].Tag); p != nil {
		return p
	}
	return proxies[0]
}

func (s *Smart) MarshalJSON() ([]byte, error) {
	all := []string{}
	for _, proxy := range s.GetProxies(false) {
		all = append(all, proxy.Name())
	}
	return json.Marshal(map[string]any{
		"type":          s.Type().String(),
		"now":           s.Now(),
		"all":           all,
		"hidden":        s.Hidden(),
		"icon":          s.Icon(),
		"emptyFallback": s.EmptyFallback().Name(),
	})
}

func (s *Smart) Providers() []P.ProxyProvider {
	return s.providers
}

func (s *Smart) Proxies() []C.Proxy {
	return s.GetProxies(false)
}

func (s *Smart) URLTest(ctx context.Context, url string, expectedStatus utils.IntRanges[uint16]) (map[string]uint16, error) {
	return s.GroupBase.URLTest(ctx, url, expectedStatus)
}

