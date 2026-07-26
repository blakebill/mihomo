package outboundgroup

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/metacubex/mihomo/adapter/outboundgroup/smartengine"
	"github.com/metacubex/mihomo/common/callback"
	"github.com/metacubex/mihomo/common/dialfeedback"
	N "github.com/metacubex/mihomo/common/net"
	"github.com/metacubex/mihomo/common/utils"
	C "github.com/metacubex/mihomo/constant"
	P "github.com/metacubex/mihomo/constant/provider"
)

// SmartOption is optional config for type: smart groups.
// dart-smart:options
type SmartOption struct {
	Mode string `group:"mode,omitempty"`
}

// Smart is a Dart Smart proxy-group with connection-level dial failover.
type Smart struct {
	*GroupBase
	eng        *engine.Engine
	selected   string
	disableUDP bool
	testUrl    string
	mode       string
}

// dart-smart:constructor
func NewSmart(option GroupCommonOption, smartOption SmartOption, emptyFallback C.Proxy, providers []P.ProxyProvider) (*Smart, error) {
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
		mode:       smartOption.Mode,
	}
	return s, nil
}

func (s *Smart) ensureEngine(proxies []C.Proxy) {
	tags := make([]string, 0, len(proxies))
	for _, p := range proxies {
		tags = append(tags, p.Name())
	}
	if s.eng == nil {
		s.eng = engine.New(tags, engine.OptionsForMode(s.mode))
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

func (s *Smart) recordLegacyDialFeedback(tag, network, signal string, success bool, duration time.Duration, errorClass string) {
	dialfeedback.Default.AddLegacySignal(s.Name(), tag, network, signal, success, duration, errorClass)
}

func (s *Smart) recordLegacyDialFeedbackDurations(
	tag, network, signal string,
	success bool,
	detailedDuration, legacyDuration time.Duration,
	errorClass string,
) {
	dialfeedback.Default.AddLegacySignalDurations(
		s.Name(),
		tag,
		network,
		signal,
		success,
		detailedDuration,
		legacyDuration,
		errorClass,
	)
}

func (s *Smart) recordSupplementalDialFeedback(tag, network, signal string, success bool, duration time.Duration, errorClass string) {
	dialfeedback.Default.AddSupplementalSignal(s.Name(), tag, network, signal, success, duration, errorClass)
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
	for _, candidate := range s.eng.SelectFor(host, engine.NetworkTCP) {
		proxy := s.proxyByName(proxies, candidate.Tag)
		if proxy == nil {
			continue
		}
		start := time.Now()
		c, err := proxy.DialContext(ctx, metadata)
		elapsed := time.Since(start)
		rttMs := float64(elapsed.Milliseconds())
		if err != nil {
			s.eng.RecordFor(host, engine.NetworkTCP, candidate.Tag, engine.OutcomeFailure, rttMs)
			s.recordLegacyDialFeedback(candidate.Tag, string(engine.NetworkTCP), "tcp", false, elapsed, dialfeedback.ErrorClass(err))
			s.onDialFailed(proxy.Type(), err, nil)
			lastErr = err
			continue
		}
		threshold := s.eng.SoftFailThresholdMs(candidate.Tag)
		if rttMs > threshold && threshold > 0 {
			_ = c.Close()
			s.eng.RecordFor(host, engine.NetworkTCP, candidate.Tag, engine.OutcomeSoftFail, rttMs)
			s.recordLegacyDialFeedback(candidate.Tag, string(engine.NetworkTCP), "tcp", false, elapsed, "soft-fail")
			lastErr = errors.New("smart soft-fail: " + candidate.Tag)
			continue
		}
		c.AppendToChains(s)
		tag := candidate.Tag
		// Defer final success until first write when the member still needs a handshake.
		// Failure / very slow first write updates engine so the next dial failovers.
		if N.NeedHandshake(c) {
			s.recordSupplementalDialFeedback(tag, string(engine.NetworkTCP), "tcp", true, elapsed, "")
			dialElapsed := elapsed
			hostKey := host
			c = callback.NewFirstWriteLatencyCallBackConn(c, func(err error, writeElapsed time.Duration) {
				hsMs := float64(writeElapsed.Milliseconds())
				totalElapsed := dialElapsed + writeElapsed
				if err != nil {
					s.eng.RecordFor(hostKey, engine.NetworkTCP, tag, engine.OutcomeFailure, hsMs)
					s.recordLegacyDialFeedbackDurations(
						tag,
						string(engine.NetworkTCP),
						"handshake",
						false,
						writeElapsed,
						totalElapsed,
						dialfeedback.ErrorClass(err),
					)
					return
				}
				th := s.eng.SoftFailThresholdMs(tag)
				if hsMs > th && th > 0 {
					// Write already completed; keep the conn but mark soft-fail for ranking.
					s.eng.RecordFor(hostKey, engine.NetworkTCP, tag, engine.OutcomeSoftFail, hsMs)
					s.recordLegacyDialFeedbackDurations(
						tag,
						string(engine.NetworkTCP),
						"handshake",
						false,
						writeElapsed,
						totalElapsed,
						"soft-fail",
					)
					return
				}
				s.eng.RecordFor(hostKey, engine.NetworkTCP, tag, engine.OutcomeSuccess, hsMs)
				s.eng.RememberHostFor(hostKey, engine.NetworkTCP, tag)
				s.recordLegacyDialFeedbackDurations(
					tag,
					string(engine.NetworkTCP),
					"handshake",
					true,
					writeElapsed,
					totalElapsed,
					"",
				)
			})
			s.selected = tag
			s.onDialSuccess()
			c = s.observeFirstByte(c, hostKey, tag)
			return c, nil
		}
		s.eng.RecordFor(host, engine.NetworkTCP, candidate.Tag, engine.OutcomeSuccess, rttMs)
		s.eng.RememberHostFor(host, engine.NetworkTCP, candidate.Tag)
		s.recordLegacyDialFeedback(candidate.Tag, string(engine.NetworkTCP), "tcp", true, elapsed, "")
		s.selected = candidate.Tag
		s.onDialSuccess()
		c = s.observeFirstByte(c, host, candidate.Tag)
		return c, nil
	}
	if lastErr != nil {
		return nil, lastErr
	}
	return s.EmptyFallback().DialContext(ctx, metadata)
}

func (s *Smart) observeFirstByte(conn C.Conn, host, tag string) C.Conn {
	return newFirstByteObserveConn(conn, func(err error, latency time.Duration) {
		s.eng.RecordFirstByteFor(host, engine.NetworkTCP, tag, err == nil, float64(latency.Milliseconds()))
		s.recordSupplementalDialFeedback(
			tag,
			string(engine.NetworkTCP),
			"first-byte",
			err == nil,
			latency,
			dialfeedback.ErrorClass(err),
		)
	})
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
	for _, candidate := range s.eng.SelectFor(host, engine.NetworkUDP) {
		proxy := s.proxyByName(proxies, candidate.Tag)
		if proxy == nil || !proxy.SupportUDP() {
			continue
		}
		start := time.Now()
		pc, err := proxy.ListenPacketContext(ctx, metadata)
		elapsed := time.Since(start)
		rttMs := float64(elapsed.Milliseconds())
		if err != nil {
			s.eng.RecordFor(host, engine.NetworkUDP, candidate.Tag, engine.OutcomeFailure, rttMs)
			s.recordLegacyDialFeedback(candidate.Tag, string(engine.NetworkUDP), "udp", false, elapsed, dialfeedback.ErrorClass(err))
			lastErr = err
			continue
		}
		pc.AppendToChains(s)
		s.eng.RecordFor(host, engine.NetworkUDP, candidate.Tag, engine.OutcomeSuccess, rttMs)
		s.eng.RememberHostFor(host, engine.NetworkUDP, candidate.Tag)
		s.recordLegacyDialFeedback(candidate.Tag, string(engine.NetworkUDP), "udp", true, elapsed, "")
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
