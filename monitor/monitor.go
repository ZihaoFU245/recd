package monitor

import (
	"context"
	"fmt"
	"runtime/debug"
	"sync"
	"time"

	"recd/channel"
	"recd/config"
)

const (
	monitorTick          = 250 * time.Millisecond
	liveCheckInterval    = 30 * time.Second
	offlineCheckInterval = 15 * time.Second
	statusRetryInterval  = 5 * time.Second
	initialRetryDelay    = time.Second
	maxRetryDelay        = 15 * time.Second
	slowRetryDelay       = 30 * time.Second
	shutdownTimeout      = 10 * time.Second
)

type runningChannel struct {
	session uint64
	channel *channel.Channel
}

type streamStatus struct {
	username  string
	online    bool
	hlsSource string
	err       error
}

// Monitor supervises independent recording sessions. Network calls never hold
// its mutex, so one slow room cannot delay another room's restart.
type Monitor struct {
	ctx *config.AppContext

	mu          sync.Mutex
	configs     map[string]config.ChannelConfig
	channels    map[string]runningChannel
	checking    map[string]bool
	nextCheck   map[string]time.Time
	failures    map[string]int
	nextSession uint64

	resultCh chan channel.Result
	statusCh chan streamStatus

	runCtx   context.Context
	cancel   context.CancelFunc
	stopOnce sync.Once
	stopCh   chan struct{}
	doneCh   chan struct{}
	wg       sync.WaitGroup
}

func New(ctx *config.AppContext, configs []config.ChannelConfig) *Monitor {
	active := make(map[string]config.ChannelConfig, len(configs))
	for _, cfg := range configs {
		if !cfg.IsPaused {
			active[cfg.Username] = cfg
		}
	}
	runCtx, cancel := context.WithCancel(context.Background())
	return &Monitor{
		ctx:       ctx,
		configs:   active,
		channels:  make(map[string]runningChannel),
		checking:  make(map[string]bool),
		nextCheck: make(map[string]time.Time),
		failures:  make(map[string]int),
		resultCh:  make(chan channel.Result, max(16, len(active)*2)),
		statusCh:  make(chan streamStatus, max(16, len(active)*2)),
		runCtx:    runCtx,
		cancel:    cancel,
		stopCh:    make(chan struct{}),
		doneCh:    make(chan struct{}),
	}
}

func (m *Monitor) Run() {
	defer func() {
		if panicValue := recover(); panicValue != nil {
			m.ctx.Logger.Error("monitor goroutine panic recovered",
				"panic", panicValue,
				"stack", string(debug.Stack()),
			)
			m.shutdown()
		}
		close(m.doneCh)
	}()

	m.ctx.Logger.Info("monitor watching channels", "count", len(m.configs))
	m.mu.Lock()
	for username := range m.configs {
		m.nextCheck[username] = time.Now()
	}
	m.mu.Unlock()

	ticker := time.NewTicker(monitorTick)
	defer ticker.Stop()

	for {
		m.tick()
		select {
		case <-m.stopCh:
			m.shutdown()
			return

		case result := <-m.resultCh:
			m.handleResult(result)

		case status := <-m.statusCh:
			m.handleStatus(status)

		case <-ticker.C:
		}
	}
}

func (m *Monitor) shutdown() {
	m.cancel()
	m.stopAll()
	done := make(chan struct{})
	go func() {
		m.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(shutdownTimeout):
		m.ctx.Logger.Warn("timed out waiting for recording sessions to stop")
	}
}

// tick starts every due room-status probe. Probes are independent goroutines;
// state is only changed when their result returns to Run's event loop.
func (m *Monitor) tick() {
	now := time.Now()
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.runCtx.Err() != nil {
		return
	}
	for username, due := range m.nextCheck {
		if due.After(now) || m.checking[username] {
			continue
		}
		if _, configured := m.configs[username]; !configured {
			delete(m.nextCheck, username)
			continue
		}
		m.checking[username] = true
		delete(m.nextCheck, username)
		m.wg.Add(1)
		go func(username string) {
			defer m.wg.Done()
			status := m.probeStreamStatus(username)
			select {
			case m.statusCh <- status:
			case <-m.stopCh:
			}
		}(username)
	}
}

func (m *Monitor) probeStreamStatus(username string) (status streamStatus) {
	status.username = username
	defer func() {
		if panicValue := recover(); panicValue != nil {
			status.online = false
			status.hlsSource = ""
			status.err = fmt.Errorf("status probe panic: %v", panicValue)
			m.ctx.Logger.Error("status probe goroutine panic recovered",
				"username", username,
				"panic", panicValue,
				"stack", string(debug.Stack()),
			)
		}
	}()
	status.online, status.hlsSource, status.err = m.checkStreamStatus(m.runCtx, username)
	return status
}

func (m *Monitor) handleStatus(status streamStatus) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.checking[status.username] = false
	if _, configured := m.configs[status.username]; !configured || m.runCtx.Err() != nil {
		return
	}

	if status.err != nil {
		delay := statusRetryInterval
		if _, recording := m.channels[status.username]; !recording {
			m.failures[status.username]++
			delay = retryDelay(m.failures[status.username])
		}
		m.nextCheck[status.username] = time.Now().Add(delay)
		m.ctx.Logger.Warn("stream status check failed", "username", status.username, "delay", delay, "error", status.err)
		return
	}

	if !status.online {
		m.failures[status.username] = 0
		if current, recording := m.channels[status.username]; recording {
			m.ctx.Logger.Info("stream offline, stopping channel", "username", status.username)
			current.channel.Stop()
			delete(m.channels, status.username)
		}
		m.nextCheck[status.username] = time.Now().Add(offlineCheckInterval)
		return
	}

	if _, recording := m.channels[status.username]; !recording {
		m.startChannelLocked(status.username, status.hlsSource)
	} else {
		// A session that survives until its next live check is healthy enough to
		// reset request-failure backoff.
		m.failures[status.username] = 0
	}
	m.nextCheck[status.username] = time.Now().Add(liveCheckInterval)
}

func (m *Monitor) startChannelLocked(username, hlsSource string) {
	cfg, ok := m.configs[username]
	if !ok {
		return
	}
	m.nextSession++
	session := m.nextSession
	ch := channel.New(m.ctx, cfg, hlsSource, session, m.resultCh, m.stopCh)
	m.channels[username] = runningChannel{session: session, channel: ch}
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		ch.Run()
	}()
	m.ctx.Logger.Info("stream online, starting channel", "username", username, "hls_source", hlsSource != "")
}

func (m *Monitor) handleResult(result channel.Result) {
	m.ctx.Logger.Info("channel finished",
		"username", result.Username,
		"status", result.Status.String(),
		"duration", result.Duration,
		"size", result.Filesize,
		"path", result.Path,
		"error", result.Err,
	)

	m.mu.Lock()
	defer m.mu.Unlock()
	current, recording := m.channels[result.Username]
	if !recording || current.session != result.Session {
		m.ctx.Logger.Debug("ignoring stale channel result", "username", result.Username, "session", result.Session)
		return
	}
	delete(m.channels, result.Username)
	if _, configured := m.configs[result.Username]; !configured || m.runCtx.Err() != nil {
		return
	}

	if result.Status == channel.StatusError || result.Status == channel.StatusDesync || result.Status == channel.StatusEnded {
		m.failures[result.Username]++
		delay := slowRetryDelay
		if result.FastRetry {
			delay = retryDelay(m.failures[result.Username])
		}
		m.nextCheck[result.Username] = time.Now().Add(delay)
		m.ctx.Logger.Warn("recording failed; scheduling stream probe",
			"username", result.Username,
			"delay", delay,
			"attempt", m.failures[result.Username],
			"fast_retry", result.FastRetry,
			"error", result.Err,
		)
		return
	}

	m.failures[result.Username] = 0
	m.nextCheck[result.Username] = time.Now()
}

func retryDelay(attempt int) time.Duration {
	delay := initialRetryDelay
	for attempt > 1 && delay < maxRetryDelay {
		delay *= 2
		attempt--
	}
	if delay > maxRetryDelay {
		return maxRetryDelay
	}
	return delay
}

// Reload only updates in-memory state and stops replaced workers. The next
// event-loop tick probes new or changed rooms without holding a mutex on I/O.
func (m *Monitor) Reload(delta []config.ChannelConfig) {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := time.Now()
	for _, cfg := range delta {
		if cfg.IsPaused {
			delete(m.configs, cfg.Username)
			delete(m.nextCheck, cfg.Username)
			delete(m.failures, cfg.Username)
			if current, recording := m.channels[cfg.Username]; recording {
				current.channel.Stop()
				delete(m.channels, cfg.Username)
			}
			m.ctx.Logger.Info("channel paused or removed, stopping", "username", cfg.Username)
			continue
		}

		m.configs[cfg.Username] = cfg
		delete(m.failures, cfg.Username)
		if current, recording := m.channels[cfg.Username]; recording {
			current.channel.Stop()
			delete(m.channels, cfg.Username)
		}
		m.nextCheck[cfg.Username] = now
	}
}

func (m *Monitor) stopAll() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for username, current := range m.channels {
		m.ctx.Logger.Info("shutting down channel", "username", username)
		current.channel.Stop()
	}
	m.channels = make(map[string]runningChannel)
}

func (m *Monitor) Stop() {
	m.stopOnce.Do(func() {
		m.cancel()
		close(m.stopCh)
	})
}

func (m *Monitor) Done() <-chan struct{} {
	return m.doneCh
}
