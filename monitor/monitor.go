package monitor

import (
	"log/slog"
	"sync"
	"time"

	"recd/channel"
	"recd/config"
)

// respawnDelayError is the delay before retrying a channel that failed with an error.
const respawnDelayError = 60 * time.Second

// shutdownTimeout is the maximum time to wait for channel goroutines to finish during shutdown.
const shutdownTimeout = 10 * time.Second

type Monitor struct {
	ctx      *config.AppContext
	configs  []config.ChannelConfig
	channels map[string]*channel.Channel
	mu       sync.Mutex
	stopCh   chan struct{}
	doneCh   chan struct{}

	// resultCh receives recording outcomes from channel goroutines.
	resultCh chan channel.Result

	// respawnAfter schedules delayed retries for channels that failed with errors.
	respawnAfter map[string]time.Time

	// wg tracks active channel goroutines for graceful shutdown.
	wg sync.WaitGroup
}

func New(ctx *config.AppContext, configs []config.ChannelConfig) *Monitor {
	return &Monitor{
		ctx:          ctx,
		configs:      configs,
		channels:     make(map[string]*channel.Channel),
		stopCh:       make(chan struct{}),
		doneCh:       make(chan struct{}),
		resultCh:     make(chan channel.Result, 64),
		respawnAfter: make(map[string]time.Time),
	}
}

func (m *Monitor) Run() {
	defer func() {
		if r := recover(); r != nil {
			m.ctx.Logger.Error("goroutine panic", "name", "monitor", "panic", r)
		}
		close(m.doneCh)
	}()

	m.ctx.Logger.Info("monitor watching channels", "count", len(m.configs))

	// Run an immediate tick to check streams without waiting for the first interval.
	m.tick()

	ticker := time.NewTicker(60 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-m.stopCh:
			m.ctx.Logger.Info("monitor stopping")
			m.shutdownAllChannels()
			// Wait for all channel goroutines to finish (best-effort, with timeout).
			waitWithTimeout(&m.wg, shutdownTimeout)
			return

		case <-ticker.C:
			m.tick()

		case result := <-m.resultCh:
			m.handleResult(result)
		}
	}
}

// waitWithTimeout waits for a WaitGroup up to the given duration, then returns.
func waitWithTimeout(wg *sync.WaitGroup, timeout time.Duration) {
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(timeout):
	}
}

func (m *Monitor) tick() {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Check channels with pending retries.
	now := time.Now()
	for username, retryAt := range m.respawnAfter {
		if now.After(retryAt) {
			delete(m.respawnAfter, username)
			m.ctx.Logger.Info("retrying after error delay", "username", username)
			if online, hlsSource := m.checkStreamStatus(username); online {
				m.startChannelLocked(username, hlsSource)
			}
		}
	}

	for _, cfg := range m.configs {
		ch, exists := m.channels[cfg.Username]

		if exists {
			if !m.isStreamOnline(cfg.Username) {
				m.ctx.Logger.Info("stream offline, stopping channel", "username", cfg.Username)
				ch.Stop()
				delete(m.channels, cfg.Username)
			}
			continue
		}

		// Skip channels that are waiting for retry.
		if _, waiting := m.respawnAfter[cfg.Username]; waiting {
			continue
		}

		if online, hlsSource := m.checkStreamStatus(cfg.Username); online {
			m.startChannelLocked(cfg.Username, hlsSource)
		}
	}
}

// startChannelLocked creates and starts a new channel goroutine.
// Must be called with m.mu held.
func (m *Monitor) startChannelLocked(username, hlsSource string) {
	var cfg config.ChannelConfig
	for _, c := range m.configs {
		if c.Username == username {
			cfg = c
			break
		}
	}
	ch := channel.New(m.ctx, cfg, hlsSource, m.resultCh)
	m.channels[username] = ch

	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		recoverable("channel:"+username, ch.Run)
	}()

	m.ctx.Logger.Info("stream online, starting channel",
		"username", username,
		"hls_source", hlsSource != "",
	)
}

// handleResult processes a recording outcome from a channel goroutine.
// Normal completions trigger an immediate respawn if the stream is still online.
// Errors schedule a delayed retry to avoid tight crash loops.
func (m *Monitor) handleResult(r channel.Result) {
	m.ctx.Logger.Info("channel finished",
		"username", r.Username,
		"status", r.Status.String(),
		"duration", r.Duration,
		"size", r.Filesize,
		"path", r.Path,
		"error", r.Err,
		"reloaded", r.Reloaded,
	)

	if r.Reloaded {
		// Reload already removed the old channel from m.channels and started
		// a new one. Don't touch m.channels, don't schedule restart/retry.
		return
	}

	m.mu.Lock()
	delete(m.channels, r.Username)
	m.mu.Unlock()

	switch r.Status {
	case channel.StatusCompleted, channel.StatusMaxDuration, channel.StatusMaxFilesize:
		m.mu.Lock()
		if online, hlsSource := m.checkStreamStatus(r.Username); online {
			m.startChannelLocked(r.Username, hlsSource)
		}
		m.mu.Unlock()

	case channel.StatusError, channel.StatusDesync:
		m.ctx.Logger.Info("scheduling retry after error",
			"username", r.Username,
			"delay", respawnDelayError,
		)
		m.mu.Lock()
		m.respawnAfter[r.Username] = time.Now().Add(respawnDelayError)
		m.mu.Unlock()
	}
}

// Reload applies a config delta to the monitor. Channels with IsPaused=true
// are stopped and removed from tracking. Other channels are added/updated:
// running channels are signaled via Reload() and a new goroutine is spawned
// immediately if the stream is online.
func (m *Monitor) Reload(delta []config.ChannelConfig) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, newCfg := range delta {
		if newCfg.IsPaused {
			if ch, ok := m.channels[newCfg.Username]; ok {
				ch.Stop()
				delete(m.channels, newCfg.Username)
				m.ctx.Logger.Info("channel paused or removed, stopping", "username", newCfg.Username)
			}
			m.removeFromConfigsLocked(newCfg.Username)
			delete(m.respawnAfter, newCfg.Username)
			continue
		}

		// Not paused — upsert config and clear any pending retry.
		m.upsertConfigLocked(newCfg)
		delete(m.respawnAfter, newCfg.Username)

		// If currently running, signal reload and forget the old channel.
		if ch, ok := m.channels[newCfg.Username]; ok {
			ch.Reload()
			delete(m.channels, newCfg.Username)
			m.ctx.Logger.Info("channel reload triggered", "username", newCfg.Username)
		}

		// Start new channel immediately if stream is online.
		if online, hlsSource := m.checkStreamStatus(newCfg.Username); online {
			m.startChannelLocked(newCfg.Username, hlsSource)
		}
	}
}

// removeFromConfigsLocked removes a channel config by username from m.configs.
// Must be called with m.mu held.
func (m *Monitor) removeFromConfigsLocked(username string) {
	for i, c := range m.configs {
		if c.Username == username {
			m.configs = append(m.configs[:i], m.configs[i+1:]...)
			return
		}
	}
}

// upsertConfigLocked replaces an existing config for username or appends a new
// one to m.configs. Must be called with m.mu held.
func (m *Monitor) upsertConfigLocked(cfg config.ChannelConfig) {
	for i, c := range m.configs {
		if c.Username == cfg.Username {
			m.configs[i] = cfg
			return
		}
	}
	m.configs = append(m.configs, cfg)
}

func (m *Monitor) isStreamOnline(username string) bool {
	online, _ := m.checkStreamStatus(username)
	return online
}

func (m *Monitor) shutdownAllChannels() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for username, ch := range m.channels {
		m.ctx.Logger.Info("shutting down channel", "username", username)
		ch.Stop()
	}
	m.channels = make(map[string]*channel.Channel)
}

func (m *Monitor) Stop() {
	close(m.stopCh)
}

func (m *Monitor) Done() <-chan struct{} {
	return m.doneCh
}

func recoverable(name string, fn func()) {
	defer func() {
		if r := recover(); r != nil {
			slog.Error("goroutine panic", "name", name, "panic", r)
		}
	}()
	fn()
}
