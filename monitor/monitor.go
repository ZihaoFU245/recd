package monitor

import (
	"context"
	"fmt"
	"runtime/debug"
	"sync"
	"time"

	"recd/config"
	"recd/recorder"
)

const (
	monitorTick          = 250 * time.Millisecond
	liveCheckInterval    = 30 * time.Second
	offlineCheckInterval = 15 * time.Second
	statusRetryInterval  = 5 * time.Second
	initialRetryDelay    = time.Second
	maxRetryDelay        = 15 * time.Second
	slowRetryDelay       = 30 * time.Second
)

type runningRecorder struct {
	session uint64
	cancel  context.CancelFunc
}

type recordingResult struct {
	username string
	session  uint64
	result   recorder.Result
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
	recorders   map[string]runningRecorder
	checking    map[string]bool
	nextCheck   map[string]time.Time
	failures    map[string]int
	nextSession uint64

	resultCh chan recordingResult
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
		recorders: make(map[string]runningRecorder),
		checking:  make(map[string]bool),
		nextCheck: make(map[string]time.Time),
		failures:  make(map[string]int),
		resultCh:  make(chan recordingResult, max(16, len(active)*2)),
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
	m.wg.Wait()
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
		if _, recording := m.recorders[status.username]; !recording {
			m.failures[status.username]++
			delay = retryDelay(m.failures[status.username])
		}
		m.nextCheck[status.username] = time.Now().Add(delay)
		m.ctx.Logger.Warn("stream status check failed", "username", status.username, "delay", delay, "error", status.err)
		return
	}

	if !status.online {
		m.failures[status.username] = 0
		if current, recording := m.recorders[status.username]; recording {
			m.ctx.Logger.Info("stream offline, stopping recorder", "username", status.username)
			current.cancel()
			delete(m.recorders, status.username)
		}
		m.nextCheck[status.username] = time.Now().Add(offlineCheckInterval)
		return
	}

	if _, recording := m.recorders[status.username]; !recording {
		m.startRecorderLocked(status.username, status.hlsSource)
	} else {
		// A session that survives until its next live check is healthy enough to
		// reset request-failure backoff.
		m.failures[status.username] = 0
	}
	m.nextCheck[status.username] = time.Now().Add(liveCheckInterval)
}

func (m *Monitor) startRecorderLocked(username, hlsSource string) {
	cfg, ok := m.configs[username]
	if !ok {
		return
	}
	m.nextSession++
	session := m.nextSession
	runCtx, cancel := context.WithCancel(m.runCtx)
	worker := recorder.New(m.ctx, cfg, hlsSource)
	m.recorders[username] = runningRecorder{session: session, cancel: cancel}
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		result := worker.Run(runCtx)
		select {
		case m.resultCh <- recordingResult{username: username, session: session, result: result}:
		case <-m.stopCh:
		}
	}()
	m.ctx.Logger.Info("stream online, starting recorder", "username", username, "hls_source", hlsSource != "")
}

func (m *Monitor) handleResult(completed recordingResult) {
	result := completed.result
	m.ctx.Logger.Info("recorder finished",
		"username", completed.username,
		"status", result.Status.String(),
		"duration", result.Duration,
		"size", result.Filesize,
		"path", result.Path,
		"error", result.Err,
	)

	m.mu.Lock()
	defer m.mu.Unlock()
	current, recording := m.recorders[completed.username]
	if !recording || current.session != completed.session {
		m.ctx.Logger.Debug("ignoring stale recorder result", "username", completed.username, "session", completed.session)
		return
	}
	delete(m.recorders, completed.username)
	if _, configured := m.configs[completed.username]; !configured || m.runCtx.Err() != nil {
		return
	}

	if result.Status == recorder.StatusError || result.Status == recorder.StatusDesync {
		m.failures[completed.username]++
		delay := slowRetryDelay
		if result.FastRetry {
			delay = retryDelay(m.failures[completed.username])
		}
		m.nextCheck[completed.username] = time.Now().Add(delay)
		m.ctx.Logger.Warn("recording failed; scheduling stream probe",
			"username", completed.username,
			"delay", delay,
			"attempt", m.failures[completed.username],
			"fast_retry", result.FastRetry,
			"error", result.Err,
		)
		return
	}

	m.failures[completed.username] = 0
	m.nextCheck[completed.username] = time.Now()
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
			if current, recording := m.recorders[cfg.Username]; recording {
				current.cancel()
				delete(m.recorders, cfg.Username)
			}
			m.ctx.Logger.Info("channel paused or removed, stopping", "username", cfg.Username)
			continue
		}

		m.configs[cfg.Username] = cfg
		delete(m.failures, cfg.Username)
		if current, recording := m.recorders[cfg.Username]; recording {
			current.cancel()
			delete(m.recorders, cfg.Username)
		}
		m.nextCheck[cfg.Username] = now
	}
}

func (m *Monitor) stopAll() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for username, current := range m.recorders {
		m.ctx.Logger.Info("shutting down recorder", "username", username)
		current.cancel()
	}
	m.recorders = make(map[string]runningRecorder)
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
