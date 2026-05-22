package channel

import (
	"sync"

	"recd/config"
)

type Channel struct {
	ctx       *config.AppContext
	cfg       config.ChannelConfig
	hlsSource string
	resultCh  chan<- Result
	stopCh    chan struct{}
	reloadCh  chan struct{}
	stopOnce  sync.Once
	mu        sync.Mutex
	active    bool
}

// New creates a new channel goroutine handle. The resultCh is used to
// send the recording outcome back to the monitor when the session ends.
func New(ctx *config.AppContext, cfg config.ChannelConfig, hlsSource string, resultCh chan<- Result) *Channel {
	return &Channel{
		ctx:       ctx,
		cfg:       cfg,
		hlsSource: hlsSource,
		resultCh:  resultCh,
		stopCh:    make(chan struct{}),
		reloadCh:  make(chan struct{}, 1),
	}
}

// Run starts the recording loop. It blocks until the channel is stopped
// or the recording finishes naturally. The outcome is sent on resultCh.
func (c *Channel) Run() {
	c.mu.Lock()
	if c.active {
		c.mu.Unlock()
		return
	}
	c.active = true
	c.mu.Unlock()

	defer func() {
		c.mu.Lock()
		c.active = false
		c.mu.Unlock()
		if r := recover(); r != nil {
			c.ctx.Logger.Error("goroutine panic", "name", "channel:"+c.cfg.Username, "panic", r)
			// Send a panic result so the monitor knows this channel died.
			if c.resultCh != nil {
				c.resultCh <- Result{
					Username: c.cfg.Username,
					Status:   StatusError,
					Err:      &panicError{panic: r},
				}
			}
		}
	}()

	c.ctx.Logger.Info("recording started",
		"username", c.cfg.Username,
		"resolution", c.cfg.Resolution,
		"framerate", c.cfg.Framerate,
		"max_duration", c.cfg.MaxDuration,
		"hls_source", c.hlsSource != "",
	)

	// Run the actual recording; blocks until stop or completion.
	result := record(c.ctx, c.cfg, c.hlsSource, c.stopCh, c.reloadCh)

	// Notify monitor of the outcome.
	if c.resultCh != nil {
		c.resultCh <- result
	}
}

// Stop signals the channel to finish recording gracefully, finalize the
// output file, and exit the Run() goroutine. Safe to call multiple times.
func (c *Channel) Stop() {
	c.stopOnce.Do(func() {
		close(c.stopCh)
	})
}

// Reload signals the channel to stop recording due to a config reload.
// The Run() goroutine will exit with a StatusCompleted result and Reloaded=true.
// Non-blocking; no-op if a reload signal is already pending.
func (c *Channel) Reload() {
	select {
	case c.reloadCh <- struct{}{}:
	default:
	}
}

// panicError wraps a recovered panic value as an error.
type panicError struct{ panic any }

func (e *panicError) Error() string { return "panic: " + sprintAny(e.panic) }

func sprintAny(v any) string {
	switch val := v.(type) {
	case string:
		return val
	case error:
		return val.Error()
	default:
		return "(unknown)"
	}
}
