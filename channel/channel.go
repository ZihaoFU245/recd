package channel

import (
	"context"
	"fmt"
	"sync"

	"recd/config"
)

// Channel owns one recording session. A Channel is never reused: the monitor
// creates a replacement when a stream needs to be restarted.
type Channel struct {
	ctx            *config.AppContext
	cfg            config.ChannelConfig
	hlsSource      string
	session        uint64
	resultCh       chan<- Result
	supervisorDone <-chan struct{}

	runCtx context.Context
	cancel context.CancelFunc

	mu      sync.Mutex
	started bool
	active  bool
}

// New creates a recording session. supervisorDone lets a worker leave without
// blocking if its monitor has already shut down.
func New(ctx *config.AppContext, cfg config.ChannelConfig, hlsSource string, session uint64, resultCh chan<- Result, supervisorDone <-chan struct{}) *Channel {
	runCtx, cancel := context.WithCancel(context.Background())
	return &Channel{
		ctx:            ctx,
		cfg:            cfg,
		hlsSource:      hlsSource,
		session:        session,
		resultCh:       resultCh,
		supervisorDone: supervisorDone,
		runCtx:         runCtx,
		cancel:         cancel,
	}
}

// Run records until Stop cancels the session. Calling Run more than once is a
// no-op; this prevents a duplicate writer for the same output.
func (c *Channel) Run() {
	c.mu.Lock()
	if c.started {
		c.mu.Unlock()
		return
	}
	c.started = true
	c.active = true
	c.mu.Unlock()

	result := Result{Username: c.cfg.Username, Session: c.session}
	defer func() {
		if panicValue := recover(); panicValue != nil {
			result.Status = StatusError
			result.Err = fmt.Errorf("panic: %v", panicValue)
			c.ctx.Logger.Error("channel panic", "username", c.cfg.Username, "panic", panicValue)
		}

		c.mu.Lock()
		c.active = false
		c.mu.Unlock()

		if c.resultCh == nil {
			return
		}
		select {
		case c.resultCh <- result:
		case <-c.supervisorDone:
		}
	}()

	c.ctx.Logger.Info("recording started",
		"username", c.cfg.Username,
		"resolution", c.cfg.Resolution,
		"framerate", c.cfg.Framerate,
		"max_duration", c.cfg.MaxDuration,
		"hls_source", c.hlsSource != "",
	)

	result = record(c.ctx, c.cfg, c.hlsSource, c.runCtx)
	result.Session = c.session
}

// Stop is idempotent and cancels every in-flight HTTP request owned by this
// recording session.
func (c *Channel) Stop() {
	c.cancel()
}
