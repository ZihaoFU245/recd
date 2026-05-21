package channel

import (
	"sync"
	"time"

	"recd/config"
)

type Channel struct {
	ctx       *config.AppContext
	cfg       config.ChannelConfig
	hlsSource string
	stopCh    chan struct{}
	stopOnce  sync.Once
	mu        sync.Mutex
	active    bool
}

func New(ctx *config.AppContext, cfg config.ChannelConfig, hlsSource string) *Channel {
	return &Channel{
		ctx:       ctx,
		cfg:       cfg,
		hlsSource: hlsSource,
		stopCh:    make(chan struct{}),
	}
}

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
		}
	}()

	c.ctx.Logger.Info("recording started",
		"username", c.cfg.Username,
		"resolution", c.cfg.Resolution,
		"framerate", c.cfg.Framerate,
		"max_duration", c.cfg.MaxDuration,
		"hls_source", c.hlsSource != "",
	)

	c.recordLoop()
}

func (c *Channel) recordLoop() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-c.stopCh:
			c.ctx.Logger.Info("recording stopped", "username", c.cfg.Username)
			return
		case <-ticker.C:
			c.downloadSegments()
		}
	}
}

func (c *Channel) downloadSegments() {
	// TODO: fetch m3u8 master playlist via c.hlsSource, parse with m3u8 lib,
	// select variant matching c.cfg.Resolution, fetch chunklist,
	// download .m4s segments via c.ctx.Resty
	c.ctx.Logger.Debug("downloading segments", "username", c.cfg.Username)
}

func (c *Channel) Stop() {
	c.stopOnce.Do(func() {
		close(c.stopCh)
	})
}
