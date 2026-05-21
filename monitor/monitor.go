package monitor

import (
	"log/slog"
	"sync"
	"time"

	"recd/channel"
	"recd/config"
)

type Monitor struct {
	ctx      *config.AppContext
	configs  []config.ChannelConfig
	channels map[string]*channel.Channel
	mu       sync.Mutex
	stopCh   chan struct{}
	doneCh   chan struct{}
}

func New(ctx *config.AppContext, configs []config.ChannelConfig) *Monitor {
	return &Monitor{
		ctx:      ctx,
		configs:  configs,
		channels: make(map[string]*channel.Channel),
		stopCh:   make(chan struct{}),
		doneCh:   make(chan struct{}),
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
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-m.stopCh:
			m.ctx.Logger.Info("monitor stopping")
			m.shutdownAllChannels()
			return
		case <-ticker.C:
			m.tick()
		}
	}
}

func (m *Monitor) tick() {
	m.mu.Lock()
	defer m.mu.Unlock()

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

		if online, hlsSource := m.checkStreamStatus(cfg.Username); online {
			ch := channel.New(m.ctx, cfg, hlsSource)
			m.channels[cfg.Username] = ch
			go recoverable("channel:"+cfg.Username, ch.Run)
			m.ctx.Logger.Info("stream online, starting channel", "username", cfg.Username, "hls_source", hlsSource != "")
		}
	}
}

func (m *Monitor) checkStreamStatus(username string) (online bool, hlsSource string) {
	// TODO: fetch https://chaturbate.com/{username}/ via m.ctx.Resty
	// extract window.initialRoomDossier, decode unicode escapes,
	// unmarshal into RoomDossier, check room_status == "public"
	return false, ""
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
