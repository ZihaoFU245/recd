package monitor

import (
	"log/slog"
	"os"
	"testing"
	"time"

	"recd/channel"
	"recd/config"
)

func testContext() *config.AppContext {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	return config.NewAppContext(logger, nil)
}

func TestMonitorLifecycle(t *testing.T) {
	ctx := testContext()
	mon := New(ctx, []config.ChannelConfig{
		{IsPaused: false, Username: "test1"},
	})

	go mon.Run()

	time.Sleep(10 * time.Millisecond)
	mon.Stop()

	select {
	case <-mon.Done():
	case <-time.After(time.Second):
		t.Fatal("monitor Run() did not return after Stop()")
	}
}

func TestMonitorNoChannels(t *testing.T) {
	ctx := testContext()
	mon := New(ctx, []config.ChannelConfig{})

	go mon.Run()

	time.Sleep(10 * time.Millisecond)
	mon.Stop()

	select {
	case <-mon.Done():
	case <-time.After(time.Second):
		t.Fatal("monitor Run() did not return after Stop()")
	}
}

func TestMonitorTick_NoOnlineStreams(t *testing.T) {
	ctx := testContext()
	mon := New(ctx, []config.ChannelConfig{
		{IsPaused: false, Username: "offline_user"},
	})

	mon.tick()

	mon.mu.Lock()
	if len(mon.channels) != 0 {
		t.Errorf("expected 0 channels, got %d", len(mon.channels))
	}
	mon.mu.Unlock()
}

func TestMonitorDoneChannel(t *testing.T) {
	ctx := testContext()
	mon := New(ctx, []config.ChannelConfig{})

	select {
	case <-mon.Done():
		t.Fatal("Done() should not be closed before Run()")
	default:
	}

	go mon.Run()
	time.Sleep(10 * time.Millisecond)
	mon.Stop()

	select {
	case <-mon.Done():
	case <-time.After(time.Second):
		t.Fatal("Done() was not closed after Stop()")
	}
}

func TestMonitorReload_PausedChannel(t *testing.T) {
	ctx := testContext()
	cfg := config.ChannelConfig{IsPaused: false, Username: "reload_paused", Resolution: 480}
	mon := New(ctx, []config.ChannelConfig{cfg})

	// Manually add a channel to simulate running state.
	mon.channels["reload_paused"] = channel.New(ctx, cfg, "", nil)

	// Reload with IsPaused=true.
	delta := config.ComputeDelta(mon.configs, []config.ChannelConfig{
		{IsPaused: true, Username: "reload_paused"},
	})
	mon.Reload(delta)

	// Channel should be removed from m.channels and m.configs.
	mon.mu.Lock()
	defer mon.mu.Unlock()
	if _, ok := mon.channels["reload_paused"]; ok {
		t.Error("expected channel to be removed from m.channels")
	}
	for _, c := range mon.configs {
		if c.Username == "reload_paused" {
			t.Error("expected channel to be removed from m.configs")
		}
	}
}

func TestMonitorReload_NewChannel(t *testing.T) {
	ctx := testContext()
	mon := New(ctx, []config.ChannelConfig{})

	// Reload adds a new channel.
	delta := config.ComputeDelta(mon.configs, []config.ChannelConfig{
		{IsPaused: false, Username: "new_user", Resolution: 480},
	})
	mon.Reload(delta)

	mon.mu.Lock()
	defer mon.mu.Unlock()

	// New channel should be added to m.configs.
	found := false
	for _, c := range mon.configs {
		if c.Username == "new_user" {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected new_user in m.configs")
	}

	// Should NOT be in m.channels (stream is offline).
	if _, ok := mon.channels["new_user"]; ok {
		t.Error("expected no channel started (stream offline)")
	}
}

func TestMonitorReload_RemovedChannel(t *testing.T) {
	ctx := testContext()
	cfg := config.ChannelConfig{IsPaused: false, Username: "remove_me", Resolution: 480}
	mon := New(ctx, []config.ChannelConfig{cfg})

	// Simulate running channel.
	mon.channels["remove_me"] = channel.New(ctx, cfg, "", nil)

	// Reload without the channel.
	delta := config.ComputeDelta(mon.configs, []config.ChannelConfig{})
	mon.Reload(delta)

	mon.mu.Lock()
	defer mon.mu.Unlock()

	if _, ok := mon.channels["remove_me"]; ok {
		t.Error("expected channel to be removed from m.channels")
	}
	for _, c := range mon.configs {
		if c.Username == "remove_me" {
			t.Error("expected channel to be removed from m.configs")
		}
	}
}

func TestMonitorReload_ConfigChanged(t *testing.T) {
	ctx := testContext()
	cfg := config.ChannelConfig{IsPaused: false, Username: "change_me", Resolution: 480}
	mon := New(ctx, []config.ChannelConfig{cfg})

	// Simulate running channel with old config.
	mon.channels["change_me"] = channel.New(ctx, cfg, "", nil)

	// Reload with changed resolution.
	newCfg := config.ChannelConfig{IsPaused: false, Username: "change_me", Resolution: 720}
	delta := config.ComputeDelta(mon.configs, []config.ChannelConfig{newCfg})
	mon.Reload(delta)

	mon.mu.Lock()
	defer mon.mu.Unlock()

	// Old channel should be removed, config updated.
	if _, ok := mon.channels["change_me"]; ok {
		t.Error("expected old channel to be removed from m.channels")
	}
	found := false
	for _, c := range mon.configs {
		if c.Username == "change_me" {
			found = true
			if c.Resolution != 720 {
				t.Errorf("expected resolution 720, got %d", c.Resolution)
			}
		}
	}
	if !found {
		t.Error("expected change_me in m.configs")
	}
}

func TestMonitorReload_RetryCleared(t *testing.T) {
	ctx := testContext()
	mon := New(ctx, []config.ChannelConfig{
		{IsPaused: false, Username: "retry_user", Resolution: 480},
	})

	// Simulate pending retry.
	mon.respawnAfter["retry_user"] = time.Now().Add(30 * time.Second)

	// Reload with changed config.
	delta := config.ComputeDelta(mon.configs, []config.ChannelConfig{
		{IsPaused: false, Username: "retry_user", Resolution: 720},
	})
	mon.Reload(delta)

	mon.mu.Lock()
	defer mon.mu.Unlock()

	if _, waiting := mon.respawnAfter["retry_user"]; waiting {
		t.Error("expected retry timer to be cleared")
	}
}

func TestMonitorHandleResult_Reloaded(t *testing.T) {
	ctx := testContext()
	mon := New(ctx, []config.ChannelConfig{
		{IsPaused: false, Username: "stale", Resolution: 480},
	})

	// Add a fake running channel.
	ch := channel.New(ctx, config.ChannelConfig{IsPaused: false, Username: "stale", Resolution: 480}, "", nil)
	mon.channels["stale"] = ch

	// Send a Reloaded result — handleResult should return early without
	// touching m.channels or scheduling restart/retry.
	mon.handleResult(channel.Result{
		Username: "stale",
		Status:   channel.StatusCompleted,
		Reloaded: true,
		Duration: time.Minute,
	})

	mon.mu.Lock()
	defer mon.mu.Unlock()

	// The channel should still be in m.channels (handleResult skipped delete).
	if _, ok := mon.channels["stale"]; !ok {
		t.Error("expected stale channel to remain in m.channels (reloaded results ignored)")
	}
	// No retry should have been scheduled.
	if _, waiting := mon.respawnAfter["stale"]; waiting {
		t.Error("expected no retry for reloaded result")
	}
}
