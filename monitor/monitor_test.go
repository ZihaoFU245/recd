package monitor

import (
	"log/slog"
	"os"
	"testing"
	"time"

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
