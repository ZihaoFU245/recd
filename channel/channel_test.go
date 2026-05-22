package channel

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

func TestChannelLifecycle(t *testing.T) {
	ctx := testContext()
	ch := New(ctx, config.ChannelConfig{Username: "test_user", Resolution: 720}, "", nil)

	done := make(chan struct{})
	go func() {
		ch.Run()
		close(done)
	}()

	time.Sleep(10 * time.Millisecond)
	ch.Stop()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("channel Run() did not return after Stop()")
	}

	ch.mu.Lock()
	active := ch.active
	ch.mu.Unlock()
	if active {
		t.Error("expected channel to be inactive after Stop()")
	}
}

func TestChannelDoubleRun(t *testing.T) {
	ctx := testContext()
	ch := New(ctx, config.ChannelConfig{Username: "test_user"}, "", nil)

	done := make(chan struct{})
	go func() {
		ch.Run()
		close(done)
	}()

	time.Sleep(10 * time.Millisecond)
	ch.Run() // second call should be a no-op

	ch.Stop()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("channel Run() did not return after Stop()")
	}
}

func TestChannelPanicRecovery(t *testing.T) {
	ctx := testContext()
	ch := &Channel{
		ctx:    ctx,
		cfg:    config.ChannelConfig{Username: "panic_user"},
		stopCh: make(chan struct{}),
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		ch.Run()
	}()

	time.Sleep(10 * time.Millisecond)
	ch.Stop()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("channel Run() did not return after Stop()")
	}
}

func TestChannelStopBeforeRun(t *testing.T) {
	ctx := testContext()
	ch := New(ctx, config.ChannelConfig{Username: "early_stop"}, "", nil)

	ch.Stop() // close stopCh before Run

	done := make(chan struct{})
	go func() {
		ch.Run()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("channel Run() did not return when stopCh was already closed")
	}
}

func TestChannelHlsSource(t *testing.T) {
	ctx := testContext()
	hls := "https://edge1-sin.live.mmcdn.com/v1/edge/streams/origin.test_user.01A2B3C4D5/llhls.m3u8?token=abc"
	ch := New(ctx, config.ChannelConfig{Username: "test_user"}, hls, nil)

	if ch.hlsSource != hls {
		t.Errorf("expected hlsSource %q, got %q", hls, ch.hlsSource)
	}

	done := make(chan struct{})
	go func() {
		ch.Run()
		close(done)
	}()

	time.Sleep(10 * time.Millisecond)
	ch.Stop()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("channel Run() did not return after Stop()")
	}
}

func TestChannelReload(t *testing.T) {
	ctx := testContext()
	resultCh := make(chan Result, 1)
	ch := New(ctx, config.ChannelConfig{Username: "reload_user"}, "", resultCh)

	done := make(chan struct{})
	go func() {
		ch.Run()
		close(done)
	}()

	time.Sleep(10 * time.Millisecond)
	ch.Reload()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("channel Run() did not return after Reload()")
	}

	// With empty hlsSource, record() returns StatusError immediately,
	// before reaching the reloadCh select case. So Reloaded=false.
	select {
	case result := <-resultCh:
		if result.Reloaded {
			t.Log("channel returned with Reloaded=true (reloadCh reached)")
		}
		if result.Status != StatusError {
			t.Errorf("expected StatusError with empty hlsSource, got %v", result.Status)
		}
	default:
	}
}

func TestChannelReload_Idempotent(t *testing.T) {
	ctx := testContext()
	ch := New(ctx, config.ChannelConfig{Username: "reload_multi"}, "", nil)

	done := make(chan struct{})
	go func() {
		ch.Run()
		close(done)
	}()

	time.Sleep(10 * time.Millisecond)

	// Multiple Reload calls should not panic.
	ch.Reload()
	ch.Reload()
	ch.Reload()
	ch.Stop()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("channel Run() did not return after Reload() + Stop()")
	}
}
