package channel

import (
	"log/slog"
	"os"
	"testing"
	"time"

	"recd/config"
)

func testContext() *config.AppContext {
	return config.NewAppContext(slog.New(slog.NewTextHandler(os.Stderr, nil)), nil)
}

func TestChannelLifecycle(t *testing.T) {
	ch := New(testContext(), config.ChannelConfig{Username: "test_user", Resolution: 720}, "", 1, nil, nil)
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
	ch := New(testContext(), config.ChannelConfig{Username: "test_user"}, "", 1, nil, nil)
	done := make(chan struct{})
	go func() {
		ch.Run()
		close(done)
	}()
	time.Sleep(10 * time.Millisecond)
	ch.Run()
	ch.Stop()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("channel Run() did not return after Stop()")
	}
}

func TestChannelStopBeforeRun(t *testing.T) {
	ch := New(testContext(), config.ChannelConfig{Username: "early_stop"}, "", 1, nil, nil)
	ch.Stop()
	done := make(chan struct{})
	go func() {
		ch.Run()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("channel Run() did not return when already stopped")
	}
}

func TestChannelStopPublishesSession(t *testing.T) {
	resultCh := make(chan Result, 1)
	ch := New(testContext(), config.ChannelConfig{Username: "stop_user"}, "", 7, resultCh, nil)
	ch.Stop()
	ch.Run()
	result := <-resultCh
	if result.Session != 7 || result.Status != StatusCompleted {
		t.Fatalf("unexpected stop result: %+v", result)
	}
}
