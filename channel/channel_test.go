package channel

import (
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
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

func TestChannelRecoversRecorderPanicAndPublishesError(t *testing.T) {
	ctx := &config.AppContext{
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		Resty:  nil,
	}
	resultCh := make(chan Result, 1)
	ch := New(ctx, config.ChannelConfig{
		Username: "panic_user",
		Pattern:  filepath.Join(t.TempDir(), "panic"),
	}, "https://stream.test/master.m3u8", 9, resultCh, nil)

	ch.Run()
	result := <-resultCh
	if result.Status != StatusError || result.Session != 9 {
		t.Fatalf("unexpected panic result: %+v", result)
	}
	if result.Err == nil || !strings.Contains(result.Err.Error(), "panic") {
		t.Fatalf("panic error = %v", result.Err)
	}
}
