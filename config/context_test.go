package config

import (
	"log/slog"
	"os"
	"testing"
)

func TestAppContext(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	headers := map[string]string{
		"User-Agent": "TestAgent/1.0",
		"X-Custom":   "value",
	}
	ctx := NewAppContext(logger, headers)

	if ctx.Resty == nil {
		t.Error("expected non-nil resty client")
	}
	if ctx.Logger != logger {
		t.Error("expected logger to be the one passed in")
	}
	if len(ctx.Headers) != 2 {
		t.Errorf("expected 2 headers, got %d", len(ctx.Headers))
	}
	if ctx.Headers["User-Agent"] != "TestAgent/1.0" {
		t.Errorf("expected User-Agent header, got %q", ctx.Headers["User-Agent"])
	}
	if got := ctx.Resty.Header.Get("User-Agent"); got != "TestAgent/1.0" {
		t.Errorf("expected resty User-Agent override, got %q", got)
	}
}

func TestAppContext_NilHeaders(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	ctx := NewAppContext(logger, nil)

	if ctx.Resty == nil {
		t.Error("expected non-nil resty client")
	}
	if ctx.Headers != nil {
		t.Error("expected nil headers when not provided")
	}
	if got := ctx.Resty.Header.Get("User-Agent"); got != DefaultUserAgent {
		t.Errorf("expected default User-Agent, got %q", got)
	}
}
