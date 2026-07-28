package config

import (
	"log/slog"
	"sync"
	"time"

	"github.com/go-resty/resty/v2"
	"github.com/quic-go/quic-go/http3"
)

const DefaultUserAgent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/150.0.0.0 Safari/537.36"

type AppContext struct {
	Resty   *resty.Client
	Logger  *slog.Logger
	Headers map[string]string

	http3Transport *http3.Transport
	closeOnce      sync.Once
	closeErr       error
}

func NewAppContext(logger *slog.Logger, headers map[string]string, enableHTTP3 bool) *AppContext {
	client := resty.New().
		SetHeader("User-Agent", DefaultUserAgent).
		SetTimeout(10 * time.Second)
	var http3Transport *http3.Transport
	if enableHTTP3 {
		http3Transport = &http3.Transport{}
		client.SetTransport(http3Transport)
	}
	if headers != nil {
		client.SetHeaders(headers)
	}
	return &AppContext{
		Resty:          client,
		Logger:         logger,
		Headers:        headers,
		http3Transport: http3Transport,
	}
}

// Close releases resources owned by the configured HTTP transport.
func (c *AppContext) Close() error {
	c.closeOnce.Do(func() {
		if c.http3Transport != nil {
			c.closeErr = c.http3Transport.Close()
		}
	})
	return c.closeErr
}
