package config

import (
	"log/slog"
	"time"

	"github.com/go-resty/resty/v2"
)

const DefaultUserAgent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/150.0.0.0 Safari/537.36"

type AppContext struct {
	Resty   *resty.Client
	Logger  *slog.Logger
	Headers map[string]string
}

func NewAppContext(logger *slog.Logger, headers map[string]string) *AppContext {
	client := resty.New().
		SetHeader("User-Agent", DefaultUserAgent).
		SetTimeout(10 * time.Second)
	if headers != nil {
		client.SetHeaders(headers)
	}
	return &AppContext{
		Resty:   client,
		Logger:  logger,
		Headers: headers,
	}
}
