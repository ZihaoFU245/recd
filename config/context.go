package config

import (
	"log/slog"

	"github.com/go-resty/resty/v2"
)

type AppContext struct {
	Resty   *resty.Client
	Logger  *slog.Logger
	Headers map[string]string
}

func NewAppContext(logger *slog.Logger, headers map[string]string) *AppContext {
	client := resty.New()
	if headers != nil {
		client.SetHeaders(headers)
	}
	return &AppContext{
		Resty:   client,
		Logger:  logger,
		Headers: headers,
	}
}
