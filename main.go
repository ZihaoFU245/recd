package main

import (
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"recd/config"
	"recd/monitor"
)

func main() {
	logLevel := flag.String("log-level", "info", "log level: debug, info, warn, error")
	headersPath := flag.String("additional-headers", "", "path to JSON file with additional HTTP headers (key-value pairs)")
	flag.Parse()

	var level slog.Level
	switch *logLevel {
	case "debug":
		level = slog.LevelDebug
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	default:
		level = slog.LevelInfo
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))
	slog.SetDefault(logger)

	if flag.NArg() < 1 {
		fmt.Fprintf(os.Stderr, "Usage: recd [--log-level=<level>] [--additional-headers=<path>] <path to json config file>\n")
		os.Exit(1)
	}

	// Ensure the videos output directory exists.
	if err := os.MkdirAll("videos", 0755); err != nil {
		logger.Error("failed to create videos directory", "error", err)
		os.Exit(1)
	}

	var headers map[string]string
	if *headersPath != "" {
		var err error
		headers, err = config.ParseHeaders(*headersPath)
		if err != nil {
			logger.Error("failed to parse headers file", "path", *headersPath, "error", err)
			os.Exit(1)
		}
		logger.Info("loaded additional headers", "count", len(headers))
	}

	configs, err := config.ParseConfig(flag.Arg(0))
	if err != nil {
		logger.Error("failed to parse config", "error", err)
		os.Exit(1)
	}

	var targets []config.ChannelConfig
	for _, c := range configs {
		if !c.IsPaused {
			targets = append(targets, c)
		}
	}

	logger.Info("master loaded channels", "total", len(configs), "active", len(targets))

	ctx := config.NewAppContext(logger, headers)

	mon := monitor.New(ctx, targets)
	go recoverable("monitor", mon.Run)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigCh
	logger.Info("received signal, shutting down", "signal", sig)
	mon.Stop()
	<-mon.Done()

	logger.Info("shutdown complete")
}

func recoverable(name string, fn func()) {
	defer func() {
		if r := recover(); r != nil {
			slog.Error("goroutine panic", "name", name, "panic", r)
		}
	}()
	fn()
}
