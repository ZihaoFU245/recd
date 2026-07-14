package main

import (
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"

	"recd/config"
	"recd/monitor"
)

func main() {
	logLevel := flag.String("log-level", "info", "log level: debug, info, warn, error")
	headersPath := flag.String("extra-headers", "", "path to JSON file with extra HTTP headers (key-value pairs)")
	pidFile := flag.String("pid-file", "recd.pid", "path to PID file")
	reload := flag.Bool("reload", false, "send SIGHUP to running recd process (reads --pid-file)")
	flag.Parse()

	// --reload mode: send SIGHUP to the running process and exit.
	if *reload {
		data, err := os.ReadFile(*pidFile)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to read pid file %s: %v\n", *pidFile, err)
			os.Exit(1)
		}
		pid, err := strconv.Atoi(strings.TrimSpace(string(data)))
		if err != nil {
			fmt.Fprintf(os.Stderr, "invalid pid in %s: %v\n", *pidFile, err)
			os.Exit(1)
		}
		process, err := os.FindProcess(pid)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to find process %d: %v\n", pid, err)
			os.Exit(1)
		}
		if err := process.Signal(syscall.SIGHUP); err != nil {
			fmt.Fprintf(os.Stderr, "failed to send SIGHUP to pid %d: %v\n", pid, err)
			os.Exit(1)
		}
		fmt.Printf("sent SIGHUP to pid %d\n", pid)
		return
	}

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
		fmt.Fprintf(os.Stderr, "Usage: recd [--log-level=<level>] [--extra-headers=<path>] [--pid-file=<path>] <config.json>\n")
		os.Exit(1)
	}

	configPath := flag.Arg(0)

	// Write PID file.
	pidDir := filepath.Dir(*pidFile)
	if pidDir != "." {
		if err := os.MkdirAll(pidDir, 0755); err != nil {
			logger.Error("failed to create pid file directory", "path", pidDir, "error", err)
			os.Exit(1)
		}
	}
	if err := os.WriteFile(*pidFile, []byte(fmt.Sprintf("%d\n", os.Getpid())), 0644); err != nil {
		logger.Error("failed to write pid file", "path", *pidFile, "error", err)
		os.Exit(1)
	}
	defer os.Remove(*pidFile)

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
		logger.Info("loaded extra headers", "count", len(headers))
	}

	allConfigs, err := config.ParseConfig(configPath)
	if err != nil {
		logger.Error("failed to parse config", "error", err)
		os.Exit(1)
	}

	var targets []config.ChannelConfig
	for _, c := range allConfigs {
		if !c.IsPaused {
			targets = append(targets, c)
		}
	}

	logger.Info("master loaded channels", "total", len(allConfigs), "active", len(targets))

	ctx := config.NewAppContext(logger, headers)

	mon := monitor.New(ctx, targets)
	go mon.Run()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)

	for sig := range sigCh {
		switch sig {
		case syscall.SIGHUP:
			newAllConfigs, err := config.ParseConfig(configPath)
			if err != nil {
				logger.Error("reload: config parse failed, keeping old config", "error", err)
				continue
			}

			delta := config.ComputeDelta(allConfigs, newAllConfigs)
			allConfigs = newAllConfigs

			targets = nil
			for _, c := range newAllConfigs {
				if !c.IsPaused {
					targets = append(targets, c)
				}
			}

			mon.Reload(delta)
			logger.Info("reload complete", "delta_total", len(delta),
				"active", len(targets), "total", len(allConfigs))

		case syscall.SIGINT, syscall.SIGTERM:
			logger.Info("received signal, shutting down", "signal", sig)
			mon.Stop()
			<-mon.Done()
			logger.Info("shutdown complete")
			return
		}
	}
}
