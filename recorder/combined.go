package recorder

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"recd/config"
)

const (
	combinedSizeCheckInterval = 250 * time.Millisecond
	ffmpegQuitTimeout         = 5 * time.Second
	ffmpegInterruptTimeout    = 3 * time.Second
)

// recordCombinedStream preserves support for variants whose media playlist
// contains both audio and video. ffmpeg owns HLS ingestion for this path because
// the Go segment collector is intentionally built around separate fMP4 tracks.
func (r *Recorder) recordCombinedStream(
	ctx context.Context,
	streamURL,
	outputPath string,
	started time.Time,
	result Result,
) Result {
	partial, err := os.CreateTemp(
		filepath.Dir(outputPath),
		"."+filepath.Base(outputPath)+".*.part",
	)
	if err != nil {
		setRecordError(&result, fmt.Errorf("create partial output: %w", err))
		return result
	}
	partialPath := partial.Name()
	if err := partial.Close(); err != nil {
		_ = os.Remove(partialPath)
		setRecordError(&result, fmt.Errorf("close partial output: %w", err))
		return result
	}
	removePartial := true
	defer func() {
		if removePartial {
			_ = os.Remove(partialPath)
		}
	}()

	if ctx.Err() != nil {
		return result
	}
	referer := fmt.Sprintf("https://chaturbate.com/%s/", r.cfg.Username)
	userAgent := config.DefaultUserAgent
	if r.app.Headers["User-Agent"] != "" {
		userAgent = r.app.Headers["User-Agent"]
	}
	headers := map[string]string{
		"Referer":    referer,
		"User-Agent": userAgent,
	}
	for key, value := range r.app.Headers {
		headers[key] = value
	}
	headers["Referer"] = referer
	var ffmpegHeaders strings.Builder
	for key, value := range headers {
		// ffmpeg expects one CRLF-delimited header argument. Reject unsafe
		// names and strip line breaks from values before constructing it.
		value = strings.ReplaceAll(value, "\r", "")
		value = strings.ReplaceAll(value, "\n", "")
		if key == "" || value == "" || strings.ContainsAny(key, "\r\n:") {
			continue
		}
		ffmpegHeaders.WriteString(key)
		ffmpegHeaders.WriteString(": ")
		ffmpegHeaders.WriteString(value)
		ffmpegHeaders.WriteString("\r\n")
	}
	command := exec.Command(
		"ffmpeg",
		"-hide_banner",
		"-loglevel", "warning",
		"-user_agent", userAgent,
		"-headers", ffmpegHeaders.String(),
		"-i", streamURL,
		"-c", "copy",
		"-f", "matroska",
		"-y",
		partialPath,
	)
	command.Stderr = os.Stderr
	stdin, err := command.StdinPipe()
	if err != nil {
		setRecordError(&result, fmt.Errorf("open ffmpeg stdin: %w", err))
		return result
	}

	r.app.Logger.Info("recording combined HLS stream",
		"username", r.cfg.Username,
		"input", safeURL(streamURL),
	)
	if err := command.Start(); err != nil {
		_ = stdin.Close()
		setRecordError(&result, fmt.Errorf("start ffmpeg: %w", err))
		return result
	}

	ffmpegDone := make(chan error, 1)
	go func() {
		ffmpegDone <- command.Wait()
	}()
	stopFFmpeg := func() {
		_, _ = io.WriteString(stdin, "q\n")
		_ = stdin.Close()
		select {
		case <-ffmpegDone:
		case <-time.After(ffmpegQuitTimeout):
			r.app.Logger.Warn("ffmpeg did not exit after quit; interrupting",
				"username", r.cfg.Username,
			)
			_ = command.Process.Signal(syscall.SIGINT)
			select {
			case <-ffmpegDone:
			case <-time.After(ffmpegInterruptTimeout):
				r.app.Logger.Warn("ffmpeg did not exit after interrupt; killing",
					"username", r.cfg.Username,
				)
				_ = command.Process.Kill()
				<-ffmpegDone
			}
		}
	}

	var durationTimer *time.Timer
	var durationReached <-chan time.Time
	if r.cfg.MaxDuration > 0 {
		remaining := time.Until(started.Add(time.Duration(r.cfg.MaxDuration) * time.Minute))
		durationTimer = time.NewTimer(remaining)
		durationReached = durationTimer.C
		defer durationTimer.Stop()
	}

	var sizeTicker *time.Ticker
	var checkSize <-chan time.Time
	if r.cfg.MaxFilesize > 0 {
		sizeTicker = time.NewTicker(combinedSizeCheckInterval)
		checkSize = sizeTicker.C
		defer sizeTicker.Stop()
	}

capturing:
	for {
		select {
		case err := <-ffmpegDone:
			_ = stdin.Close()
			if err != nil {
				setRecordError(&result, fmt.Errorf("ffmpeg exited: %w", err))
			} else {
				setRecordError(&result, fmt.Errorf(
					"ffmpeg ended before the monitor stopped the recording",
				))
			}
			break capturing

		case <-ctx.Done():
			result.Status = StatusCompleted
			stopFFmpeg()
			break capturing

		case <-durationReached:
			result.Status = StatusMaxDuration
			stopFFmpeg()
			break capturing

		case <-checkSize:
			if fileSize(partialPath) >= r.cfg.MaxFilesize {
				result.Status = StatusMaxFilesize
				stopFFmpeg()
				break capturing
			}
		}
	}

	if err := probeMediaFile(partialPath); err != nil {
		result.Status = StatusError
		result.Err = errors.Join(result.Err, fmt.Errorf("validate combined media: %w", err))
		return result
	}
	if fileSize(partialPath) == 0 {
		result.Status = StatusError
		result.Err = errors.Join(result.Err, fmt.Errorf("validated partial output is empty"))
		return result
	}
	if err := os.Rename(partialPath, outputPath); err != nil {
		result.Status = StatusError
		result.Err = errors.Join(result.Err, fmt.Errorf("publish output: %w", err))
		return result
	}
	removePartial = false
	result.Filesize = fileSize(outputPath)
	if result.Filesize == 0 {
		result.Status = StatusError
		result.Err = errors.Join(result.Err, fmt.Errorf("published output is empty"))
		return result
	}
	r.app.Logger.Info("recording finalized",
		"username", r.cfg.Username,
		"path", outputPath,
		"size", result.Filesize,
		"duration", time.Since(started),
		"status", result.Status,
		"error", result.Err,
	)
	return result
}
