package channel

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"recd/config"

	"github.com/grafov/m3u8"
)

const (
	maxDesyncSeconds    = 60.0
	segmentPollInterval = 300 * time.Millisecond
	maxInitialAVOffset  = 1 * time.Second

	maxOutputPathTries = 10000
)

var (
	mergeMediaFiles = mergeTempFiles
)

type trackState struct {
	url         string
	writer      io.WriteCloser
	lastSeq     uint64
	haveLastSeq bool
	initURL     string
	duration    float64
	size        int64
	username    string
	name        string
}

// requestError marks a failure that should make the monitor obtain fresh room
// state quickly. HTTP failures also carry a status used to decide whether one
// retry of the same URL is safe.
type requestError struct {
	err    error
	status int
}

func (e *requestError) Error() string { return e.err.Error() }
func (e *requestError) Unwrap() error { return e.err }

func (e *requestError) retrySameURL() bool {
	return e.status == 0 ||
		e.status == http.StatusRequestTimeout ||
		e.status == http.StatusTooManyRequests ||
		e.status >= 500
}

type segmentCacheEntry struct {
	Username        string `json:"username"`
	Track           string `json:"track"`
	Kind            string `json:"kind"`
	Seq             uint64 `json:"seq,omitempty"`
	URI             string `json:"uri"`
	URL             string `json:"url"`
	Path            string `json:"path"`
	SHA256          string `json:"sha256"`
	Size            int    `json:"size"`
	DurationSeconds string `json:"duration_seconds,omitempty"`
	ProgramDateTime string `json:"program_date_time,omitempty"`
}

// record starts recording for a channel. Separate audio/video fMP4 variants are
// downloaded into temp files and merged by ffmpeg with source timestamps
// preserved. Single-stream variants are handed to ffmpeg's HLS demuxer.
func record(ctx *config.AppContext, cfg config.ChannelConfig, hlsSource string, recordingCtx context.Context) (res Result) {
	res.Username = cfg.Username

	t0 := time.Now()
	if recordingCtx.Err() != nil {
		res.Status = StatusCompleted
		return
	}
	outputPath, err := nextOutputPath(cfg.Pattern, cfg.Username, t0)
	if err != nil {
		res.Status = StatusError
		res.Err = err
		return
	}

	if dir := filepath.Dir(outputPath); dir != "" {
		if err := os.MkdirAll(dir, 0755); err != nil {
			res.Status = StatusError
			res.Err = fmt.Errorf("mkdir %s: %w", dir, err)
			return
		}
	}

	videoChunkURL, audioChunkURL, hasAudio, err := fetchAndSelectVariant(recordingCtx, ctx, hlsSource, cfg)
	if err != nil {
		if recordingCtx.Err() != nil {
			res.Status = StatusCompleted
			res.Duration = time.Since(t0)
			res.Path = outputPath
			return
		}
		setRecordError(&res, fmt.Errorf("variant selection: %w", err))
		return
	}

	referer := fmt.Sprintf("https://chaturbate.com/%s/", cfg.Username)

	if hasAudio {
		return recordWithTempFiles(recordingCtx, ctx, cfg, videoChunkURL, audioChunkURL, referer, outputPath, t0)
	}
	return recordWithFFmpegHLS(recordingCtx, ctx, cfg, videoChunkURL, referer, outputPath, t0)
}

func nextOutputPath(pattern, username string, start time.Time) (string, error) {
	compiled, err := config.CompilePathPattern(pattern)
	if err != nil {
		return "", fmt.Errorf("pattern expansion: %w", err)
	}
	seen := make(map[string]struct{})
	for seq := 0; seq < maxOutputPathTries; seq++ {
		path, err := compiled.Expand(config.PathVars(username, start, seq))
		if err != nil {
			return "", fmt.Errorf("pattern expansion: %w", err)
		}
		if strings.TrimSpace(path) == "" {
			return "", fmt.Errorf("pattern expansion produced an empty path")
		}
		if _, repeated := seen[path]; repeated {
			path += "_" + strconv.Itoa(seq)
		}
		seen[path] = struct{}{}
		path += ".mkv"

		if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
			return path, nil
		} else if err != nil {
			return "", fmt.Errorf("stat output path %s: %w", path, err)
		}
	}
	return "", fmt.Errorf("no available output path after %d attempts", maxOutputPathTries)
}

func setRecordError(res *Result, err error) {
	res.Status = StatusError
	res.Err = err
	var requestErr *requestError
	res.FastRetry = errors.As(err, &requestErr)
}

func recordWithFFmpegHLS(recordingCtx context.Context, ctx *config.AppContext, cfg config.ChannelConfig, videoChunkURL, referer, outputPath string, t0 time.Time) (res Result) {
	res.Username = cfg.Username

	args := []string{
		"-hide_banner", "-loglevel", "warning",
		"-user_agent", ffmpegUserAgent(ctx),
		"-headers", ffmpegHeaders(ctx, referer),
		"-i", videoChunkURL,
		"-c", "copy",
	}
	args = append(args, "-f", "matroska", "-y", outputPath)

	ffCmd := exec.Command("ffmpeg", args...)
	ffCmd.Stderr = os.Stderr
	stdin, err := ffCmd.StdinPipe()
	if err != nil {
		res.Status = StatusError
		res.Err = fmt.Errorf("open ffmpeg stdin: %w", err)
		return
	}

	ctx.Logger.Info("starting ffmpeg hls recorder",
		"username", cfg.Username,
		"output", outputPath,
		"input", safeURL(videoChunkURL),
	)
	if err := ffCmd.Start(); err != nil {
		res.Status = StatusError
		res.Err = fmt.Errorf("start ffmpeg: %w", err)
		return
	}

	ffDone := make(chan error, 1)
	go func() { ffDone <- ffCmd.Wait() }()

	stopFFmpeg := func() {
		_, _ = io.WriteString(stdin, "q\n")
		_ = stdin.Close()
		select {
		case <-ffDone:
		case <-time.After(5 * time.Second):
			ctx.Logger.Warn("ffmpeg did not exit after quit, interrupting", "username", cfg.Username)
			_ = ffCmd.Process.Signal(syscall.SIGINT)
			select {
			case <-ffDone:
			case <-time.After(3 * time.Second):
				ctx.Logger.Warn("ffmpeg did not exit after interrupt, killing", "username", cfg.Username)
				_ = ffCmd.Process.Kill()
				<-ffDone
			}
		}
	}

	var durationTimer *time.Timer
	var durationReached <-chan time.Time
	if cfg.MaxDuration > 0 {
		durationTimer = time.NewTimer(time.Duration(cfg.MaxDuration) * time.Minute)
		durationReached = durationTimer.C
		defer durationTimer.Stop()
	}

	var sizeTicker *time.Ticker
	var checkSize <-chan time.Time
	if cfg.MaxFilesize > 0 {
		sizeTicker = time.NewTicker(250 * time.Millisecond)
		checkSize = sizeTicker.C
		defer sizeTicker.Stop()
	}

	for {
		select {
		case err := <-ffDone:
			if err != nil {
				res.Status = StatusError
				res.Err = fmt.Errorf("ffmpeg exited: %w", err)
			} else {
				res.Status = StatusEnded
				res.Err = fmt.Errorf("ffmpeg ended before the monitor stopped the recording")
			}
			goto finish

		case <-recordingCtx.Done():
			ctx.Logger.Info("recording stopped by monitor", "username", cfg.Username)
			res.Status = StatusCompleted
			stopFFmpeg()
			goto finish

		case <-durationReached:
			ctx.Logger.Info("max duration reached",
				"username", cfg.Username,
				"limit_minutes", cfg.MaxDuration,
			)
			res.Status = StatusMaxDuration
			stopFFmpeg()
			goto finish

		case <-checkSize:
			if fi, err := os.Stat(outputPath); err == nil && fi.Size() >= cfg.MaxFilesize {
				ctx.Logger.Info("max filesize reached",
					"username", cfg.Username,
					"size", fi.Size(),
					"limit", cfg.MaxFilesize,
				)
				res.Status = StatusMaxFilesize
				stopFFmpeg()
				goto finish
			}
		}
	}

finish:
	if fi, err := os.Stat(outputPath); err == nil {
		res.Filesize = fi.Size()
	}
	res.Duration = time.Since(t0)
	res.Path = outputPath

	ctx.Logger.Info("recording finalized",
		"username", cfg.Username,
		"path", outputPath,
		"size", res.Filesize,
		"duration", res.Duration,
		"status", res.Status,
	)
	return
}

func ffmpegUserAgent(ctx *config.AppContext) string {
	if ctx != nil && ctx.Headers != nil && ctx.Headers["User-Agent"] != "" {
		return ctx.Headers["User-Agent"]
	}
	return config.DefaultUserAgent
}

func ffmpegHeaders(ctx *config.AppContext, referer string) string {
	headers := map[string]string{
		"Referer":    referer,
		"User-Agent": ffmpegUserAgent(ctx),
	}
	if ctx != nil {
		for k, v := range ctx.Headers {
			headers[k] = v
		}
	}
	headers["Referer"] = referer

	keys := make([]string, 0, len(headers))
	for k := range headers {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var b strings.Builder
	for _, k := range keys {
		v := sanitizeHeaderValue(headers[k])
		if k == "" || v == "" {
			continue
		}
		b.WriteString(k)
		b.WriteString(": ")
		b.WriteString(v)
		b.WriteString("\r\n")
	}
	return b.String()
}

func sanitizeHeaderValue(v string) string {
	v = strings.ReplaceAll(v, "\r", "")
	v = strings.ReplaceAll(v, "\n", "")
	return v
}

// recordWithTempFiles downloads separate video and audio fMP4 renditions and
// then merges them into one MKV.
func recordWithTempFiles(recordingCtx context.Context, ctx *config.AppContext, cfg config.ChannelConfig, videoChunkURL, audioChunkURL, referer, outputPath string, t0 time.Time) (res Result) {
	res.Username = cfg.Username

	videoFile, err := os.CreateTemp("", "rec_video_*.bin")
	if err != nil {
		res.Status = StatusError
		res.Err = fmt.Errorf("create video temp file: %w", err)
		return
	}
	videoPath := videoFile.Name()

	audioFile, err := os.CreateTemp("", "rec_audio_*.bin")
	if err != nil {
		_ = videoFile.Close()
		_ = os.Remove(videoPath)
		res.Status = StatusError
		res.Err = fmt.Errorf("create audio temp file: %w", err)
		return
	}
	audioPath := audioFile.Name()

	vs := trackState{url: videoChunkURL, writer: videoFile, username: cfg.Username, name: "video"}
	as := trackState{url: audioChunkURL, writer: audioFile, username: cfg.Username, name: "audio"}

	ctx.Logger.Info("recording loop started (temp files)",
		"username", cfg.Username,
		"resolution", cfg.Resolution,
		"output", outputPath,
		"video_tmp", videoPath,
		"audio_tmp", audioPath,
	)

	poll := time.NewTicker(segmentPollInterval)
	defer poll.Stop()

	if err := alignInitialTracks(recordingCtx, ctx, referer, &vs, &as); err != nil {
		if recordingCtx.Err() != nil {
			res.Status = StatusCompleted
		} else {
			ctx.Logger.Error("initial track alignment failed", "username", cfg.Username, "error", err)
			setRecordError(&res, err)
		}
		goto finish
	}

	for {
		if err := processTrack(recordingCtx, ctx, referer, &vs); err != nil {
			if recordingCtx.Err() != nil {
				res.Status = StatusCompleted
			} else {
				ctx.Logger.Error("video track error", "username", cfg.Username, "error", err)
				setRecordError(&res, err)
			}
			goto finish
		}

		if err := processTrack(recordingCtx, ctx, referer, &as); err != nil {
			if recordingCtx.Err() != nil {
				res.Status = StatusCompleted
			} else {
				ctx.Logger.Error("audio track error", "username", cfg.Username, "error", err)
				setRecordError(&res, err)
			}
			goto finish
		}

		drift := vs.duration - as.duration
		if drift < 0 {
			drift = -drift
		}
		if drift > maxDesyncSeconds {
			err := fmt.Errorf("audio/video desync: video=%.1fs audio=%.1fs drift=%.1fs",
				vs.duration, as.duration, drift)
			ctx.Logger.Error("desync detected", "username", cfg.Username, "error", err)
			res.Status = StatusDesync
			res.Err = err
			goto finish
		}

		if maxDurationReached(cfg.MaxDuration, vs.duration, as.duration) {
			ctx.Logger.Info("max duration reached",
				"username", cfg.Username,
				"video_duration", time.Duration(vs.duration*float64(time.Second)),
				"audio_duration", time.Duration(as.duration*float64(time.Second)),
				"limit_minutes", cfg.MaxDuration,
			)
			res.Status = StatusMaxDuration
			goto finish
		}
		if maxFilesizeReached(cfg.MaxFilesize, vs.size, as.size) {
			ctx.Logger.Info("max filesize reached",
				"username", cfg.Username,
				"captured_size", vs.size+as.size,
				"limit", cfg.MaxFilesize,
			)
			res.Status = StatusMaxFilesize
			goto finish
		}

		select {
		case <-recordingCtx.Done():
			ctx.Logger.Info("recording stopped by monitor", "username", cfg.Username)
			res.Status = StatusCompleted
			goto finish
		case <-poll.C:
		}
	}

finish:
	if closeErr := errors.Join(videoFile.Close(), audioFile.Close()); closeErr != nil {
		res.Status = StatusError
		res.Err = errors.Join(res.Err, fmt.Errorf("close temporary media: %w", closeErr))
		res.Duration = time.Since(t0)
		res.Path = outputPath
		ctx.Logger.Error("failed to close temporary media",
			"username", cfg.Username,
			"video_tmp", videoPath,
			"audio_tmp", audioPath,
			"error", closeErr,
		)
		return
	}

	finalizeTempRecording(ctx, cfg.Username, videoPath, audioPath, outputPath, t0, &res)
	return
}

func maxDurationReached(maxMinutes int, durations ...float64) bool {
	if maxMinutes <= 0 {
		return false
	}
	limit := float64(maxMinutes * 60)
	for _, duration := range durations {
		if duration >= limit {
			return true
		}
	}
	return false
}

func maxFilesizeReached(maxBytes int64, sizes ...int64) bool {
	if maxBytes <= 0 {
		return false
	}
	var total int64
	for _, size := range sizes {
		if size >= maxBytes-total {
			return true
		}
		total += size
	}
	return false
}

func finalizeTempRecording(ctx *config.AppContext, username, videoPath, audioPath, outputPath string, t0 time.Time, res *Result) {
	res.Duration = time.Since(t0)
	res.Path = outputPath

	videoSize := mustFileSize(videoPath)
	audioSize := mustFileSize(audioPath)
	if videoSize == 0 || audioSize == 0 {
		if res.Err == nil {
			res.Err = fmt.Errorf("no media segments recorded")
		}
		res.Status = StatusError
		ctx.Logger.Error("recording has no mergeable media",
			"username", username,
			"video_tmp", videoPath,
			"audio_tmp", audioPath,
			"video_size", videoSize,
			"audio_size", audioSize,
			"error", res.Err,
		)
		return
	}

	mergeErr := mergeMediaFiles(ctx, username, videoPath, audioPath, outputPath, videoSize, audioSize)
	if mergeErr != nil {
		res.Status = StatusError
		if res.Err != nil {
			res.Err = fmt.Errorf("%w; merge: %v", res.Err, mergeErr)
		} else {
			res.Err = fmt.Errorf("merge: %w", mergeErr)
		}
		ctx.Logger.Error("recording kept temp files after merge failure",
			"username", username,
			"video_tmp", videoPath,
			"audio_tmp", audioPath,
			"error", res.Err,
		)
		return
	}

	if fi, err := os.Stat(outputPath); err == nil {
		res.Filesize = fi.Size()
	}
	if res.Filesize == 0 {
		res.Status = StatusError
		if res.Err != nil {
			res.Err = fmt.Errorf("%w; empty output after merge", res.Err)
		} else {
			res.Err = fmt.Errorf("empty output after merge")
		}
		ctx.Logger.Error("recording kept temp files after empty merge output",
			"username", username,
			"video_tmp", videoPath,
			"audio_tmp", audioPath,
			"output", outputPath,
			"error", res.Err,
		)
		return
	}

	removeTempFile(ctx, username, "video", videoPath)
	removeTempFile(ctx, username, "audio", audioPath)

	ctx.Logger.Info("recording finalized",
		"username", username,
		"path", outputPath,
		"size", res.Filesize,
		"duration", res.Duration,
		"status", res.Status,
	)
}

func mergeTempFiles(ctx *config.AppContext, username, videoPath, audioPath, outputPath string, videoSize, audioSize int64) error {
	ctx.Logger.Info("merging video and audio",
		"username", username,
		"output", outputPath,
		"video_size", videoSize,
		"audio_size", audioSize,
	)

	mergeCmd := exec.Command("ffmpeg",
		"-hide_banner",
		"-loglevel", "warning",
		"-copyts",
		"-start_at_zero",
		"-i", videoPath,
		"-i", audioPath,
		"-map", "0:v",
		"-map", "1:a",
		"-c", "copy",
		"-f", "matroska",
		"-y",
		outputPath,
	)
	mergeCmd.Stderr = os.Stderr
	if err := mergeCmd.Run(); err != nil {
		ctx.Logger.Error("merge failed", "username", username, "error", err)
		return err
	}
	return nil
}

func removeTempFile(ctx *config.AppContext, username, track, path string) {
	if err := os.Remove(path); err != nil {
		ctx.Logger.Warn("failed to remove temp file",
			"username", username,
			"track", track,
			"path", path,
			"error", err,
		)
	}
}

// mustFileSize returns the file size in bytes, or 0 on error.
func mustFileSize(path string) int64 {
	fi, err := os.Stat(path)
	if err != nil {
		return 0
	}
	return fi.Size()
}

func fetchAndSelectVariant(reqCtx context.Context, ctx *config.AppContext, hlsSource string, cfg config.ChannelConfig) (
	videoURL, audioURL string, hasAudio bool, err error,
) {
	referer := fmt.Sprintf("https://chaturbate.com/%s/", cfg.Username)
	body, err := fetchHTTPBytes(reqCtx, ctx, referer, "master playlist", hlsSource)
	if err != nil {
		return "", "", false, err
	}

	playlist, listType, err := m3u8.DecodeFrom(bytes.NewReader(body), true)
	if err != nil {
		return "", "", false, fmt.Errorf("parse master playlist: %w", err)
	}
	if listType != m3u8.MASTER {
		return "", "", false, fmt.Errorf("expected master playlist, got %v", listType)
	}

	master := playlist.(*m3u8.MasterPlaylist)

	best := selectVariant(master, cfg.Resolution, cfg.Framerate)
	if best == nil {
		return "", "", false, fmt.Errorf("no video variant found")
	}
	videoURL, err = resolveURL(hlsSource, best.URI)
	if err != nil {
		return "", "", false, fmt.Errorf("resolve video playlist URI: %w", err)
	}

	for _, alt := range best.Alternatives {
		if alt != nil && alt.Type == "AUDIO" && alt.GroupId == best.Audio && alt.URI != "" {
			audioURL, err = resolveURL(hlsSource, alt.URI)
			if err != nil {
				return "", "", false, fmt.Errorf("resolve audio playlist URI: %w", err)
			}
			ctx.Logger.Debug("HLS variant selected",
				"username", cfg.Username,
				"resolution", best.Resolution,
				"framerate", best.FrameRate,
				"bandwidth", best.Bandwidth,
				"video", safeURL(videoURL),
				"audio", safeURL(audioURL),
			)
			return videoURL, audioURL, true, nil
		}
	}

	ctx.Logger.Debug("HLS variant selected without separate audio",
		"username", cfg.Username,
		"resolution", best.Resolution,
		"framerate", best.FrameRate,
		"bandwidth", best.Bandwidth,
		"video", safeURL(videoURL),
	)
	return videoURL, "", false, nil
}

func selectVariant(master *m3u8.MasterPlaylist, targetHeight, targetFramerate int) *m3u8.Variant {
	var best *m3u8.Variant
	var fallback *m3u8.Variant
	bestHeightDist := math.MaxInt
	bestFrameDist := math.Inf(1)
	for _, v := range master.Variants {
		if v == nil || v.URI == "" {
			continue
		}
		if fallback == nil {
			fallback = v
		}
		if targetHeight <= 0 {
			return fallback
		}
		parts := strings.Split(v.VariantParams.Resolution, "x")
		if len(parts) != 2 {
			continue
		}
		h, err := strconv.Atoi(parts[1])
		if err != nil || h <= 0 {
			continue
		}
		heightDist := h - targetHeight
		if heightDist < 0 {
			heightDist = -heightDist
		}

		frameDist := 0.0
		if targetFramerate > 0 {
			if v.VariantParams.FrameRate <= 0 {
				frameDist = math.Inf(1)
			} else {
				frameDist = math.Abs(v.VariantParams.FrameRate - float64(targetFramerate))
			}
		}

		if heightDist < bestHeightDist || heightDist == bestHeightDist && frameDist < bestFrameDist {
			bestHeightDist = heightDist
			bestFrameDist = frameDist
			best = v
		}
	}
	if best != nil {
		return best
	}
	return fallback
}

func fetchMediaPlaylist(recordingCtx context.Context, ctx *config.AppContext, referer string, ts *trackState) (*m3u8.MediaPlaylist, error) {
	body, err := downloadBytesWithRetry(recordingCtx, ctx, referer, ts.name+" playlist", ts.url)
	if err != nil {
		return nil, err
	}

	playlist, listType, err := m3u8.DecodeFrom(bytes.NewReader(body), true)
	if err != nil {
		return nil, fmt.Errorf("parse %s chunklist: %w", ts.name, err)
	}
	if listType != m3u8.MEDIA {
		return nil, fmt.Errorf("expected media playlist for %s, got %v", ts.name, listType)
	}

	media := playlist.(*m3u8.MediaPlaylist)
	ctx.Logger.Debug("media playlist parsed",
		"username", ts.username,
		"track", ts.name,
		"sequence", media.SeqNo,
		"segments", media.Count(),
		"closed", media.Closed,
		"url", safeURL(ts.url),
	)
	return media, nil
}

func writeInitMap(recordingCtx context.Context, ctx *config.AppContext, referer string, ts *trackState, initMap *m3u8.Map) error {
	if initMap == nil || initMap.URI == "" {
		return nil
	}
	initURL, err := resolveURL(ts.url, initMap.URI)
	if err != nil {
		return fmt.Errorf("%s init URI: %w", ts.name, err)
	}
	if initURL == ts.initURL {
		return nil
	}

	body, err := downloadBytesWithRetry(recordingCtx, ctx, referer, ts.name+" init", initURL)
	if err != nil {
		return fmt.Errorf("%s init segment: %w", ts.name, err)
	}
	if err := writeAll(ts.writer, body); err != nil {
		return fmt.Errorf("%s init segment write: %w", ts.name, err)
	}
	ts.size += int64(len(body))
	if err := cacheDownloadedSegment(ts, segmentCacheEntry{
		Username: ts.username,
		Track:    ts.name,
		Kind:     "init",
		URI:      initMap.URI,
		URL:      initURL,
	}, body); err != nil {
		return fmt.Errorf("%s init segment cache: %w", ts.name, err)
	}
	ctx.Logger.Debug("initialization segment recorded",
		"username", ts.username,
		"track", ts.name,
		"bytes", len(body),
		"url", safeURL(initURL),
		"changed", ts.initURL != "",
	)
	ts.initURL = initURL
	return nil
}

func alignInitialTracks(recordingCtx context.Context, ctx *config.AppContext, referer string, video, audio *trackState) error {
	if video.haveLastSeq || audio.haveLastSeq {
		return nil
	}

	videoMedia, err := fetchMediaPlaylist(recordingCtx, ctx, referer, video)
	if err != nil {
		return err
	}
	audioMedia, err := fetchMediaPlaylist(recordingCtx, ctx, referer, audio)
	if err != nil {
		return err
	}

	videoStart, audioStart, offset, ok := chooseAlignedStart(videoMedia.Segments, audioMedia.Segments)
	if !ok {
		ctx.Logger.Warn("initial track program times unavailable, falling back to playlist starts",
			"video_track", video.name,
			"audio_track", audio.name,
		)
		return nil
	}

	if offset > maxInitialAVOffset {
		ctx.Logger.Warn("large initial audio/video program-time offset",
			"offset", offset,
			"max_expected", maxInitialAVOffset,
		)
	}

	if err := writeInitMap(recordingCtx, ctx, referer, video, videoMedia.Map); err != nil {
		return err
	}
	if err := writeInitMap(recordingCtx, ctx, referer, audio, audioMedia.Map); err != nil {
		return err
	}

	ctx.Logger.Info("initial tracks aligned",
		"video_seq", videoMedia.Segments[videoStart].SeqId,
		"video_time", videoMedia.Segments[videoStart].ProgramDateTime,
		"audio_seq", audioMedia.Segments[audioStart].SeqId,
		"audio_time", audioMedia.Segments[audioStart].ProgramDateTime,
		"offset", offset,
	)

	if err := processTrackSegments(recordingCtx, ctx, referer, video, videoMedia.Segments[videoStart:]); err != nil {
		return err
	}
	return processTrackSegments(recordingCtx, ctx, referer, audio, audioMedia.Segments[audioStart:])
}

func chooseAlignedStart(videoSegments, audioSegments []*m3u8.MediaSegment) (videoIndex, audioIndex int, offset time.Duration, ok bool) {
	bestOffset := time.Duration(1<<63 - 1)
	for vi, vseg := range videoSegments {
		if vseg == nil || vseg.ProgramDateTime.IsZero() {
			continue
		}
		for ai, aseg := range audioSegments {
			if aseg == nil || aseg.ProgramDateTime.IsZero() {
				continue
			}
			delta := vseg.ProgramDateTime.Sub(aseg.ProgramDateTime)
			if delta < 0 {
				delta = -delta
			}
			if delta < bestOffset {
				bestOffset = delta
				videoIndex = vi
				audioIndex = ai
				ok = true
			}
		}
	}
	return videoIndex, audioIndex, bestOffset, ok
}

func processTrack(recordingCtx context.Context, ctx *config.AppContext, referer string, ts *trackState) error {
	media, err := fetchMediaPlaylist(recordingCtx, ctx, referer, ts)
	if err != nil {
		return err
	}
	if err := writeInitMap(recordingCtx, ctx, referer, ts, media.Map); err != nil {
		return err
	}
	return processTrackSegments(recordingCtx, ctx, referer, ts, media.Segments)
}

func processTrackSegments(recordingCtx context.Context, ctx *config.AppContext, referer string, ts *trackState, segments []*m3u8.MediaSegment) error {
	var newSegments []*m3u8.MediaSegment
	for _, seg := range segments {
		if seg == nil {
			continue
		}
		if !ts.haveLastSeq || seg.SeqId > ts.lastSeq {
			newSegments = append(newSegments, seg)
		}
	}
	if len(newSegments) == 0 {
		if ts.haveLastSeq && len(segments) != 0 {
			last := segments[len(segments)-1]
			if last != nil && last.SeqId < ts.lastSeq {
				return &requestError{err: fmt.Errorf(
					"%s media sequence moved backwards: last=%d playlist_last=%d",
					ts.name,
					ts.lastSeq,
					last.SeqId,
				)}
			}
		}
		return nil
	}
	if ts.haveLastSeq && newSegments[0].SeqId > ts.lastSeq+1 {
		return &requestError{err: fmt.Errorf(
			"%s missed segment(s): last=%d next=%d",
			ts.name,
			ts.lastSeq,
			newSegments[0].SeqId,
		)}
	}

	for _, seg := range newSegments {
		if seg.Discontinuity {
			ctx.Logger.Warn("HLS discontinuity",
				"username", ts.username,
				"track", ts.name,
				"sequence", seg.SeqId,
			)
		}
		if err := writeInitMap(recordingCtx, ctx, referer, ts, seg.Map); err != nil {
			return err
		}
		segURL, err := resolveURL(ts.url, seg.URI)
		if err != nil {
			return fmt.Errorf("%s segment seq %d URI: %w", ts.name, seg.SeqId, err)
		}
		if err := downloadSegmentWithRetry(recordingCtx, ctx, referer, ts, seg, segURL); err != nil {
			return fmt.Errorf("%s segment seq %d: %w", ts.name, seg.SeqId, err)
		}
		ts.lastSeq = seg.SeqId
		ts.haveLastSeq = true
		ts.duration += seg.Duration
	}

	return nil
}

func downloadSegmentWithRetry(recordingCtx context.Context, ctx *config.AppContext, referer string, ts *trackState, seg *m3u8.MediaSegment, segURL string) error {
	body, err := downloadBytesWithRetry(recordingCtx, ctx, referer, ts.name+" segment", segURL)
	if err != nil {
		return err
	}
	if err := writeAll(ts.writer, body); err != nil {
		return err
	}
	ts.size += int64(len(body))
	entry := segmentCacheEntry{
		Username:        ts.username,
		Track:           ts.name,
		Kind:            "segment",
		Seq:             seg.SeqId,
		URI:             seg.URI,
		URL:             segURL,
		DurationSeconds: strconv.FormatFloat(seg.Duration, 'f', 6, 64),
	}
	if !seg.ProgramDateTime.IsZero() {
		entry.ProgramDateTime = seg.ProgramDateTime.Format(time.RFC3339Nano)
	}
	if err := cacheDownloadedSegment(ts, entry, body); err != nil {
		return err
	}
	ctx.Logger.Debug("media segment recorded",
		"username", ts.username,
		"track", ts.name,
		"sequence", seg.SeqId,
		"duration", seg.Duration,
		"bytes", len(body),
		"url", safeURL(segURL),
	)
	return nil
}

func downloadBytesWithRetry(recordingCtx context.Context, ctx *config.AppContext, referer, kind, requestURL string) ([]byte, error) {
	body, err := fetchHTTPBytes(recordingCtx, ctx, referer, kind, requestURL)
	if err == nil {
		return body, nil
	}
	var requestErr *requestError
	if !errors.As(err, &requestErr) || !requestErr.retrySameURL() {
		return nil, err
	}
	select {
	case <-recordingCtx.Done():
		return nil, recordingCtx.Err()
	case <-time.After(time.Second):
	}
	retryBody, retryErr := fetchHTTPBytes(recordingCtx, ctx, referer, kind+" retry", requestURL)
	if retryErr != nil {
		return nil, fmt.Errorf("download failed after retry: %w (initial error: %v)", retryErr, err)
	}
	return retryBody, nil
}

func fetchHTTPBytes(requestCtx context.Context, ctx *config.AppContext, referer, kind, requestURL string) ([]byte, error) {
	resp, err := ctx.Resty.R().SetContext(requestCtx).SetHeader("Referer", referer).Get(requestURL)
	if err != nil {
		return nil, &requestError{err: fmt.Errorf("%s GET %s: %w", kind, safeURL(requestURL), err)}
	}
	ctx.Logger.Debug("HTTP media response",
		"kind", kind,
		"status", resp.StatusCode(),
		"bytes", len(resp.Body()),
		"content_type", resp.Header().Get("Content-Type"),
		"url", safeURL(requestURL),
	)
	if resp.StatusCode() != 200 {
		return nil, &requestError{
			err:    fmt.Errorf("%s GET %s: HTTP %d", kind, safeURL(requestURL), resp.StatusCode()),
			status: resp.StatusCode(),
		}
	}
	return resp.Body(), nil
}

func writeAll(w io.Writer, body []byte) error {
	n, err := w.Write(body)
	if err != nil {
		return err
	}
	if n != len(body) {
		return io.ErrShortWrite
	}
	return nil
}

func cacheDownloadedSegment(ts *trackState, entry segmentCacheEntry, body []byte) error {
	cacheRoot := os.Getenv("RECD_SEGMENT_CACHE_DIR")
	if cacheRoot == "" {
		return nil
	}

	username := sanitizePathPart(entry.Username)
	if username == "" {
		username = "unknown"
	}
	track := sanitizePathPart(entry.Track)
	if track == "" {
		track = "track"
	}

	trackDir := filepath.Join(cacheRoot, username, track)
	if err := os.MkdirAll(trackDir, 0755); err != nil {
		return err
	}

	hash := sha256.Sum256(body)
	entry.SHA256 = hex.EncodeToString(hash[:])
	entry.Size = len(body)
	entry.URI = safeReference(entry.URI)
	entry.URL = safeURL(entry.URL)

	filename := cacheFilename(entry)
	entry.Path = filepath.Join(trackDir, filename)
	if err := os.WriteFile(entry.Path, body, 0644); err != nil {
		return err
	}

	manifestPath := filepath.Join(cacheRoot, username, "manifest.jsonl")
	manifestFile, err := os.OpenFile(manifestPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	defer manifestFile.Close()

	encoded, err := json.Marshal(entry)
	if err != nil {
		return err
	}
	if _, err := manifestFile.Write(append(encoded, '\n')); err != nil {
		return err
	}
	return nil
}

func cacheFilename(entry segmentCacheEntry) string {
	switch entry.Kind {
	case "init":
		return fmt.Sprintf("000000000000_init_%s.m4s", entry.SHA256[:12])
	case "segment":
		return fmt.Sprintf("%012d_%s.m4s", entry.Seq, entry.SHA256[:12])
	default:
		return fmt.Sprintf("%012d_%s_%s.m4s", entry.Seq, sanitizePathPart(entry.Kind), entry.SHA256[:12])
	}
}

func sanitizePathPart(s string) string {
	var b strings.Builder
	for _, r := range s {
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '_' || r == '-' || r == '.' {
			b.WriteRune(r)
		} else {
			b.WriteByte('_')
		}
	}
	return b.String()
}

func resolveURL(base, uri string) (string, error) {
	uri = strings.TrimSpace(uri)
	if uri == "" {
		return "", fmt.Errorf("empty URI")
	}
	if uri[0] == '"' || uri[0] == '\'' || uri[len(uri)-1] == '"' || uri[len(uri)-1] == '\'' {
		return "", fmt.Errorf("URI contains an unexpected surrounding quote")
	}
	baseURL, err := url.Parse(base)
	if err != nil {
		return "", fmt.Errorf("parse base URL: %w", err)
	}
	if (baseURL.Scheme != "http" && baseURL.Scheme != "https") || baseURL.Host == "" {
		return "", fmt.Errorf("base URL must be absolute HTTP(S)")
	}
	ref, err := url.Parse(uri)
	if err != nil {
		return "", fmt.Errorf("parse URI: %w", err)
	}
	resolved := baseURL.ResolveReference(ref)
	if (resolved.Scheme != "http" && resolved.Scheme != "https") || resolved.Host == "" {
		return "", fmt.Errorf("resolved URL must be absolute HTTP(S)")
	}
	return resolved.String(), nil
}

func safeURL(value string) string {
	parsed, err := url.Parse(value)
	if err != nil {
		return "<invalid-url>"
	}
	parsed.RawQuery = ""
	parsed.ForceQuery = false
	parsed.Fragment = ""
	parsed.User = nil
	return parsed.String()
}

func safeReference(value string) string {
	parsed, err := url.Parse(value)
	if err != nil {
		return "<invalid-uri>"
	}
	parsed.RawQuery = ""
	parsed.ForceQuery = false
	parsed.Fragment = ""
	parsed.User = nil
	return parsed.String()
}
