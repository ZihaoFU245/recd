package channel

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
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

	maxChunklistRetries = 3
	maxOutputPathTries  = 10000
)

var (
	errChunklistExpired = fmt.Errorf("chunklist expired")
	mergeMediaFiles     = mergeTempFiles
)

type trackState struct {
	url         string
	writer      io.WriteCloser
	lastSeq     uint64
	haveLastSeq bool
	wroteInit   bool
	duration    float64
	firstPDT    time.Time
	lastPDT     time.Time
	username    string
	name        string
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

type recorderSignal struct {
	status   Status
	reloaded bool
}

// record starts recording for a channel. Separate audio/video fMP4 variants are
// downloaded into temp files and merged by ffmpeg with source timestamps
// preserved. Single-stream variants are handed to ffmpeg's HLS demuxer.
func record(ctx *config.AppContext, cfg config.ChannelConfig, hlsSource string,
	stopCh <-chan struct{}, reloadCh <-chan struct{}) (res Result) {
	res.Username = cfg.Username

	t0 := time.Now()
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

	reqCtx, finishInitialRequest := watchRecorderSignals(stopCh, reloadCh)
	videoChunkURL, audioChunkURL, hasAudio, err := fetchAndSelectVariant(reqCtx, ctx, hlsSource, cfg)
	if sig, ok := finishInitialRequest(); ok {
		res.Status = sig.status
		res.Reloaded = sig.reloaded
		res.Duration = time.Since(t0)
		res.Path = outputPath
		return
	}
	if err != nil {
		res.Status = StatusError
		res.Err = fmt.Errorf("variant selection: %w", err)
		return
	}

	referer := fmt.Sprintf("https://chaturbate.com/%s/", cfg.Username)

	if hasAudio {
		return recordWithTempFiles(ctx, cfg, hlsSource, videoChunkURL, audioChunkURL, referer, outputPath, t0, stopCh, reloadCh)
	}
	return recordWithFFmpegHLS(ctx, cfg, videoChunkURL, audioChunkURL, hasAudio, referer, outputPath, t0, stopCh, reloadCh)
}

func nextOutputPath(pattern, username string, start time.Time) (string, error) {
	for seq := 0; seq < maxOutputPathTries; seq++ {
		path, err := config.ExpandPattern(pattern, config.PathVars(username, start, seq))
		if err != nil {
			return "", fmt.Errorf("pattern expansion: %w", err)
		}
		path += ".mkv"

		if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
			return path, nil
		} else if err != nil {
			return "", fmt.Errorf("stat output path %s: %w", path, err)
		}
	}
	return "", fmt.Errorf("no available output path after %d attempts", maxOutputPathTries)
}

func watchRecorderSignals(stopCh, reloadCh <-chan struct{}) (context.Context, func() (recorderSignal, bool)) {
	reqCtx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	sigCh := make(chan recorderSignal, 1)

	go func() {
		select {
		case <-stopCh:
			sigCh <- recorderSignal{status: StatusCompleted}
			cancel()
		case <-reloadCh:
			sigCh <- recorderSignal{status: StatusCompleted, reloaded: true}
			cancel()
		case <-done:
		}
	}()

	return reqCtx, func() (recorderSignal, bool) {
		close(done)
		cancel()
		select {
		case sig := <-sigCh:
			return sig, true
		default:
			return recorderSignal{}, false
		}
	}
}

func recordWithFFmpegHLS(ctx *config.AppContext, cfg config.ChannelConfig, videoChunkURL, audioChunkURL string, hasAudio bool, referer, outputPath string, t0 time.Time, stopCh, reloadCh <-chan struct{}) (res Result) {
	res.Username = cfg.Username

	args := []string{"-hide_banner", "-loglevel", "warning"}
	appendInput := func(inputURL string) {
		args = append(args,
			"-user_agent", ffmpegUserAgent(ctx),
			"-headers", ffmpegHeaders(ctx, referer),
			"-i", inputURL,
		)
	}
	appendInput(videoChunkURL)
	if hasAudio {
		appendInput(audioChunkURL)
		args = append(args,
			"-map", "0:v:0",
			"-map", "1:a:0",
		)
	}
	args = append(args, "-c", "copy")
	if cfg.MaxFilesize > 0 {
		args = append(args, "-fs", strconv.FormatInt(cfg.MaxFilesize, 10))
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
		"separate_audio", hasAudio,
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
		case <-time.After(30 * time.Second):
			ctx.Logger.Warn("ffmpeg did not exit after quit, interrupting", "username", cfg.Username)
			_ = ffCmd.Process.Signal(syscall.SIGINT)
			select {
			case <-ffDone:
			case <-time.After(10 * time.Second):
				ctx.Logger.Warn("ffmpeg did not exit after interrupt, killing", "username", cfg.Username)
				_ = ffCmd.Process.Kill()
				<-ffDone
			}
		}
	}

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case err := <-ffDone:
			if err != nil {
				res.Status = StatusError
				res.Err = fmt.Errorf("ffmpeg exited: %w", err)
			} else if cfg.MaxDuration > 0 && time.Since(t0).Seconds() >= float64(cfg.MaxDuration*60) {
				res.Status = StatusMaxDuration
			} else if cfg.MaxFilesize > 0 {
				if fi, statErr := os.Stat(outputPath); statErr == nil && fi.Size() >= cfg.MaxFilesize {
					res.Status = StatusMaxFilesize
				}
			}
			goto finish

		case <-stopCh:
			ctx.Logger.Info("recording stopped by monitor", "username", cfg.Username)
			res.Status = StatusCompleted
			stopFFmpeg()
			goto finish

		case <-reloadCh:
			ctx.Logger.Info("recording reload triggered", "username", cfg.Username)
			res.Status = StatusCompleted
			res.Reloaded = true
			stopFFmpeg()
			goto finish

		case <-ticker.C:
			if cfg.MaxDuration > 0 && time.Since(t0).Seconds() >= float64(cfg.MaxDuration*60) {
				ctx.Logger.Info("max duration reached",
					"username", cfg.Username,
					"duration", time.Since(t0),
					"limit_minutes", cfg.MaxDuration,
				)
				res.Status = StatusMaxDuration
				stopFFmpeg()
				goto finish
			}
			if cfg.MaxFilesize > 0 {
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

// recordWithTempFiles downloads video and audio fMP4 segments into separate
// temp files, then merges them into a single MKV at the end. Used for the
// older format where audio is a separate variant.
func recordWithTempFiles(ctx *config.AppContext, cfg config.ChannelConfig, hlsSource, videoChunkURL, audioChunkURL, referer, outputPath string, t0 time.Time, stopCh, reloadCh <-chan struct{}) (res Result) {
	res.Username = cfg.Username

	videoFile, err := os.CreateTemp("", "rec_video_*.bin")
	if err != nil {
		res.Status = StatusError
		res.Err = fmt.Errorf("create video temp file: %w", err)
		return
	}
	videoPath := videoFile.Name()
	defer func() {
		videoFile.Close()
	}()

	audioFile, err := os.CreateTemp("", "rec_audio_*.bin")
	if err != nil {
		res.Status = StatusError
		res.Err = fmt.Errorf("create audio temp file: %w", err)
		return
	}
	audioPath := audioFile.Name()
	defer func() {
		audioFile.Close()
	}()

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

	chunklistRetries := 0

	if err := alignInitialTracks(ctx, referer, &vs, &as); err != nil {
		ctx.Logger.Error("initial track alignment failed", "username", cfg.Username, "error", err)
		res.Status = StatusError
		res.Err = err
		goto finish
	}

	for {
		if err := processTrack(ctx, referer, &vs); err != nil {
			if err == errChunklistExpired && chunklistRetries < maxChunklistRetries {
				chunklistRetries++
				ctx.Logger.Warn("chunklist expired, refreshing",
					"track", "video",
					"attempt", chunklistRetries,
					"max", maxChunklistRetries,
				)
				vURL, aURL, _, refreshErr := fetchAndSelectVariant(context.Background(), ctx, hlsSource, cfg)
				if refreshErr != nil {
					ctx.Logger.Error("failed to refresh variant URLs", "error", refreshErr)
					res.Status = StatusError
					res.Err = err
					goto finish
				}
				vs.url = vURL
				vs.wroteInit = false
				as.url = aURL
				as.wroteInit = false
				time.Sleep(time.Duration(chunklistRetries) * time.Second)
				continue
			}
			ctx.Logger.Error("video track error", "username", cfg.Username, "error", err)
			res.Status = StatusError
			res.Err = err
			goto finish
		}
		chunklistRetries = 0

		if err := processTrack(ctx, referer, &as); err != nil {
			if err == errChunklistExpired && chunklistRetries < maxChunklistRetries {
				chunklistRetries++
				ctx.Logger.Warn("chunklist expired, refreshing",
					"track", "audio",
					"attempt", chunklistRetries,
					"max", maxChunklistRetries,
				)
				vURL, aURL, _, refreshErr := fetchAndSelectVariant(context.Background(), ctx, hlsSource, cfg)
				if refreshErr != nil {
					ctx.Logger.Error("failed to refresh variant URLs", "error", refreshErr)
					res.Status = StatusError
					res.Err = err
					goto finish
				}
				vs.url = vURL
				vs.wroteInit = false
				as.url = aURL
				as.wroteInit = false
				time.Sleep(time.Duration(chunklistRetries) * time.Second)
				continue
			}
			ctx.Logger.Error("audio track error", "username", cfg.Username, "error", err)
			res.Status = StatusError
			res.Err = err
			goto finish
		}
		chunklistRetries = 0

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

		if cfg.MaxDuration > 0 && time.Since(t0).Seconds() >= float64(cfg.MaxDuration*60) {
			ctx.Logger.Info("max duration reached",
				"username", cfg.Username,
				"duration", time.Since(t0),
				"limit_minutes", cfg.MaxDuration,
			)
			res.Status = StatusMaxDuration
			goto finish
		}

		select {
		case <-stopCh:
			ctx.Logger.Info("recording stopped by monitor", "username", cfg.Username)
			res.Status = StatusCompleted
			goto finish
		case <-reloadCh:
			ctx.Logger.Info("recording reload triggered", "username", cfg.Username)
			res.Status = StatusCompleted
			res.Reloaded = true
			goto finish
		case <-poll.C:
		}
	}

finish:
	videoFile.Close()
	audioFile.Close()

	finalizeTempRecording(ctx, cfg.Username, videoPath, audioPath, outputPath, t0, &res)
	return
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
	if reqCtx == nil {
		reqCtx = context.Background()
	}
	referer := fmt.Sprintf("https://chaturbate.com/%s/", cfg.Username)
	resp, err := ctx.Resty.R().SetContext(reqCtx).SetHeader("Referer", referer).Get(hlsSource)
	if err != nil {
		return "", "", false, fmt.Errorf("fetch master playlist: %w", err)
	}
	if resp.StatusCode() != 200 {
		return "", "", false, fmt.Errorf("master playlist HTTP %d", resp.StatusCode())
	}

	body := resp.String()

	playlist, listType, err := m3u8.DecodeFrom(strings.NewReader(body), true)
	if err != nil {
		return "", "", false, fmt.Errorf("parse master playlist: %w", err)
	}
	if listType != m3u8.MASTER {
		return "", "", false, fmt.Errorf("expected master playlist, got %v", listType)
	}

	master := playlist.(*m3u8.MasterPlaylist)

	targetHeight := cfg.Resolution
	var best *m3u8.Variant
	bestDist := 99999
	for _, v := range master.Variants {
		if v == nil || v.URI == "" {
			continue
		}
		parts := strings.Split(v.VariantParams.Resolution, "x")
		if len(parts) != 2 {
			continue
		}
		h, _ := strconv.Atoi(parts[1])
		dist := h - targetHeight
		if dist < 0 {
			dist = -dist
		}
		if dist < bestDist {
			bestDist = dist
			best = v
		}
	}
	if best == nil {
		return "", "", false, fmt.Errorf("no video variant found")
	}
	videoURL = resolveURL(hlsSource, best.URI)

	for _, alt := range best.Alternatives {
		if alt != nil && alt.Type == "AUDIO" && alt.GroupId == best.Audio && alt.URI != "" {
			audioURL = resolveURL(hlsSource, alt.URI)
			return videoURL, audioURL, true, nil
		}
	}

	return videoURL, "", false, nil
}

func fetchMediaPlaylist(ctx *config.AppContext, referer string, ts *trackState) (*m3u8.MediaPlaylist, error) {
	resp, err := ctx.Resty.R().SetHeader("Referer", referer).Get(ts.url)
	if err != nil {
		return nil, fmt.Errorf("fetch %s chunklist: %w", ts.name, err)
	}
	if resp.StatusCode() == 403 {
		return nil, errChunklistExpired
	}
	if resp.StatusCode() != 200 {
		return nil, fmt.Errorf("%s chunklist HTTP %d", ts.name, resp.StatusCode())
	}

	playlist, listType, err := m3u8.DecodeFrom(strings.NewReader(resp.String()), true)
	if err != nil {
		return nil, fmt.Errorf("parse %s chunklist: %w", ts.name, err)
	}
	if listType != m3u8.MEDIA {
		return nil, fmt.Errorf("expected media playlist for %s, got %v", ts.name, listType)
	}

	return playlist.(*m3u8.MediaPlaylist), nil
}

func writeInitSegment(ctx *config.AppContext, referer string, ts *trackState, media *m3u8.MediaPlaylist) error {
	if !ts.wroteInit && media.Map != nil && media.Map.URI != "" {
		initURL := resolveURL(ts.url, media.Map.URI)
		body, err := downloadBytes(ctx, referer, initURL)
		if err != nil {
			return fmt.Errorf("%s init segment: %w", ts.name, err)
		}
		if err := writeAll(ts.writer, body); err != nil {
			return fmt.Errorf("%s init segment write: %w", ts.name, err)
		}
		if err := cacheDownloadedSegment(ts, segmentCacheEntry{
			Username: ts.username,
			Track:    ts.name,
			Kind:     "init",
			URI:      media.Map.URI,
			URL:      initURL,
		}, body); err != nil {
			return fmt.Errorf("%s init segment cache: %w", ts.name, err)
		}
		ts.wroteInit = true
	}
	return nil
}

func alignInitialTracks(ctx *config.AppContext, referer string, video, audio *trackState) error {
	if video.haveLastSeq || audio.haveLastSeq {
		return nil
	}

	videoMedia, err := fetchMediaPlaylist(ctx, referer, video)
	if err != nil {
		return err
	}
	audioMedia, err := fetchMediaPlaylist(ctx, referer, audio)
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

	if err := writeInitSegment(ctx, referer, video, videoMedia); err != nil {
		return err
	}
	if err := writeInitSegment(ctx, referer, audio, audioMedia); err != nil {
		return err
	}

	ctx.Logger.Info("initial tracks aligned",
		"video_seq", videoMedia.Segments[videoStart].SeqId,
		"video_time", videoMedia.Segments[videoStart].ProgramDateTime,
		"audio_seq", audioMedia.Segments[audioStart].SeqId,
		"audio_time", audioMedia.Segments[audioStart].ProgramDateTime,
		"offset", offset,
	)

	if err := processTrackSegments(ctx, referer, video, videoMedia.Segments[videoStart:]); err != nil {
		return err
	}
	return processTrackSegments(ctx, referer, audio, audioMedia.Segments[audioStart:])
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

func processTrack(ctx *config.AppContext, referer string, ts *trackState) error {
	media, err := fetchMediaPlaylist(ctx, referer, ts)
	if err != nil {
		return err
	}
	if err := writeInitSegment(ctx, referer, ts, media); err != nil {
		return err
	}
	return processTrackSegments(ctx, referer, ts, media.Segments)
}

func processTrackSegments(ctx *config.AppContext, referer string, ts *trackState, segments []*m3u8.MediaSegment) error {
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
		return nil
	}
	if ts.haveLastSeq && newSegments[0].SeqId > ts.lastSeq+1 {
		return fmt.Errorf("%s missed segment(s): last=%d next=%d", ts.name, ts.lastSeq, newSegments[0].SeqId)
	}

	for _, seg := range newSegments {
		segURL := resolveURL(ts.url, seg.URI)
		if err := downloadSegmentWithRetry(ctx, referer, ts, seg, segURL); err != nil {
			return fmt.Errorf("%s segment seq %d: %w", ts.name, seg.SeqId, err)
		}
		ts.lastSeq = seg.SeqId
		ts.haveLastSeq = true
		ts.duration += seg.Duration
		if !seg.ProgramDateTime.IsZero() {
			if ts.firstPDT.IsZero() {
				ts.firstPDT = seg.ProgramDateTime
			}
			ts.lastPDT = seg.ProgramDateTime
		}
	}

	return nil
}

func downloadSegmentWithRetry(ctx *config.AppContext, referer string, ts *trackState, seg *m3u8.MediaSegment, segURL string) error {
	err := downloadSegment(ctx, referer, ts, seg, segURL)
	if err == nil {
		return nil
	}
	time.Sleep(1 * time.Second)
	retryErr := downloadSegment(ctx, referer, ts, seg, segURL)
	if retryErr == nil {
		return nil
	}
	return fmt.Errorf("download failed after retry: %w (initial error: %v)", retryErr, err)
}

func downloadSegment(ctx *config.AppContext, referer string, ts *trackState, seg *m3u8.MediaSegment, segURL string) error {
	body, err := downloadBytes(ctx, referer, segURL)
	if err != nil {
		return err
	}
	if err := writeAll(ts.writer, body); err != nil {
		return err
	}
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
	return cacheDownloadedSegment(ts, entry, body)
}

func downloadBytes(ctx *config.AppContext, referer, segURL string) ([]byte, error) {
	resp, err := ctx.Resty.R().SetHeader("Referer", referer).Get(segURL)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode() != 200 {
		return nil, fmt.Errorf("HTTP %d", resp.StatusCode())
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

func resolveURL(base, uri string) string {
	if strings.HasPrefix(uri, "http") {
		return uri
	}
	baseURL, err := url.Parse(base)
	if err != nil {
		return base + "/" + uri
	}
	ref, err := url.Parse(uri)
	if err != nil {
		return base + "/" + uri
	}
	return baseURL.ResolveReference(ref).String()
}
