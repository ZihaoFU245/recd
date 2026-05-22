package channel

import (
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

	maxChunklistRetries = 3

	defaultUserAgent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/148.0.0.0 Safari/537.36"
)

var (
	errChunklistExpired = fmt.Errorf("chunklist expired")
)

type trackState struct {
	url         string
	writer      io.WriteCloser
	lastSeq     uint64
	haveLastSeq bool
	wroteInit   bool
	duration    float64
	name        string
}

// record starts recording for a channel. Separate audio/video fMP4 variants are
// downloaded into temp files and merged by ffmpeg with source timestamps
// preserved. Single-stream variants are handed to ffmpeg's HLS demuxer.
func record(ctx *config.AppContext, cfg config.ChannelConfig, hlsSource string,
	stopCh <-chan struct{}, reloadCh <-chan struct{}) (res Result) {
	res.Username = cfg.Username

	t0 := time.Now()
	outputPath, err := config.ExpandPattern(cfg.Pattern, config.PathVars(cfg.Username, t0, 0))
	if err != nil {
		res.Status = StatusError
		res.Err = fmt.Errorf("pattern expansion: %w", err)
		return
	}
	outputPath += ".mkv"

	if dir := filepath.Dir(outputPath); dir != "" {
		if err := os.MkdirAll(dir, 0755); err != nil {
			res.Status = StatusError
			res.Err = fmt.Errorf("mkdir %s: %w", dir, err)
			return
		}
	}

	videoChunkURL, audioChunkURL, hasAudio, err := fetchAndSelectVariant(ctx, hlsSource, cfg)
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
	return defaultUserAgent
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
		os.Remove(videoPath)
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
		os.Remove(audioPath)
	}()

	vs := trackState{url: videoChunkURL, writer: videoFile, name: "video"}
	as := trackState{url: audioChunkURL, writer: audioFile, name: "audio"}

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

	for {
		if err := processTrack(ctx, referer, &vs); err != nil {
			if err == errChunklistExpired && chunklistRetries < maxChunklistRetries {
				chunklistRetries++
				ctx.Logger.Warn("chunklist expired, refreshing",
					"track", "video",
					"attempt", chunklistRetries,
					"max", maxChunklistRetries,
				)
				vURL, aURL, _, refreshErr := fetchAndSelectVariant(ctx, hlsSource, cfg)
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
				vURL, aURL, _, refreshErr := fetchAndSelectVariant(ctx, hlsSource, cfg)
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

	if res.Status == StatusError || res.Status == StatusDesync {
		res.Duration = time.Since(t0)
		res.Path = outputPath
		return
	}
	if mustFileSize(videoPath) == 0 || mustFileSize(audioPath) == 0 {
		res.Status = StatusError
		res.Err = fmt.Errorf("no media segments recorded")
		res.Duration = time.Since(t0)
		res.Path = outputPath
		return
	}

	ctx.Logger.Info("merging video and audio",
		"username", cfg.Username,
		"output", outputPath,
		"video_size", mustFileSize(videoPath),
		"audio_size", mustFileSize(audioPath),
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
		outputPath,
		"-y",
	)
	mergeCmd.Stderr = os.Stderr
	if err := mergeCmd.Run(); err != nil {
		ctx.Logger.Error("merge failed", "username", cfg.Username, "error", err)
		res.Status = StatusError
		res.Err = fmt.Errorf("merge: %w", err)
		return // defer will clean up temps
	}

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

// mustFileSize returns the file size in bytes, or 0 on error.
func mustFileSize(path string) int64 {
	fi, err := os.Stat(path)
	if err != nil {
		return 0
	}
	return fi.Size()
}

func fetchAndSelectVariant(ctx *config.AppContext, hlsSource string, cfg config.ChannelConfig) (
	videoURL, audioURL string, hasAudio bool, err error,
) {
	referer := fmt.Sprintf("https://chaturbate.com/%s/", cfg.Username)
	resp, err := ctx.Resty.R().SetHeader("Referer", referer).Get(hlsSource)
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
		if err := downloadToWriter(ctx, referer, initURL, ts.writer); err != nil {
			return fmt.Errorf("%s init segment: %w", ts.name, err)
		}
		ts.wroteInit = true
	}
	return nil
}

func processTrack(ctx *config.AppContext, referer string, ts *trackState) error {
	media, err := fetchMediaPlaylist(ctx, referer, ts)
	if err != nil {
		return err
	}
	if err := writeInitSegment(ctx, referer, ts, media); err != nil {
		return err
	}

	var newSegments []*m3u8.MediaSegment
	for _, seg := range media.Segments {
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
		if err := downloadSegmentWithRetry(ctx, referer, segURL, ts.writer); err != nil {
			return fmt.Errorf("%s segment seq %d: %w", ts.name, seg.SeqId, err)
		}
		ts.lastSeq = seg.SeqId
		ts.haveLastSeq = true
		ts.duration += seg.Duration
	}

	return nil
}

func downloadSegmentWithRetry(ctx *config.AppContext, referer, segURL string, w io.Writer) error {
	err := downloadToWriter(ctx, referer, segURL, w)
	if err == nil {
		return nil
	}
	time.Sleep(1 * time.Second)
	retryErr := downloadToWriter(ctx, referer, segURL, w)
	if retryErr == nil {
		return nil
	}
	return fmt.Errorf("download failed after retry: %w (initial error: %v)", retryErr, err)
}

func downloadToWriter(ctx *config.AppContext, referer, segURL string, w io.Writer) error {
	resp, err := ctx.Resty.R().SetHeader("Referer", referer).Get(segURL)
	if err != nil {
		return err
	}
	if resp.StatusCode() != 200 {
		return fmt.Errorf("HTTP %d", resp.StatusCode())
	}
	n, err := w.Write(resp.Body())
	if err != nil {
		return err
	}
	if n != len(resp.Body()) {
		return io.ErrShortWrite
	}
	return nil
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
