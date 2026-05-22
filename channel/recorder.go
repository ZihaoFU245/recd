package channel

import (
	"fmt"
	"io"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"recd/config"

	"github.com/grafov/m3u8"
)

const (
	maxDesyncSeconds    = 60.0
	segmentPollInterval = 800 * time.Millisecond

	maxChunklistRetries = 3
)

var (
	errChunklistExpired = fmt.Errorf("chunklist expired")
)

type trackState struct {
	url       string
	writer    io.WriteCloser
	lastSeq   uint64
	wroteInit bool
	duration  float64
	name      string
}

// record starts recording for a channel. For streams with muxed audio (newer
// mpegts format), video segments are piped to ffmpeg directly. For streams
// with separate audio (older fMP4 format), video and audio segments are
// accumulated in temp files and merged with ffmpeg at the end.
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
	return recordWithPipe(ctx, cfg, hlsSource, videoChunkURL, referer, outputPath, t0, stopCh, reloadCh)
}

// recordWithPipe streams video segments through a single OS pipe to ffmpeg.
// Used for the newer mpegts format where audio is already muxed in.
func recordWithPipe(ctx *config.AppContext, cfg config.ChannelConfig, hlsSource, videoChunkURL, referer, outputPath string, t0 time.Time, stopCh, reloadCh <-chan struct{}) (res Result) {
	res.Username = cfg.Username

	vRead, vWrite, err := os.Pipe()
	if err != nil {
		res.Status = StatusError
		res.Err = fmt.Errorf("create pipe: %w", err)
		return
	}
	defer vWrite.Close()
	defer vRead.Close()

	ffCmd := exec.Command("ffmpeg",
		"-i", "pipe:3",
		"-c", "copy",
		"-f", "matroska",
		outputPath,
		"-y",
	)
	ffCmd.ExtraFiles = []*os.File{vRead}

	ctx.Logger.Info("starting ffmpeg muxer",
		"username", cfg.Username,
		"output", outputPath,
	)
	if err := ffCmd.Start(); err != nil {
		res.Status = StatusError
		res.Err = fmt.Errorf("start ffmpeg: %w", err)
		return
	}

	ffDone := make(chan error, 1)
	go func() { ffDone <- ffCmd.Wait() }()

	vs := trackState{url: videoChunkURL, writer: vWrite, name: "video"}

	ctx.Logger.Info("recording loop started",
		"username", cfg.Username,
		"resolution", cfg.Resolution,
		"output", outputPath,
	)

	poll := time.NewTicker(segmentPollInterval)
	defer poll.Stop()

	chunklistRetries := 0

	for {
		if err := processTrack(ctx, referer, &vs); err != nil {
			if err == errChunklistExpired && chunklistRetries < maxChunklistRetries {
				chunklistRetries++
				ctx.Logger.Warn("chunklist expired, refreshing",
					"attempt", chunklistRetries,
					"max", maxChunklistRetries,
				)
				vURL, _, _, refreshErr := fetchAndSelectVariant(ctx, hlsSource, cfg)
				if refreshErr != nil {
					ctx.Logger.Error("failed to refresh variant URLs", "error", refreshErr)
					res.Status = StatusError
					res.Err = err
					goto finish
				}
				vs.url = vURL
				vs.wroteInit = false
				time.Sleep(time.Duration(chunklistRetries) * time.Second)
				continue
			}
			ctx.Logger.Error("video track error", "username", cfg.Username, "error", err)
			res.Status = StatusError
			res.Err = err
			goto finish
		}
		chunklistRetries = 0

		if cfg.MaxDuration > 0 && time.Since(t0).Seconds() >= float64(cfg.MaxDuration*60) {
			ctx.Logger.Info("max duration reached",
				"username", cfg.Username,
				"duration", time.Since(t0),
				"limit_minutes", cfg.MaxDuration,
			)
			res.Status = StatusMaxDuration
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
				goto finish
			}
		}

		select {
		case ffErr := <-ffDone:
			ctx.Logger.Error("ffmpeg exited unexpectedly",
				"username", cfg.Username,
				"error", ffErr,
			)
			res.Status = StatusError
			res.Err = fmt.Errorf("ffmpeg exited: %w", ffErr)
			goto finish
		default:
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
	vWrite.Close()

	ctx.Logger.Info("waiting for ffmpeg to finalize", "username", cfg.Username)
	if ffCmd.ProcessState == nil || !ffCmd.ProcessState.Exited() {
		select {
		case <-ffDone:
		case <-time.After(30 * time.Second):
			ctx.Logger.Warn("ffmpeg did not exit in time, killing", "username", cfg.Username)
			ffCmd.Process.Kill()
		}
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

	ctx.Logger.Info("merging video and audio",
		"username", cfg.Username,
		"output", outputPath,
		"video_size", mustFileSize(videoPath),
		"audio_size", mustFileSize(audioPath),
	)

	mergeCmd := exec.Command("ffmpeg",
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
	resp, err := ctx.Resty.R().Get(hlsSource)
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

func processTrack(ctx *config.AppContext, referer string, ts *trackState) error {
	resp, err := ctx.Resty.R().SetHeader("Referer", referer).Get(ts.url)
	if err != nil {
		return fmt.Errorf("fetch %s chunklist: %w", ts.name, err)
	}
	if resp.StatusCode() == 403 {
		return errChunklistExpired
	}
	if resp.StatusCode() != 200 {
		return fmt.Errorf("%s chunklist HTTP %d", ts.name, resp.StatusCode())
	}

	playlist, listType, err := m3u8.DecodeFrom(strings.NewReader(resp.String()), true)
	if err != nil {
		return fmt.Errorf("parse %s chunklist: %w", ts.name, err)
	}
	if listType != m3u8.MEDIA {
		return fmt.Errorf("expected media playlist for %s, got %v", ts.name, listType)
	}

	media := playlist.(*m3u8.MediaPlaylist)

	if !ts.wroteInit && media.Map != nil && media.Map.URI != "" {
		initURL := resolveURL(ts.url, media.Map.URI)
		if err := downloadToWriter(ctx, referer, initURL, ts.writer); err != nil {
			return fmt.Errorf("%s init segment: %w", ts.name, err)
		}
		ts.wroteInit = true
	}

	var newSegments []*m3u8.MediaSegment
	for _, seg := range media.Segments {
		if seg == nil {
			continue
		}
		if seg.SeqId > ts.lastSeq {
			newSegments = append(newSegments, seg)
		}
	}
	if len(newSegments) == 0 {
		return nil
	}

	for _, seg := range newSegments {
		segURL := resolveURL(ts.url, seg.URI)
		if err := downloadToWriter(ctx, referer, segURL, ts.writer); err != nil {
			time.Sleep(1 * time.Second)
			if err := downloadToWriter(ctx, referer, segURL, ts.writer); err != nil {
				ctx.Logger.Warn("segment download failed, skipping",
					"track", ts.name,
					"seq", seg.SeqId,
					"error", err,
				)
				continue
			}
		}
		ts.lastSeq = seg.SeqId
		ts.duration += seg.Duration
	}

	return nil
}

func downloadToWriter(ctx *config.AppContext, referer, segURL string, w io.Writer) error {
	resp, err := ctx.Resty.R().SetHeader("Referer", referer).Get(segURL)
	if err != nil {
		return err
	}
	if resp.StatusCode() != 200 {
		return fmt.Errorf("HTTP %d", resp.StatusCode())
	}
	_, err = io.Copy(w, strings.NewReader(resp.String()))
	return err
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
