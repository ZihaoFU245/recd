package channel

import (
	"fmt"
	"io"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"recd/config"

	"github.com/grafov/m3u8"
)

const (
	maxDesyncSeconds    = 60.0
	segmentPollInterval = 800 * time.Millisecond

	// maxChunklistRetries is the number of times we re-fetch the master
	// playlist to get a fresh chunklist URL before giving up.
	maxChunklistRetries = 3
)

var (
	// errChunklistExpired is returned by processTrack when the chunklist URL
	// returns 403 (expired). The caller should refresh the master playlist.
	errChunklistExpired = fmt.Errorf("chunklist expired")
)

// trackState holds the state for a single media track during recording.
type trackState struct {
	url       string
	writer    io.WriteCloser
	lastSeq   uint64
	wroteInit bool
	duration  float64
	name      string
}

// record starts a long-running ffmpeg process that reads video (+ optional audio)
// from OS pipes and writes a single MKV output file continuously.
// Segments are downloaded and fed directly into the pipes — no temp files.
func record(ctx *config.AppContext, cfg config.ChannelConfig, hlsSource string, stopCh <-chan struct{}) (res Result) {
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

	// Fetch master playlist and select the appropriate variant.
	videoChunkURL, audioChunkURL, hasAudio, err := fetchAndSelectVariant(ctx, hlsSource, cfg)
	if err != nil {
		res.Status = StatusError
		res.Err = fmt.Errorf("variant selection: %w", err)
		return
	}

	referer := fmt.Sprintf("https://chaturbate.com/%s/", cfg.Username)

	// Create OS pipes. If no separate audio track, only video pipe is used.
	vRead, vWrite, err := os.Pipe()
	if err != nil {
		res.Status = StatusError
		res.Err = fmt.Errorf("create video pipe: %w", err)
		return
	}
	defer vWrite.Close()
	defer vRead.Close()

	var aRead, aWrite *os.File
	if hasAudio {
		aRead, aWrite, err = os.Pipe()
		if err != nil {
			res.Status = StatusError
			res.Err = fmt.Errorf("create audio pipe: %w", err)
			return
		}
		defer aWrite.Close()
		defer aRead.Close()
	}

	// Build ffmpeg command. With separate audio: two inputs, otherwise one.
	var ffCmd *exec.Cmd
	if hasAudio {
		ffCmd = exec.Command("ffmpeg",
			"-i", "pipe:3",
			"-i", "pipe:4",
			"-c", "copy",
			"-f", "matroska",
			outputPath,
			"-y",
		)
		ffCmd.ExtraFiles = []*os.File{vRead, aRead}
	} else {
		ffCmd = exec.Command("ffmpeg",
			"-i", "pipe:3",
			"-c", "copy",
			"-f", "matroska",
			outputPath,
			"-y",
		)
		ffCmd.ExtraFiles = []*os.File{vRead}
	}
	ffCmd.Stderr = os.Stderr

	ctx.Logger.Info("starting ffmpeg muxer",
		"username", cfg.Username,
		"output", outputPath,
		"has_audio", hasAudio,
	)
	if err := ffCmd.Start(); err != nil {
		res.Status = StatusError
		res.Err = fmt.Errorf("start ffmpeg: %w", err)
		return
	}

	ffDone := make(chan error, 1)
	go func() { ffDone <- ffCmd.Wait() }()

	vs := trackState{url: videoChunkURL, writer: vWrite, name: "video"}
	as := trackState{url: audioChunkURL, writer: aWrite, name: "audio"}

	ctx.Logger.Info("recording loop started",
		"username", cfg.Username,
		"resolution", cfg.Resolution,
		"output", outputPath,
	)

	poll := time.NewTicker(segmentPollInterval)
	defer poll.Stop()

	// Track consecutive chunklist errors for retry backoff.
	chunklistRetries := 0

	for {
		// Process video track every cycle.
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
				if hasAudio {
					as.url = aURL
					as.wroteInit = false
				}
				time.Sleep(time.Duration(chunklistRetries) * time.Second)
				continue
			}
			ctx.Logger.Error("video track error", "username", cfg.Username, "error", err)
			res.Status = StatusError
			res.Err = err
			goto finish
		}
		chunklistRetries = 0 // reset on success

		// Process audio track only if we have a separate audio stream.
		if hasAudio {
			if err := processTrack(ctx, referer, &as); err != nil {
				if err == errChunklistExpired && chunklistRetries < maxChunklistRetries {
					chunklistRetries++
					ctx.Logger.Warn("chunklist expired, refreshing",
						"track", "audio",
						"attempt", chunklistRetries,
						"max", maxChunklistRetries,
					)
			vURL, aURL, audioExists, refreshErr := fetchAndSelectVariant(ctx, hlsSource, cfg)
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
					hasAudio = audioExists
					if !hasAudio {
						ctx.Logger.Warn("audio track lost after refresh, continuing with video only")
					}
					time.Sleep(time.Duration(chunklistRetries) * time.Second)
					continue
				}
				ctx.Logger.Error("audio track error", "username", cfg.Username, "error", err)
				res.Status = StatusError
				res.Err = err
				goto finish
			}
			chunklistRetries = 0

			// Sync check.
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
		}

		// Duration limit (max_duration is in minutes).
		if cfg.MaxDuration > 0 && time.Since(t0).Seconds() >= float64(cfg.MaxDuration*60) {
			ctx.Logger.Info("max duration reached",
				"username", cfg.Username,
				"duration", time.Since(t0),
				"limit_minutes", cfg.MaxDuration,
			)
			res.Status = StatusMaxDuration
			goto finish
		}

		// File size limit: check the growing output file.
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

		// Check if ffmpeg process died unexpectedly.
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
		case <-poll.C:
		}
	}

finish:
	vWrite.Close()
	if hasAudio {
		aWrite.Close()
	}

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

// fetchAndSelectVariant downloads the master m3u8 playlist, selects the
// video variant closest to the desired resolution, and returns chunklist URLs.
// hasAudio is false when the stream has audio muxed into the video track.
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

	// Select video variant closest to the desired resolution height.
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

	// Check for separate audio via #EXT-X-MEDIA tags.
	reAudio := regexp.MustCompile(`#EXT-X-MEDIA:[^\n]*TYPE=AUDIO[^\n]*URI="([^"]+)"`)
	audioMatch := reAudio.FindStringSubmatch(body)
	if len(audioMatch) >= 2 {
		audioURL = resolveURL(hlsSource, audioMatch[1])
		return videoURL, audioURL, true, nil
	}

	// No separate audio track; audio is muxed into the video variant.
	return videoURL, "", false, nil
}

// processTrack fetches a chunklist and writes new segments into the pipe writer.
// Returns errChunklistExpired when the chunklist URL returns 403 (expired).
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

	// Write init segment once.
	if !ts.wroteInit && media.Map != nil && media.Map.URI != "" {
		initURL := resolveURL(ts.url, media.Map.URI)
		if err := downloadToWriter(ctx, referer, initURL, ts.writer); err != nil {
			return fmt.Errorf("%s init segment: %w", ts.name, err)
		}
		ts.wroteInit = true
	}

	// Find and write new segments in sequence order.
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
			// Retry once after a short delay.
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

// downloadToWriter fetches a URL and writes the response body to w.
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

// resolveURL resolves a relative URI against a base URL using proper URL resolution.
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
