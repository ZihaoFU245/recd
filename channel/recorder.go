package channel

import (
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	ffmpeg "github.com/u2takey/ffmpeg-go"

	"recd/config"

	"github.com/grafov/m3u8"
)

// recordingTempPrefix is placed in the videos/ directory for temporary track files.
const recordingTempPrefix = ".rec_"

// maxDesyncSeconds is the maximum allowed drift between audio and video track durations.
const maxDesyncSeconds = 60.0

// segmentPollInterval is how long to wait before re-fetching the chunklist.
// LL-HLS segments are ~1.6s each, so 800ms gives roughly 2 polls per segment.
const segmentPollInterval = 800 * time.Millisecond

// trackState holds the state for a single media track (video or audio) during recording.
type trackState struct {
	url       string
	file      *os.File
	lastSeq   uint64
	wroteInit bool
	duration  float64
	name      string
}

// record is the main recording loop for a channel.
// It fetches the master playlist, selects a variant, then continuously
// downloads video + audio segments into temp files, finally merging them
// with ffmpeg into an MKV output.
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

	// Ensure parent directories exist.
	if dir := filepath.Dir(outputPath); dir != "" {
		if err := os.MkdirAll(dir, 0755); err != nil {
			res.Status = StatusError
			res.Err = fmt.Errorf("mkdir %s: %w", dir, err)
			return
		}
	}

	// Create temp files for video and audio tracks.
	videoTemp := filepath.Join(filepath.Dir(outputPath), recordingTempPrefix+"v_"+cfg.Username+".m4s")
	audioTemp := filepath.Join(filepath.Dir(outputPath), recordingTempPrefix+"a_"+cfg.Username+".m4s")

	vt, err := os.Create(videoTemp)
	if err != nil {
		res.Status = StatusError
		res.Err = fmt.Errorf("create video temp: %w", err)
		return
	}
	defer vt.Close()
	defer os.Remove(videoTemp)

	at, err := os.Create(audioTemp)
	if err != nil {
		res.Status = StatusError
		res.Err = fmt.Errorf("create audio temp: %w", err)
		return
	}
	defer at.Close()
	defer os.Remove(audioTemp)

	// Fetch and parse the master m3u8 playlist.
	edgeBase, videoChunkURL, audioChunkURL, err := fetchAndSelectVariant(ctx, hlsSource, cfg)
	if err != nil {
		res.Status = StatusError
		res.Err = fmt.Errorf("variant selection: %w", err)
		return
	}

	// Generate the referer header for this channel.
	referer := fmt.Sprintf("https://chaturbate.com/%s/", cfg.Username)

	vs := trackState{url: videoChunkURL, file: vt, name: "video"}
	as := trackState{url: audioChunkURL, file: at, name: "audio"}

	ctx.Logger.Info("recording loop started",
		"username", cfg.Username,
		"resolution", cfg.Resolution,
		"output", outputPath,
	)

	// Main polling loop.
	poll := time.NewTicker(segmentPollInterval)
	defer poll.Stop()

	for {
		// Process video track: fetch chunklist, download new segments.
		if err := processTrack(ctx, edgeBase, referer, &vs); err != nil {
			ctx.Logger.Error("video track error", "username", cfg.Username, "error", err)
			res.Status = StatusError
			res.Err = err
			return
		}

		// Process audio track.
		if err := processTrack(ctx, edgeBase, referer, &as); err != nil {
			ctx.Logger.Error("audio track error", "username", cfg.Username, "error", err)
			res.Status = StatusError
			res.Err = err
			return
		}

		// Sync check: abort if audio and video have drifted too far apart.
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
			return
		}

		// Duration limit check.
		if cfg.MaxDuration > 0 {
			recordedSec := time.Since(t0).Seconds()
			if recordedSec >= float64(cfg.MaxDuration) {
				ctx.Logger.Info("max duration reached",
					"username", cfg.Username,
					"duration", recordedSec,
					"limit", cfg.MaxDuration,
				)
				res.Status = StatusMaxDuration
				break
			}
		}

		// File size limit check.
		if cfg.MaxFilesize > 0 {
			vi, _ := vt.Stat()
			ai, _ := at.Stat()
			totalBytes := vi.Size() + ai.Size()
			if totalBytes >= cfg.MaxFilesize {
				ctx.Logger.Info("max filesize reached",
					"username", cfg.Username,
					"size", totalBytes,
					"limit", cfg.MaxFilesize,
				)
				res.Status = StatusMaxFilesize
				break
			}
		}

		select {
		case <-stopCh:
			ctx.Logger.Info("recording stopped by monitor", "username", cfg.Username)
			res.Status = StatusCompleted
			goto finalize
		case <-poll.C:
			// continue loop
		}
	}

finalize:
	// Flush and sync temp files before merging.
	vt.Sync()
	at.Sync()
	vt.Close()
	at.Close()

	// Merge video + audio temps into final MKV using ffmpeg stream copy (no re-encode).
	ctx.Logger.Info("merging tracks with ffmpeg", "username", cfg.Username, "output", outputPath)
	vStream := ffmpeg.Input(videoTemp)
	aStream := ffmpeg.Input(audioTemp)
	mergeErr := ffmpeg.Output([]*ffmpeg.Stream{vStream, aStream}, outputPath,
		ffmpeg.KwArgs{"c": "copy"}).
		OverWriteOutput().
		ErrorToStdOut().
		Run()

	if mergeErr != nil {
		ctx.Logger.Error("ffmpeg merge failed", "username", cfg.Username, "error", mergeErr)
		res.Status = StatusError
		res.Err = fmt.Errorf("ffmpeg merge: %w", mergeErr)
		return
	}

	// Populate result metadata.
	fi, err := os.Stat(outputPath)
	if err == nil {
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
// video variant closest to the desired resolution, and returns the edge
// base URL and the video + audio chunklist URLs.
func fetchAndSelectVariant(ctx *config.AppContext, hlsSource string, cfg config.ChannelConfig) (
	edgeBase, videoURL, audioURL string, err error,
) {
	resp, err := ctx.Resty.R().Get(hlsSource)
	if err != nil {
		return "", "", "", fmt.Errorf("fetch master playlist: %w", err)
	}
	if resp.StatusCode() != 200 {
		return "", "", "", fmt.Errorf("master playlist HTTP %d", resp.StatusCode())
	}

	body := resp.String()

	playlist, listType, err := m3u8.DecodeFrom(strings.NewReader(body), true)
	if err != nil {
		return "", "", "", fmt.Errorf("parse master playlist: %w", err)
	}
	if listType != m3u8.MASTER {
		return "", "", "", fmt.Errorf("expected master playlist, got %v", listType)
	}

	master := playlist.(*m3u8.MasterPlaylist)

	// Derive edge base URL (scheme + host) from the hlsSource.
	u, err := url.Parse(hlsSource)
	if err != nil {
		return "", "", "", fmt.Errorf("parse hls source URL: %w", err)
	}
	edgeBase = fmt.Sprintf("%s://%s", u.Scheme, u.Host)

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
		return "", "", "", fmt.Errorf("no video variant found")
	}
	videoURL = resolveURL(edgeBase, best.URI)

	// Extract audio URI from raw #EXT-X-MEDIA tags (not fully parsed by m3u8 lib).
	reAudio := regexp.MustCompile(`#EXT-X-MEDIA:[^\n]*TYPE=AUDIO[^\n]*URI="([^"]+)"`)
	audioMatch := reAudio.FindStringSubmatch(body)
	if len(audioMatch) >= 2 {
		audioURL = resolveURL(edgeBase, audioMatch[1])
	} else {
		// Fallback: use video URL for both — some streams embed audio in video track.
		audioURL = videoURL
	}

	return edgeBase, videoURL, audioURL, nil
}

// processTrack fetches a chunklist and downloads any new segments into the temp file.
// It tracks sequence numbers to avoid re-downloading segments already written.
func processTrack(ctx *config.AppContext, edgeBase, referer string, ts *trackState) error {
	resp, err := ctx.Resty.R().SetHeader("Referer", referer).Get(ts.url)
	if err != nil {
		return fmt.Errorf("fetch %s chunklist: %w", ts.name, err)
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

	// Download init segment if we haven't yet and a Map is present.
	if !ts.wroteInit && media.Map != nil && media.Map.URI != "" {
		initURL := resolveURL(edgeBase, media.Map.URI)
		if err := downloadAppend(ctx, referer, initURL, ts.file); err != nil {
			return fmt.Errorf("%s init segment: %w", ts.name, err)
		}
		ts.wroteInit = true
	}

	// Find and download new segments (SeqId > lastSeq).
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

	// Download in sequence order; each segment is appended immediately.
	for _, seg := range newSegments {
		segURL := resolveURL(edgeBase, seg.URI)
		if err := downloadAppend(ctx, referer, segURL, ts.file); err != nil {
			ctx.Logger.Warn("segment download failed, skipping",
				"track", ts.name,
				"seq", seg.SeqId,
				"uri", seg.URI,
				"error", err,
			)
			continue
		}
		ts.lastSeq = seg.SeqId
		ts.duration += seg.Duration
	}

	return nil
}

// downloadAppend fetches a URL and appends the response body to the given file.
func downloadAppend(ctx *config.AppContext, referer, segURL string, f *os.File) error {
	resp, err := ctx.Resty.R().SetHeader("Referer", referer).Get(segURL)
	if err != nil {
		return err
	}
	if resp.StatusCode() != 200 {
		return fmt.Errorf("HTTP %d", resp.StatusCode())
	}
	_, err = io.Copy(f, strings.NewReader(resp.String()))
	return err
}

// resolveURL resolves a potentially relative URI against the edge base URL.
func resolveURL(base, uri string) string {
	if strings.HasPrefix(uri, "http") {
		return uri
	}
	if strings.HasPrefix(uri, "/") {
		return base + uri
	}
	return base + "/" + uri
}
