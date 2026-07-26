package recorder

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"recd/config"

	"github.com/grafov/m3u8"
)

const (
	maxResponseBodyBytes = 16 << 20
	retryWait            = time.Second
)

// requestError tells the monitor to obtain a fresh room session quickly.
// retrySameURL is limited to failures that can safely reuse a signed media URL.
type requestError struct {
	err          error
	retrySameURL bool
}

func (e *requestError) Error() string { return e.err.Error() }
func (e *requestError) Unwrap() error { return e.err }

type trackWorker struct {
	app      *config.AppContext
	username string
	name     string
	url      string
	referer  string
	writer   io.Writer

	bodyBuf bytes.Buffer

	lastSeq        uint64
	haveLastSeq    bool
	initURL        string
	duration       float64
	size           int64
	mediaEnd       time.Time
	targetDuration float64
}

type trackProgress struct {
	name           string
	duration       float64
	size           int64
	mediaEnd       time.Time
	targetDuration float64
}

type trackEvent struct {
	progress *trackProgress
	err      error
	done     bool
}

func fetchAndSelectStreams(ctx context.Context, app *config.AppContext, hlsSource string, cfg config.ChannelConfig) (string, string, error) {
	referer := fmt.Sprintf("https://chaturbate.com/%s/", cfg.Username)
	body, err := fetchHTTPBytes(ctx, app, referer, "master playlist", hlsSource)
	if err != nil {
		return "", "", err
	}

	playlist, listType, err := m3u8.DecodeFrom(bytes.NewReader(body), true)
	if err != nil {
		return "", "", fmt.Errorf("parse master playlist: %w", err)
	}
	if listType != m3u8.MASTER {
		return "", "", fmt.Errorf("expected master playlist, got %v", listType)
	}

	best := selectVariant(playlist.(*m3u8.MasterPlaylist), cfg.Resolution, cfg.Framerate)
	if best == nil {
		return "", "", fmt.Errorf("no video variant found")
	}
	videoURL, err := resolveURL(hlsSource, best.URI)
	if err != nil {
		return "", "", fmt.Errorf("resolve video playlist URI: %w", err)
	}

	for _, alt := range best.Alternatives {
		if alt == nil || alt.Type != "AUDIO" || alt.GroupId != best.Audio || alt.URI == "" {
			continue
		}
		audioURL, err := resolveURL(hlsSource, alt.URI)
		if err != nil {
			return "", "", fmt.Errorf("resolve audio playlist URI: %w", err)
		}
		app.Logger.Debug("HLS streams selected",
			"username", cfg.Username,
			"resolution", best.Resolution,
			"framerate", best.FrameRate,
			"bandwidth", best.Bandwidth,
			"video", safeURL(videoURL),
			"audio", safeURL(audioURL),
		)
		return videoURL, audioURL, nil
	}

	app.Logger.Debug("HLS stream selected without separate audio",
		"username", cfg.Username,
		"resolution", best.Resolution,
		"framerate", best.FrameRate,
		"bandwidth", best.Bandwidth,
		"video", safeURL(videoURL),
	)
	return videoURL, "", nil
}

func selectVariant(master *m3u8.MasterPlaylist, targetHeight, targetFramerate int) *m3u8.Variant {
	var best *m3u8.Variant
	var fallback *m3u8.Variant
	bestHeightDist := math.MaxInt
	bestFrameDist := math.Inf(1)
	for _, variant := range master.Variants {
		if variant == nil || variant.URI == "" {
			continue
		}
		if fallback == nil {
			fallback = variant
		}
		if targetHeight <= 0 {
			return fallback
		}
		parts := strings.Split(variant.Resolution, "x")
		if len(parts) != 2 {
			continue
		}
		height, err := strconv.Atoi(parts[1])
		if err != nil || height <= 0 {
			continue
		}
		heightDist := absInt(height - targetHeight)
		frameDist := 0.0
		if targetFramerate > 0 {
			if variant.FrameRate <= 0 {
				frameDist = math.Inf(1)
			} else {
				frameDist = math.Abs(variant.FrameRate - float64(targetFramerate))
			}
		}
		if heightDist < bestHeightDist || heightDist == bestHeightDist && frameDist < bestFrameDist {
			best = variant
			bestHeightDist = heightDist
			bestFrameDist = frameDist
		}
	}
	if best != nil {
		return best
	}
	return fallback
}

func absInt(value int) int {
	if value < 0 {
		return -value
	}
	return value
}

func (w *trackWorker) fetchPlaylist(ctx context.Context) (*m3u8.MediaPlaylist, error) {
	body, err := w.downloadWithRetry(ctx, w.name+" playlist", w.url)
	if err != nil {
		return nil, err
	}
	playlist, listType, err := m3u8.DecodeFrom(bytes.NewReader(body), true)
	if err != nil {
		return nil, fmt.Errorf("parse %s playlist: %w", w.name, err)
	}
	if listType != m3u8.MEDIA {
		return nil, fmt.Errorf("expected media playlist for %s, got %v", w.name, listType)
	}
	media := playlist.(*m3u8.MediaPlaylist)
	w.targetDuration = media.TargetDuration
	w.app.Logger.Debug("media playlist parsed",
		"username", w.username,
		"track", w.name,
		"sequence", media.SeqNo,
		"segments", media.Count(),
		"closed", media.Closed,
		"url", safeURL(w.url),
	)
	return media, nil
}

func (w *trackWorker) run(ctx context.Context, initial *m3u8.MediaPlaylist, start int, events chan<- trackEvent, pollInterval time.Duration) {
	err := w.processPlaylist(ctx, initial, start)
	if err == nil {
		err = w.sendProgress(ctx, events)
	}
	for err == nil {
		timer := time.NewTimer(pollInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			err = ctx.Err()
		case <-timer.C:
			var media *m3u8.MediaPlaylist
			media, err = w.fetchPlaylist(ctx)
			if err == nil {
				err = w.processPlaylist(ctx, media, 0)
			}
			if err == nil {
				err = w.sendProgress(ctx, events)
			}
		}
	}

	events <- trackEvent{err: err, done: true}
}

func (w *trackWorker) sendProgress(ctx context.Context, events chan<- trackEvent) error {
	progress := &trackProgress{
		name:           w.name,
		duration:       w.duration,
		size:           w.size,
		mediaEnd:       w.mediaEnd,
		targetDuration: w.targetDuration,
	}
	select {
	case events <- trackEvent{progress: progress}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (w *trackWorker) processPlaylist(ctx context.Context, media *m3u8.MediaPlaylist, start int) error {
	w.targetDuration = media.TargetDuration
	if err := w.writeInitMap(ctx, media.Map); err != nil {
		return err
	}
	if start < 0 || start > len(media.Segments) {
		return fmt.Errorf("%s invalid playlist start index %d", w.name, start)
	}
	return w.processSegments(ctx, media.Segments[start:])
}

func (w *trackWorker) processSegments(ctx context.Context, segments []*m3u8.MediaSegment) error {
	var newSegments []*m3u8.MediaSegment
	for _, segment := range segments {
		if segment != nil && (!w.haveLastSeq || segment.SeqId > w.lastSeq) {
			newSegments = append(newSegments, segment)
		}
	}
	if len(newSegments) == 0 {
		if w.haveLastSeq {
			// Decoded playlists have a capacity-sized slice with a nil tail, so
			// scan backward instead of inspecting segments[len(segments)-1].
			for index := len(segments) - 1; index >= 0; index-- {
				if last := segments[index]; last != nil {
					if last.SeqId < w.lastSeq {
						return &requestError{err: fmt.Errorf(
							"%s media sequence moved backwards: last=%d playlist_last=%d",
							w.name,
							w.lastSeq,
							last.SeqId,
						)}
					}
					break
				}
			}
		}
		return nil
	}
	if w.haveLastSeq && newSegments[0].SeqId > w.lastSeq+1 {
		return &requestError{err: fmt.Errorf(
			"%s missed segment(s): last=%d next=%d",
			w.name,
			w.lastSeq,
			newSegments[0].SeqId,
		)}
	}

	for _, segment := range newSegments {
		if segment.Discontinuity {
			w.app.Logger.Warn("HLS discontinuity",
				"username", w.username,
				"track", w.name,
				"sequence", segment.SeqId,
			)
		}
		if err := w.writeInitMap(ctx, segment.Map); err != nil {
			return err
		}
		segmentURL, err := resolveURL(w.url, segment.URI)
		if err != nil {
			return fmt.Errorf("%s segment seq %d URI: %w", w.name, segment.SeqId, err)
		}
		body, err := w.downloadWithRetry(ctx, w.name+" segment", segmentURL)
		if err != nil {
			return fmt.Errorf("%s segment seq %d: %w", w.name, segment.SeqId, err)
		}
		if err := writeAll(w.writer, body); err != nil {
			return fmt.Errorf("%s segment seq %d write: %w", w.name, segment.SeqId, err)
		}
		w.size += int64(len(body))
		w.lastSeq = segment.SeqId
		w.haveLastSeq = true
		w.duration += segment.Duration
		if !segment.ProgramDateTime.IsZero() {
			w.mediaEnd = segment.ProgramDateTime.Add(durationFromSeconds(segment.Duration))
		} else if !w.mediaEnd.IsZero() {
			w.mediaEnd = w.mediaEnd.Add(durationFromSeconds(segment.Duration))
		}
		w.app.Logger.Debug("media segment recorded",
			"username", w.username,
			"track", w.name,
			"sequence", segment.SeqId,
			"duration", segment.Duration,
			"bytes", len(body),
			"url", safeURL(segmentURL),
		)
	}
	return nil
}

func (w *trackWorker) writeInitMap(ctx context.Context, initMap *m3u8.Map) error {
	if initMap == nil || initMap.URI == "" {
		return nil
	}
	initURL, err := resolveURL(w.url, initMap.URI)
	if err != nil {
		return fmt.Errorf("%s init URI: %w", w.name, err)
	}
	if initURL == w.initURL {
		return nil
	}
	body, err := w.downloadWithRetry(ctx, w.name+" init", initURL)
	if err != nil {
		return fmt.Errorf("%s init segment: %w", w.name, err)
	}
	if err := writeAll(w.writer, body); err != nil {
		return fmt.Errorf("%s init segment write: %w", w.name, err)
	}
	w.size += int64(len(body))
	w.initURL = initURL
	w.app.Logger.Debug("initialization segment recorded",
		"username", w.username,
		"track", w.name,
		"bytes", len(body),
		"url", safeURL(initURL),
	)
	return nil
}

func (w *trackWorker) downloadWithRetry(ctx context.Context, kind, requestURL string) ([]byte, error) {
	body, err := w.fetchBody(ctx, kind, requestURL)
	if err == nil {
		return body, nil
	}
	var requestErr *requestError
	if !errors.As(err, &requestErr) || !requestErr.retrySameURL {
		return nil, err
	}
	timer := time.NewTimer(retryWait)
	select {
	case <-ctx.Done():
		timer.Stop()
		return nil, ctx.Err()
	case <-timer.C:
	}
	body, retryErr := w.fetchBody(ctx, kind+" retry", requestURL)
	if retryErr != nil {
		return nil, fmt.Errorf("download failed after retry: %w (initial error: %v)", retryErr, err)
	}
	return body, nil
}

func (w *trackWorker) fetchBody(ctx context.Context, kind, requestURL string) ([]byte, error) {
	w.bodyBuf.Reset()
	response, err := w.app.Resty.R().
		SetContext(ctx).
		SetHeader("Referer", w.referer).
		SetDoNotParseResponse(true).
		Get(requestURL)
	if err != nil {
		if response != nil && response.RawBody() != nil {
			_ = response.RawBody().Close()
		}
		return nil, &requestError{
			err:          fmt.Errorf("%s GET %s: %w", kind, safeURL(requestURL), err),
			retrySameURL: true,
		}
	}
	body := response.RawBody()
	if body == nil {
		return nil, fmt.Errorf("%s GET %s: empty response body", kind, safeURL(requestURL))
	}
	defer body.Close()

	status := response.StatusCode()
	if status != http.StatusOK {
		_, _ = io.Copy(io.Discard, io.LimitReader(body, 64<<10))
		return nil, &requestError{
			err: fmt.Errorf("%s GET %s: HTTP %d", kind, safeURL(requestURL), status),
			retrySameURL: status == http.StatusRequestTimeout ||
				status == http.StatusTooManyRequests ||
				status >= 500,
		}
	}
	if length := response.RawResponse.ContentLength; length > maxResponseBodyBytes {
		return nil, fmt.Errorf("%s GET %s: response body is %d bytes, limit is %d",
			kind, safeURL(requestURL), length, maxResponseBodyBytes)
	} else if length > 0 {
		w.bodyBuf.Grow(int(length))
	}

	limited := io.LimitReader(body, maxResponseBodyBytes+1)
	size, err := w.bodyBuf.ReadFrom(limited)
	if err != nil {
		return nil, &requestError{
			err:          fmt.Errorf("%s GET %s: read body: %w", kind, safeURL(requestURL), err),
			retrySameURL: true,
		}
	}
	if size > maxResponseBodyBytes {
		w.bodyBuf.Reset()
		return nil, fmt.Errorf("%s GET %s: response body exceeds %d bytes",
			kind, safeURL(requestURL), maxResponseBodyBytes)
	}

	w.app.Logger.Debug("HTTP media response",
		"kind", kind,
		"status", status,
		"bytes", size,
		"content_type", response.Header().Get("Content-Type"),
		"url", safeURL(requestURL),
	)
	return w.bodyBuf.Bytes(), nil
}

func fetchHTTPBytes(ctx context.Context, app *config.AppContext, referer, kind, requestURL string) ([]byte, error) {
	response, err := app.Resty.R().SetContext(ctx).SetHeader("Referer", referer).Get(requestURL)
	if err != nil {
		return nil, &requestError{
			err:          fmt.Errorf("%s GET %s: %w", kind, safeURL(requestURL), err),
			retrySameURL: true,
		}
	}
	if response.StatusCode() != http.StatusOK {
		status := response.StatusCode()
		return nil, &requestError{
			err: fmt.Errorf("%s GET %s: HTTP %d", kind, safeURL(requestURL), status),
			retrySameURL: status == http.StatusRequestTimeout ||
				status == http.StatusTooManyRequests ||
				status >= 500,
		}
	}
	return response.Body(), nil
}

func chooseAlignedStart(videoSegments, audioSegments []*m3u8.MediaSegment) (int, int, time.Duration, bool) {
	bestOffset := time.Duration(1<<63 - 1)
	var videoIndex, audioIndex int
	var found bool
	for vi, video := range videoSegments {
		if video == nil || video.ProgramDateTime.IsZero() {
			continue
		}
		for ai, audio := range audioSegments {
			if audio == nil || audio.ProgramDateTime.IsZero() {
				continue
			}
			offset := video.ProgramDateTime.Sub(audio.ProgramDateTime)
			if offset < 0 {
				offset = -offset
			}
			if offset < bestOffset {
				videoIndex = vi
				audioIndex = ai
				bestOffset = offset
				found = true
			}
		}
	}
	return videoIndex, audioIndex, bestOffset, found
}

func writeAll(writer io.Writer, body []byte) error {
	written, err := writer.Write(body)
	if err != nil {
		return err
	}
	if written != len(body) {
		return io.ErrShortWrite
	}
	return nil
}

func durationFromSeconds(seconds float64) time.Duration {
	return time.Duration(seconds * float64(time.Second))
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
	reference, err := url.Parse(uri)
	if err != nil {
		return "", fmt.Errorf("parse URI: %w", err)
	}
	resolved := baseURL.ResolveReference(reference)
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
