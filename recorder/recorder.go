package recorder

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime/debug"
	"strconv"
	"strings"
	"time"

	"recd/config"

	"github.com/grafov/m3u8"
)

const (
	segmentPollInterval = 300 * time.Millisecond
	maxInitialAVOffset  = time.Second
	minStallWindow      = 10 * time.Second
	maxOutputPathTries  = 10000
)

// Recorder performs exactly one recording session. Its caller owns lifecycle,
// cancellation, session identity, and result delivery.
type Recorder struct {
	app       *config.AppContext
	cfg       config.ChannelConfig
	hlsSource string
}

func New(app *config.AppContext, cfg config.ChannelConfig, hlsSource string) *Recorder {
	return &Recorder{app: app, cfg: cfg, hlsSource: hlsSource}
}

func (r *Recorder) Run(ctx context.Context) (result Result) {
	started := time.Now()
	result.Status = StatusCompleted
	defer func() {
		if recovered := recover(); recovered != nil {
			result.Status = StatusError
			result.Err = fmt.Errorf("panic: %v", recovered)
			r.app.Logger.Error("recorder panic recovered",
				"username", r.cfg.Username,
				"panic", recovered,
				"stack", string(debug.Stack()),
			)
		}
		result.Duration = time.Since(started)
	}()

	if ctx.Err() != nil {
		return result
	}
	outputPath, err := nextOutputPath(r.cfg.Pattern, r.cfg.Username, started)
	if err != nil {
		setRecordError(&result, err)
		return result
	}
	result.Path = outputPath
	if err := os.MkdirAll(filepath.Dir(outputPath), 0755); err != nil {
		setRecordError(&result, fmt.Errorf("create output directory: %w", err))
		return result
	}

	videoURL, audioURL, err := fetchAndSelectStreams(ctx, r.app, r.hlsSource, r.cfg)
	if err != nil {
		if ctx.Err() == nil {
			setRecordError(&result, fmt.Errorf("stream selection: %w", err))
		}
		return result
	}
	if audioURL == "" {
		return r.recordCombinedStream(ctx, videoURL, outputPath, started, result)
	}

	videoFile, err := os.CreateTemp("", "rec_video_*.bin")
	if err != nil {
		setRecordError(&result, fmt.Errorf("create video temp file: %w", err))
		return result
	}
	videoPath := videoFile.Name()
	audioFile, err := os.CreateTemp("", "rec_audio_*.bin")
	if err != nil {
		_ = videoFile.Close()
		_ = os.Remove(videoPath)
		setRecordError(&result, fmt.Errorf("create audio temp file: %w", err))
		return result
	}
	audioPath := audioFile.Name()
	filesClosed := false
	defer func() {
		if !filesClosed {
			_ = videoFile.Close()
			_ = audioFile.Close()
		}
	}()

	referer := fmt.Sprintf("https://chaturbate.com/%s/", r.cfg.Username)
	video := &trackWorker{
		app: r.app, username: r.cfg.Username, name: "video",
		url: videoURL, referer: referer, writer: videoFile,
	}
	audio := &trackWorker{
		app: r.app, username: r.cfg.Username, name: "audio",
		url: audioURL, referer: referer, writer: audioFile,
	}
	r.app.Logger.Info("recording started",
		"username", r.cfg.Username,
		"resolution", r.cfg.Resolution,
		"video_tmp", videoPath,
		"audio_tmp", audioPath,
	)

	captureCtx, cancelCapture := context.WithCancel(ctx)
	defer cancelCapture()
	result = r.capture(captureCtx, cancelCapture, video, audio, result)
	cancelCapture()

	if closeErr := errors.Join(videoFile.Close(), audioFile.Close()); closeErr != nil {
		filesClosed = true
		result.Status = StatusError
		result.Err = errors.Join(result.Err, fmt.Errorf("close temporary media: %w", closeErr))
		return result
	}
	filesClosed = true
	if !video.haveLastSeq || !audio.haveLastSeq {
		result.Status = StatusError
		result.Err = errors.Join(result.Err, fmt.Errorf(
			"no complete media pair recorded: video=%t audio=%t",
			video.haveLastSeq,
			audio.haveLastSeq,
		))
		return result
	}

	finalizeRecording(r.app, r.cfg.Username, videoPath, audioPath, outputPath, &result)
	r.app.Logger.Info("recording finalized",
		"username", r.cfg.Username,
		"path", result.Path,
		"size", result.Filesize,
		"duration", time.Since(started),
		"status", result.Status,
		"error", result.Err,
	)
	return result
}

func (r *Recorder) capture(
	ctx context.Context,
	cancel context.CancelFunc,
	video, audio *trackWorker,
	result Result,
) Result {
	type initialResult struct {
		worker *trackWorker
		media  *m3u8.MediaPlaylist
		err    error
	}
	initial := make(chan initialResult, 2)
	for _, worker := range []*trackWorker{video, audio} {
		go func(worker *trackWorker) {
			media, err := worker.fetchPlaylist(ctx)
			initial <- initialResult{worker: worker, media: media, err: err}
		}(worker)
	}

	playlists := make(map[string]*m3u8.MediaPlaylist, 2)
	for range 2 {
		fetched := <-initial
		if fetched.err != nil && result.Err == nil {
			if ctx.Err() == nil {
				setRecordError(&result, fmt.Errorf("initial %s playlist: %w", fetched.worker.name, fetched.err))
			}
			cancel()
		}
		if fetched.media != nil {
			playlists[fetched.worker.name] = fetched.media
		}
	}
	if result.Err != nil || ctx.Err() != nil {
		return result
	}

	videoStart, audioStart, offset, ok := chooseAlignedStart(
		playlists["video"].Segments,
		playlists["audio"].Segments,
	)
	if !ok {
		setRecordError(&result, fmt.Errorf("initial playlists have no comparable program date times"))
		return result
	}
	if offset > maxInitialAVOffset {
		setRecordError(&result, fmt.Errorf(
			"initial audio/video offset %s exceeds %s",
			offset,
			maxInitialAVOffset,
		))
		return result
	}
	r.app.Logger.Info("initial tracks aligned",
		"username", r.cfg.Username,
		"video_seq", playlists["video"].Segments[videoStart].SeqId,
		"audio_seq", playlists["audio"].Segments[audioStart].SeqId,
		"offset", offset,
	)

	events := make(chan trackEvent, 8)
	go video.run(ctx, playlists["video"], videoStart, events, segmentPollInterval)
	go audio.run(ctx, playlists["audio"], audioStart, events, segmentPollInterval)

	progress := make(map[string]trackProgress, 2)
	active := 2
	stopping := false
	for active > 0 {
		event := <-events
		if event.done {
			active--
			if event.err != nil &&
				!errors.Is(event.err, context.Canceled) &&
				!errors.Is(event.err, context.DeadlineExceeded) &&
				result.Err == nil &&
				!stopping {
				setRecordError(&result, event.err)
				cancel()
				stopping = true
			}
			continue
		}
		if event.progress == nil {
			continue
		}
		progress[event.progress.name] = *event.progress
		if stopping {
			continue
		}

		if maxDurationReached(r.cfg.MaxDuration, progress) {
			result.Status = StatusMaxDuration
			cancel()
			stopping = true
			continue
		}
		if maxFilesizeReached(r.cfg.MaxFilesize, progress) {
			result.Status = StatusMaxFilesize
			cancel()
			stopping = true
			continue
		}
		if drift, limit, exceeded := trackDrift(progress); exceeded {
			result.Status = StatusDesync
			result.Err = fmt.Errorf("audio/video end-time drift %s exceeds %s", drift, limit)
			cancel()
			stopping = true
		}
	}
	if ctx.Err() != nil && !stopping && result.Err == nil {
		result.Status = StatusCompleted
	}
	return result
}

func setRecordError(result *Result, err error) {
	result.Status = StatusError
	result.Err = err
	var requestErr *requestError
	result.FastRetry = errors.As(err, &requestErr)
}

func maxDurationReached(maxMinutes int, progress map[string]trackProgress) bool {
	if maxMinutes <= 0 || len(progress) < 2 {
		return false
	}
	limit := float64(maxMinutes * 60)
	for _, state := range progress {
		if state.duration >= limit {
			return true
		}
	}
	return false
}

func maxFilesizeReached(maxBytes int64, progress map[string]trackProgress) bool {
	if maxBytes <= 0 || len(progress) < 2 {
		return false
	}
	var total int64
	for _, state := range progress {
		if state.size >= maxBytes-total {
			return true
		}
		total += state.size
	}
	return false
}

func trackDrift(progress map[string]trackProgress) (time.Duration, time.Duration, bool) {
	video, hasVideo := progress["video"]
	audio, hasAudio := progress["audio"]
	if !hasVideo || !hasAudio || video.mediaEnd.IsZero() || audio.mediaEnd.IsZero() {
		return 0, 0, false
	}
	drift := video.mediaEnd.Sub(audio.mediaEnd)
	if drift < 0 {
		drift = -drift
	}
	target := max(video.targetDuration, audio.targetDuration)
	limit := max(minStallWindow, durationFromSeconds(3*target))
	return drift, limit, drift > limit
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
