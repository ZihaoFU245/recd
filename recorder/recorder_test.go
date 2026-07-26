package recorder

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"recd/config"

	"github.com/grafov/m3u8"
)

func TestTrackWorkerWritesSequenceZeroAndReusesBodyBuffer(t *testing.T) {
	bodies := map[string][]byte{
		"/video.m3u8": []byte(mediaPlaylist(0, "init.mp4", []timedSegment{{
			uri: "seg0.m4s", at: time.Now().UTC(),
		}})),
		"/init.mp4": []byte("init|"),
		"/seg0.m4s": bytes.Repeat([]byte("s"), 1024),
	}
	app := testApp(roundTripFunc(func(request *http.Request) (*http.Response, error) {
		body, ok := bodies[request.URL.Path]
		if !ok {
			return response(request, http.StatusNotFound, nil), nil
		}
		return response(request, http.StatusOK, body), nil
	}))
	var output bytes.Buffer
	worker := &trackWorker{
		app: app, username: "test", name: "video",
		url:     "https://segments.test/video.m3u8",
		referer: "https://chaturbate.com/test/",
		writer:  &output,
	}
	if worker.bodyBuf.Cap() != 0 {
		t.Fatal("body buffer allocated before first request")
	}
	playlist, err := worker.fetchPlaylist(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := worker.processPlaylist(context.Background(), playlist, 0); err != nil {
		t.Fatal(err)
	}
	if !worker.haveLastSeq || worker.lastSeq != 0 {
		t.Fatalf("sequence state = have:%t last:%d", worker.haveLastSeq, worker.lastSeq)
	}
	if got := output.String(); !strings.HasPrefix(got, "init|") || len(got) != len("init|")+1024 {
		t.Fatalf("unexpected recorded body length %d", len(got))
	}
	firstCapacity := worker.bodyBuf.Cap()
	if firstCapacity < 1024 {
		t.Fatalf("buffer capacity = %d, want at least 1024", firstCapacity)
	}

	bodies["/small"] = []byte("small")
	if _, err := worker.fetchBody(context.Background(), "small", "https://segments.test/small"); err != nil {
		t.Fatal(err)
	}
	if worker.bodyBuf.Cap() != firstCapacity {
		t.Fatalf("buffer capacity changed from %d to %d for smaller body", firstCapacity, worker.bodyBuf.Cap())
	}
}

func TestTrackWorkerRejectsBodyOverLimit(t *testing.T) {
	app := testApp(roundTripFunc(func(request *http.Request) (*http.Response, error) {
		response := response(request, http.StatusOK, nil)
		response.ContentLength = maxResponseBodyBytes + 1
		return response, nil
	}))
	worker := &trackWorker{
		app: app, name: "video",
		referer: "https://chaturbate.com/test/",
	}
	_, err := worker.fetchBody(context.Background(), "segment", "https://segments.test/large.m4s")
	if err == nil || !strings.Contains(err.Error(), "limit") {
		t.Fatalf("oversize response error = %v", err)
	}
}

func TestTrackWorkerDetectsSequenceGap(t *testing.T) {
	app := testApp(nil)
	worker := &trackWorker{
		app: app, name: "video",
		haveLastSeq: true, lastSeq: 3,
	}
	err := worker.processSegments(context.Background(), []*m3u8.MediaSegment{{SeqId: 5, URI: "5.m4s"}})
	if err == nil || !strings.Contains(err.Error(), "missed segment") {
		t.Fatalf("sequence gap error = %v", err)
	}
	var requestErr *requestError
	if !errors.As(err, &requestErr) {
		t.Fatalf("sequence gap is not a request error: %v", err)
	}
}

func TestTrackWorkerDetectsSequenceResetInDecodedPlaylist(t *testing.T) {
	decoded, listType, err := m3u8.DecodeFrom(strings.NewReader(mediaPlaylist(
		3,
		"init.mp4",
		[]timedSegment{{uri: "seg3.m4s", at: time.Now().UTC()}},
	)), true)
	if err != nil {
		t.Fatal(err)
	}
	if listType != m3u8.MEDIA {
		t.Fatalf("playlist type = %v, want media", listType)
	}
	media := decoded.(*m3u8.MediaPlaylist)
	if len(media.Segments) <= int(media.Count()) {
		t.Fatalf("test requires a padded decoded segment slice")
	}

	worker := &trackWorker{
		app:         testApp(nil),
		name:        "video",
		haveLastSeq: true,
		lastSeq:     100,
	}
	err = worker.processSegments(context.Background(), media.Segments)
	if err == nil || !strings.Contains(err.Error(), "moved backwards") {
		t.Fatalf("sequence reset error = %v", err)
	}
	var requestErr *requestError
	if !errors.As(err, &requestErr) {
		t.Fatalf("sequence reset is not a request error: %v", err)
	}
}

func TestChooseAlignedStartUsesClosestProgramTimes(t *testing.T) {
	base := time.Now().UTC()
	video, audio, offset, ok := chooseAlignedStart(
		[]*m3u8.MediaSegment{
			{SeqId: 1, ProgramDateTime: base},
			{SeqId: 2, ProgramDateTime: base.Add(1600 * time.Millisecond)},
		},
		[]*m3u8.MediaSegment{
			{SeqId: 10, ProgramDateTime: base.Add(1300 * time.Millisecond)},
			{SeqId: 11, ProgramDateTime: base.Add(2900 * time.Millisecond)},
		},
	)
	if !ok || video != 1 || audio != 0 || offset != 300*time.Millisecond {
		t.Fatalf("alignment = video:%d audio:%d offset:%s ok:%t", video, audio, offset, ok)
	}
}

func TestTrackDriftUsesWindowBasedLimit(t *testing.T) {
	base := time.Now()
	progress := map[string]trackProgress{
		"video": {name: "video", mediaEnd: base.Add(11 * time.Second), targetDuration: 2},
		"audio": {name: "audio", mediaEnd: base, targetDuration: 2},
	}
	drift, limit, exceeded := trackDrift(progress)
	if drift != 11*time.Second || limit != 10*time.Second || !exceeded {
		t.Fatalf("drift=%s limit=%s exceeded=%t", drift, limit, exceeded)
	}
	progress["video"] = trackProgress{name: "video", mediaEnd: base.Add(9 * time.Second), targetDuration: 4}
	if _, limit, exceeded := trackDrift(progress); limit != 12*time.Second || exceeded {
		t.Fatalf("target-derived limit=%s exceeded=%t", limit, exceeded)
	}
}

func TestRecorderFetchesInitialTracksConcurrentlyAndUsesTMPDIR(t *testing.T) {
	base := time.Now().UTC().Truncate(time.Second)
	videoStarted := make(chan struct{})
	audioStarted := make(chan struct{})
	segments := make(chan string, 2)
	var onceVideo, onceAudio sync.Once

	app := testApp(roundTripFunc(func(request *http.Request) (*http.Response, error) {
		switch request.URL.Path {
		case "/master.m3u8":
			return response(request, http.StatusOK, []byte(masterPlaylist())), nil
		case "/video.m3u8":
			onceVideo.Do(func() { close(videoStarted) })
			select {
			case <-audioStarted:
			case <-request.Context().Done():
				return nil, request.Context().Err()
			}
			return response(request, http.StatusOK, []byte(mediaPlaylist(0, "vinit.mp4", []timedSegment{{
				uri: "v0.m4s", at: base,
			}}))), nil
		case "/audio.m3u8":
			onceAudio.Do(func() { close(audioStarted) })
			select {
			case <-videoStarted:
			case <-request.Context().Done():
				return nil, request.Context().Err()
			}
			return response(request, http.StatusOK, []byte(mediaPlaylist(0, "ainit.mp4", []timedSegment{{
				uri: "a0.m4s", at: base,
			}}))), nil
		case "/vinit.mp4":
			return response(request, http.StatusOK, []byte("vinit")), nil
		case "/ainit.mp4":
			return response(request, http.StatusOK, []byte("ainit")), nil
		case "/v0.m4s", "/a0.m4s":
			segments <- request.URL.Path
			return response(request, http.StatusOK, []byte(request.URL.Path)), nil
		default:
			return response(request, http.StatusNotFound, nil), nil
		}
	}))

	tempDir := t.TempDir()
	t.Setenv("TMPDIR", tempDir)
	outputDir := t.TempDir()
	oldFinalizer := finalizeMediaFiles
	t.Cleanup(func() { finalizeMediaFiles = oldFinalizer })
	var videoPath, audioPath string
	finalizeMediaFiles = func(_ *config.AppContext, _ string, video, audio, output string) error {
		videoPath, audioPath = video, audio
		return os.WriteFile(output, []byte("valid-mkv"), 0644)
	}

	runCtx, cancel := context.WithCancel(context.Background())
	done := make(chan Result, 1)
	go func() {
		done <- New(app, config.ChannelConfig{
			Username: "parallel",
			Pattern:  filepath.Join(outputDir, "{{.Username}}"),
		}, "https://segments.test/master.m3u8").Run(runCtx)
	}()
	for range 2 {
		select {
		case <-segments:
		case <-time.After(time.Second):
			cancel()
			t.Fatal("both tracks did not download initial segments")
		}
	}
	cancel()
	result := <-done
	if result.Status != StatusCompleted {
		t.Fatalf("recording result = %+v", result)
	}
	if filepath.Dir(videoPath) != tempDir || filepath.Dir(audioPath) != tempDir {
		t.Fatalf("TMPDIR not used: video=%q audio=%q", videoPath, audioPath)
	}
	if _, err := os.Stat(result.Path); err != nil {
		t.Fatalf("final output missing: %v", err)
	}
}

func TestFetchAndSelectStreamsAllowsCombinedMedia(t *testing.T) {
	app := testApp(roundTripFunc(func(request *http.Request) (*http.Response, error) {
		return response(request, http.StatusOK, []byte(
			"#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=1000,RESOLUTION=1280x720\nvideo.m3u8\n",
		)), nil
	}))
	video, audio, err := fetchAndSelectStreams(
		context.Background(),
		app,
		"https://segments.test/master.m3u8",
		config.ChannelConfig{Username: "test", Resolution: 720},
	)
	if err != nil {
		t.Fatal(err)
	}
	if video != "https://segments.test/video.m3u8" || audio != "" {
		t.Fatalf("selected video=%q audio=%q", video, audio)
	}
}

func TestRecorderCapturesCombinedMediaWithFFmpeg(t *testing.T) {
	binDir := t.TempDir()
	startedMarker := filepath.Join(binDir, "ffmpeg-started")
	writeExecutable(t, binDir, "ffmpeg", `#!/bin/sh
for output_path in "$@"; do :; done
printf 'combined-media' > "$output_path"
: > "$FAKE_FFMPEG_STARTED"
read -r _
`)
	writeExecutable(t, binDir, "ffprobe", `#!/bin/sh
printf '%s\n' '{"streams":[{"codec_type":"video"},{"codec_type":"audio"}],"format":{"duration":"1.5"}}'
`)
	t.Setenv("PATH", binDir+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("FAKE_FFMPEG_STARTED", startedMarker)

	app := testApp(roundTripFunc(func(request *http.Request) (*http.Response, error) {
		if request.URL.Path != "/master.m3u8" {
			return response(request, http.StatusNotFound, nil), nil
		}
		return response(request, http.StatusOK, []byte(
			"#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=1000,RESOLUTION=1280x720\ncombined.m3u8\n",
		)), nil
	}))
	outputDir := t.TempDir()
	runCtx, cancel := context.WithCancel(context.Background())
	done := make(chan Result, 1)
	go func() {
		done <- New(app, config.ChannelConfig{
			Username:   "combined",
			Pattern:    filepath.Join(outputDir, "{{.Username}}"),
			Resolution: 720,
		}, "https://segments.test/master.m3u8").Run(runCtx)
	}()

	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, err := os.Stat(startedMarker); err == nil {
			break
		}
		if time.Now().After(deadline) {
			cancel()
			t.Fatal("ffmpeg did not start")
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()

	select {
	case result := <-done:
		if result.Status != StatusCompleted || result.Err != nil {
			t.Fatalf("combined recording result = %+v", result)
		}
		if result.Filesize == 0 {
			t.Fatalf("combined recording is empty: %+v", result)
		}
		assertExists(t, result.Path)
	case <-time.After(2 * time.Second):
		t.Fatal("combined recorder did not stop")
	}
}

func TestFinalizeRecordingPublishesOnlyValidatedOutput(t *testing.T) {
	dir := t.TempDir()
	video := writeFile(t, dir, "video.bin", []byte("video"))
	audio := writeFile(t, dir, "audio.bin", []byte("audio"))
	output := filepath.Join(dir, "output.mkv")

	oldFinalizer := finalizeMediaFiles
	t.Cleanup(func() { finalizeMediaFiles = oldFinalizer })
	finalizeMediaFiles = func(_ *config.AppContext, _ string, _, _, partial string) error {
		return os.WriteFile(partial, []byte("validated"), 0644)
	}
	result := Result{Status: StatusCompleted, Path: output}
	finalizeRecording(testApp(nil), "test", video, audio, output, &result)
	if result.Status != StatusCompleted || result.Filesize == 0 {
		t.Fatalf("finalized result = %+v", result)
	}
	assertMissing(t, video)
	assertMissing(t, audio)
	assertExists(t, output)
}

func TestFinalizeRecordingRetainsBinsOnValidationFailure(t *testing.T) {
	dir := t.TempDir()
	video := writeFile(t, dir, "video.bin", []byte("video"))
	audio := writeFile(t, dir, "audio.bin", []byte("audio"))
	output := filepath.Join(dir, "output.mkv")

	oldFinalizer := finalizeMediaFiles
	t.Cleanup(func() { finalizeMediaFiles = oldFinalizer })
	finalizeMediaFiles = func(_ *config.AppContext, _ string, _, _, _ string) error {
		return errors.New("ffprobe rejected output")
	}
	result := Result{Status: StatusCompleted, Path: output}
	finalizeRecording(testApp(nil), "test", video, audio, output, &result)
	if result.Status != StatusError || !strings.Contains(result.Err.Error(), "ffprobe") {
		t.Fatalf("failed result = %+v", result)
	}
	assertExists(t, video)
	assertExists(t, audio)
	assertMissing(t, output)
}

func TestNextOutputPathAddsSequence(t *testing.T) {
	dir := t.TempDir()
	pattern := filepath.Join(dir, "{{.Username}}")
	writeFile(t, dir, "test.mkv", []byte("existing"))
	path, err := nextOutputPath(pattern, "test", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if path != filepath.Join(dir, "test_1.mkv") {
		t.Fatalf("next output path = %q", path)
	}
}

func testApp(transport http.RoundTripper) *config.AppContext {
	app := config.NewAppContext(slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
	if transport != nil {
		app.Resty.SetTransport(transport)
	}
	return app
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (function roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return function(request)
}

func response(request *http.Request, status int, body []byte) *http.Response {
	return &http.Response{
		StatusCode:    status,
		Status:        fmt.Sprintf("%d %s", status, http.StatusText(status)),
		Header:        http.Header{"Content-Type": []string{"application/octet-stream"}},
		Body:          io.NopCloser(bytes.NewReader(body)),
		ContentLength: int64(len(body)),
		Request:       request,
	}
}

func masterPlaylist() string {
	return `#EXTM3U
#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID="audio",NAME="audio",DEFAULT=YES,AUTOSELECT=YES,URI="audio.m3u8"
#EXT-X-STREAM-INF:BANDWIDTH=1000000,RESOLUTION=960x540,FRAME-RATE=30.000,AUDIO="audio"
video.m3u8
`
}

type timedSegment struct {
	uri string
	at  time.Time
}

func mediaPlaylist(sequence uint64, init string, segments []timedSegment) string {
	var body strings.Builder
	fmt.Fprintf(&body,
		"#EXTM3U\n#EXT-X-VERSION:7\n#EXT-X-TARGETDURATION:2\n#EXT-X-MEDIA-SEQUENCE:%d\n#EXT-X-MAP:URI=\"%s\"\n",
		sequence,
		init,
	)
	for _, segment := range segments {
		fmt.Fprintf(&body, "#EXT-X-PROGRAM-DATE-TIME:%s\n", segment.at.Format(time.RFC3339Nano))
		fmt.Fprintf(&body, "#EXTINF:1.600000,\n%s\n", segment.uri)
	}
	return body.String()
}

func writeFile(t *testing.T, dir, name string, body []byte) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, body, 0644); err != nil {
		t.Fatal(err)
	}
	return path
}

func writeExecutable(t *testing.T, dir, name, body string) string {
	t.Helper()
	path := writeFile(t, dir, name, []byte(body))
	if err := os.Chmod(path, 0755); err != nil {
		t.Fatal(err)
	}
	return path
}

func assertExists(t *testing.T, path string) {
	t.Helper()
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("%s should exist: %v", path, err)
	}
}

func assertMissing(t *testing.T, path string) {
	t.Helper()
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("%s should not exist: %v", path, err)
	}
}
