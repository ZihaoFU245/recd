package channel

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"recd/config"

	"github.com/grafov/m3u8"
)

func TestProcessTrackWritesSequenceZero(t *testing.T) {
	server := newSegmentServer(map[string][]byte{
		"/init.mp4": {0x00, 0xff, 0x01},
		"/seg0.m4s": []byte("seg-zero"),
	})

	server.setPlaylist("/video.m3u8", mediaPlaylist(0, []string{
		"seg0.m4s",
	}))

	var out bytes.Buffer
	ts := trackState{
		url:    server.URL + "/video.m3u8",
		writer: nopWriteCloser{&out},
		name:   "video",
	}

	if err := processTrack(testContextWithTransport(server), "https://chaturbate.com/test/", &ts); err != nil {
		t.Fatalf("processTrack() error: %v", err)
	}

	want := append([]byte{0x00, 0xff, 0x01}, []byte("seg-zero")...)
	if !bytes.Equal(out.Bytes(), want) {
		t.Fatalf("unexpected bytes written: got %v want %v", out.Bytes(), want)
	}
	if !ts.haveLastSeq || ts.lastSeq != 0 {
		t.Fatalf("expected last seq 0 to be recorded, got have=%v seq=%d", ts.haveLastSeq, ts.lastSeq)
	}
}

func TestProcessTrackErrorsOnSkippedSequence(t *testing.T) {
	server := newSegmentServer(map[string][]byte{
		"/init.mp4": {0x00, 0xff, 0x01},
		"/seg0.m4s": []byte("seg-zero"),
		"/seg2.m4s": []byte("seg-two"),
	})

	server.setPlaylist("/video.m3u8", mediaPlaylist(0, []string{
		"seg0.m4s",
	}))

	var out bytes.Buffer
	ts := trackState{
		url:    server.URL + "/video.m3u8",
		writer: nopWriteCloser{&out},
		name:   "video",
	}
	ctx := testContextWithTransport(server)
	if err := processTrack(ctx, "https://chaturbate.com/test/", &ts); err != nil {
		t.Fatalf("first processTrack() error: %v", err)
	}

	server.setPlaylist("/video.m3u8", mediaPlaylist(2, []string{
		"seg2.m4s",
	}))

	err := processTrack(ctx, "https://chaturbate.com/test/", &ts)
	if err == nil {
		t.Fatal("expected skipped sequence error")
	}
	if !strings.Contains(err.Error(), "missed segment") {
		t.Fatalf("expected missed segment error, got %v", err)
	}
}

func TestAlignInitialTracksUsesClosestProgramDateTimes(t *testing.T) {
	base := time.Date(2026, 5, 22, 14, 10, 0, 0, time.UTC)
	server := newSegmentServer(map[string][]byte{
		"/vinit.mp4": []byte("vinit|"),
		"/ainit.mp4": []byte("ainit|"),
		"/v100.m4s":  []byte("v100|"),
		"/v101.m4s":  []byte("v101|"),
		"/v102.m4s":  []byte("v102|"),
		"/a200.m4s":  []byte("a200|"),
		"/a201.m4s":  []byte("a201|"),
		"/a202.m4s":  []byte("a202|"),
	})

	server.setPlaylist("/video.m3u8", mediaPlaylistWithTimes(100, "vinit.mp4", []timedSegment{
		{uri: "v100.m4s", programTime: base},
		{uri: "v101.m4s", programTime: base.Add(1600 * time.Millisecond)},
		{uri: "v102.m4s", programTime: base.Add(3200 * time.Millisecond)},
	}))
	server.setPlaylist("/audio.m3u8", mediaPlaylistWithTimes(200, "ainit.mp4", []timedSegment{
		{uri: "a200.m4s", programTime: base.Add(1300 * time.Millisecond)},
		{uri: "a201.m4s", programTime: base.Add(2900 * time.Millisecond)},
		{uri: "a202.m4s", programTime: base.Add(4500 * time.Millisecond)},
	}))

	var videoOut, audioOut bytes.Buffer
	video := trackState{
		url:    server.URL + "/video.m3u8",
		writer: nopWriteCloser{&videoOut},
		name:   "video",
	}
	audio := trackState{
		url:    server.URL + "/audio.m3u8",
		writer: nopWriteCloser{&audioOut},
		name:   "audio",
	}

	if err := alignInitialTracks(testContextWithTransport(server), "https://chaturbate.com/test/", &video, &audio); err != nil {
		t.Fatalf("alignInitialTracks() error: %v", err)
	}

	if got, want := videoOut.String(), "vinit|v101|v102|"; got != want {
		t.Fatalf("unexpected video output: got %q want %q", got, want)
	}
	if got, want := audioOut.String(), "ainit|a200|a201|a202|"; got != want {
		t.Fatalf("unexpected audio output: got %q want %q", got, want)
	}
	if video.firstPDT.Sub(audio.firstPDT) != 300*time.Millisecond {
		t.Fatalf("unexpected first program time offset: video=%s audio=%s", video.firstPDT, audio.firstPDT)
	}
	if video.lastSeq != 102 || audio.lastSeq != 202 {
		t.Fatalf("unexpected last seq: video=%d audio=%d", video.lastSeq, audio.lastSeq)
	}
}

func TestChooseAlignedStartReturnsFalseWithoutProgramDateTimes(t *testing.T) {
	_, _, _, ok := chooseAlignedStart([]*m3u8.MediaSegment{
		{SeqId: 1, URI: "v1.m4s"},
	}, []*m3u8.MediaSegment{
		{SeqId: 1, URI: "a1.m4s"},
	})
	if ok {
		t.Fatal("expected no alignment without program date times")
	}
}

func TestCacheDownloadedSegmentWritesBodyAndManifest(t *testing.T) {
	cacheDir := t.TempDir()
	t.Setenv("RECD_SEGMENT_CACHE_DIR", cacheDir)

	body := []byte("segment-body")
	entry := segmentCacheEntry{
		Username:        "user/name",
		Track:           "video",
		Kind:            "segment",
		Seq:             42,
		URI:             "seg.m4s",
		URL:             "https://segments.test/seg.m4s",
		DurationSeconds: "1.600000",
		ProgramDateTime: time.Date(2026, 5, 23, 1, 2, 3, 0, time.UTC).Format(time.RFC3339Nano),
	}
	if err := cacheDownloadedSegment(&trackState{}, entry, body); err != nil {
		t.Fatalf("cacheDownloadedSegment() error: %v", err)
	}

	manifestPath := filepath.Join(cacheDir, "user_name", "manifest.jsonl")
	manifestData, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	var got segmentCacheEntry
	if err := json.Unmarshal(bytes.TrimSpace(manifestData), &got); err != nil {
		t.Fatalf("decode manifest: %v", err)
	}
	if got.Seq != 42 || got.Track != "video" || got.Kind != "segment" || got.Size != len(body) || got.SHA256 == "" {
		t.Fatalf("unexpected manifest entry: %+v", got)
	}
	cachedBody, err := os.ReadFile(got.Path)
	if err != nil {
		t.Fatalf("read cached segment: %v", err)
	}
	if !bytes.Equal(cachedBody, body) {
		t.Fatalf("cached body mismatch: got %q want %q", cachedBody, body)
	}
}

func TestFinalizeTempRecordingMergesAfterCaptureErrorAndRemovesTemps(t *testing.T) {
	ctx := testContext()
	dir := t.TempDir()
	videoPath := writeTestFile(t, dir, "video.bin", []byte("video"))
	audioPath := writeTestFile(t, dir, "audio.bin", []byte("audio"))
	outputPath := filepath.Join(dir, "out.mkv")

	oldMerge := mergeMediaFiles
	t.Cleanup(func() { mergeMediaFiles = oldMerge })

	var mergeCalled bool
	mergeMediaFiles = func(_ *config.AppContext, username, gotVideoPath, gotAudioPath, gotOutputPath string, videoSize, audioSize int64) error {
		mergeCalled = true
		if username != "angel_from_sky" {
			t.Fatalf("unexpected username %q", username)
		}
		if gotVideoPath != videoPath || gotAudioPath != audioPath || gotOutputPath != outputPath {
			t.Fatalf("unexpected merge paths: %q %q %q", gotVideoPath, gotAudioPath, gotOutputPath)
		}
		if videoSize == 0 || audioSize == 0 {
			t.Fatalf("expected non-zero temp sizes, got video=%d audio=%d", videoSize, audioSize)
		}
		return os.WriteFile(outputPath, []byte("merged"), 0644)
	}

	captureErr := errors.New("audio segment HTTP 403")
	res := Result{Username: "angel_from_sky", Status: StatusError, Err: captureErr}
	finalizeTempRecording(ctx, "angel_from_sky", videoPath, audioPath, outputPath, time.Now(), &res)

	if !mergeCalled {
		t.Fatal("expected merge to be attempted after capture error")
	}
	if res.Status != StatusError {
		t.Fatalf("expected original error status to remain, got %v", res.Status)
	}
	if !errors.Is(res.Err, captureErr) {
		t.Fatalf("expected original capture error to remain, got %v", res.Err)
	}
	if res.Filesize == 0 {
		t.Fatal("expected finalized output filesize to be recorded")
	}
	assertMissing(t, videoPath)
	assertMissing(t, audioPath)
	assertExists(t, outputPath)
}

func TestFinalizeTempRecordingKeepsTempsWhenMergeFails(t *testing.T) {
	ctx := testContext()
	dir := t.TempDir()
	videoPath := writeTestFile(t, dir, "video.bin", []byte("video"))
	audioPath := writeTestFile(t, dir, "audio.bin", []byte("audio"))
	outputPath := filepath.Join(dir, "out.mkv")

	oldMerge := mergeMediaFiles
	t.Cleanup(func() { mergeMediaFiles = oldMerge })

	mergeErr := errors.New("ffmpeg failed")
	mergeMediaFiles = func(_ *config.AppContext, _, _, _, _ string, _, _ int64) error {
		return mergeErr
	}

	captureErr := errors.New("audio segment HTTP 403")
	res := Result{Username: "angel_from_sky", Status: StatusError, Err: captureErr}
	finalizeTempRecording(ctx, "angel_from_sky", videoPath, audioPath, outputPath, time.Now(), &res)

	if res.Status != StatusError {
		t.Fatalf("expected StatusError, got %v", res.Status)
	}
	if !errors.Is(res.Err, captureErr) {
		t.Fatalf("expected capture error to be preserved, got %v", res.Err)
	}
	if !strings.Contains(res.Err.Error(), "merge") || !strings.Contains(res.Err.Error(), mergeErr.Error()) {
		t.Fatalf("expected merge failure in result error, got %v", res.Err)
	}
	assertExists(t, videoPath)
	assertExists(t, audioPath)
	assertMissing(t, outputPath)
}

func TestFinalizeTempRecordingKeepsTempsWhenOutputIsEmpty(t *testing.T) {
	ctx := testContext()
	dir := t.TempDir()
	videoPath := writeTestFile(t, dir, "video.bin", []byte("video"))
	audioPath := writeTestFile(t, dir, "audio.bin", []byte("audio"))
	outputPath := filepath.Join(dir, "out.mkv")

	oldMerge := mergeMediaFiles
	t.Cleanup(func() { mergeMediaFiles = oldMerge })

	mergeMediaFiles = func(_ *config.AppContext, _, _, _, gotOutputPath string, _, _ int64) error {
		return os.WriteFile(gotOutputPath, nil, 0644)
	}

	res := Result{Username: "angel_from_sky", Status: StatusCompleted}
	finalizeTempRecording(ctx, "angel_from_sky", videoPath, audioPath, outputPath, time.Now(), &res)

	if res.Status != StatusError {
		t.Fatalf("expected StatusError for empty output, got %v", res.Status)
	}
	if res.Err == nil || !strings.Contains(res.Err.Error(), "empty output") {
		t.Fatalf("expected empty output error, got %v", res.Err)
	}
	assertExists(t, videoPath)
	assertExists(t, audioPath)
}

func TestNextOutputPathUsesBaseWhenAvailable(t *testing.T) {
	dir := t.TempDir()
	pattern := filepath.Join(dir, "{{.Username}}{{if .Sequence}}_{{.Sequence}}{{end}}")
	start := time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)

	got, err := nextOutputPath(pattern, "testuser", start)
	if err != nil {
		t.Fatalf("nextOutputPath() error: %v", err)
	}

	want := filepath.Join(dir, "testuser.mkv")
	if got != want {
		t.Fatalf("unexpected output path: got %q want %q", got, want)
	}
}

func TestNextOutputPathUsesSequenceWhenBaseExists(t *testing.T) {
	dir := t.TempDir()
	pattern := filepath.Join(dir, "{{.Username}}{{if .Sequence}}_{{.Sequence}}{{end}}")
	start := time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)

	writeTestFile(t, dir, "testuser.mkv", []byte("existing"))

	got, err := nextOutputPath(pattern, "testuser", start)
	if err != nil {
		t.Fatalf("nextOutputPath() error: %v", err)
	}

	want := filepath.Join(dir, "testuser_1.mkv")
	if got != want {
		t.Fatalf("unexpected output path: got %q want %q", got, want)
	}
}

type nopWriteCloser struct {
	*bytes.Buffer
}

func (w nopWriteCloser) Close() error { return nil }

type segmentServer struct {
	URL       string
	bodies    map[string][]byte
	playlists map[string]string
}

func newSegmentServer(bodies map[string][]byte) *segmentServer {
	return &segmentServer{
		URL:       "https://segments.test",
		bodies:    bodies,
		playlists: make(map[string]string),
	}
}

func (s *segmentServer) setPlaylist(path, body string) {
	s.playlists[path] = body
}

func testContextWithTransport(server *segmentServer) *config.AppContext {
	ctx := testContext()
	ctx.Resty.SetTransport(roundTripFunc(func(req *http.Request) (*http.Response, error) {
		if playlist, ok := server.playlists[req.URL.Path]; ok {
			return response(req, 200, "application/vnd.apple.mpegurl", []byte(playlist)), nil
		}
		if body, ok := server.bodies[req.URL.Path]; ok {
			return response(req, 200, "application/octet-stream", body), nil
		}
		return response(req, 404, "text/plain", []byte("not found")), nil
	}))
	return ctx
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func response(req *http.Request, status int, contentType string, body []byte) *http.Response {
	header := make(http.Header)
	header.Set("Content-Type", contentType)
	return &http.Response{
		StatusCode:    status,
		Status:        fmt.Sprintf("%d %s", status, http.StatusText(status)),
		Header:        header,
		Body:          io.NopCloser(bytes.NewReader(body)),
		ContentLength: int64(len(body)),
		Request:       req,
	}
}

func mediaPlaylist(seq uint64, segments []string) string {
	return mediaPlaylistWithMap(seq, "init.mp4", segments)
}

func mediaPlaylistWithMap(seq uint64, init string, segments []string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "#EXTM3U\n#EXT-X-VERSION:7\n#EXT-X-TARGETDURATION:2\n#EXT-X-MEDIA-SEQUENCE:%d\n#EXT-X-MAP:URI=\"%s\"\n", seq, init)
	for _, segment := range segments {
		fmt.Fprintf(&b, "#EXTINF:1.000000,\n%s\n", segment)
	}
	return b.String()
}

type timedSegment struct {
	uri         string
	programTime time.Time
}

func mediaPlaylistWithTimes(seq uint64, init string, segments []timedSegment) string {
	var b strings.Builder
	fmt.Fprintf(&b, "#EXTM3U\n#EXT-X-VERSION:7\n#EXT-X-TARGETDURATION:2\n#EXT-X-MEDIA-SEQUENCE:%d\n#EXT-X-MAP:URI=\"%s\"\n", seq, init)
	for _, segment := range segments {
		fmt.Fprintf(&b, "#EXT-X-PROGRAM-DATE-TIME:%s\n", segment.programTime.Format(time.RFC3339Nano))
		fmt.Fprintf(&b, "#EXTINF:1.600000,\n%s\n", segment.uri)
	}
	return b.String()
}

func writeTestFile(t *testing.T, dir, name string, data []byte) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatal(err)
	}
	return path
}

func assertExists(t *testing.T, path string) {
	t.Helper()
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("expected %s to exist: %v", path, err)
	}
}

func assertMissing(t *testing.T, path string) {
	t.Helper()
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected %s to be missing, stat err=%v", path, err)
	}
}
