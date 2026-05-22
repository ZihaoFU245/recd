package channel

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	"recd/config"
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
