package monitor

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"recd/config"
	"recd/recorder"
)

func testContext() *config.AppContext {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	ctx := config.NewAppContext(logger, nil, false)
	ctx.Resty.SetTransport(roundTripFunc(func(req *http.Request) (*http.Response, error) {
		return response(req, 200, "text/html", []byte(roomDossierHTML(config.RoomDossier{
			RoomStatus:          "offline",
			BroadcasterUsername: strings.Trim(req.URL.Path, "/"),
		}))), nil
	}))
	return ctx
}

func TestMonitorStopCancelsStreamStatusCheck(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	ctx := config.NewAppContext(logger, nil, false)
	ctx.Resty.SetTransport(roundTripFunc(func(req *http.Request) (*http.Response, error) {
		<-req.Context().Done()
		return nil, req.Context().Err()
	}))
	mon := New(ctx, []config.ChannelConfig{{Username: "slow_user"}})
	go mon.Run()
	time.Sleep(10 * time.Millisecond)
	mon.Stop()

	select {
	case <-mon.Done():
	case <-time.After(time.Second):
		t.Fatal("monitor Run() did not return after Stop() canceled stream check")
	}
}

func TestMonitorLifecycle(t *testing.T) {
	mon := New(testContext(), []config.ChannelConfig{{Username: "offline_user"}})
	go mon.Run()
	time.Sleep(10 * time.Millisecond)
	mon.Stop()
	select {
	case <-mon.Done():
	case <-time.After(time.Second):
		t.Fatal("monitor Run() did not return after Stop()")
	}
}

func TestMonitorRunRecoversPanicAndStops(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	mon := New(config.NewAppContext(logger, nil, false), []config.ChannelConfig{{Username: "panic_user"}})
	mon.checking = nil // Force tick's map assignment to panic inside Run.

	go mon.Run()
	select {
	case <-mon.Done():
	case <-time.After(time.Second):
		t.Fatal("monitor did not recover its event-loop panic")
	}
	if mon.runCtx.Err() == nil {
		t.Fatal("monitor panic recovery did not cancel its context")
	}
}

func TestMonitorStatusChecksDoNotBlockEachOther(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	ctx := config.NewAppContext(logger, nil, false)
	fastChecked := make(chan struct{}, 1)
	ctx.Resty.SetTransport(roundTripFunc(func(req *http.Request) (*http.Response, error) {
		if req.URL.Path == "/slow/" {
			<-req.Context().Done()
			return nil, req.Context().Err()
		}
		fastChecked <- struct{}{}
		return response(req, 200, "text/html", []byte(roomDossierHTML(config.RoomDossier{
			RoomStatus:          "offline",
			BroadcasterUsername: "fast",
		}))), nil
	}))

	mon := New(ctx, []config.ChannelConfig{{Username: "slow"}, {Username: "fast"}})
	go mon.Run()
	select {
	case <-fastChecked:
	case <-time.After(time.Second):
		mon.Stop()
		t.Fatal("fast room check was blocked by slow room check")
	}
	mon.Stop()
	select {
	case <-mon.Done():
	case <-time.After(time.Second):
		t.Fatal("monitor did not stop after concurrent status checks")
	}
}

func TestStatusProbeRecoversPanic(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	ctx := config.NewAppContext(logger, nil, false)
	ctx.Resty.SetTransport(roundTripFunc(func(*http.Request) (*http.Response, error) {
		panic("transport failure")
	}))
	mon := New(ctx, []config.ChannelConfig{{Username: "panic_user"}})

	status := mon.probeStreamStatus("panic_user")
	if status.err == nil || !strings.Contains(status.err.Error(), "status probe panic") {
		t.Fatalf("probe status error = %v, want recovered panic", status.err)
	}
	if status.online || status.hlsSource != "" {
		t.Fatalf("unexpected recovered status: online=%v hls=%q", status.online, status.hlsSource)
	}
}

func TestMonitorFastRequestRetry(t *testing.T) {
	mon := New(testContext(), []config.ChannelConfig{{Username: "fast"}})
	mon.recorders["fast"] = runningRecorder{session: 1, cancel: func() {}}
	mon.handleResult(recordingResult{
		username: "fast",
		session:  1,
		result: recorder.Result{
			Status:    recorder.StatusError,
			FastRetry: true,
			Err:       errors.New("HTTP 503"),
		},
	})

	delay := time.Until(mon.nextCheck["fast"])
	if delay < 800*time.Millisecond || delay > 1100*time.Millisecond {
		t.Fatalf("fast request retry delay = %s, want about %s", delay, initialRetryDelay)
	}
}

func TestOnlineProbePreservesBackoffForNewSession(t *testing.T) {
	mon := New(testContext(), []config.ChannelConfig{{Username: "retry"}})
	mon.failures["retry"] = 2
	mon.handleStatus(streamStatus{username: "retry", online: true, hlsSource: "https://stream.test/master.m3u8"})
	if got := mon.failures["retry"]; got != 2 {
		t.Fatalf("failure count = %d, want 2 until the session survives a health check", got)
	}
	mon.Stop()
	done := make(chan struct{})
	go func() {
		mon.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("test recording session did not stop")
	}
}

func TestMonitorSlowRecorderRetry(t *testing.T) {
	mon := New(testContext(), []config.ChannelConfig{{Username: "ended"}})
	mon.recorders["ended"] = runningRecorder{session: 1, cancel: func() {}}
	mon.handleResult(recordingResult{
		username: "ended",
		session:  1,
		result: recorder.Result{
			Status: recorder.StatusError,
			Err:    errors.New("ffmpeg exited"),
		},
	})

	delay := time.Until(mon.nextCheck["ended"])
	if delay < slowRetryDelay-time.Second || delay > slowRetryDelay+time.Second {
		t.Fatalf("slow retry delay = %s, want about %s", delay, slowRetryDelay)
	}
}

func TestMonitorIgnoresStaleResult(t *testing.T) {
	mon := New(testContext(), []config.ChannelConfig{{Username: "same"}})
	mon.recorders["same"] = runningRecorder{session: 2, cancel: func() {}}
	mon.handleResult(recordingResult{
		username: "same",
		session:  1,
		result:   recorder.Result{Status: recorder.StatusError, FastRetry: true},
	})
	if got := mon.recorders["same"].session; got != 2 {
		t.Fatalf("stale result replaced current session: got %d", got)
	}
}

func TestMonitorStatusFailureKeepsRecording(t *testing.T) {
	mon := New(testContext(), []config.ChannelConfig{{Username: "running"}})
	mon.recorders["running"] = runningRecorder{session: 1, cancel: func() {}}
	mon.handleStatus(streamStatus{username: "running", err: errors.New("temporary failure")})
	if _, ok := mon.recorders["running"]; !ok {
		t.Fatal("status failure stopped a healthy recording")
	}
	if delay := time.Until(mon.nextCheck["running"]); delay < statusRetryInterval-time.Second {
		t.Fatalf("status retry delay = %s, want about %s", delay, statusRetryInterval)
	}
}

func TestMonitorReloadRemovesAndReplaces(t *testing.T) {
	ctx := testContext()
	old := config.ChannelConfig{Username: "reload", Resolution: 480}
	mon := New(ctx, []config.ChannelConfig{old})
	cancelled := false
	mon.recorders["reload"] = runningRecorder{
		session: 1,
		cancel:  func() { cancelled = true },
	}

	newCfg := config.ChannelConfig{Username: "reload", Resolution: 720}
	mon.Reload([]config.ChannelConfig{newCfg})
	if got := mon.configs["reload"].Resolution; got != 720 {
		t.Fatalf("resolution = %d, want 720", got)
	}
	if _, ok := mon.recorders["reload"]; ok {
		t.Fatal("old recording still tracked after reload")
	}
	if !cancelled {
		t.Fatal("old recorder was not cancelled")
	}
	if mon.nextCheck["reload"].After(time.Now().Add(time.Second)) {
		t.Fatal("reload did not schedule an immediate probe")
	}

	mon.Reload([]config.ChannelConfig{{Username: "reload", IsPaused: true}})
	if _, ok := mon.configs["reload"]; ok {
		t.Fatal("paused channel remained configured")
	}
}

func TestParseRoomDossierAllowsEscapedQuoteSemicolon(t *testing.T) {
	payload := `{"room_status":"public","hls_source":"https://edge.example.test/live.m3u8","broadcaster_username":"quoted_user","num_viewers":12,"room_title":"quote \\\"; still inside"}`
	escaped := strings.Trim(strconv.Quote(payload), `"`)
	html := `<script>window.initialRoomDossier = "` + escaped + `";</script>`

	dossier, err := parseRoomDossier(html)
	if err != nil {
		t.Fatalf("parseRoomDossier() error: %v", err)
	}
	if dossier.RoomStatus != "public" || dossier.HlsSource == "" || dossier.BroadcasterUsername != "quoted_user" {
		t.Fatalf("unexpected dossier: %+v", dossier)
	}
}

func TestParseRoomDossierSkipsNonAssignmentMarker(t *testing.T) {
	html := `<script>const initialRoomDossierHelper = "not the dossier";</script>` +
		roomDossierHTML(config.RoomDossier{
			RoomStatus:          "public",
			HlsSource:           "https://edge.example.test/live.m3u8",
			BroadcasterUsername: "real_user",
		})
	dossier, err := parseRoomDossier(html)
	if err != nil {
		t.Fatalf("parseRoomDossier() error: %v", err)
	}
	if dossier.BroadcasterUsername != "real_user" {
		t.Fatalf("unexpected dossier: %+v", dossier)
	}
}

func TestCheckStreamStatusRejectsBroadcasterMismatch(t *testing.T) {
	ctx := testContext()
	ctx.Resty.SetTransport(roundTripFunc(func(req *http.Request) (*http.Response, error) {
		return response(req, 200, "text/html", []byte(roomDossierHTML(config.RoomDossier{
			RoomStatus:          "public",
			HlsSource:           "https://edge.example.test/live.m3u8",
			BroadcasterUsername: "different_user",
		}))), nil
	}))
	mon := New(ctx, []config.ChannelConfig{{Username: "requested_user"}})
	online, hlsSource, err := mon.checkStreamStatus(mon.runCtx, "requested_user")
	if err == nil || !strings.Contains(err.Error(), "broadcaster mismatch") {
		t.Fatalf("checkStreamStatus() error = %v", err)
	}
	if online || hlsSource != "" {
		t.Fatalf("unexpected status: online=%v hls=%q", online, hlsSource)
	}
}

func TestCheckStreamStatusDropsHLSURLForNonPublicRoom(t *testing.T) {
	ctx := testContext()
	ctx.Resty.SetTransport(roundTripFunc(func(req *http.Request) (*http.Response, error) {
		return response(req, 200, "text/html", []byte(roomDossierHTML(config.RoomDossier{
			RoomStatus:          "private",
			HlsSource:           "https://edge.example.test/stale.m3u8",
			BroadcasterUsername: "private_user",
		}))), nil
	}))
	mon := New(ctx, []config.ChannelConfig{{Username: "private_user"}})
	online, hlsSource, err := mon.checkStreamStatus(mon.runCtx, "private_user")
	if err != nil {
		t.Fatalf("checkStreamStatus() error: %v", err)
	}
	if online || hlsSource != "" {
		t.Fatalf("unexpected status: online=%v hls=%q", online, hlsSource)
	}
}

func TestMonitorDoneSignal(t *testing.T) {
	mon := New(testContext(), nil)
	select {
	case <-mon.Done():
		t.Fatal("Done() should not be closed before Run()")
	default:
	}
	go mon.Run()
	time.Sleep(10 * time.Millisecond)
	mon.Stop()
	select {
	case <-mon.Done():
	case <-time.After(time.Second):
		t.Fatal("Done() was not closed after Stop()")
	}
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

func roomDossierHTML(dossier config.RoomDossier) string {
	payload, _ := json.Marshal(dossier)
	escaped := strings.Trim(strconv.Quote(string(payload)), `"`)
	return `<script>window.initialRoomDossier = "` + escaped + `";</script>`
}
