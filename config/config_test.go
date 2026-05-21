package config

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestParseConfig(t *testing.T) {
	channels := []ChannelConfig{
		{IsPaused: false, Username: "test1", Framerate: 30, Resolution: 720, MaxDuration: 120},
		{IsPaused: true, Username: "test2", Framerate: 60, Resolution: 1080, MaxDuration: 60},
	}

	dir := t.TempDir()
	path := filepath.Join(dir, "channels.json")
	data, err := json.Marshal(channels)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatal(err)
	}

	got, err := ParseConfig(path)
	if err != nil {
		t.Fatalf("ParseConfig() error: %v", err)
	}

	if len(got) != 2 {
		t.Fatalf("expected 2 channels, got %d", len(got))
	}
	if got[0].Username != "test1" {
		t.Errorf("expected username test1, got %s", got[0].Username)
	}
	if !got[1].IsPaused {
		t.Error("expected second channel to be paused")
	}
}

func TestParseConfig_FileNotFound(t *testing.T) {
	_, err := ParseConfig("/nonexistent/path.json")
	if err == nil {
		t.Error("expected error for missing file")
	}
}

func TestParseConfig_InvalidJSON(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "invalid.json")
	if err := os.WriteFile(path, []byte("not json"), 0644); err != nil {
		t.Fatal(err)
	}

	_, err := ParseConfig(path)
	if err == nil {
		t.Error("expected error for invalid JSON")
	}
}

func TestParseHeaders(t *testing.T) {
	headers := map[string]string{
		"User-Agent":    "Mozilla/5.0 Test",
		"Accept":        "application/json",
		"Authorization": "Bearer token123",
	}

	dir := t.TempDir()
	path := filepath.Join(dir, "headers.json")
	data, err := json.Marshal(headers)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatal(err)
	}

	got, err := ParseHeaders(path)
	if err != nil {
		t.Fatalf("ParseHeaders() error: %v", err)
	}

	if len(got) != 3 {
		t.Fatalf("expected 3 headers, got %d", len(got))
	}
	if got["User-Agent"] != "Mozilla/5.0 Test" {
		t.Errorf("expected User-Agent 'Mozilla/5.0 Test', got %q", got["User-Agent"])
	}
	if got["Authorization"] != "Bearer token123" {
		t.Errorf("expected Authorization 'Bearer token123', got %q", got["Authorization"])
	}
}

func TestParseHeaders_FileNotFound(t *testing.T) {
	_, err := ParseHeaders("/nonexistent/headers.json")
	if err == nil {
		t.Error("expected error for missing file")
	}
}

func TestParseHeaders_InvalidJSON(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "invalid.json")
	if err := os.WriteFile(path, []byte("not json"), 0644); err != nil {
		t.Fatal(err)
	}

	_, err := ParseHeaders(path)
	if err == nil {
		t.Error("expected error for invalid JSON")
	}
}

func TestRoomDossier_Unmarshal(t *testing.T) {
	jsonData := `{
		"room_status": "public",
		"hls_source": "https://edge1-sin.live.mmcdn.com/v1/edge/streams/origin.test_user.01ABCDE/llhls.m3u8?token=abc",
		"broadcaster_username": "test_user",
		"num_viewers": 1234
	}`
	var d RoomDossier
	if err := json.Unmarshal([]byte(jsonData), &d); err != nil {
		t.Fatalf("unmarshal error: %v", err)
	}
	if d.RoomStatus != "public" {
		t.Errorf("expected RoomStatus 'public', got %q", d.RoomStatus)
	}
	if d.HlsSource == "" {
		t.Error("expected non-empty HlsSource")
	}
	if d.BroadcasterUsername != "test_user" {
		t.Errorf("expected BroadcasterUsername 'test_user', got %q", d.BroadcasterUsername)
	}
	if d.NumViewers != 1234 {
		t.Errorf("expected NumViewers 1234, got %d", d.NumViewers)
	}
}

func TestRoomDossier_Offline(t *testing.T) {
	jsonData := `{
		"room_status": "offline",
		"hls_source": "",
		"broadcaster_username": "offline_user",
		"num_viewers": 6
	}`
	var d RoomDossier
	if err := json.Unmarshal([]byte(jsonData), &d); err != nil {
		t.Fatalf("unmarshal error: %v", err)
	}
	if d.RoomStatus != "offline" {
		t.Errorf("expected RoomStatus 'offline', got %q", d.RoomStatus)
	}
	if d.HlsSource != "" {
		t.Errorf("expected empty HlsSource for offline, got %q", d.HlsSource)
	}
}

func TestParseHeaders_EmptyFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "empty.json")
	if err := os.WriteFile(path, []byte("{}"), 0644); err != nil {
		t.Fatal(err)
	}

	got, err := ParseHeaders(path)
	if err != nil {
		t.Fatalf("ParseHeaders() unexpected error: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("expected 0 headers, got %d", len(got))
	}
}

func TestExpandPattern(t *testing.T) {
	vars := PathVars("testuser", time.Date(2026, 5, 22, 14, 30, 0, 0, time.UTC), 0)
	pattern := "videos/{{.Username}}_{{.Year}}-{{.Month}}-{{.Day}}_{{.Hour}}-{{.Minute}}-{{.Second}}"

	got, err := ExpandPattern(pattern, vars)
	if err != nil {
		t.Fatalf("ExpandPattern error: %v", err)
	}
	expected := "videos/testuser_2026-05-22_14-30-00"
	if got != expected {
		t.Errorf("expected %q, got %q", expected, got)
	}
}

func TestExpandPattern_WithSequence(t *testing.T) {
	vars := PathVars("testuser", time.Date(2026, 5, 22, 14, 30, 0, 0, time.UTC), 3)
	pattern := "videos/{{.Username}}_{{.Year}}-{{.Month}}-{{.Day}}_{{.Hour}}-{{.Minute}}-{{.Second}}{{if .Sequence}}_{{.Sequence}}{{end}}"

	got, err := ExpandPattern(pattern, vars)
	if err != nil {
		t.Fatalf("ExpandPattern error: %v", err)
	}
	if !strings.Contains(got, "_3") {
		t.Errorf("expected sequence suffix in %q", got)
	}
}

func TestExpandPattern_NoSequenceWhenZero(t *testing.T) {
	vars := PathVars("testuser", time.Date(2026, 5, 22, 14, 30, 0, 0, time.UTC), 0)
	pattern := "videos/{{.Username}}_{{.Year}}-{{.Month}}-{{.Day}}_{{.Hour}}-{{.Minute}}-{{.Second}}{{if .Sequence}}_{{.Sequence}}{{end}}"

	got, err := ExpandPattern(pattern, vars)
	if err != nil {
		t.Fatalf("ExpandPattern error: %v", err)
	}
	if strings.Contains(got, "_0") {
		t.Errorf("expected no sequence suffix when seq=0, got %q", got)
	}
}
