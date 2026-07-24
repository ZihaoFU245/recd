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
		{IsPaused: false, Username: "test1", Framerate: 30, Resolution: 720, Pattern: "videos/{{.Username}}", MaxDuration: 120},
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

func TestValidateConfigsRejectsInvalidRecordingInputs(t *testing.T) {
	valid := ChannelConfig{
		Username:   "valid_user1",
		Resolution: 720,
		Framerate:  30,
		Pattern:    "videos/{{.Username}}",
	}
	tests := []struct {
		name    string
		configs []ChannelConfig
		want    string
	}{
		{
			name:    "hyphenated username",
			configs: []ChannelConfig{{Username: "arcadian-platypus", Pattern: valid.Pattern}},
			want:    "invalid username",
		},
		{
			name:    "duplicate username",
			configs: []ChannelConfig{valid, {Username: "VALID_USER1", Pattern: valid.Pattern}},
			want:    "duplicates username",
		},
		{
			name:    "empty pattern",
			configs: []ChannelConfig{{Username: "valid_user"}},
			want:    "empty output pattern",
		},
		{
			name:    "invalid pattern",
			configs: []ChannelConfig{{Username: "valid_user", Pattern: "{{"}},
			want:    "invalid output pattern",
		},
		{
			name:    "negative duration",
			configs: []ChannelConfig{{Username: "valid_user", Pattern: valid.Pattern, MaxDuration: -1}},
			want:    "negative max_duration",
		},
		{
			name:    "negative filesize",
			configs: []ChannelConfig{{Username: "valid_user", Pattern: valid.Pattern, MaxFilesize: -1}},
			want:    "negative max_filesize",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := ValidateConfigs(test.configs)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("ValidateConfigs() error = %v, want text %q", err, test.want)
			}
		})
	}
}

func TestValidateConfigsAllowsPausedEntryWithoutPattern(t *testing.T) {
	if err := ValidateConfigs([]ChannelConfig{{Username: "paused_user", IsPaused: true}}); err != nil {
		t.Fatalf("ValidateConfigs() error: %v", err)
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

func TestComputeDelta_NoChange(t *testing.T) {
	old := []ChannelConfig{
		{IsPaused: false, Username: "user1", Resolution: 480},
	}
	new := []ChannelConfig{
		{IsPaused: false, Username: "user1", Resolution: 480},
	}
	delta := ComputeDelta(old, new)
	if len(delta) != 0 {
		t.Errorf("expected empty delta, got %d entries", len(delta))
	}
}

func TestComputeDelta_Added(t *testing.T) {
	old := []ChannelConfig{
		{IsPaused: false, Username: "user1", Resolution: 480},
	}
	new := []ChannelConfig{
		{IsPaused: false, Username: "user1", Resolution: 480},
		{IsPaused: false, Username: "user2", Resolution: 720},
	}
	delta := ComputeDelta(old, new)
	if len(delta) != 1 {
		t.Fatalf("expected 1 delta entry, got %d", len(delta))
	}
	if delta[0].Username != "user2" {
		t.Errorf("expected user2, got %s", delta[0].Username)
	}
}

func TestComputeDelta_Removed(t *testing.T) {
	old := []ChannelConfig{
		{IsPaused: false, Username: "user1", Resolution: 480},
		{IsPaused: false, Username: "user2", Resolution: 720},
	}
	new := []ChannelConfig{
		{IsPaused: false, Username: "user1", Resolution: 480},
	}
	delta := ComputeDelta(old, new)
	if len(delta) != 1 {
		t.Fatalf("expected 1 delta entry, got %d", len(delta))
	}
	if delta[0].Username != "user2" {
		t.Errorf("expected user2, got %s", delta[0].Username)
	}
	if !delta[0].IsPaused {
		t.Error("expected removed channel to be marked IsPaused=true")
	}
}

func TestComputeDelta_Changed(t *testing.T) {
	old := []ChannelConfig{
		{IsPaused: false, Username: "user1", Resolution: 480, MaxDuration: 60},
	}
	new := []ChannelConfig{
		{IsPaused: false, Username: "user1", Resolution: 720, MaxDuration: 120},
	}
	delta := ComputeDelta(old, new)
	if len(delta) != 1 {
		t.Fatalf("expected 1 delta entry, got %d", len(delta))
	}
	if delta[0].Username != "user1" {
		t.Errorf("expected user1, got %s", delta[0].Username)
	}
	if delta[0].Resolution != 720 {
		t.Errorf("expected resolution 720, got %d", delta[0].Resolution)
	}
}

func TestComputeDelta_PausedToActive(t *testing.T) {
	old := []ChannelConfig{
		{IsPaused: true, Username: "user1", Resolution: 480},
	}
	new := []ChannelConfig{
		{IsPaused: false, Username: "user1", Resolution: 480},
	}
	delta := ComputeDelta(old, new)
	if len(delta) != 1 {
		t.Fatalf("expected 1 delta entry, got %d", len(delta))
	}
	if delta[0].IsPaused {
		t.Error("expected IsPaused=false")
	}
}

func TestComputeDelta_ActiveToPaused(t *testing.T) {
	old := []ChannelConfig{
		{IsPaused: false, Username: "user1", Resolution: 480},
	}
	new := []ChannelConfig{
		{IsPaused: true, Username: "user1", Resolution: 480},
	}
	delta := ComputeDelta(old, new)
	if len(delta) != 1 {
		t.Fatalf("expected 1 delta entry, got %d", len(delta))
	}
	if !delta[0].IsPaused {
		t.Error("expected IsPaused=true")
	}
}

func TestComputeDelta_Mixed(t *testing.T) {
	old := []ChannelConfig{
		{IsPaused: false, Username: "user1", Resolution: 480},
		{IsPaused: true, Username: "user2", Resolution: 720},
		{IsPaused: false, Username: "user3", Resolution: 1080},
	}
	new := []ChannelConfig{
		{IsPaused: false, Username: "user1", Resolution: 720},
		{IsPaused: false, Username: "user2", Resolution: 720},
		{IsPaused: false, Username: "user4", Resolution: 360},
	}
	delta := ComputeDelta(old, new)
	if len(delta) != 4 {
		t.Fatalf("expected 4 delta entries (user1 changed, user2 unpaused, user3 removed, user4 added), got %d", len(delta))
	}

	find := func(username string) *ChannelConfig {
		for i, d := range delta {
			if d.Username == username {
				return &delta[i]
			}
		}
		return nil
	}

	if d := find("user1"); d == nil || d.Resolution != 720 || d.IsPaused {
		t.Error("user1 should be changed (resolution 720, not paused)")
	}
	if d := find("user2"); d == nil || d.IsPaused {
		t.Error("user2 should be unpaused")
	}
	if d := find("user3"); d == nil || !d.IsPaused {
		t.Error("user3 should be removed (IsPaused=true)")
	}
	if d := find("user4"); d == nil || d.Resolution != 360 || d.IsPaused {
		t.Error("user4 should be added")
	}
}
