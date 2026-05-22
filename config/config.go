package config

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"text/template"
	"time"
)

type ChannelConfig struct {
	IsPaused    bool   `json:"is_paused"`
	Username    string `json:"username"`
	Framerate   int    `json:"framerate"`
	Resolution  int    `json:"resolution"`
	Pattern     string `json:"pattern"`
	MaxDuration int    `json:"max_duration"`
	MaxFilesize int64  `json:"max_filesize"`
	CreatedAt   int64  `json:"created_at"`
}

// ComputeDelta compares old and new channel configs and returns the channels
// that changed. Channels added or modified appear with their new config.
// Channels that exist in old but not in new appear with IsPaused=true.
func ComputeDelta(old, new []ChannelConfig) []ChannelConfig {
	oldMap := make(map[string]ChannelConfig, len(old))
	for _, c := range old {
		oldMap[c.Username] = c
	}
	newMap := make(map[string]ChannelConfig, len(new))
	for _, c := range new {
		newMap[c.Username] = c
	}

	var delta []ChannelConfig

	// Added or changed: present in new config.
	for _, newCfg := range new {
		oldCfg, existed := oldMap[newCfg.Username]
		if !existed || oldCfg != newCfg {
			delta = append(delta, newCfg)
		}
	}

	// Removed: present in old but missing from new. Mark as paused.
	for _, oldCfg := range old {
		if _, exists := newMap[oldCfg.Username]; !exists {
			cfg := oldCfg
			cfg.IsPaused = true
			delta = append(delta, cfg)
		}
	}

	return delta
}

type RoomDossier struct {
	RoomStatus          string `json:"room_status"`
	HlsSource           string `json:"hls_source"`
	BroadcasterUsername string `json:"broadcaster_username"`
	NumViewers          int    `json:"num_viewers"`
}

// PatternVars holds the template variables for file naming pattern expansion.
type PatternVars struct {
	Username string
	Year     string
	Month    string
	Day      string
	Hour     string
	Minute   string
	Second   string
	Sequence int
}

// ExpandPattern renders a Go template pattern into a file path.
// The pattern uses Go template syntax, e.g.:
//
//	videos/{{.Username}}_{{.Year}}-{{.Month}}-{{.Day}}_{{.Hour}}-{{.Minute}}-{{.Second}}{{if .Sequence}}_{{.Sequence}}{{end}}
func ExpandPattern(pattern string, vars PatternVars) (string, error) {
	tmpl, err := template.New("path").Parse(pattern)
	if err != nil {
		return "", fmt.Errorf("pattern parse error: %w", err)
	}
	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, vars); err != nil {
		return "", fmt.Errorf("pattern exec error: %w", err)
	}
	return buf.String(), nil
}

// PathVars creates PatternVars from a username and an optional start time.
// Sequence 0 means no sequence suffix is rendered.
func PathVars(username string, t time.Time, seq int) PatternVars {
	return PatternVars{
		Username: username,
		Year:     fmt.Sprintf("%04d", t.Year()),
		Month:    fmt.Sprintf("%02d", int(t.Month())),
		Day:      fmt.Sprintf("%02d", t.Day()),
		Hour:     fmt.Sprintf("%02d", t.Hour()),
		Minute:   fmt.Sprintf("%02d", t.Minute()),
		Second:   fmt.Sprintf("%02d", t.Second()),
		Sequence: seq,
	}
}

func ParseConfig(path string) ([]ChannelConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var configs []ChannelConfig
	if err := json.Unmarshal(data, &configs); err != nil {
		return nil, err
	}
	return configs, nil
}

func ParseHeaders(path string) (map[string]string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var headers map[string]string
	if err := json.Unmarshal(data, &headers); err != nil {
		return nil, err
	}
	return headers, nil
}
