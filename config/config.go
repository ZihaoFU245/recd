package config

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"strings"
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

// PathPattern is a compiled output path template. Reusing it avoids parsing the
// same pattern for every filename collision check.
type PathPattern struct {
	tmpl *template.Template
}

func CompilePathPattern(pattern string) (*PathPattern, error) {
	tmpl, err := template.New("path").Parse(pattern)
	if err != nil {
		return nil, fmt.Errorf("pattern parse error: %w", err)
	}
	return &PathPattern{tmpl: tmpl}, nil
}

func (p *PathPattern) Expand(vars PatternVars) (string, error) {
	var buf bytes.Buffer
	if err := p.tmpl.Execute(&buf, vars); err != nil {
		return "", fmt.Errorf("pattern exec error: %w", err)
	}
	return buf.String(), nil
}

// ExpandPattern renders a Go template pattern into a file path.
// The pattern uses Go template syntax, e.g.:
//
//	videos/{{.Username}}_{{.Year}}-{{.Month}}-{{.Day}}_{{.Hour}}-{{.Minute}}-{{.Second}}{{if .Sequence}}_{{.Sequence}}{{end}}
func ExpandPattern(pattern string, vars PatternVars) (string, error) {
	compiled, err := CompilePathPattern(pattern)
	if err != nil {
		return "", err
	}
	return compiled.Expand(vars)
}

// PathVars creates PatternVars from a username and an optional start time.
// Sequence 0 means no sequence suffix is rendered.
func PathVars(username string, t time.Time, seq int) PatternVars {
	return PatternVars{
		Username: username,
		Year:     t.Format("2006"),
		Month:    t.Format("01"),
		Day:      t.Format("02"),
		Hour:     t.Format("15"),
		Minute:   t.Format("04"),
		Second:   t.Format("05"),
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
	if err := ValidateConfigs(configs); err != nil {
		return nil, err
	}
	return configs, nil
}

func ValidateConfigs(configs []ChannelConfig) error {
	seen := make(map[string]struct{}, len(configs))
	for index, cfg := range configs {
		if !validUsername(cfg.Username) {
			return fmt.Errorf(
				"channel %d has invalid username %q: use only ASCII letters, digits, and underscores",
				index,
				cfg.Username,
			)
		}
		key := strings.ToLower(cfg.Username)
		if _, duplicate := seen[key]; duplicate {
			return fmt.Errorf("channel %d duplicates username %q", index, cfg.Username)
		}
		seen[key] = struct{}{}

		if cfg.Resolution < 0 {
			return fmt.Errorf("channel %q has negative resolution", cfg.Username)
		}
		if cfg.Framerate < 0 {
			return fmt.Errorf("channel %q has negative framerate", cfg.Username)
		}
		if cfg.MaxDuration < 0 {
			return fmt.Errorf("channel %q has negative max_duration", cfg.Username)
		}
		if cfg.MaxFilesize < 0 {
			return fmt.Errorf("channel %q has negative max_filesize", cfg.Username)
		}
		if cfg.IsPaused {
			continue
		}
		if strings.TrimSpace(cfg.Pattern) == "" {
			return fmt.Errorf("channel %q has an empty output pattern", cfg.Username)
		}
		if _, err := CompilePathPattern(cfg.Pattern); err != nil {
			return fmt.Errorf("channel %q has an invalid output pattern: %w", cfg.Username, err)
		}
	}
	return nil
}

func validUsername(username string) bool {
	if username == "" {
		return false
	}
	for _, r := range username {
		if r >= 'a' && r <= 'z' ||
			r >= 'A' && r <= 'Z' ||
			r >= '0' && r <= '9' ||
			r == '_' {
			continue
		}
		return false
	}
	return true
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
