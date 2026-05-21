package config

import (
	"encoding/json"
	"os"
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

type RoomDossier struct {
	RoomStatus          string `json:"room_status"`
	HlsSource           string `json:"hls_source"`
	BroadcasterUsername string `json:"broadcaster_username"`
	NumViewers          int    `json:"num_viewers"`
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
