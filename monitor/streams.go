package monitor

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"recd/config"
)

const statusRequestTimeout = 10 * time.Second

// checkStreamStatus fetches the room dossier and returns the current HLS URL.
// The caller supplies the monitor's cancellable context so shutdown does not
// wait for the HTTP client's timeout.
func (m *Monitor) checkStreamStatus(parent context.Context, username string) (bool, string, error) {
	reqCtx, cancel := context.WithTimeout(parent, statusRequestTimeout)
	defer cancel()

	url := fmt.Sprintf("https://chaturbate.com/%s/", username)
	resp, err := m.ctx.Resty.R().SetContext(reqCtx).Get(url)
	if err != nil {
		return false, "", fmt.Errorf("fetch room page: %w", err)
	}
	if resp.StatusCode() != 200 {
		return false, "", fmt.Errorf("room page HTTP %d", resp.StatusCode())
	}

	dossier, err := parseRoomDossier(resp.String())
	if err != nil {
		return false, "", err
	}
	return dossier.RoomStatus == "public" && dossier.HlsSource != "", dossier.HlsSource, nil
}

func parseRoomDossier(body string) (config.RoomDossier, error) {
	const marker = `initialRoomDossier`
	start := strings.Index(body, marker)
	if start < 0 {
		return config.RoomDossier{}, fmt.Errorf("initialRoomDossier not found")
	}

	assignment := body[start+len(marker):]
	eq := strings.IndexByte(assignment, '=')
	if eq < 0 {
		return config.RoomDossier{}, fmt.Errorf("initialRoomDossier assignment not found")
	}
	quoted := strings.TrimLeft(assignment[eq+1:], " \t\r\n")
	if quoted == "" || quoted[0] != '"' {
		return config.RoomDossier{}, fmt.Errorf("initialRoomDossier quoted value not found")
	}

	end := 1
	for ; end < len(quoted); end++ {
		if quoted[end] == '\\' {
			end++
			continue
		}
		if quoted[end] == '"' {
			break
		}
	}
	if end == len(quoted) {
		return config.RoomDossier{}, fmt.Errorf("initialRoomDossier closing quote not found")
	}

	decoded, err := strconv.Unquote(quoted[:end+1])
	if err != nil {
		return config.RoomDossier{}, fmt.Errorf("unquote room dossier: %w", err)
	}
	var dossier config.RoomDossier
	if err := json.Unmarshal([]byte(decoded), &dossier); err != nil {
		return config.RoomDossier{}, fmt.Errorf("decode room dossier: %w", err)
	}
	return dossier, nil
}
