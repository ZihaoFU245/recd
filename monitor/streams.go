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
	m.ctx.Logger.Debug("room page fetched",
		"username", username,
		"status", resp.StatusCode(),
		"bytes", len(resp.Body()),
	)
	if resp.StatusCode() != 200 {
		return false, "", fmt.Errorf("room page HTTP %d", resp.StatusCode())
	}

	dossier, err := parseRoomDossier(resp.String())
	if err != nil {
		return false, "", err
	}
	m.ctx.Logger.Debug("room dossier parsed",
		"username", username,
		"broadcaster", dossier.BroadcasterUsername,
		"status", dossier.RoomStatus,
		"viewers", dossier.NumViewers,
		"hls_source", dossier.HlsSource != "",
	)
	if !strings.EqualFold(dossier.BroadcasterUsername, username) {
		return false, "", fmt.Errorf(
			"room page broadcaster mismatch: requested %q, got %q",
			username,
			dossier.BroadcasterUsername,
		)
	}
	if dossier.RoomStatus != "public" || dossier.HlsSource == "" {
		return false, "", nil
	}
	return true, dossier.HlsSource, nil
}

func parseRoomDossier(body string) (config.RoomDossier, error) {
	const marker = `initialRoomDossier`
	searchFrom := 0
	for {
		relativeStart := strings.Index(body[searchFrom:], marker)
		if relativeStart < 0 {
			break
		}
		start := searchFrom + relativeStart
		searchFrom = start + len(marker)
		if start > 0 && isJSIdentifierByte(body[start-1]) {
			continue
		}
		if searchFrom < len(body) && isJSIdentifierByte(body[searchFrom]) {
			continue
		}

		assignment := strings.TrimLeft(body[searchFrom:], " \t\r\n")
		if assignment == "" || assignment[0] != '=' {
			continue
		}
		quoted := strings.TrimLeft(assignment[1:], " \t\r\n")
		if quoted == "" || quoted[0] != '"' {
			continue
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
	return config.RoomDossier{}, fmt.Errorf("initialRoomDossier assignment not found")
}

func isJSIdentifierByte(value byte) bool {
	return value >= 'a' && value <= 'z' ||
		value >= 'A' && value <= 'Z' ||
		value >= '0' && value <= '9' ||
		value == '_' ||
		value == '$'
}
