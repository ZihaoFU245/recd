package monitor

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"recd/config"
)

// checkStreamStatus fetches the chaturbate page for the given username,
// extracts the window.initialRoomDossier JSON, decodes unicode escapes,
// and returns whether the stream is online along with the hls_source URL.
func (m *Monitor) checkStreamStatus(username string) (online bool, hlsSource string, err error) {
	url := fmt.Sprintf("https://chaturbate.com/%s/", username)

	reqCtx, finish := m.requestContext()
	defer finish()

	resp, err := m.ctx.Resty.R().SetContext(reqCtx).Get(url)
	if err != nil {
		if reqCtx.Err() != nil {
			m.ctx.Logger.Debug("stream status check canceled", "username", username)
			return false, "", reqCtx.Err()
		}
		m.ctx.Logger.Error("failed to fetch page", "username", username, "error", err)
		return false, "", err
	}
	if resp.StatusCode() != 200 {
		m.ctx.Logger.Error("non-200 page response", "username", username, "status", resp.StatusCode())
		return false, "", fmt.Errorf("page HTTP %d", resp.StatusCode())
	}

	dossier, err := parseRoomDossier(resp.String())
	if err != nil {
		m.ctx.Logger.Error("failed to parse room dossier JSON", "username", username, "error", err)
		return false, "", err
	}

	online = dossier.RoomStatus == "public" && dossier.HlsSource != ""
	if online {
		m.ctx.Logger.Info("stream is online",
			"username", username,
			"viewers", dossier.NumViewers,
		)
	} else {
		m.ctx.Logger.Info("stream is offline", "username", username, "status", dossier.RoomStatus)
	}

	return online, dossier.HlsSource, nil
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

	escapedJSON, err := readQuotedJSString(quoted)
	if err != nil {
		return config.RoomDossier{}, err
	}

	decoded, err := strconv.Unquote(`"` + escapedJSON + `"`)
	if err != nil {
		return config.RoomDossier{}, fmt.Errorf("unquote room dossier: %w", err)
	}

	var dossier config.RoomDossier
	if err := json.Unmarshal([]byte(decoded), &dossier); err != nil {
		return config.RoomDossier{}, fmt.Errorf("decode room dossier: %w", err)
	}
	return dossier, nil
}

func readQuotedJSString(s string) (string, error) {
	for i := 1; i < len(s); i++ {
		switch s[i] {
		case '\\':
			i++
		case '"':
			return s[1:i], nil
		}
	}
	return "", fmt.Errorf("initialRoomDossier closing quote not found")
}

func (m *Monitor) requestContext() (context.Context, func()) {
	reqCtx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})

	go func() {
		select {
		case <-m.stopCh:
			cancel()
		case <-done:
		}
	}()

	return reqCtx, func() {
		close(done)
		cancel()
	}
}
