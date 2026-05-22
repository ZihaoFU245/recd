package monitor

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"recd/config"
)

// checkStreamStatus fetches the chaturbate page for the given username,
// extracts the window.initialRoomDossier JSON, decodes unicode escapes,
// and returns whether the stream is online along with the hls_source URL.
func (m *Monitor) checkStreamStatus(username string) (online bool, hlsSource string) {
	url := fmt.Sprintf("https://chaturbate.com/%s/", username)

	resp, err := m.ctx.Resty.R().Get(url)
	if err != nil {
		m.ctx.Logger.Error("failed to fetch page", "username", username, "error", err)
		return false, ""
	}
	if resp.StatusCode() != 200 {
		m.ctx.Logger.Error("non-200 page response", "username", username, "status", resp.StatusCode())
		return false, ""
	}

	body := resp.String()

	// Extract: window.initialRoomDossier = "{escaped JSON}";
	const marker = `initialRoomDossier = "`
	start := strings.Index(body, marker)
	if start < 0 {
		m.ctx.Logger.Error("initialRoomDossier not found in page", "username", username)
		return false, ""
	}
	start += len(marker)
	end := strings.Index(body[start:], `";`)
	if end < 0 {
		m.ctx.Logger.Error("initialRoomDossier closing marker not found", "username", username)
		return false, ""
	}
	escapedJSON := body[start : start+end]

	// Decode Go-style unicode escape sequences (\u0022 -> ", \u002D -> -, etc.).
	decoded, err := strconv.Unquote(`"` + escapedJSON + `"`)
	if err != nil {
		m.ctx.Logger.Error("failed to unquote room dossier", "username", username, "error", err)
		return false, ""
	}

	var dossier config.RoomDossier
	if err := json.Unmarshal([]byte(decoded), &dossier); err != nil {
		m.ctx.Logger.Error("failed to parse room dossier JSON", "username", username, "error", err)
		return false, ""
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

	return online, dossier.HlsSource
}
