# Chaturbate Stream Analysis

## Summary

Analysis performed on:
- **Online**: `https://chaturbate.com/angel_from_sky/`
- **Offline**: `https://chaturbate.com/snow_is_falling/`

Both pages return HTTP 200 with identical headers (Cloudflare CDN). Headers alone cannot distinguish online vs offline status.

---

## 1. Detecting Online vs Offline

### Method: Parse `window.initialRoomDossier`

The HTML page embeds a JavaScript assignment containing a JSON string with all room data:

```html
<script>
  window.initialRoomDossier = "{\u0022viewer_uid\u0022: null, ...}";
</script>
```

**Key**: All JSON characters are Unicode-escaped (`\u0022` = `"`, `\u002D` = `-`, `\u003D` = `=`).

### Detection Strategies

| Field | Online Value | Offline Value |
|-------|-------------|---------------|
| `room_status` | `"public"` | `"offline"` |
| `hls_source` | Full m3u8 URL (1101 chars) | `""` (empty) |
| `num_viewers` | Active count (e.g. 2596) | Low count (e.g. 6) |
| `edge_region` | e.g. `"SIN"` | `""` |

**Recommended approach**: Extract `initialRoomDossier`, decode Unicode escapes, parse JSON, then check `room_status == "public"` **or** `hls_source != ""`. Combining both is most reliable.

### Extraction algorithm (Go):

```
1. Fetch https://chaturbate.com/{username}/ with UA header
2. Find substring: window.initialRoomDossier = "
3. Find closing: "; (double-quote semicolon)
4. Decode \u00XX escape sequences (strconv.Unquote)
5. json.Unmarshal into struct
6. Read RoomStatus field
```

---

## 2. Getting the m3u8 Master Playlist

### URL Extraction

The `hls_source` field in `initialRoomDossier` contains the full master playlist URL:

```
https://edge13-sin.live.mmcdn.com/v1/edge/streams/origin.angel_from_sky.01KS5E9Z9KV6EEK0AYWDBYE06A/llhls.m3u8?token=eyJhbGciOiJSU0EtT0FFUC0yNTYi...
```

### URL Anatomy

| Component | Value | Notes |
|-----------|-------|-------|
| Edge host | `edge{N}-{region}.live.mmcdn.com` | N varies (13, 14...), region from `edge_region` field |
| Stream path | `origin.{username}.{stream_id}` | `stream_id` is a 26-char ULID-like identifier |
| Playlist file | `llhls.m3u8` | LL-HLS (Low-Latency HLS) |
| Auth | `?token={jwe_token}` | JWE-encrypted token, short-lived |

### Token Lifetime

The JWE token in the HTML is short-lived. After initial access with the token, the master playlist returns **session-based** sub-playlist URLs with a `?session={uuid}` parameter. The session UUID persists for the lifetime of the stream viewing session.

---

## 3. Master Playlist Structure

### Multi-Bitrate Variants

The master playlist contains 6 quality levels:

| Chunklist | Resolution | Bitrate | FPS |
|-----------|-----------|---------|-----|
| `chunklist_0_video` | 640x360 | 896 kbps | 30 |
| `chunklist_1_video` | 852x480 | 1296 kbps | 30 |
| `chunklist_2_video` | 960x540 | 2096 kbps | 30 |
| `chunklist_3_video` | 1280x720 | 3296 kbps | 30 |
| `chunklist_4_video` | 1280x720 | 4596 kbps | 60 |
| `chunklist_5_video` | 1920x1080 | 7128 kbps | 30 |

### Audio

Separate audio chunklist, referenced via `#EXT-X-MEDIA` tag with group `audio_aac_{bitrate}`:
- 128 kbps AAC (`audio_aac_128`)
- 96 kbps AAC (`audio_aac_96`)

### Variant Selection Logic

To match the configured resolution, choose the variant with resolution height closest to the target:
- **480p** -> `chunklist_1` (852x480)
- **540p** -> `chunklist_2` (960x540)
- **720p** -> `chunklist_3` (1280x720)
- **1080p** -> `chunklist_5` (1920x1080)

---

## 4. Chunklist (Sub-Playlist) Structure

### Example (540p variant)

```
#EXTINF:1.600000s
/v1/edge/streams/origin.{user}.{id}/seg_2_2005_video_{session_id}_llhls.m4s?session={session}
#EXTINF:1.600000s
/v1/edge/streams/origin.{user}.{id}/seg_2_2006_video_{session_id}_llhls.m4s?session={session}
...
```

### Key Properties

| Property | Value |
|----------|-------|
| Segment format | `.m4s` (fMP4 - fragmented MP4 for LL-HLS) |
| Segment duration | ~1.6 seconds (varies slightly) |
| Segment size | ~400 KB (540p), varies by resolution |
| Window size | ~4 segments (LL-HLS keeps small sliding window) |
| Partial segments | Supported (LL-HLS `#EXT-X-PART` tags may appear) |

### Segment Naming

```
seg_{variant_idx}_{seq_num}_video_{id}_llhls.m4s?session={uuid}
```

- `variant_idx`: Quality level (0-5)
- `seq_num`: Monotonically increasing sequence number
- `id`: Numeric session identifier
- `session`: UUID from master playlist response

### Fetch Strategy

```
1. GET master m3u8 (with Referer header)
2. Parse variants, select target resolution
3. GET chunklist m3u8 every tick interval
4. Download new .m4s segments (keep track of seq_num to avoid duplicates)
5. Append segments to output file
```

---

## 5. HTTP Client Requirements

### Required Headers

```
User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/148.0.0.0 Safari/537.36
Referer: https://chaturbate.com/{username}/
```

### Endpoints

| Purpose | URL | Method |
|---------|-----|--------|
| Get room data | `https://chaturbate.com/{username}/` | GET |
| Master playlist | From `hls_source` field | GET |
| Chunklist | Relative URLs in master | GET (absolute via urljoin) |
| Segments | Relative URLs in chunklist | GET (absolute via urljoin) |

---

## 6. Implementation Notes

### Unicode Decoding

The `initialRoomDossier` string uses `\u00XX` escape sequences for all special characters. In Go, use:

```go
decoded, err := strconv.Unquote(`"` + escapedJSON + `"`)
```

Then `json.Unmarshal` the result.

### Concurrent Safety

- The JWE token in `hls_source` authenticates the master playlist fetch
- After that, all sub-playlists use the `session` UUID
- The session UUID is stable; re-use it for all chunklist/segment requests
- If segments return 403/404, re-fetch the page for a fresh `initialRoomDossier`

### Anti-Blocking

- Use the Chrome UA from AGENTS.md
- Always include `Referer: https://chaturbate.com/{username}/`
- Respect the CDN edge region (don't hardcode edge hostname)
