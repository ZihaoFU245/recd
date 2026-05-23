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

---

## 7. Worker Recorder Process

The worker side is the `channel` package. The monitor starts one `Channel`
goroutine per online target, and that goroutine owns exactly one recording
session for the configured username.

### Lifecycle

1. `monitor.startChannelLocked` creates `channel.New(ctx, cfg, hlsSource, resultCh)`.
2. `Channel.Run` marks the channel active and calls `record(...)`.
3. `record` chooses a non-conflicting output path from the configured pattern,
   fetches the master playlist from `hls_source`, selects the closest video
   variant to `cfg.Resolution`, and checks whether the selected variant has a
   separate audio rendition.
4. The recorder runs one of two capture paths:
   - `recordWithFFmpegHLS` for a single HLS media playlist. `ffmpeg` reads the
     playlist directly with the required UA and `Referer` headers and remuxes to
     MKV with `-c copy`.
   - `recordWithTempFiles` for separate audio/video fMP4 playlists. The worker
     polls both media playlists, downloads only unseen segments, writes each
     track to a temp file, then merges the temp video/audio files into the final
     MKV with `ffmpeg`.
5. When recording ends, `Channel.Run` sends a `Result` back to the monitor.
   Normal end states are `completed`, `max_duration`, and `max_filesize`.
   Unexpected recorder failures use `error`; excessive audio/video drift uses
   `desync`.

### Stop and Reload

- `Channel.Stop` closes `stopCh`. The worker exits gracefully, finalizes any
  mergeable media, and returns `StatusCompleted`.
- `Channel.Reload` sends a non-blocking signal on `reloadCh`. The worker exits
  with `Reloaded=true` so the monitor does not delete or retry a newly spawned
  replacement channel.
- Initial master-playlist fetching is cancellable. If the monitor stops or
  reloads while that HTTP request is in flight, the request context is canceled
  and the worker exits instead of hanging inside Resty.

### Segment Handling

- Each `trackState` stores the media playlist URL, output writer, last sequence
  number, init-segment state, cumulative duration, and first/last
  `EXT-X-PROGRAM-DATE-TIME`.
- `alignInitialTracks` uses program date times to choose the closest initial
  audio/video segment pair before any bytes are written. This reduces startup
  A/V offset for separate tracks.
- `processTrackSegments` skips already-recorded sequence numbers and errors if
  the next visible sequence jumps ahead, because that means the LL-HLS sliding
  window moved before the worker downloaded all segments.
- Optional segment caching is controlled by `RECD_SEGMENT_CACHE_DIR`; when set,
  downloaded init segments and media segments are written with a JSONL manifest
  containing track, sequence, URI, size, hash, duration, and program time.

### Bugs Fixed in This Pass

- Shared Resty clients now use the required Chrome User-Agent by default, while
  still allowing `--additional-headers` to override it.
- Resty now has a finite timeout, and monitor stream-status checks are
  cancellable on shutdown.
- Channel startup no longer hangs forever if `Stop` or `Reload` happens during
  the initial master-playlist request.
- Recorder output paths now use the existing `.Sequence` template variable when
  the base output file already exists, avoiding accidental overwrites on quick
  respawns.
- The temp-file merge command now passes `-y` before the output path.
- Removed the unused `downloadToWriter` helper.
- The real-segment verifier no longer requires a missing `headers.json` file;
  the application default UA now covers that requirement.
