# Chaturbate Stream Discovery and HLS Analysis

## Verification scope

This document was re-verified on **2026-07-24** with `curl` and the required
user agent:

```text
Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/150.0.0.0 Safari/537.36
```

Observed fixtures and live test rooms:

| Role | Room | Observation |
|---|---|---|
| Offline fixture | `snow_is_falling` | `room_status="offline"`, empty `hls_source` |
| Configured online fixture | `angel_from_sky` | Offline during this verification, so it could not validate live HLS |
| Requested sample | `arcadian-platypus` | Invalid room identifier: Chaturbate usernames use letters, digits, and underscores; the hyphenated path returned 404 |
| Live test | `ms_dira` | Public; room, master, video/audio playlists, init objects, and first complete segments returned HTTP 200 |
| Live test | `onlykira` | Public; room, master, video/audio playlists, init objects, and first complete segments returned HTTP 200 |
| Live test | `telladreamer_` | Public; room, master, video/audio playlists, init objects, and first complete segments returned HTTP 200 |

The room-list endpoint used to find a live target was:

```text
GET https://chaturbate.com/api/ts/roomlist/room-list/
X-Requested-With: XMLHttpRequest
Referer: https://chaturbate.com/
```

Room availability, viewer counts, edge selection, stream IDs, renditions,
bitrates, and playlist contents are transient. The tables below describe the
verification snapshot, not a fixed service contract. No authentication token
or session value is retained in this document.

The valid room pages returned HTTP 200. Their response headers were
not byte-for-byte identical (cookies and request-specific values differed), but
ordinary status and response headers did not reveal whether a room was live.
The HTML dossier is the useful source of room state.

## 1. Detecting whether a room is recordable

### `window.initialRoomDossier`

The room HTML embeds a JavaScript string literal whose decoded value is JSON:

```html
<script>
  window.initialRoomDossier = "{\u0022room_status\u0022: \u0022public\u0022, ...}";
</script>
```

This is two decoding layers:

1. Parse the JavaScript-compatible quoted string.
2. Parse the resulting text as JSON.

Quotes and several HTML-sensitive characters are represented with `\u00XX`
escapes, but it is inaccurate to say that every JSON character is Unicode
escaped. Do not implement decoding as a global replacement of `\u00XX`
sequences.

The closing quote must be found with an escape-aware scan. Searching for the
first literal `";` is unsafe because a JSON string value can itself contain an
escaped quote followed by a semicolon. The implementation in
`monitor/streams.go` correctly skips escaped characters and then uses
`strconv.Unquote` followed by `json.Unmarshal`.

### Start condition

Fields relevant to recording are:

| Field | Recordable public room | Offline room | Use |
|---|---|---|---|
| `room_status` | `"public"` | `"offline"` | Primary state |
| `hls_source` | Authenticated master URL | `""` | Required input |
| `num_viewers` | Any non-negative count | Often a small count | Metadata only |
| `edge_region` | An edge code in this snapshot | `""` in both offline fixtures | Diagnostic metadata |

Start a recorder only when both conditions hold:

```text
room_status == "public" AND hls_source is not empty
```

Using `OR` is unsafe: a non-public state must not be treated as recordable
merely because a URL happens to be present, and `"public"` without a usable URL
cannot be recorded. Viewer count and HTTP headers are not status indicators.

Recommended probe flow:

```text
1. GET https://chaturbate.com/{username}/ with the configured User-Agent.
2. Require HTTP 200.
3. Locate the initialRoomDossier assignment.
4. Scan and decode its quoted JavaScript string.
5. JSON-decode the result.
6. Require room_status == "public" and hls_source != "".
```

## 2. Master URL, token, and session behavior

The live snapshot supplied a URL shaped like:

```text
https://edge22-mad.live.mmcdn.com/v1/edge/streams/
origin.{username}.{stream_id}/llhls.m3u8?token={redacted}
```

Observed components:

| Component | Observation |
|---|---|
| Host | `edge{number}-{region}.live.mmcdn.com`; both values may change |
| Stream path | `origin.{username}.{stream_id}` |
| Playlist | `llhls.m3u8` |
| Query credential | `token={...}` |

The token had five compact-JWE parts. Its decoded protected header declared
`alg=RSA-OAEP-256` and `enc=A256GCM`. Treat the whole `hls_source` as an opaque
credential: do not log it, persist it, edit it, or construct it manually.

Two consecutive room-page fetches for the same live stream returned the same
stream path but different tokens. The first GET of a fresh master URL returned
HTTP 200 and child playlist URLs containing an opaque `session` query
parameter. Repeating the same tokenized master request returned:

```text
HTTP 403
x-reason: w3: session_duplicated
```

Therefore:

- Consume a tokenized master URL once.
- Resolve and retain the child playlist URLs returned by that successful
  response; all entries in one master snapshot used the same session value.
- A new room-page/master-token exchange created a different session.
- Do not claim a fixed token or session lifetime from these observations.
- If master acquisition fails, or an established session becomes unusable,
  fetch the room page again and establish a fresh session. Do not retry the
  same master token indefinitely.

## 3. Master playlist

The verified master was HLS version 6 with separate audio renditions:

```text
#EXTM3U
#EXT-X-VERSION:6
#EXT-X-INDEPENDENT-SEGMENTS
#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID="audio_aac_128",...
#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID="audio_aac_96",...
#EXT-X-STREAM-INF:...,RESOLUTION=640x360,...,AUDIO="audio_aac_96"
...
```

### 2026-07-23 snapshot

| Video index | Resolution | Declared bandwidth | Frame rate | Audio group |
|---:|---:|---:|---:|---|
| 0 | 640x360 | 896,000 | 30.020 | `audio_aac_96` |
| 1 | 960x540 | 1,696,000 | 30.020 | `audio_aac_96` |
| 2 | 1280x720 | 3,096,000 | 30.020 | `audio_aac_96` |
| 3 | 1280x720 | 4,096,000 | 60.039 | `audio_aac_96` |
| 4 | 1920x1080 | 7,128,000 | 60.039 | `audio_aac_128` |

This contradicts the earlier six-rendition table: this stream had no 480p
variant, used different bandwidths, and exposed 1080p60 rather than 1080p30.
Another broadcaster or encoder configuration may expose a different ladder.

Never infer quality from `chunklist_N` alone. Parse every
`#EXT-X-STREAM-INF`, select using its declared resolution/frame rate/bandwidth,
and then follow the referenced audio group. Resolve child URIs against the
master URL with a URL resolver rather than string concatenation.

## 4. Media playlists and segments

The selected 540p video and 96-kbit audio playlists both returned HTTP 200.
The video snapshot contained:

```text
#EXT-X-TARGETDURATION:2
#EXT-X-SERVER-CONTROL:CAN-BLOCK-RELOAD=YES,PART-HOLD-BACK=2.430000
#EXT-X-PART-INF:PART-TARGET=0.799000
#EXT-X-MEDIA-SEQUENCE:{sequence}
#EXT-X-MAP:URI="...init...m4s?session={redacted}"
#EXT-X-PROGRAM-DATE-TIME:...
#EXTINF:1.632000,
...seg...m4s?session={redacted}
#EXT-X-PART:DURATION=0.799000,URI="...part...m4s?session={redacted}"
#EXT-X-PRELOAD-HINT:TYPE=PART,URI="...part...m4s?session={redacted}"
```

Observed properties:

| Property | Snapshot observation |
|---|---|
| Complete segment format | Fragmented MP4 (`.m4s`), not MPEG-TS (`.ts`) |
| Initialization | Separate `#EXT-X-MAP` object, required before media fragments |
| Complete segment duration | About 1.6 seconds |
| Partial segment duration | About 0.8 seconds, with occasional shorter tail parts |
| Complete-segment window | Four segments in the sampled video playlist |
| Timing | `#EXT-X-PROGRAM-DATE-TIME` on complete segments |
| Synchronization | Separate audio and video playlists with close but non-identical boundaries |

A freshly selected 540p segment was 330,652 bytes and had a `video/mp4`
response; prepending its 671-byte init object allowed
`ffprobe` to identify H.264 at 960x540 and 30 fps. Segment size varies with
content and rendition, so it is not safe to assume a fixed 400 KB size.

The sliding window is short. A segment from an earlier playlist returned 403
after it had left the active window, while a segment selected immediately from
the refreshed playlist returned 200. The collector must poll promptly, dedupe
by media sequence, and treat a forward sequence gap as lost media rather than
silently producing a complete-looking file.

For the current full-segment implementation:

```text
1. Download the active EXT-X-MAP object before its media fragments and again
   whenever its URI changes.
2. Poll both video and audio media playlists.
3. Download complete EXTINF segments that have not already been written.
4. Do not also append EXT-X-PART objects; they overlap the later complete segment.
5. Align initial audio/video using PROGRAM-DATE-TIME where available.
6. Remux the separate tracks into the output container.
```

An `EXT-X-DISCONTINUITY` is logged. A changed segment map is downloaded before
the affected segment. Neither case appeared in the live verification snapshot;
the behavior is covered by synthetic tests.

## 5. HTTP behavior

The project should send:

```text
User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/150.0.0.0 Safari/537.36
Referer: https://chaturbate.com/{username}/
```

The user agent is a project requirement. Sending the room referer is a prudent
compatibility policy and is what the recorder currently does. It is not
accurate to call the referer demonstrably mandatory: in this verification,
authenticated master, media-playlist, and current-segment requests also
returned 200 when the referer was omitted.

The anonymous checks did not share a cookie jar between the room page and CDN;
the token/session query credentials were sufficient. That is an observation,
not a guarantee that cookies or additional anti-bot handling will never be
required.

The verification requests used HTTP/2 explicitly. Responses advertised HTTP/3
through `Alt-Svc`, but this does not mean the current Go client uses HTTP/3.
Go's standard `net/http` transport can use HTTP/2 here; the code has no HTTP/3
transport.

Always validate HTTP status before parsing. In particular, a duplicate master
token produced a small text error body rather than an HLS playlist. Content
type is useful diagnostic evidence but should not replace parsing and status
checks.

## 6. Recorder implementation audit

The implementation is one Go process with goroutines, not one OS process per
channel. Each active target gets one `recorder.Recorder` goroutine; `ffmpeg`
and `ffprobe` are external subprocesses used to ingest combined HLS variants or
to finalize and validate captured tracks.

Verified behavior in the current code:

- `monitor.checkStreamStatus` uses the safe dossier parser and requires
  `room_status == "public" && hls_source != ""`.
- Status probes run independently without holding the monitor state mutex
  during network I/O.
- A numbered session/generation prevents a late result from an old worker from
  stopping or rescheduling a newer worker.
- The shared Resty client has the required default user agent and a 10-second
  timeout. Recorder requests also carry their recording context and referer.
- `fetchAndSelectStreams` parses the master, chooses the closest resolution
  height, and uses configured frame rate to choose among equally close
  resolutions. It resolves both video and matching separate-audio URLs.
- For matching separate audio/video playlists, the recorder aligns their
  initial complete segments by program time, then runs independent, ordered
  track workers so a slow video request does not delay audio or vice versa.
- For variants whose media playlist contains both audio and video, `ffmpeg`
  reads HLS directly and stream-copies it into the partial MKV.
- Every track worker owns a lazily allocated reusable response buffer capped at
  16 MiB. It downloads unseen full segments, errors on a sequence gap, and
  appends only complete responses to its temporary fMP4 track.
- Finalization remuxes the two temporary tracks to a same-directory partial MKV,
  verifies video, audio, and positive duration with `ffprobe`, and atomically
  publishes the final path. Temporary tracks are removed only after success.
- Debug logging reports room status, selected rendition metadata, playlist
  sequence/count, sanitized request paths and status codes, and each committed
  segment without exposing token/session query strings.
- Terminal statuses are `completed`, `max_duration`, `max_filesize`, `error`,
  and `desync`. An unexpected `ffmpeg` exit is reported as `error`.

### Current limitations

1. **Encrypted and byte-range media are not implemented by the Go segment
   downloader.** The verified live streams were unencrypted standalone fMP4
   objects.
2. **Session recovery occurs outside the recorder.** A request failure exits
   the recording attempt so the monitor can probe the room again. The recorder
   should not reuse the consumed master token itself.
3. **Temporary files are intentionally retained on failed/invalid remuxes for
   diagnosis.** They are removed after successful ffprobe validation and
   publication. Their location follows the Unix `TMPDIR` environment.

On the separate-track path, maximum file size is checked after complete
segments are written and uses the combined bytes captured for the two tracks.
The threshold can therefore be exceeded by the final pair of segments, and MKV
container overhead means the final output size need not exactly equal the
captured fMP4 byte count.

Only network errors, HTTP 408/429, and 5xx responses are retried once against
the same media URL. Expired 403/404 segment URLs are not retried; the recorder
returns to the monitor for a fresh room/session probe.

Retry behavior is contextual: an offline room is checked every 15 seconds; a
live recording is checked every 30 seconds; status faults while idle and
request-class recorder failures back off from 1 second to a 15-second cap;
status faults during an active recording retry after 5 seconds; other recorder
failures wait 30 seconds before a new probe.

## 7. Collector flow

The resulting end-to-end design is:

```text
master process
  -> parse configured targets
  -> monitor each room page
  -> decode initialRoomDossier
  -> require public status plus hls_source
  -> start one recorder goroutine
  -> consume the tokenized master URL once
  -> select video resolution/frame rate and, when present, its audio rendition
  -> poll short LL-HLS windows
  -> download init objects and unseen complete .m4s fragments
  -> remux tracks to MKV
  -> on loss/auth/session failure, return to a fresh room-page probe
```

The original requirement's reference to downloading many `.ts` segments does
not match the verified upstream format. The collector currently receives many
fragmented-MP4 `.m4s` objects, often as separate video and audio tracks.
