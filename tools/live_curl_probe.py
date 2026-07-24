#!/usr/bin/env python3
"""Probe live room, master, media, init, and segment URLs with curl."""

import argparse
import csv
import json
import re
import subprocess
import tempfile
from pathlib import Path
from urllib.parse import urljoin, urlsplit


DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/150.0.0.0 Safari/537.36"
)


def curl(url, output, user_agent, referer=""):
    command = [
        "curl",
        "-A",
        user_agent,
        "-L",
        "--max-time",
        "20",
        "-sS",
    ]
    if referer:
        command.extend(["-H", f"Referer: {referer}"])
    command.extend(
        [
            "-o",
            str(output),
            "-w",
            "%{http_code}\t%{content_type}",
            url,
        ]
    )
    proc = subprocess.run(command, text=True, capture_output=True)
    if proc.returncode != 0:
        raise RuntimeError(proc.stderr.strip() or f"curl exited {proc.returncode}")
    status_text, _, content_type = proc.stdout.partition("\t")
    return int(status_text), content_type


def public_url(url):
    parsed = urlsplit(url)
    return f"{parsed.scheme}://{parsed.netloc}{parsed.path}"


def parse_dossier(page):
    match = re.search(r"initialRoomDossier\s*=\s*(\"(?:\\.|[^\"\\])*\")", page)
    if not match:
        raise RuntimeError("initialRoomDossier assignment not found")
    encoded_json = json.loads(match.group(1))
    return json.loads(encoded_json)


def parse_attributes(line):
    _, _, text = line.partition(":")
    fields = next(csv.reader([text], skipinitialspace=True))
    attributes = {}
    for field in fields:
        key, separator, value = field.partition("=")
        if separator:
            if len(value) >= 2 and value[0] == '"' and value[-1] == '"':
                value = value[1:-1]
            attributes[key] = value
    return attributes


def parse_master(master):
    lines = [line.strip() for line in master.splitlines() if line.strip()]
    audio = {}
    variants = []
    pending = None
    for line in lines:
        if line.startswith("#EXT-X-MEDIA:"):
            attributes = parse_attributes(line)
            if attributes.get("TYPE") == "AUDIO" and attributes.get("URI"):
                audio[attributes.get("GROUP-ID", "")] = attributes["URI"]
        elif line.startswith("#EXT-X-STREAM-INF:"):
            pending = parse_attributes(line)
        elif not line.startswith("#") and pending is not None:
            variants.append((pending, line))
            pending = None
    if not variants:
        raise RuntimeError("master has no variants")
    return audio, variants


def parse_media(media):
    lines = [line.strip() for line in media.splitlines() if line.strip()]
    init_uri = ""
    segment_uri = ""
    expect_segment = False
    for line in lines:
        if line.startswith("#EXT-X-MAP:"):
            init_uri = parse_attributes(line).get("URI", "")
        elif line.startswith("#EXTINF:"):
            expect_segment = True
        elif not line.startswith("#") and expect_segment:
            segment_uri = line
            break
    if not segment_uri:
        raise RuntimeError("media playlist has no complete segment")
    return init_uri, segment_uri


def require_ok(kind, status, content_type):
    if status != 200:
        raise RuntimeError(f"{kind} HTTP {status}")
    return {"status": status, "content_type": content_type}


def probe_track(kind, playlist_url, referer, work, user_agent):
    playlist_path = work / f"{kind}.m3u8"
    status, content_type = curl(
        playlist_url, playlist_path, user_agent, referer=referer
    )
    result = {
        "playlist": public_url(playlist_url),
        **require_ok(f"{kind} playlist", status, content_type),
    }
    init_uri, segment_uri = parse_media(playlist_path.read_text())
    if init_uri:
        init_url = urljoin(playlist_url, init_uri)
        status, content_type = curl(
            init_url, work / f"{kind}-init.bin", user_agent, referer=referer
        )
        result["init"] = {
            "url": public_url(init_url),
            **require_ok(f"{kind} init", status, content_type),
        }
    segment_url = urljoin(playlist_url, segment_uri)
    status, content_type = curl(
        segment_url, work / f"{kind}-segment.bin", user_agent, referer=referer
    )
    result["segment"] = {
        "url": public_url(segment_url),
        **require_ok(f"{kind} segment", status, content_type),
    }
    return result


def probe(username, root, user_agent):
    work = root / username
    work.mkdir(parents=True, exist_ok=True)
    referer = f"https://chaturbate.com/{username}/"
    page_path = work / "room.html"
    status, content_type = curl(referer, page_path, user_agent)
    result = {
        "username": username,
        "room": {
            "url": referer,
            **require_ok("room page", status, content_type),
        },
    }

    dossier = parse_dossier(page_path.read_text())
    result["room_status"] = dossier.get("room_status", "")
    hls_source = dossier.get("hls_source", "")
    if result["room_status"] != "public" or not hls_source:
        raise RuntimeError(
            f"room is not recordable: status={result['room_status']!r}"
        )

    master_path = work / "master.m3u8"
    status, content_type = curl(
        hls_source, master_path, user_agent, referer=referer
    )
    result["master"] = {
        "url": public_url(hls_source),
        **require_ok("master playlist", status, content_type),
    }

    audio, variants = parse_master(master_path.read_text())
    attributes, video_uri = variants[0]
    video_url = urljoin(hls_source, video_uri)
    result["selected_variant"] = {
        "resolution": attributes.get("RESOLUTION", ""),
        "frame_rate": attributes.get("FRAME-RATE", ""),
        "bandwidth": attributes.get("BANDWIDTH", ""),
        "audio_group": attributes.get("AUDIO", ""),
    }
    result["video"] = probe_track(
        "video", video_url, referer, work, user_agent
    )

    audio_group = attributes.get("AUDIO", "")
    if audio_group and audio_group in audio:
        audio_url = urljoin(hls_source, audio[audio_group])
        result["audio"] = probe_track(
            "audio", audio_url, referer, work, user_agent
        )
    return result


def main():
    parser = argparse.ArgumentParser(
        description="Use curl to verify live room and HLS URL paths"
    )
    parser.add_argument("--username", action="append", required=True)
    parser.add_argument("--user-agent", default=DEFAULT_USER_AGENT)
    parser.add_argument("--work-dir", default="")
    args = parser.parse_args()

    if args.work_dir:
        root = Path(args.work_dir)
        root.mkdir(parents=True, exist_ok=True)
    else:
        root = Path(tempfile.mkdtemp(prefix="recd-live-curl-"))

    reports = []
    failed = False
    for username in args.username:
        try:
            reports.append(probe(username, root, args.user_agent))
        except Exception as exc:
            reports.append({"username": username, "error": str(exc)})
            failed = True
    print(json.dumps({"work_dir": str(root), "channels": reports}, indent=2))
    if failed:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
