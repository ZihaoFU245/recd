#!/usr/bin/env python3
import argparse
import json
import os
import re
import signal
import subprocess
import sys
import tempfile
import time
from pathlib import Path


def run(cmd, cwd, env=None, check=True):
    proc = subprocess.run(cmd, cwd=cwd, env=env, text=True, capture_output=True)
    if check and proc.returncode != 0:
        raise RuntimeError(
            f"command failed ({proc.returncode}): {' '.join(cmd)}\n"
            f"stdout:\n{proc.stdout}\nstderr:\n{proc.stderr}"
        )
    return proc


def packet_count(path, stream, cwd):
    proc = run(
        [
            "ffprobe",
            "-v",
            "error",
            "-select_streams",
            stream,
            "-count_packets",
            "-show_entries",
            "stream=nb_read_packets",
            "-of",
            "default=nk=1:nw=1",
            str(path),
        ],
        cwd,
    )
    text = proc.stdout.strip()
    if not text or text == "N/A":
        raise RuntimeError(f"no packet count for {stream} in {path}")
    return int(text)


def packet_gaps(path, stream, threshold, cwd):
    proc = run(
        [
            "ffprobe",
            "-v",
            "error",
            "-select_streams",
            stream,
            "-show_packets",
            "-show_entries",
            "packet=dts_time,pts_time",
            "-of",
            "json",
            str(path),
        ],
        cwd,
    )
    packets = json.loads(proc.stdout).get("packets", [])
    previous = None
    bad = []
    gaps = []
    for index, packet in enumerate(packets, start=1):
        value = packet.get("dts_time", packet.get("pts_time"))
        if value is None or value == "N/A":
            continue
        current = float(value)
        if previous is not None:
            delta = current - previous
            if delta <= 0:
                bad.append([index, previous, current, delta])
            if delta > threshold:
                gaps.append([index, previous, current, delta])
        previous = current
    return {"bad": bad, "gaps": gaps, "count": len(packets)}


def decode_output(path, cwd):
    return run(
        [
            "ffmpeg",
            "-v",
            "error",
            "-i",
            str(path),
            "-map",
            "0:v:0",
            "-map",
            "0:a:0",
            "-f",
            "null",
            "-",
        ],
        cwd,
    )


def build_expected(cache_root, username):
    manifest_path = cache_root / sanitize(username) / "manifest.jsonl"
    if not manifest_path.exists():
        raise RuntimeError(f"missing segment cache manifest: {manifest_path}")

    by_track = {"video": [], "audio": []}
    with manifest_path.open() as f:
        for line in f:
            if not line.strip():
                continue
            entry = json.loads(line)
            track = entry.get("track")
            if track in by_track:
                by_track[track].append(entry)

    expected = {}
    for track, entries in by_track.items():
        if not entries:
            raise RuntimeError(f"no cached {track} entries in {manifest_path}")
        out_path = cache_root / sanitize(username) / f"expected_{track}.bin"
        with out_path.open("wb") as out:
            for entry in entries:
                with open(entry["path"], "rb") as segment:
                    out.write(segment.read())
        expected[track] = out_path
    return expected, manifest_path


def sanitize(value):
    return "".join(c if c.isalnum() or c in "_.-" else "_" for c in value) or "unknown"


def build_binary(repo, work):
    binary = work / "recd"
    env = os.environ.copy()
    env.setdefault("GOCACHE", str(work / "go-build-cache"))
    env.setdefault("CCACHE_DIR", str(work / "ccache"))
    run(["go", "build", "-o", str(binary), "."], repo, env=env)
    return binary


def run_recorder(repo, binary, usernames, resolution, duration, work, cache_root):
    config_path = work / "config.json"
    pid_path = work / "recd.pid"
    config = []
    for username in usernames:
        pattern = f"videos/verify_segments_{sanitize(username)}_{{{{.Year}}}}-{{{{.Month}}}}-{{{{.Day}}}}_{{{{.Hour}}}}-{{{{.Minute}}}}-{{{{.Second}}}}"
        config.append(
            {
                "is_paused": False,
                "username": username,
                "framerate": 30,
                "resolution": resolution,
                "pattern": pattern,
                "max_duration": 0,
                "max_filesize": 0,
                "created_at": 0,
            }
        )
    config_path.write_text(json.dumps(config, indent=2))

    env = os.environ.copy()
    env["RECD_SEGMENT_CACHE_DIR"] = str(cache_root)
    cmd = [
        str(binary),
        "--log-level=info",
        "--additional-headers",
        "headers.json",
        "--pid-file",
        str(pid_path),
        str(config_path),
    ]
    proc = subprocess.Popen(
        cmd,
        cwd=repo,
        env=env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    try:
        time.sleep(duration)
        proc.send_signal(signal.SIGINT)
        output, _ = proc.communicate(timeout=90)
    except Exception:
        proc.kill()
        output, _ = proc.communicate(timeout=30)
        raise

    log_path = work / "run.log"
    log_path.write_text(output)
    if proc.returncode != 0:
        raise RuntimeError(f"recorder exited {proc.returncode}; log at {log_path}\n{output}")

    output_paths = {}
    for username in usernames:
        matches = re.findall(rf"username={re.escape(username)}\b.*?path=(\S+\.mkv)", output)
        if matches:
            output_paths[username] = repo / matches[-1]
            continue
        candidates = sorted((repo / "videos").glob(f"verify_segments_{sanitize(username)}_*.mkv"))
        if not candidates:
            raise RuntimeError(f"no output path found for {username}; log at {log_path}\n{output}")
        output_paths[username] = candidates[-1]
    return output_paths, log_path, output


def verify_output(repo, username, output_path, cache_root, log_path):
    expected, manifest_path = build_expected(cache_root, username)

    decode_output(output_path, repo)
    expected_video_packets = packet_count(expected["video"], "v:0", repo)
    expected_audio_packets = packet_count(expected["audio"], "a:0", repo)
    output_video_packets = packet_count(output_path, "v:0", repo)
    output_audio_packets = packet_count(output_path, "a:0", repo)
    video_gaps = packet_gaps(output_path, "v:0", 0.2, repo)
    audio_gaps = packet_gaps(output_path, "a:0", 0.1, repo)

    report = {
        "username": username,
        "output": str(output_path),
        "cache": str(cache_root),
        "manifest": str(manifest_path),
        "log": str(log_path),
        "expected_video_packets": expected_video_packets,
        "output_video_packets": output_video_packets,
        "expected_audio_packets": expected_audio_packets,
        "output_audio_packets": output_audio_packets,
        "video_bad_timestamps": len(video_gaps["bad"]),
        "video_gaps": len(video_gaps["gaps"]),
        "audio_bad_timestamps": len(audio_gaps["bad"]),
        "audio_gaps": len(audio_gaps["gaps"]),
    }

    failures = []
    if expected_video_packets != output_video_packets:
        failures.append("video packet count mismatch")
    if expected_audio_packets != output_audio_packets:
        failures.append("audio packet count mismatch")
    if video_gaps["bad"] or video_gaps["gaps"]:
        failures.append("video timestamp issue")
    if audio_gaps["bad"] or audio_gaps["gaps"]:
        failures.append("audio timestamp issue")
    return report, failures


def main():
    parser = argparse.ArgumentParser(description="Real HLS segment cache verification for recd")
    parser.add_argument(
        "--username",
        action="append",
        default=[],
        help="channel username; pass more than once to test concurrent recording",
    )
    parser.add_argument("--resolution", type=int, default=480)
    parser.add_argument("--duration", type=int, default=60)
    parser.add_argument("--work-dir", default="")
    args = parser.parse_args()

    repo = Path(__file__).resolve().parents[1]
    work = Path(args.work_dir) if args.work_dir else Path(tempfile.mkdtemp(prefix="recd-real-verify-"))
    work.mkdir(parents=True, exist_ok=True)
    cache_root = work / "segment-cache"

    usernames = args.username or ["chloe_show"]
    binary = build_binary(repo, work)
    output_paths, log_path, _ = run_recorder(
        repo, binary, usernames, args.resolution, args.duration, work, cache_root
    )

    reports = []
    all_failures = []
    for username in usernames:
        report, failures = verify_output(repo, username, output_paths[username], cache_root, log_path)
        reports.append(report)
        for failure in failures:
            all_failures.append(f"{username}: {failure}")

    print(json.dumps({"channels": reports}, indent=2))
    if all_failures:
        raise SystemExit("; ".join(all_failures))


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"error: {exc}", file=sys.stderr)
        raise SystemExit(1)
