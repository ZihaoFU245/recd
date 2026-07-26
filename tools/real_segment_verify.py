#!/usr/bin/env python3
"""Build recd, record a current public stream, and validate the MKV with ffprobe.

By default the script discovers a current public channel with curl, records it
for 20 seconds, and leaves the verified output in the repository's videos/
directory. Pass --username to test a specific channel instead.
"""

import argparse
import json
import os
import re
import shutil
import signal
import subprocess
import sys
import tempfile
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path


DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/150.0.0.0 Safari/537.36"
)
ROOM_LIST_URL = "https://chaturbate.com/api/ts/roomlist/room-list/"
USERNAME_RE = re.compile(r"^[A-Za-z0-9_]+$")


def run(command, cwd, env=None, check=True):
    proc = subprocess.run(
        command,
        cwd=cwd,
        env=env,
        text=True,
        capture_output=True,
    )
    if check and proc.returncode != 0:
        raise RuntimeError(
            f"command failed ({proc.returncode}): {' '.join(map(str, command))}\n"
            f"stdout:\n{proc.stdout}\nstderr:\n{proc.stderr}"
        )
    return proc


def require_commands(*commands):
    missing = [command for command in commands if shutil.which(command) is None]
    if missing:
        raise RuntimeError(f"required command(s) not found: {', '.join(missing)}")


def discover_live_username(user_agent):
    proc = run(
        [
            "curl",
            "-A",
            user_agent,
            "-H",
            "X-Requested-With: XMLHttpRequest",
            "-H",
            "Referer: https://chaturbate.com/",
            "-L",
            "--fail",
            "--max-time",
            "20",
            "-sS",
            ROOM_LIST_URL,
        ],
        cwd=None,
    )
    try:
        payload = json.loads(proc.stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"room-list response is not JSON: {exc}") from exc

    for room in payload.get("rooms", []):
        username = room.get("username", "")
        if (
            room.get("current_show") == "public"
            and isinstance(username, str)
            and USERNAME_RE.fullmatch(username)
        ):
            return username
    raise RuntimeError("room list contained no valid public channel")


def build_binary(repo, work):
    binary = work / "recd"
    env = os.environ.copy()
    env.setdefault("GOCACHE", str(work / "go-build-cache"))
    run(["go", "build", "-o", str(binary), "."], cwd=repo, env=env)
    return binary


def write_config(work, output_dir, username, resolution, framerate):
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    run_id = uuid.uuid4().hex[:8]
    prefix = f"recd_smoke_{username}_{stamp}_{run_id}"
    pattern = output_dir / (
        prefix
        + "_{{.Year}}-{{.Month}}-{{.Day}}"
        + "_{{.Hour}}-{{.Minute}}-{{.Second}}"
        + "{{if .Sequence}}_{{.Sequence}}{{end}}"
    )
    config = [
        {
            "is_paused": False,
            "username": username,
            "framerate": framerate,
            "resolution": resolution,
            "pattern": str(pattern),
            "max_duration": 0,
            "max_filesize": 0,
            "created_at": 0,
        }
    ]
    config_path = work / "config.json"
    config_path.write_text(json.dumps(config, indent=2) + "\n")
    return config_path, prefix


def wait_for_recording(proc, log_path, username, timeout):
    deadline = time.monotonic() + timeout
    marker = "recording started"
    username_marker = f"username={username}"
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            log = log_path.read_text(errors="replace")
            raise RuntimeError(
                f"recd exited before recording started ({proc.returncode})\n"
                f"{log[-12000:]}"
            )
        log = log_path.read_text(errors="replace")
        if marker in log and username_marker in log:
            return
        time.sleep(0.25)
    log = log_path.read_text(errors="replace")
    raise RuntimeError(
        f"recording did not start for {username} within {timeout:.0f}s\n"
        f"{log[-12000:]}"
    )


def stop_process(proc, timeout):
    proc.send_signal(signal.SIGINT)
    try:
        proc.wait(timeout=timeout)
    except subprocess.TimeoutExpired as exc:
        os.killpg(proc.pid, signal.SIGKILL)
        proc.wait(timeout=10)
        raise RuntimeError(
            f"recd did not finish finalization within {timeout:.0f}s"
        ) from exc


def record(repo, binary, config_path, username, capture_seconds, startup_timeout, work):
    pid_path = work / "recd.pid"
    log_path = work / "recd.log"
    command = [
        str(binary),
        "--log-level=debug",
        "--pid-file",
        str(pid_path),
        str(config_path),
    ]
    with log_path.open("w") as log_file:
        proc = subprocess.Popen(
            command,
            cwd=repo,
            text=True,
            stdout=log_file,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        try:
            wait_for_recording(proc, log_path, username, startup_timeout)
            time.sleep(capture_seconds)
            stop_process(proc, timeout=120)
        except Exception:
            if proc.poll() is None:
                os.killpg(proc.pid, signal.SIGKILL)
                proc.wait(timeout=10)
            raise

    log = log_path.read_text(errors="replace")
    if proc.returncode != 0:
        raise RuntimeError(
            f"recd exited with status {proc.returncode}; log: {log_path}\n"
            f"{log[-12000:]}"
        )
    if "shutdown complete" not in log:
        raise RuntimeError(
            f"recd did not report a clean shutdown; log: {log_path}\n"
            f"{log[-12000:]}"
        )
    return log_path


def find_output(output_dir, prefix):
    candidates = sorted(
        output_dir.glob(f"{prefix}_*.mkv"),
        key=lambda path: path.stat().st_mtime_ns,
    )
    if not candidates:
        raise RuntimeError(f"no MKV matching {prefix}_*.mkv in {output_dir}")
    return candidates[-1]


def ffprobe(path, repo, min_duration):
    proc = run(
        [
            "ffprobe",
            "-v",
            "error",
            "-show_entries",
            "stream=index,codec_type,codec_name:format=format_name,duration,size",
            "-of",
            "json",
            str(path),
        ],
        cwd=repo,
    )
    try:
        metadata = json.loads(proc.stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"ffprobe output is not JSON: {exc}") from exc

    streams = metadata.get("streams", [])
    video = [stream for stream in streams if stream.get("codec_type") == "video"]
    audio = [stream for stream in streams if stream.get("codec_type") == "audio"]
    if not video or not audio:
        raise RuntimeError(
            f"ffprobe found video={bool(video)} audio={bool(audio)} in {path}"
        )

    media_format = metadata.get("format", {})
    try:
        duration = float(media_format.get("duration", ""))
        reported_size = int(media_format.get("size", ""))
    except (TypeError, ValueError) as exc:
        raise RuntimeError(f"ffprobe returned invalid format metadata: {media_format}") from exc
    actual_size = path.stat().st_size
    if duration < min_duration:
        raise RuntimeError(
            f"recording duration {duration:.3f}s is below minimum {min_duration:.3f}s"
        )
    if reported_size <= 0 or actual_size <= 0:
        raise RuntimeError(
            f"recording is empty: ffprobe={reported_size} filesystem={actual_size}"
        )

    return {
        "duration_seconds": duration,
        "size_bytes": actual_size,
        "format": media_format.get("format_name", ""),
        "video_codecs": [stream.get("codec_name", "") for stream in video],
        "audio_codecs": [stream.get("codec_name", "") for stream in audio],
    }


def verify_decode(path, repo):
    run(
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
        cwd=repo,
    )


def main():
    parser = argparse.ArgumentParser(
        description="Build recd, record a public stream, and validate its MKV"
    )
    parser.add_argument(
        "--username",
        default="",
        help="channel to record; defaults to a current public room found with curl",
    )
    parser.add_argument("--capture-seconds", type=float, default=20)
    parser.add_argument("--startup-timeout", type=float, default=45)
    parser.add_argument("--min-duration", type=float, default=3)
    parser.add_argument("--resolution", type=int, default=480)
    parser.add_argument("--framerate", type=int, default=30)
    parser.add_argument(
        "--output-dir",
        default="videos",
        help="output directory, relative to the repository by default",
    )
    parser.add_argument(
        "--work-dir",
        default="",
        help="directory for build/config/log artifacts; defaults to /tmp",
    )
    parser.add_argument("--user-agent", default=DEFAULT_USER_AGENT)
    args = parser.parse_args()

    if args.capture_seconds <= 0:
        parser.error("--capture-seconds must be positive")
    if args.startup_timeout <= 0:
        parser.error("--startup-timeout must be positive")
    if args.min_duration <= 0:
        parser.error("--min-duration must be positive")
    if args.resolution < 0 or args.framerate < 0:
        parser.error("--resolution and --framerate cannot be negative")

    require_commands("curl", "ffmpeg", "ffprobe", "go")
    repo = Path(__file__).resolve().parents[1]
    output_dir = Path(args.output_dir)
    if not output_dir.is_absolute():
        output_dir = repo / output_dir
    output_dir = output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    if args.work_dir:
        work_root = Path(args.work_dir).resolve()
        work_root.mkdir(parents=True, exist_ok=True)
        work = Path(tempfile.mkdtemp(prefix="run-", dir=work_root))
    else:
        work = Path(tempfile.mkdtemp(prefix="recd-real-verify-"))

    username = args.username or discover_live_username(args.user_agent)
    if not USERNAME_RE.fullmatch(username):
        parser.error("--username may contain only ASCII letters, digits, and underscores")

    binary = build_binary(repo, work)
    config_path, prefix = write_config(
        work,
        output_dir,
        username,
        args.resolution,
        args.framerate,
    )
    log_path = record(
        repo,
        binary,
        config_path,
        username,
        args.capture_seconds,
        args.startup_timeout,
        work,
    )
    output_path = find_output(output_dir, prefix)
    media = ffprobe(output_path, repo, args.min_duration)
    verify_decode(output_path, repo)

    print(
        json.dumps(
            {
                "status": "passed",
                "username": username,
                "output": str(output_path),
                "log": str(log_path),
                "work_dir": str(work),
                **media,
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"error: {exc}", file=sys.stderr)
        raise SystemExit(1)
