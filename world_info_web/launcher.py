from __future__ import annotations

import os
import subprocess
import sys
import time
import urllib.error
import urllib.request
import webbrowser
from pathlib import Path


DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 5080


def repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def build_backend_command(python_executable: str | None = None) -> list[str]:
    return [python_executable or sys.executable, "-m", "world_info_web.backend.app"]


def build_base_url(host: str = DEFAULT_HOST, port: int = DEFAULT_PORT) -> str:
    return f"http://{host}:{port}"


def backend_log_path() -> Path:
    return repo_root() / "world_info_web" / "data" / "backend-launcher.log"


def backend_stdout_log_path() -> Path:
    return repo_root() / "world_info_web" / "data" / "backend_stdout.log"


def backend_stderr_log_path() -> Path:
    return repo_root() / "world_info_web" / "data" / "backend_stderr.log"


def backend_log_hint() -> str:
    return (
        f"Backend stdout log: {backend_stdout_log_path()}\n"
        f"Backend stderr log: {backend_stderr_log_path()}"
    )


def _parse_listening_pid(port: int) -> int | None:
    result = subprocess.run(
        ["netstat", "-ano", "-p", "tcp"],
        capture_output=True,
        text=True,
        check=False,
    )
    port_suffix = f":{port}"
    for line in result.stdout.splitlines():
        if "LISTENING" not in line.upper():
            continue
        parts = line.split()
        if len(parts) < 5:
            continue
        local_address = parts[1]
        state = parts[3].upper()
        pid_text = parts[4]
        if not local_address.endswith(port_suffix) or state != "LISTENING":
            continue
        try:
            return int(pid_text)
        except ValueError:
            return None
    return None


def stop_existing_backend(port: int) -> bool:
    pid = _parse_listening_pid(port)
    if pid is None:
        return False
    result = subprocess.run(["taskkill", "/PID", str(pid), "/F"], check=False, capture_output=True, text=True)
    if result.returncode != 0:
        print(
            f"Unable to stop existing backend PID {pid} on port {port}: "
            f"{(result.stderr or result.stdout or 'taskkill failed').strip()}",
            file=sys.stderr,
        )
        return False
    deadline = time.time() + 5.0
    while time.time() < deadline:
        if _parse_listening_pid(port) is None:
            return True
        time.sleep(0.2)
    return _parse_listening_pid(port) is None


def is_server_ready(base_url: str) -> bool:
    health_url = f"{base_url}/api/v1/health"
    try:
        with urllib.request.urlopen(health_url, timeout=1.5) as response:
            return 200 <= response.status < 300
    except (OSError, urllib.error.URLError):
        return False


def wait_for_server(base_url: str, *, timeout_seconds: float = 30.0, interval_seconds: float = 0.5) -> bool:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        if is_server_ready(base_url):
            return True
        time.sleep(interval_seconds)
    return False


def start_backend(host: str = DEFAULT_HOST, port: int = DEFAULT_PORT) -> subprocess.Popen[bytes]:
    env = os.environ.copy()
    env["WORLD_INFO_WEB_PORT"] = str(port)
    env.setdefault("PYTHONDONTWRITEBYTECODE", "1")
    if env.get("WORLD_INFO_USE_SYSTEM_PROXY", "").strip() != "1":
        for key in (
            "HTTP_PROXY",
            "HTTPS_PROXY",
            "ALL_PROXY",
            "http_proxy",
            "https_proxy",
            "all_proxy",
            "GIT_HTTP_PROXY",
            "GIT_HTTPS_PROXY",
        ):
            env.pop(key, None)

    stdout_log_path = backend_stdout_log_path()
    stderr_log_path = backend_stderr_log_path()
    stdout_log_path.parent.mkdir(parents=True, exist_ok=True)
    powershell = [
        "powershell",
        "-NoProfile",
        "-ExecutionPolicy",
        "Bypass",
        "-Command",
        (
            f"$env:WORLD_INFO_WEB_PORT='{port}'; "
            "$env:PYTHONDONTWRITEBYTECODE='1'; "
            f"Set-Location '{repo_root()}'; "
            f"Start-Process -FilePath '{sys.executable}' "
            f"-ArgumentList '-m','world_info_web.backend.app' "
            f"-WorkingDirectory '{repo_root()}' "
            f"-WindowStyle Hidden "
            f"-RedirectStandardOutput '{stdout_log_path}' "
            f"-RedirectStandardError '{stderr_log_path}'"
        ),
    ]
    return subprocess.Popen(
        powershell,
        cwd=str(repo_root()),
        env=env,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def main() -> int:
    host = os.getenv("WORLD_INFO_WEB_HOST", DEFAULT_HOST)
    port = int(os.getenv("WORLD_INFO_WEB_PORT", str(DEFAULT_PORT)))
    open_browser = os.getenv("WORLD_INFO_WEB_OPEN_BROWSER", "1") != "0"
    force_restart = os.getenv("WORLD_INFO_WEB_FORCE_RESTART", "0") == "1"
    base_url = build_base_url(host, port)

    if is_server_ready(base_url):
        if force_restart:
            stopped = stop_existing_backend(port)
            if not stopped:
                if is_server_ready(base_url):
                    print(
                        f"World Info Web is already running at {base_url}. "
                        "Restart was skipped because the existing process could not be stopped.",
                        file=sys.stderr,
                    )
                    return 0
                print(f"Failed to stop existing World Info Web process on port {port}.", file=sys.stderr)
                return 1
        else:
            if open_browser:
                webbrowser.open(base_url)
            print(f"World Info Web is already running at {base_url}")
            return 0

    if is_server_ready(base_url):
        if open_browser:
            webbrowser.open(base_url)
        print(f"World Info Web is already running at {base_url}")
        return 0

    process = start_backend(host=host, port=port)
    if not wait_for_server(base_url):
        print(
            f"World Info Web did not become ready within 30 seconds. "
            f"Check the backend logs. PID={process.pid}\n"
            f"{backend_log_hint()}",
            file=sys.stderr,
        )
        return 1

    if open_browser:
        webbrowser.open(base_url)
    print(f"World Info Web is running at {base_url}")
    print(backend_log_hint())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
