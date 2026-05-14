import world_info_web.launcher as launcher


def test_build_backend_command_uses_requested_python():
    assert launcher.build_backend_command("C:\\Python\\python.exe") == [
        "C:\\Python\\python.exe",
        "-m",
        "world_info_web.backend.app",
    ]


def test_wait_for_server_returns_true_when_ready(monkeypatch):
    calls = {"count": 0}

    def fake_is_server_ready(base_url: str) -> bool:
        calls["count"] += 1
        return calls["count"] >= 3

    monkeypatch.setattr(launcher, "is_server_ready", fake_is_server_ready)

    assert launcher.wait_for_server("http://127.0.0.1:5080", timeout_seconds=1.0, interval_seconds=0.0)


def test_wait_for_server_returns_false_on_timeout(monkeypatch):
    monkeypatch.setattr(launcher, "is_server_ready", lambda base_url: False)

    assert not launcher.wait_for_server(
        "http://127.0.0.1:5080",
        timeout_seconds=0.01,
        interval_seconds=0.0,
    )


def test_start_backend_redirects_stdout_and_stderr_to_different_files(monkeypatch):
    captured = {}

    class DummyProcess:
        pid = 12345

    def fake_popen(command, **kwargs):
        captured["command"] = command
        captured["kwargs"] = kwargs
        return DummyProcess()

    monkeypatch.setattr(launcher.subprocess, "Popen", fake_popen)

    process = launcher.start_backend(port=5081)

    assert process.pid == 12345
    command = captured["command"]
    powershell_command = command[-1]
    assert str(launcher.backend_stdout_log_path()) in powershell_command
    assert str(launcher.backend_stderr_log_path()) in powershell_command
    assert str(launcher.backend_stdout_log_path()) != str(launcher.backend_stderr_log_path())


def test_backend_log_hint_mentions_stdout_and_stderr_logs():
    hint = launcher.backend_log_hint()

    assert "Backend stdout log:" in hint
    assert "Backend stderr log:" in hint
    assert str(launcher.backend_stdout_log_path()) in hint
    assert str(launcher.backend_stderr_log_path()) in hint
