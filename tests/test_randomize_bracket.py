import sys
from pathlib import Path
import types


def test_randomize_bracket_format(monkeypatch):
    base = Path(__file__).resolve().parent.parent / "track_results"
    sys.path.insert(0, str(base))
    import ui as ui_module

    players = ["A", "B", "C", "D"]
    obj = types.SimpleNamespace(champ_players=players)

    monkeypatch.setattr(ui_module.random, "shuffle", lambda lst: None)
    captured = {}

    def fake_info(title, message):
        captured["msg"] = message

    monkeypatch.setattr(ui_module.messagebox, "showinfo", fake_info)
    monkeypatch.setattr(ui_module.messagebox, "showwarning", lambda *a, **k: None)

    ui_module.RacingUI._randomize_bracket(obj)
    assert captured["msg"] == "A vs B\nC vs D"
