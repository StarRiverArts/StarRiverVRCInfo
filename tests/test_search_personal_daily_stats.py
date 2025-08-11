import types
import sys
from pathlib import Path


def test_search_personal_daily_stats_per_player(monkeypatch):
    base = Path(__file__).resolve().parent.parent
    sys.path.append(str(base))
    sys.path.append(str(base / "world_info"))
    from world_info import ui as ui_module

    def fake_search_user(user_id, headers):
        return [{"id": "w1", "name": "World"}]

    def fake_save_worlds(worlds, file):
        pass

    def fake_update_history(worlds):
        pass

    def fake_load_history():
        return {}

    monkeypatch.setattr(ui_module, "search_user", fake_search_user)
    monkeypatch.setattr(ui_module, "save_worlds", fake_save_worlds)
    monkeypatch.setattr(ui_module, "update_history", fake_update_history)
    monkeypatch.setattr(ui_module, "load_history", fake_load_history)

    recorded_files: list[str] = []

    def fake_update_daily_stats(source, worlds):
        recorded_files.append(f"daily_stats_{source}.xlsx")

    monkeypatch.setattr(ui_module, "update_daily_stats", fake_update_daily_stats)

    class MockTree:
        def get_children(self):
            return []

        def delete(self, item):
            pass

        def insert(self, parent, index, values):
            pass

    def make_ui(player_id: str):
        ui = types.SimpleNamespace()
        ui._load_auth_headers = lambda: None
        ui.settings = {"player_id": player_id, "personal_file": "custom.xlsx"}
        ui.headers = {}
        ui.user_tree = MockTree()
        ui.user_data = []
        ui.history = {}
        ui._update_history_options = lambda: None
        ui.nb = types.SimpleNamespace(select=lambda frame: None)
        ui.tab_user = types.SimpleNamespace(frame=object())
        ui._load_local_tables = lambda: None
        return ui

    ui1 = make_ui("user123")
    ui_module.WorldInfoUI._search_personal(ui1)

    ui2 = make_ui("user/456")
    ui_module.WorldInfoUI._search_personal(ui2)

    assert recorded_files == [
        "daily_stats_user123.xlsx",
        "daily_stats_user_456.xlsx",
    ]

