import types
import sys
from pathlib import Path


def test_search_personal_no_duplicates(monkeypatch):
    base = Path(__file__).resolve().parent.parent
    sys.path.append(str(base))
    sys.path.append(str(base / "world_info"))
    from world_info import ui as ui_module

    saved_worlds: list[dict] = []
    saved_files: list[Path] = []
    loaded_files: list[Path] = []

    def fake_search_user(user_id, headers):
        return [{"id": "w1", "name": "World"}]

    def fake_save_worlds(worlds, file):
        saved_worlds.extend(worlds)
        saved_files.append(file)

    def fake_update_history(worlds):
        pass

    def fake_load_history():
        return {}

    def fake_update_daily_stats(source, worlds):
        pass

    def fake_load_local_tables(self):
        # record which file path was requested
        path = ui_module.BASE / "scraper" / self.settings.get(
            "personal_file", ui_module.PERSONAL_FILE.name
        )
        loaded_files.append(path)
        # intentionally do not clear tree here to ensure _search_personal does
        self.user_data = [{"世界ID": w["id"]} for w in saved_worlds]
        for w in saved_worlds:
            self.user_tree.insert("", None, values=(w["id"],))

    monkeypatch.setattr(ui_module, "search_user", fake_search_user)
    monkeypatch.setattr(ui_module, "save_worlds", fake_save_worlds)
    monkeypatch.setattr(ui_module, "update_history", fake_update_history)
    monkeypatch.setattr(ui_module, "load_history", fake_load_history)
    monkeypatch.setattr(ui_module, "update_daily_stats", fake_update_daily_stats)

    class MockTree:
        def __init__(self):
            self.items = []

        def get_children(self):
            return list(self.items)

        def delete(self, item):
            self.items.remove(item)

        def insert(self, parent, index, values):
            self.items.append(values)

    ui = types.SimpleNamespace()
    ui._load_auth_headers = lambda: None
    ui.settings = {"player_id": "user123", "personal_file": "custom.xlsx"}
    ui.headers = {}
    ui.user_tree = MockTree()
    ui.user_data = []
    ui.history = {}
    ui._update_history_options = lambda: None
    ui.nb = types.SimpleNamespace(select=lambda frame: None)
    ui.tab_user = types.SimpleNamespace(frame=object())
    ui._load_local_tables = lambda: fake_load_local_tables(ui)

    ui_module.WorldInfoUI._search_personal(ui)
    assert len(ui.user_tree.get_children()) == 1
    expected = ui_module.BASE / "scraper" / "custom.xlsx"
    assert saved_files == [expected]
    assert loaded_files == [expected]

    ui_module.WorldInfoUI._search_personal(ui)
    assert len(ui.user_tree.get_children()) == 1

