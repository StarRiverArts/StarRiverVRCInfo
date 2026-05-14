import types
import sys
from pathlib import Path


def _base_patches(ui_module, monkeypatch, save_worlds_fn=None):
    """Apply common monkeypatches; optionally accept a custom save_worlds."""

    def fake_search_user(user_id, headers):
        return [{"id": "w1", "name": "World"}]

    def fake_update_history(worlds):
        pass

    def fake_load_history():
        return {}

    def fake_update_daily_stats(source, worlds):
        pass

    monkeypatch.setattr(ui_module, "search_user", fake_search_user)
    monkeypatch.setattr(ui_module, "update_history", fake_update_history)
    monkeypatch.setattr(ui_module, "load_history", fake_load_history)
    monkeypatch.setattr(ui_module, "update_daily_stats", fake_update_daily_stats)
    if save_worlds_fn is not None:
        monkeypatch.setattr(ui_module, "save_worlds", save_worlds_fn)


class _MockTree:
    def __init__(self):
        self.items = []

    def get_children(self):
        return list(self.items)

    def delete(self, item):
        if item in self.items:
            self.items.remove(item)

    def insert(self, parent, index, values):
        self.items.append(values)


def test_search_personal_no_duplicates(monkeypatch):
    """Calling _search_personal multiple times must not accumulate rows."""
    base = Path(__file__).resolve().parent.parent
    sys.path.append(str(base))
    sys.path.append(str(base / "world_info"))
    from world_info import ui as ui_module

    saved_files: list[Path] = []

    def tracking_save(worlds, file):
        saved_files.append(file)

    _base_patches(ui_module, monkeypatch, save_worlds_fn=tracking_save)

    tree = _MockTree()

    ui = types.SimpleNamespace()
    ui._load_auth_headers = lambda: None
    ui.settings = {"player_id": "user123"}
    ui.headers = {}
    ui.user_tree = tree
    ui.user_data = []
    ui.history = {}
    ui._refresh_history_table = lambda: None
    ui.nb = types.SimpleNamespace(select=lambda frame: None)
    ui.tab_user = types.SimpleNamespace(frame=object())

    def load_local(self_ui=ui):
        for item in list(self_ui.user_tree.get_children()):
            self_ui.user_tree.delete(item)
        self_ui.user_data = [{"世界ID": "w1"}]
        self_ui.user_tree.insert("", None, values=("w1",))

    ui._load_local_tables = load_local

    ui_module.WorldInfoUI._search_personal(ui)
    assert len(ui.user_tree.get_children()) == 1, "First call: 1 item"

    ui_module.WorldInfoUI._search_personal(ui)
    assert len(ui.user_tree.get_children()) == 1, "Second call: still 1 item (no duplicates)"

    expected = ui_module.STAR_RIVER_FILE
    assert saved_files == [expected, expected], "save_worlds called once per run"


def test_search_personal_no_duplicates_raw_data(monkeypatch):
    """_load_local_tables is called each run so the table reflects current state."""
    base = Path(__file__).resolve().parent.parent
    sys.path.append(str(base))
    sys.path.append(str(base / "world_info"))
    from world_info import ui as ui_module

    saved_files: list[Path] = []
    loaded_files: list[Path] = []

    def tracking_save(worlds, file):
        saved_files.append(file)

    _base_patches(ui_module, monkeypatch, save_worlds_fn=tracking_save)

    tree = _MockTree()

    ui = types.SimpleNamespace()
    ui._load_auth_headers = lambda: None
    ui.settings = {"player_id": "user123"}
    ui.headers = {}
    ui.user_tree = tree
    ui.user_data = []
    ui.history = {}
    ui._refresh_history_table = lambda: None
    ui.nb = types.SimpleNamespace(select=lambda frame: None)
    ui.tab_user = types.SimpleNamespace(frame=object())

    def load_local(self_ui=ui):
        loaded_files.append(ui_module.STAR_RIVER_FILE)
        for item in list(self_ui.user_tree.get_children()):
            self_ui.user_tree.delete(item)
        self_ui.user_data = [{"世界ID": "w1"}]
        self_ui.user_tree.insert("", None, values=("w1",))

    ui._load_local_tables = load_local

    ui_module.WorldInfoUI._search_personal(ui)
    assert len(ui.user_tree.get_children()) == 1
    expected = ui_module.STAR_RIVER_FILE
    assert saved_files == [expected]
    assert loaded_files == [expected]

    ui_module.WorldInfoUI._search_personal(ui)
    assert len(ui.user_tree.get_children()) == 1, "Always shows current state, not cumulative"
    assert saved_files == [expected, expected]
    assert loaded_files == [expected, expected]
