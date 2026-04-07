import sys
from pathlib import Path


def test_search_fixed_deduplicates(monkeypatch):
    base = Path(__file__).resolve().parent.parent / "world_info"
    sys.path.append(str(base))
    import actions as actions_module

    def fake_fetch_worlds(keyword, limit, headers):
        if keyword == "a":
            return [{"id": "w1"}, {"id": "w2"}]
        if keyword == "b":
            return [{"id": "w2"}, {"id": "w3"}]
        return []

    monkeypatch.setattr(actions_module, "fetch_worlds", fake_fetch_worlds)

    result = actions_module.search_fixed("a,b", headers={}, blacklist=set())
    ids = [w["id"] for w in result]
    assert ids == ["w1", "w2", "w3"]
    assert len(ids) == len(set(ids))
