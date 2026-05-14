import importlib.util
from pathlib import Path


spec = importlib.util.spec_from_file_location(
    "scraper_module",
    Path(__file__).resolve().parent.parent / "world_info" / "scraper" / "scraper.py",
)
scraper = importlib.util.module_from_spec(spec)
spec.loader.exec_module(scraper)  # type: ignore


class _DummyResponse:
    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self._payload


class _DummySession:
    def __init__(self, calls):
        self.calls = calls
        self.trust_env = True

    def get(self, url, headers=None, timeout=None):
        self.calls.append({"url": url, "headers": headers, "timeout": timeout, "trust_env": self.trust_env})
        return _DummyResponse([{"id": "wrld_1", "name": "Alpha"}])


def test_get_user_worlds_uses_world_search_api(monkeypatch):
    calls = []

    monkeypatch.delenv("WORLD_INFO_USE_SYSTEM_PROXY", raising=False)
    monkeypatch.setattr(scraper.requests, "Session", lambda: _DummySession(calls))
    monkeypatch.setattr(scraper.time, "sleep", lambda _: None)

    worlds = scraper.get_user_worlds("usr_123", limit=5, delay=0)

    assert worlds == [{"id": "wrld_1", "name": "Alpha"}]
    assert len(calls) == 1
    assert calls[0]["url"].startswith("https://api.vrchat.cloud/api/1/worlds?")
    assert "userId=usr_123" in calls[0]["url"]
    assert "sort=updated" in calls[0]["url"]
    assert calls[0]["trust_env"] is False


def test_search_worlds_uses_keyword_query(monkeypatch):
    calls = []

    class _KeywordSession(_DummySession):
        def get(self, url, headers=None, timeout=None):
            self.calls.append({"url": url, "headers": headers, "timeout": timeout, "trust_env": self.trust_env})
            return _DummyResponse([])

    monkeypatch.delenv("WORLD_INFO_USE_SYSTEM_PROXY", raising=False)
    monkeypatch.setattr(scraper.requests, "Session", lambda: _KeywordSession(calls))

    scraper.search_worlds("Taiwan world", limit=3, delay=0)

    assert len(calls) == 1
    assert "search=Taiwan+world" in calls[0]["url"]
    assert "sort=relevance" in calls[0]["url"]
    assert calls[0]["trust_env"] is False


def test_search_worlds_query_supports_tags_and_active_endpoint(monkeypatch):
    calls = []

    class _TagSession(_DummySession):
        def get(self, url, headers=None, timeout=None):
            self.calls.append({"url": url, "headers": headers, "timeout": timeout, "trust_env": self.trust_env})
            return _DummyResponse([])

    monkeypatch.delenv("WORLD_INFO_USE_SYSTEM_PROXY", raising=False)
    monkeypatch.setattr(scraper.requests, "Session", lambda: _TagSession(calls))

    scraper.search_worlds_query(
        tags=["author_tag_game", "admin_spacejam3"],
        notags="admin_hidden",
        sort="heat",
        active=True,
        limit=10,
        delay=0,
    )

    assert len(calls) == 1
    assert calls[0]["url"].startswith("https://api.vrchat.cloud/api/1/worlds/active?")
    assert "tag=author_tag_game%2Cadmin_spacejam3" in calls[0]["url"]
    assert "notag=admin_hidden" in calls[0]["url"]
    assert "sort=heat" in calls[0]["url"]
    assert "n=10" in calls[0]["url"]


def test_search_worlds_query_omits_none_like_optional_filters(monkeypatch):
    calls = []

    class _TagSession(_DummySession):
        def get(self, url, headers=None, timeout=None):
            self.calls.append({"url": url, "headers": headers, "timeout": timeout, "trust_env": self.trust_env})
            return _DummyResponse([])

    monkeypatch.delenv("WORLD_INFO_USE_SYSTEM_PROXY", raising=False)
    monkeypatch.setattr(scraper.requests, "Session", lambda: _TagSession(calls))

    scraper.search_worlds_query(
        tags="admin_spacejam3",
        release_status="None",
        platform=None,
        limit=5,
        delay=0,
    )

    assert len(calls) == 1
    assert "tag=admin_spacejam3" in calls[0]["url"]
    assert "releaseStatus" not in calls[0]["url"]
    assert "platform" not in calls[0]["url"]


def test_scraper_can_opt_in_to_system_proxy(monkeypatch):
    calls = []

    class _RequestsModule:
        class exceptions:
            HTTPError = Exception

        def get(self, url, headers=None, timeout=None):
            calls.append({"url": url, "headers": headers, "timeout": timeout})
            return _DummyResponse([])

    monkeypatch.setattr(scraper, "requests", _RequestsModule())
    monkeypatch.setenv("WORLD_INFO_USE_SYSTEM_PROXY", "1")

    scraper.search_worlds("proxy", limit=1, delay=0)

    assert len(calls) == 1
    assert "search=proxy" in calls[0]["url"]


def test_get_user_worlds_reports_missing_auth_on_401(monkeypatch):
    class _HttpError(Exception):
        def __init__(self, response):
            super().__init__("401")
            self.response = response

    class _Response:
        status_code = 401

        def raise_for_status(self):
            raise _HttpError(self)

        def json(self):
            return []

    class _RequestsModule:
        class exceptions:
            HTTPError = _HttpError

        def Session(self):
            class _Session:
                trust_env = False

                def get(self, url, headers=None, timeout=None):
                    return _Response()

            return _Session()

    monkeypatch.setattr(scraper, "requests", _RequestsModule())
    monkeypatch.delenv("WORLD_INFO_USE_SYSTEM_PROXY", raising=False)
    scraper.HEADERS = {"User-Agent": "test"}

    try:
        scraper.get_user_worlds("usr_123", limit=1, delay=0)
        assert False, "expected RuntimeError"
    except RuntimeError as exc:
        assert "headers.json" in str(exc)
        assert "401 Unauthorized" in str(exc)


def test_get_user_worlds_retries_after_429(monkeypatch):
    calls = []
    sleeps = []

    class _HttpError(Exception):
        def __init__(self, response):
            super().__init__("429")
            self.response = response

    class _RateLimitedResponse:
        status_code = 429
        headers = {"Retry-After": "2"}

        def raise_for_status(self):
            raise _HttpError(self)

        def json(self):
            return []

    class _SuccessResponse:
        status_code = 200
        headers = {}

        def raise_for_status(self):
            return None

        def json(self):
            return [{"id": "wrld_retry", "name": "Recovered"}]

    class _RequestsModule:
        class exceptions:
            HTTPError = _HttpError

        def Session(self):
            class _Session:
                trust_env = False

                def get(self, url, headers=None, timeout=None):
                    calls.append(url)
                    if len(calls) == 1:
                        return _RateLimitedResponse()
                    return _SuccessResponse()

            return _Session()

    monkeypatch.setattr(scraper, "requests", _RequestsModule())
    monkeypatch.delenv("WORLD_INFO_USE_SYSTEM_PROXY", raising=False)
    monkeypatch.setattr(scraper.time, "sleep", lambda seconds: sleeps.append(seconds))

    worlds = scraper.get_user_worlds("usr_123", limit=1, delay=0)

    assert worlds == [{"id": "wrld_retry", "name": "Recovered"}]
    assert sleeps == [2]
    assert len(calls) == 2


def test_get_user_worlds_raises_after_single_429_retry(monkeypatch):
    sleeps = []

    class _HttpError(Exception):
        def __init__(self, response):
            super().__init__("429")
            self.response = response

    class _RateLimitedResponse:
        status_code = 429
        headers = {}

        def raise_for_status(self):
            raise _HttpError(self)

        def json(self):
            return []

    class _RequestsModule:
        class exceptions:
            HTTPError = _HttpError

        def Session(self):
            class _Session:
                trust_env = False

                def get(self, url, headers=None, timeout=None):
                    return _RateLimitedResponse()

            return _Session()

    monkeypatch.setattr(scraper, "requests", _RequestsModule())
    monkeypatch.delenv("WORLD_INFO_USE_SYSTEM_PROXY", raising=False)
    monkeypatch.setattr(scraper.time, "sleep", lambda seconds: sleeps.append(seconds))

    try:
        scraper.get_user_worlds("usr_123", limit=1, delay=0)
        assert False, "expected RuntimeError"
    except RuntimeError as exc:
        assert "429 Too Many Requests" in str(exc)

    assert sleeps == [15]
