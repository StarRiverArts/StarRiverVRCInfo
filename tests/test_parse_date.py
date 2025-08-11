import datetime as dt
import importlib.util
from pathlib import Path

spec = importlib.util.spec_from_file_location(
    "scraper_module",
    Path(__file__).resolve().parent.parent / "world_info" / "scraper" / "scraper.py",
)
scraper = importlib.util.module_from_spec(spec)
spec.loader.exec_module(scraper)  # type: ignore
_parse_date = scraper._parse_date


def test_parse_date_numeric_and_digit_string():
    ts = 1_234_567_890
    expected = dt.datetime.fromtimestamp(ts, dt.timezone.utc)

    int_dt = _parse_date(ts)
    float_dt = _parse_date(float(ts))
    str_dt = _parse_date(str(ts))

    assert int_dt == float_dt == str_dt == expected
