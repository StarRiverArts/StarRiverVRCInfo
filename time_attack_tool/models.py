from __future__ import annotations

from dataclasses import asdict, dataclass, field


def parse_lap_time_to_ms(value: str | int | float | None) -> int:
    """Parse lap time text into milliseconds."""
    if value is None:
        raise ValueError("lap time is required")

    if isinstance(value, int):
        return value

    if isinstance(value, float):
        return int(round(value))

    text = str(value).strip()
    if not text:
        raise ValueError("lap time is empty")

    if text.isdigit():
        return int(text)

    minutes = 0
    seconds_text = text
    if ":" in text:
        minute_text, seconds_text = text.split(":", 1)
        minutes = int(minute_text)

    if "." in seconds_text:
        seconds_part, millis_part = seconds_text.split(".", 1)
        millis = int((millis_part + "000")[:3])
    else:
        seconds_part = seconds_text
        millis = 0

    seconds = int(seconds_part)
    total_ms = ((minutes * 60) + seconds) * 1000 + millis
    return total_ms


def format_lap_time_ms(lap_time_ms: int) -> str:
    """Format milliseconds into m:ss.mmm or s.mmm."""
    if lap_time_ms < 0:
        raise ValueError("lap time cannot be negative")

    minutes, remainder = divmod(lap_time_ms, 60_000)
    seconds, millis = divmod(remainder, 1000)
    if minutes:
        return f"{minutes}:{seconds:02d}.{millis:03d}"
    return f"{seconds}.{millis:03d}"


@dataclass(slots=True)
class NormalizedRecord:
    record_id: str
    submission_id: str | None
    source_type: str
    review_status: str
    record_tier: str
    racer_id: str
    racer_display_name: str
    vehicle_id: str
    vehicle_display_name: str
    track_family_id: str | None
    track_family_name: str | None
    track_variant_id: str
    track_variant_name: str
    track_route_id: str
    track_route_name: str
    lap_time_ms: int
    lap_time_text: str
    platform: str | None
    fps_value: str | None
    recorded_at: str | None
    approved_at: str | None
    event_id: str | None
    notes: str | None
    primary_badge: str | None = None
    badges: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, object]:
        return asdict(self)

