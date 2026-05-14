from __future__ import annotations

from collections import defaultdict

from time_attack_tool.models import NormalizedRecord


def _record_sort_key(record: NormalizedRecord) -> tuple[int, str]:
    return (record.lap_time_ms, record.record_id)


def apply_badges(records: list[NormalizedRecord]) -> None:
    """Assign TR / CR / PR badges using route as the scoring unit."""
    by_route: dict[str, list[NormalizedRecord]] = defaultdict(list)
    for record in records:
        by_route[record.track_route_id].append(record)

    for route_records in by_route.values():
        sorted_route_records = sorted(route_records, key=_record_sort_key)
        if not sorted_route_records:
            continue

        tr_record = sorted_route_records[0]
        tr_record.badges = ["TR"]
        tr_record.primary_badge = "TR"

        best_vehicle: dict[str, NormalizedRecord] = {}
        best_player: dict[str, NormalizedRecord] = {}
        for record in sorted_route_records:
            best_vehicle.setdefault(record.vehicle_id, record)
            best_player.setdefault(record.racer_id, record)

        for record in best_vehicle.values():
            if record.primary_badge is None:
                record.badges = ["CR"]
                record.primary_badge = "CR"

        for record in best_player.values():
            if record.primary_badge is None:
                record.badges = ["PR"]
                record.primary_badge = "PR"

        for record in sorted_route_records:
            if record.primary_badge is None:
                record.badges = []


def build_leaderboards(
    records: list[NormalizedRecord],
    default_tier: str = "qualified",
    include_recorded: bool = False,
) -> dict[str, object]:
    eligible = [
        record
        for record in records
        if record.review_status == "approved"
        and (include_recorded or record.record_tier == default_tier)
    ]
    apply_badges(eligible)

    by_route: dict[str, object] = {}
    route_buckets: dict[str, list[NormalizedRecord]] = defaultdict(list)
    variant_buckets: dict[str, list[NormalizedRecord]] = defaultdict(list)
    recent_records = sorted(
        eligible,
        key=lambda record: (
            record.recorded_at or "",
            record.approved_at or "",
            record.record_id,
        ),
        reverse=True,
    )

    for record in eligible:
        route_buckets[record.track_route_id].append(record)
        variant_buckets[record.track_variant_id].append(record)

    for route_id, route_records in sorted(route_buckets.items()):
        sorted_records = sorted(route_records, key=_record_sort_key)
        fastest = sorted_records[0]

        vehicle_best: list[dict[str, object]] = []
        player_best: list[dict[str, object]] = []
        seen_vehicle: set[str] = set()
        seen_player: set[str] = set()
        for record in sorted_records:
            if record.vehicle_id not in seen_vehicle:
                vehicle_best.append(
                    {
                        "vehicle_id": record.vehicle_id,
                        "vehicle_display_name": record.vehicle_display_name,
                        "record_id": record.record_id,
                        "racer_id": record.racer_id,
                        "racer_display_name": record.racer_display_name,
                        "lap_time_ms": record.lap_time_ms,
                        "lap_time_text": record.lap_time_text,
                    }
                )
                seen_vehicle.add(record.vehicle_id)
            if record.racer_id not in seen_player:
                player_best.append(
                    {
                        "racer_id": record.racer_id,
                        "racer_display_name": record.racer_display_name,
                        "record_id": record.record_id,
                        "vehicle_id": record.vehicle_id,
                        "vehicle_display_name": record.vehicle_display_name,
                        "lap_time_ms": record.lap_time_ms,
                        "lap_time_text": record.lap_time_text,
                    }
                )
                seen_player.add(record.racer_id)

        by_route[route_id] = {
            "track_variant_id": fastest.track_variant_id,
            "track_variant_name": fastest.track_variant_name,
            "track_route_id": fastest.track_route_id,
            "track_route_name": fastest.track_route_name,
            "fastest_overall": {
                "record_id": fastest.record_id,
                "racer_id": fastest.racer_id,
                "racer_display_name": fastest.racer_display_name,
                "vehicle_id": fastest.vehicle_id,
                "vehicle_display_name": fastest.vehicle_display_name,
                "lap_time_ms": fastest.lap_time_ms,
                "lap_time_text": fastest.lap_time_text,
            },
            "top_records": [record.to_dict() for record in sorted_records[:20]],
            "vehicle_best": vehicle_best,
            "player_best": player_best,
        }

    by_variant: dict[str, object] = {}
    for variant_id, variant_records in sorted(variant_buckets.items()):
        sorted_variant_records = sorted(variant_records, key=_record_sort_key)
        fastest = sorted_variant_records[0]
        route_ids = sorted({record.track_route_id for record in variant_records})
        by_variant[variant_id] = {
            "track_variant_id": variant_id,
            "track_variant_name": fastest.track_variant_name,
            "track_family_id": fastest.track_family_id,
            "track_family_name": fastest.track_family_name,
            "route_ids": route_ids,
            "record_count": len(variant_records),
            "fastest_record": {
                "record_id": fastest.record_id,
                "track_route_id": fastest.track_route_id,
                "track_route_name": fastest.track_route_name,
                "racer_display_name": fastest.racer_display_name,
                "vehicle_display_name": fastest.vehicle_display_name,
                "lap_time_ms": fastest.lap_time_ms,
                "lap_time_text": fastest.lap_time_text,
            },
        }

    return {
        "default_record_tier": default_tier,
        "include_recorded": include_recorded,
        "record_count": len(eligible),
        "by_route": by_route,
        "by_variant": by_variant,
        "recent_records": [record.to_dict() for record in recent_records[:20]],
    }

