from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parent.parent))

from time_attack_tool.io_utils import (  # noqa: E402
    build_review_summary,
    load_approved_records,
    records_to_dicts,
)
from time_attack_tool.leaderboard_builder import build_leaderboards  # noqa: E402


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )


def build_artifacts(
    approved_records_path: str,
    output_dir: str,
    submissions_path: str | None = None,
    approved_records_sheet: str | None = None,
    approved_records_gid: str | None = None,
    submissions_sheet: str | None = None,
    submissions_gid: str | None = None,
    default_tier: str = "qualified",
    include_recorded: bool = False,
) -> dict[str, object]:
    records = load_approved_records(
        approved_records_path,
        sheet_name=approved_records_sheet,
        sheet_gid=approved_records_gid,
    )
    leaderboards = build_leaderboards(
        records,
        default_tier=default_tier,
        include_recorded=include_recorded,
    )
    review_summary = build_review_summary(
        submissions_path,
        sheet_name=submissions_sheet,
        sheet_gid=submissions_gid,
    )

    generated_at = _utc_now()
    output = Path(output_dir)

    records_payload = {
        "generated_at": generated_at,
        "record_count": len(records),
        "records": records_to_dicts(records),
    }
    leaderboards_payload = {
        "generated_at": generated_at,
        **leaderboards,
    }
    manifest_payload = {
        "generated_at": generated_at,
        "inputs": {
            "approved_records": approved_records_path,
            "submissions": submissions_path,
        },
        "outputs": [
            "records.json",
            "leaderboards.json",
            "manifest.json",
        ]
        + (["review_summary.json"] if review_summary is not None else []),
    }

    _write_json(output / "records.json", records_payload)
    _write_json(output / "leaderboards.json", leaderboards_payload)
    _write_json(output / "manifest.json", manifest_payload)
    if review_summary is not None:
        _write_json(
            output / "review_summary.json",
            {
                "generated_at": generated_at,
                **review_summary,
            },
        )

    return {
        "generated_at": generated_at,
        "record_count": len(records),
        "review_summary_present": review_summary is not None,
        "output_dir": str(output),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Build normalized JSON artifacts for the next-generation time attack workflow.",
    )
    parser.add_argument("--approved-records", required=True, help="CSV/XLSX export of approved_records")
    parser.add_argument("--submissions", help="Optional CSV/XLSX export of submissions")
    parser.add_argument("--output-dir", default="time_attack_tool/out", help="Directory for generated artifacts")
    parser.add_argument("--approved-records-sheet", help="Worksheet name when approved_records input is XLSX")
    parser.add_argument("--approved-records-gid", help="Worksheet gid when approved_records input is a Google Sheet URL")
    parser.add_argument("--submissions-sheet", help="Worksheet name when submissions input is XLSX")
    parser.add_argument("--submissions-gid", help="Worksheet gid when submissions input is a Google Sheet URL")
    parser.add_argument("--default-tier", default="qualified", help="Default record tier used for leaderboard filtering")
    parser.add_argument(
        "--include-recorded",
        action="store_true",
        help="Include record_tier=recorded rows in leaderboard outputs",
    )
    args = parser.parse_args(argv)

    result = build_artifacts(
        approved_records_path=args.approved_records,
        submissions_path=args.submissions,
        output_dir=args.output_dir,
        approved_records_sheet=args.approved_records_sheet,
        approved_records_gid=args.approved_records_gid,
        submissions_sheet=args.submissions_sheet,
        submissions_gid=args.submissions_gid,
        default_tier=args.default_tier,
        include_recorded=args.include_recorded,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
