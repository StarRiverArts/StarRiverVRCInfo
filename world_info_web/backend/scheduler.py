from __future__ import annotations

import json
import logging
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any

from world_info.scraper.scraper import VRChatRateLimitError

if TYPE_CHECKING:
    from .service import WorldInfoService

logger = logging.getLogger(__name__)

VALID_INTERVALS = {
    "disabled": 0,
    "1h": 3600,
    "3h": 10800,
    "6h": 21600,
    "12h": 43200,
    "1d": 86400,
    "2d": 172800,
    "7d": 604800,
}

DEFAULT_CONFIG: dict[str, Any] = {}
GLOBAL_CONFIG_KEY = "__global__"


class AutoSyncScheduler:
    def __init__(self, service: WorldInfoService, config_path: Path) -> None:
        self._service = service
        self._config_path = config_path
        self._thread: threading.Thread | None = None
        self._stop = threading.Event()

    def load_config(self) -> dict[str, Any]:
        if self._config_path.exists():
            try:
                return json.loads(self._config_path.read_text(encoding="utf-8"))
            except Exception:
                pass
        return {}

    def save_config(self, config: dict[str, Any]) -> None:
        self._config_path.parent.mkdir(parents=True, exist_ok=True)
        self._config_path.write_text(
            json.dumps(config, ensure_ascii=False, indent=2), encoding="utf-8"
        )

    def get_rate_limit_state(self, config: dict[str, Any] | None = None) -> dict[str, Any]:
        config = config if config is not None else self.load_config()
        global_cfg = config.get(GLOBAL_CONFIG_KEY, {}) if isinstance(config.get(GLOBAL_CONFIG_KEY, {}), dict) else {}
        cooldown_until = global_cfg.get("rate_limit_until")
        cooldown_dt = None
        if cooldown_until:
            try:
                cooldown_dt = datetime.fromisoformat(cooldown_until)
            except ValueError:
                cooldown_dt = None
        now = datetime.now(tz=timezone.utc)
        active = bool(cooldown_dt and cooldown_dt > now)
        remaining_seconds = max(int((cooldown_dt - now).total_seconds()), 0) if cooldown_dt else 0
        return {
            "active": active,
            "cooldown_until": cooldown_dt.isoformat() if cooldown_dt else None,
            "remaining_seconds": remaining_seconds,
            "retry_after_seconds": int(global_cfg.get("retry_after_seconds") or 0),
            "last_message": global_cfg.get("last_rate_limit_message"),
            "last_event_at": global_cfg.get("last_rate_limit_at"),
        }

    def record_rate_limit(
        self,
        *,
        job_key: str | None,
        retry_after_seconds: int,
        cooldown_seconds: int,
        cooldown_until: str,
        message: str,
    ) -> None:
        config = self.load_config()
        global_cfg = config.setdefault(GLOBAL_CONFIG_KEY, {})
        now_iso = datetime.now(tz=timezone.utc).isoformat()
        global_cfg["rate_limit_until"] = cooldown_until
        global_cfg["retry_after_seconds"] = retry_after_seconds
        global_cfg["last_rate_limit_message"] = message
        global_cfg["last_rate_limit_at"] = now_iso
        if job_key:
            job_cfg = config.setdefault(job_key, {})
            job_cfg["last_attempt_at"] = now_iso
            job_cfg["last_error"] = message
            job_cfg["running"] = False
        self.save_config(config)

    def _resolve_last_run_iso(self, job_key: str, job_cfg: dict[str, Any]) -> str | None:
        candidates = []
        configured = job_cfg.get("last_auto_run")
        if configured:
            candidates.append(configured)
        for run in self._service.storage.list_runs(limit=20, job_key=job_key):
            if run.get("status") == "completed":
                candidates.append(run.get("finished_at") or run.get("started_at"))
                break
        valid = []
        for value in candidates:
            if not value:
                continue
            try:
                valid.append(datetime.fromisoformat(value))
            except ValueError:
                continue
        if not valid:
            return None
        return max(valid, key=lambda item: item.timestamp()).isoformat()

    def _latest_completed_run(self, job_key: str) -> dict[str, Any] | None:
        for run in self._service.storage.list_runs(limit=20, job_key=job_key):
            if run.get("status") == "completed":
                return run
        return None

    def get_status(self) -> dict[str, Any]:
        config = self.load_config()
        if self._normalise_schedule(config):
            self.save_config(config)
        rate_limit_state = self.get_rate_limit_state(config)
        jobs_info = self._service.list_jobs()
        result = {}
        for job in jobs_info:
            key = job["job_key"]
            job_cfg = config.get(key, {})
            interval_key = job_cfg.get("interval", "disabled")
            interval_sec = VALID_INTERVALS.get(interval_key, 0)
            last_run_iso = self._resolve_last_run_iso(key, job_cfg)
            latest_run = self._latest_completed_run(key)
            next_run_iso = None
            overdue = False
            running = bool(job_cfg.get("running"))
            if running and job_cfg.get("last_attempt_at"):
                try:
                    attempt_dt = datetime.fromisoformat(job_cfg["last_attempt_at"])
                    stale_after = max(interval_sec * 2, 21600)
                    running = (datetime.now(tz=timezone.utc).timestamp() - attempt_dt.timestamp()) < stale_after
                except ValueError:
                    running = False
            if interval_sec > 0 and last_run_iso:
                last_dt = datetime.fromisoformat(last_run_iso)
                next_dt = datetime.fromtimestamp(
                    last_dt.timestamp() + interval_sec, tz=timezone.utc
                )
                next_run_iso = next_dt.isoformat()
                overdue = not running and datetime.now(tz=timezone.utc) >= next_dt
            result[key] = {
                "job_key": key,
                "label": job.get("label", key),
                "interval": interval_key,
                "interval_seconds": interval_sec,
                "last_auto_run": last_run_iso,
                "last_success_trigger": latest_run.get("trigger_type") if latest_run else None,
                "last_success_run_id": latest_run.get("id") if latest_run else None,
                "last_attempt_at": job_cfg.get("last_attempt_at"),
                "next_run": next_run_iso,
                "overdue": overdue,
                "running": running,
                "last_error": job_cfg.get("last_error"),
                "rate_limit_active": rate_limit_state["active"],
                "rate_limit_until": rate_limit_state["cooldown_until"],
            }
        return result

    def set_interval(self, job_key: str, interval_key: str) -> None:
        if interval_key not in VALID_INTERVALS:
            raise ValueError(f"Invalid interval: {interval_key}. Valid: {list(VALID_INTERVALS)}")
        config = self.load_config()
        config.setdefault(job_key, {})["interval"] = interval_key
        if interval_key == "disabled":
            config[job_key].pop("last_auto_run", None)
            config[job_key].pop("stagger_interval", None)
        else:
            self._rebalance_interval_group(config, interval_key)
            for grouped_job_key, job_cfg in config.items():
                if isinstance(job_cfg, dict) and job_cfg.get("interval") == interval_key:
                    job_cfg["stagger_interval"] = interval_key
        self.save_config(config)

    def _rebalance_interval_group(
        self,
        config: dict[str, Any],
        interval_key: str,
        *,
        reference: datetime | None = None,
    ) -> None:
        interval_sec = VALID_INTERVALS.get(interval_key, 0)
        if interval_sec <= 0:
            return
        reference = reference or datetime.now(tz=timezone.utc)
        group = sorted(
            job_key
            for job_key, job_cfg in config.items()
            if isinstance(job_cfg, dict) and job_cfg.get("interval", "disabled") == interval_key
        )
        if not group:
            return
        slot_sec = interval_sec / max(len(group), 1)
        for index, grouped_job_key in enumerate(group):
            delay_sec = int(round(slot_sec * index))
            next_dt = datetime.fromtimestamp(reference.timestamp() + delay_sec, tz=timezone.utc)
            last_run_dt = datetime.fromtimestamp(next_dt.timestamp() - interval_sec, tz=timezone.utc)
            config.setdefault(grouped_job_key, {})["last_auto_run"] = last_run_dt.isoformat()

    def _normalise_schedule(self, config: dict[str, Any]) -> bool:
        changed = False
        interval_groups = sorted(
            {
                job_cfg.get("interval")
                for job_cfg in config.values()
                if isinstance(job_cfg, dict) and VALID_INTERVALS.get(job_cfg.get("interval", "disabled"), 0) > 0
            }
        )
        for interval_key in interval_groups:
            group = [
                job_key
                for job_key, job_cfg in config.items()
                if isinstance(job_cfg, dict) and job_cfg.get("interval") == interval_key
            ]
            if any(config[job_key].get("stagger_interval") != interval_key for job_key in group):
                self._rebalance_interval_group(config, interval_key)
                for job_key in group:
                    config[job_key]["stagger_interval"] = interval_key
                changed = True
        return changed

    def remove_job(self, job_key: str) -> None:
        config = self.load_config()
        if job_key in config:
            config.pop(job_key)
            self.save_config(config)

    def _record_run(self, job_key: str) -> None:
        config = self.load_config()
        now_iso = datetime.now(tz=timezone.utc).isoformat()
        job_cfg = config.setdefault(job_key, {})
        job_cfg["last_auto_run"] = now_iso
        job_cfg["last_attempt_at"] = now_iso
        job_cfg["last_error"] = None
        job_cfg["running"] = False
        self.save_config(config)

    def record_run(self, job_key: str) -> None:
        self._record_run(job_key)

    def _record_attempt(self, job_key: str) -> None:
        config = self.load_config()
        job_cfg = config.setdefault(job_key, {})
        job_cfg["last_attempt_at"] = datetime.now(tz=timezone.utc).isoformat()
        job_cfg["last_error"] = None
        job_cfg["running"] = True
        self.save_config(config)

    def _record_failure(self, job_key: str, message: str) -> None:
        config = self.load_config()
        job_cfg = config.setdefault(job_key, {})
        job_cfg["last_attempt_at"] = datetime.now(tz=timezone.utc).isoformat()
        job_cfg["last_error"] = message
        job_cfg["running"] = False
        self.save_config(config)

    def _tick(self) -> None:
        config = self.load_config()
        if self._normalise_schedule(config):
            self.save_config(config)
        rate_limit_state = self.get_rate_limit_state(config)
        if rate_limit_state["active"]:
            logger.warning(
                "AutoSync: global cooldown active until %s, skipping scheduler tick",
                rate_limit_state["cooldown_until"],
            )
            return
        now = datetime.now(tz=timezone.utc)
        for job_key, job_cfg in config.items():
            if job_key == GLOBAL_CONFIG_KEY:
                continue
            interval_key = job_cfg.get("interval", "disabled")
            interval_sec = VALID_INTERVALS.get(interval_key, 0)
            if interval_sec <= 0:
                continue
            last_run_iso = self._resolve_last_run_iso(job_key, job_cfg)
            if last_run_iso:
                last_dt = datetime.fromisoformat(last_run_iso)
                if (now.timestamp() - last_dt.timestamp()) < interval_sec:
                    continue
            logger.info("AutoSync: running job %s (interval %s)", job_key, interval_key)
            try:
                self._record_attempt(job_key)
                self._service.run_job(job_key, trigger_type="auto")
                self._record_run(job_key)
            except VRChatRateLimitError as exc:
                rate_limit_info = self._service.record_rate_limit_event(
                    error=exc,
                    source_key=f"job:{job_key}",
                    job_key=job_key,
                    trigger_type="auto",
                )
                self.record_rate_limit(
                    job_key=job_key,
                    retry_after_seconds=rate_limit_info["retry_after_seconds"],
                    cooldown_seconds=rate_limit_info["cooldown_seconds"],
                    cooldown_until=rate_limit_info["cooldown_until"],
                    message=rate_limit_info["message"],
                )
                logger.error("AutoSync: job %s hit VRChat rate limit: %s", job_key, exc)
                break
            except Exception as exc:
                self._record_failure(job_key, str(exc))
                logger.error("AutoSync: job %s failed: %s", job_key, exc)

    def _loop(self) -> None:
        while not self._stop.is_set():
            try:
                self._tick()
            except Exception as exc:
                logger.error("AutoSync loop error: %s", exc)
            if self._stop.wait(60):  # check every minute
                break

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._loop, daemon=True, name="AutoSyncScheduler")
        self._thread.start()
        logger.info("AutoSyncScheduler started")

    def stop(self) -> None:
        self._stop.set()
