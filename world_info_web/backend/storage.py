from __future__ import annotations

import json
import logging
import sqlite3
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


class WorldInfoStorage:
    def __init__(self, db_path: Path) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.db_path), timeout=60)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=60000")
        return conn

    def _initialize(self) -> None:
        existing_db = self.db_path.exists() and self.db_path.stat().st_size > 0
        try:
            self._initialize_schema(prefer_wal=True)
        except sqlite3.OperationalError as exc:
            lowered = str(exc).casefold()
            if "disk i/o" in lowered:
                logger.warning("SQLite WAL init failed for %s; retrying without WAL: %s", self.db_path, exc)
                self._cleanup_sqlite_sidecars()
                try:
                    self._initialize_schema(prefer_wal=False)
                    return
                except sqlite3.OperationalError as retry_exc:
                    lowered_retry = str(retry_exc).casefold()
                    if existing_db and ("locked" in lowered_retry or "disk i/o" in lowered_retry):
                        logger.warning("SQLite schema init skipped because database is locked: %s", retry_exc)
                        return
                    raise
            if existing_db and ("locked" in lowered or "disk i/o" in lowered):
                logger.warning("SQLite schema init skipped because database is locked: %s", exc)
                return
            raise

    def _cleanup_sqlite_sidecars(self) -> None:
        for candidate in (
            self.db_path,
            self.db_path.with_name(f"{self.db_path.name}-wal"),
            self.db_path.with_name(f"{self.db_path.name}-shm"),
            self.db_path.with_suffix(f"{self.db_path.suffix}-journal"),
        ):
            try:
                if candidate.exists() and candidate.stat().st_size == 0:
                    candidate.unlink()
            except OSError:
                continue

    def _initialize_schema(self, *, prefer_wal: bool) -> None:
        with self._connect() as conn:
            if prefer_wal:
                try:
                    conn.execute("PRAGMA journal_mode=WAL")
                except sqlite3.OperationalError as exc:
                    logger.warning("SQLite journal mode update skipped: %s", exc)
            else:
                try:
                    conn.execute("PRAGMA journal_mode=DELETE")
                except sqlite3.OperationalError as exc:
                    logger.warning("SQLite fallback journal mode update skipped: %s", exc)
            try:
                conn.execute("PRAGMA synchronous=NORMAL")
                conn.execute("PRAGMA temp_store=MEMORY")
            except sqlite3.OperationalError as exc:
                logger.warning("SQLite performance pragma update skipped: %s", exc)
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS sync_runs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    source_key TEXT NOT NULL,
                    job_key TEXT,
                    trigger_type TEXT NOT NULL,
                    query_label TEXT,
                    status TEXT NOT NULL,
                    started_at TEXT NOT NULL,
                    finished_at TEXT,
                    world_count INTEGER NOT NULL DEFAULT 0,
                    error_text TEXT
                );

                CREATE TABLE IF NOT EXISTS world_snapshots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id INTEGER NOT NULL,
                    source_key TEXT NOT NULL,
                    fetched_at TEXT NOT NULL,
                    world_id TEXT,
                    name TEXT,
                    author_id TEXT,
                    author_name TEXT,
                    capacity INTEGER,
                    visits INTEGER,
                    favorites INTEGER,
                    heat INTEGER,
                    popularity INTEGER,
                    created_at TEXT,
                    updated_at TEXT,
                    publication_date TEXT,
                    labs_publication_date TEXT,
                    release_status TEXT,
                    image_url TEXT,
                    thumbnail_url TEXT,
                    world_url TEXT,
                    tags_json TEXT NOT NULL,
                    favorite_rate REAL,
                    labs_to_publication_days INTEGER,
                    days_since_update INTEGER,
                    visits_per_day REAL,
                    raw_json TEXT NOT NULL,
                    FOREIGN KEY(run_id) REFERENCES sync_runs(id)
                );

                CREATE TABLE IF NOT EXISTS daily_stats (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    source_key TEXT NOT NULL,
                    date TEXT NOT NULL,
                    total_worlds INTEGER NOT NULL,
                    new_worlds_today INTEGER NOT NULL,
                    UNIQUE(source_key, date)
                );

                CREATE TABLE IF NOT EXISTS run_queries (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id INTEGER NOT NULL,
                    query_index INTEGER NOT NULL DEFAULT 0,
                    query_kind TEXT NOT NULL,
                    query_value TEXT NOT NULL,
                    query_label TEXT,
                    query_payload_json TEXT NOT NULL,
                    result_count INTEGER NOT NULL DEFAULT 0,
                    kept_count INTEGER NOT NULL DEFAULT 0,
                    new_world_count INTEGER NOT NULL DEFAULT 0,
                    FOREIGN KEY(run_id) REFERENCES sync_runs(id)
                );

                CREATE TABLE IF NOT EXISTS run_query_hits (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_query_id INTEGER NOT NULL,
                    world_id TEXT NOT NULL,
                    world_name TEXT,
                    author_id TEXT,
                    rank_index INTEGER NOT NULL DEFAULT 0,
                    is_new_global INTEGER NOT NULL DEFAULT 0,
                    FOREIGN KEY(run_query_id) REFERENCES run_queries(id)
                );

                CREATE TABLE IF NOT EXISTS rate_limit_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_at TEXT NOT NULL,
                    source_key TEXT,
                    job_key TEXT,
                    trigger_type TEXT,
                    query_kind TEXT,
                    query_value TEXT,
                    retry_after_seconds INTEGER NOT NULL DEFAULT 0,
                    cooldown_seconds INTEGER NOT NULL DEFAULT 0,
                    cooldown_until TEXT,
                    error_text TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS topics (
                    topic_key TEXT PRIMARY KEY,
                    label TEXT NOT NULL,
                    description TEXT,
                    color TEXT,
                    sort_order INTEGER NOT NULL DEFAULT 0,
                    is_active INTEGER NOT NULL DEFAULT 1
                );

                CREATE TABLE IF NOT EXISTS topic_rules (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    topic_key TEXT NOT NULL,
                    rule_type TEXT NOT NULL,
                    rule_value TEXT NOT NULL,
                    sort_order INTEGER NOT NULL DEFAULT 0,
                    is_active INTEGER NOT NULL DEFAULT 1,
                    FOREIGN KEY(topic_key) REFERENCES topics(topic_key)
                );

                CREATE TABLE IF NOT EXISTS topic_memberships (
                    topic_key TEXT NOT NULL,
                    world_id TEXT NOT NULL,
                    first_seen_at TEXT NOT NULL,
                    last_seen_at TEXT NOT NULL,
                    matched_by TEXT,
                    PRIMARY KEY(topic_key, world_id),
                    FOREIGN KEY(topic_key) REFERENCES topics(topic_key)
                );

                CREATE TABLE IF NOT EXISTS analysis_cache (
                    scope_key TEXT PRIMARY KEY,
                    scope_type TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    source_run_id INTEGER,
                    payload_json TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS creators (
                    creator_id TEXT PRIMARY KEY,
                    display_name TEXT,
                    primary_group_id TEXT,
                    home_region TEXT,
                    notes TEXT,
                    last_seen_at TEXT
                );

                CREATE TABLE IF NOT EXISTS groups (
                    group_id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    region TEXT,
                    category TEXT,
                    description TEXT,
                    managed_status TEXT,
                    external_links_json TEXT,
                    last_synced_at TEXT
                );

                CREATE TABLE IF NOT EXISTS group_world_memberships (
                    group_id TEXT NOT NULL,
                    world_id TEXT NOT NULL,
                    membership_role TEXT,
                    linked_at TEXT NOT NULL,
                    source_key TEXT,
                    PRIMARY KEY(group_id, world_id),
                    FOREIGN KEY(group_id) REFERENCES groups(group_id)
                );

                CREATE TABLE IF NOT EXISTS managed_groups (
                    group_id TEXT PRIMARY KEY,
                    workspace_key TEXT,
                    posting_enabled INTEGER NOT NULL DEFAULT 0,
                    notes TEXT,
                    updated_at TEXT NOT NULL,
                    FOREIGN KEY(group_id) REFERENCES groups(group_id)
                );

                CREATE TABLE IF NOT EXISTS scheduled_posts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    group_id TEXT NOT NULL,
                    content_type TEXT NOT NULL,
                    status TEXT NOT NULL,
                    scheduled_for TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    delivered_at TEXT,
                    FOREIGN KEY(group_id) REFERENCES groups(group_id)
                );

                CREATE INDEX IF NOT EXISTS idx_snapshots_source_world
                ON world_snapshots(source_key, world_id, fetched_at DESC, id DESC);

                CREATE INDEX IF NOT EXISTS idx_snapshots_world
                ON world_snapshots(world_id, fetched_at DESC, id DESC);

                CREATE INDEX IF NOT EXISTS idx_runs_job_started
                ON sync_runs(job_key, started_at DESC, id DESC);

                CREATE INDEX IF NOT EXISTS idx_run_queries_run
                ON run_queries(run_id, query_index, id);

                CREATE INDEX IF NOT EXISTS idx_run_query_hits_query
                ON run_query_hits(run_query_id, rank_index, id);

                CREATE INDEX IF NOT EXISTS idx_run_query_hits_world
                ON run_query_hits(world_id, run_query_id);

                CREATE INDEX IF NOT EXISTS idx_rate_limit_events_at
                ON rate_limit_events(event_at DESC, id DESC);

                CREATE INDEX IF NOT EXISTS idx_topic_rules_topic
                ON topic_rules(topic_key, sort_order, id);

                CREATE INDEX IF NOT EXISTS idx_topic_memberships_topic
                ON topic_memberships(topic_key, last_seen_at DESC, world_id ASC);

                CREATE INDEX IF NOT EXISTS idx_snapshots_author
                ON world_snapshots(author_id, fetched_at DESC, id DESC);

                CREATE INDEX IF NOT EXISTS idx_analysis_cache_scope_type
                ON analysis_cache(scope_type, updated_at DESC, scope_key ASC);

                CREATE INDEX IF NOT EXISTS idx_creators_last_seen
                ON creators(last_seen_at DESC, creator_id ASC);

                CREATE INDEX IF NOT EXISTS idx_groups_managed_status
                ON groups(managed_status, last_synced_at DESC, group_id ASC);

                CREATE INDEX IF NOT EXISTS idx_group_world_memberships_world
                ON group_world_memberships(world_id, group_id);

                CREATE INDEX IF NOT EXISTS idx_scheduled_posts_group_status
                ON scheduled_posts(group_id, status, scheduled_for ASC, id ASC);
                """
            )

    def create_run(
        self,
        *,
        source_key: str,
        job_key: str | None,
        trigger_type: str,
        query_label: str | None,
        started_at: str,
    ) -> int:
        with self._connect() as conn:
            cur = conn.execute(
                """
                INSERT INTO sync_runs (
                    source_key, job_key, trigger_type, query_label, status, started_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (source_key, job_key, trigger_type, query_label, "running", started_at),
            )
            return int(cur.lastrowid)

    def finish_run(
        self,
        run_id: int,
        *,
        status: str,
        finished_at: str,
        world_count: int = 0,
        error_text: str | None = None,
    ) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE sync_runs
                SET status = ?, finished_at = ?, world_count = ?, error_text = ?
                WHERE id = ?
                """,
                (status, finished_at, world_count, error_text, run_id),
            )

    def insert_world_snapshots(
        self,
        *,
        run_id: int,
        source_key: str,
        fetched_at: str,
        worlds: list[dict[str, Any]],
    ) -> None:
        rows = []
        for world in worlds:
            metrics = world.get("metrics", {})
            world_fetched_at = str(world.get("fetched_at") or fetched_at)
            rows.append(
                (
                    run_id,
                    source_key,
                    world_fetched_at,
                    world.get("id"),
                    world.get("name"),
                    world.get("author_id"),
                    world.get("author_name"),
                    world.get("capacity"),
                    world.get("visits"),
                    world.get("favorites"),
                    world.get("heat"),
                    world.get("popularity"),
                    world.get("created_at"),
                    world.get("updated_at"),
                    world.get("publication_date"),
                    world.get("labs_publication_date"),
                    world.get("release_status"),
                    world.get("image_url"),
                    world.get("thumbnail_url"),
                    world.get("world_url"),
                    json.dumps(world.get("tags", []), ensure_ascii=False),
                    metrics.get("favorite_rate"),
                    metrics.get("labs_to_publication_days"),
                    metrics.get("days_since_update"),
                    metrics.get("visits_per_day"),
                    json.dumps(world, ensure_ascii=False),
                )
            )
        if not rows:
            return
        with self._connect() as conn:
            conn.executemany(
                """
                INSERT INTO world_snapshots (
                    run_id, source_key, fetched_at, world_id, name, author_id, author_name,
                    capacity, visits, favorites, heat, popularity, created_at, updated_at,
                    publication_date, labs_publication_date, release_status, image_url,
                    thumbnail_url, world_url, tags_json, favorite_rate,
                    labs_to_publication_days, days_since_update, visits_per_day, raw_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )

    def upsert_daily_stats(
        self,
        *,
        source_key: str,
        date: str,
        total_worlds: int,
        new_worlds_today: int,
    ) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO daily_stats (source_key, date, total_worlds, new_worlds_today)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(source_key, date) DO UPDATE SET
                    total_worlds = excluded.total_worlds,
                    new_worlds_today = excluded.new_worlds_today
                """,
                (source_key, date, total_worlds, new_worlds_today),
            )

    def get_existing_world_ids(self, world_ids: set[str]) -> set[str]:
        if not world_ids:
            return set()
        placeholders = ",".join("?" * len(world_ids))
        query = f"""
            SELECT DISTINCT world_id
            FROM world_snapshots
            WHERE world_id IN ({placeholders})
        """
        with self._connect() as conn:
            rows = conn.execute(query, tuple(sorted(world_ids))).fetchall()
        return {str(row["world_id"]) for row in rows if row["world_id"]}

    def insert_run_queries(self, *, run_id: int, queries: list[dict[str, Any]]) -> None:
        if not queries:
            return
        with self._connect() as conn:
            for item in queries:
                cur = conn.execute(
                    """
                    INSERT INTO run_queries (
                        run_id,
                        query_index,
                        query_kind,
                        query_value,
                        query_label,
                        query_payload_json,
                        result_count,
                        kept_count,
                        new_world_count
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        run_id,
                        item.get("query_index", 0),
                        item.get("query_kind", "keyword"),
                        item.get("query_value", ""),
                        item.get("query_label"),
                        json.dumps(item.get("query_payload", {}), ensure_ascii=False),
                        item.get("result_count", 0),
                        item.get("kept_count", 0),
                        item.get("new_world_count", 0),
                    ),
                )
                run_query_id = int(cur.lastrowid)
                hits = item.get("hits", [])
                if not hits:
                    continue
                conn.executemany(
                    """
                    INSERT INTO run_query_hits (
                        run_query_id,
                        world_id,
                        world_name,
                        author_id,
                        rank_index,
                        is_new_global
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    [
                        (
                            run_query_id,
                            hit.get("world_id"),
                            hit.get("world_name"),
                            hit.get("author_id"),
                            hit.get("rank_index", 0),
                            1 if hit.get("is_new_global") else 0,
                        )
                        for hit in hits
                        if hit.get("world_id")
                    ],
                )

    def list_run_queries(self, run_ids: list[int]) -> list[dict[str, Any]]:
        if not run_ids:
            return []
        placeholders = ",".join("?" * len(run_ids))
        query = f"""
            SELECT
                rq.id,
                rq.run_id,
                rq.query_index,
                rq.query_kind,
                rq.query_value,
                rq.query_label,
                rq.query_payload_json,
                rq.result_count,
                rq.kept_count,
                rq.new_world_count
            FROM run_queries rq
            WHERE rq.run_id IN ({placeholders})
            ORDER BY rq.run_id DESC, rq.query_index ASC, rq.id ASC
        """
        with self._connect() as conn:
            rows = conn.execute(query, tuple(run_ids)).fetchall()
        items: list[dict[str, Any]] = []
        for row in rows:
            item = dict(row)
            item["query_payload"] = json.loads(item.pop("query_payload_json") or "{}")
            items.append(item)
        return items

    def list_run_query_hits(self, run_query_ids: list[int]) -> list[dict[str, Any]]:
        if not run_query_ids:
            return []
        placeholders = ",".join("?" * len(run_query_ids))
        query = f"""
            SELECT
                run_query_id,
                world_id,
                world_name,
                author_id,
                rank_index,
                is_new_global
            FROM run_query_hits
            WHERE run_query_id IN ({placeholders})
            ORDER BY run_query_id ASC, rank_index ASC, id ASC
        """
        with self._connect() as conn:
            rows = conn.execute(query, tuple(run_query_ids)).fetchall()
        return [dict(row) for row in rows]

    def list_topic_memberships_for_worlds(self, world_ids: set[str]) -> list[dict[str, Any]]:
        if not world_ids:
            return []
        placeholders = ",".join("?" * len(world_ids))
        query = f"""
            SELECT
                tm.world_id,
                tm.topic_key,
                t.label AS topic_label,
                t.is_active
            FROM topic_memberships tm
            LEFT JOIN topics t ON t.topic_key = tm.topic_key
            WHERE tm.world_id IN ({placeholders})
            ORDER BY tm.world_id ASC, tm.topic_key ASC
        """
        with self._connect() as conn:
            rows = conn.execute(query, tuple(sorted(world_ids))).fetchall()
        return [dict(row) for row in rows]

    def insert_rate_limit_event(
        self,
        *,
        event_at: str,
        source_key: str | None,
        job_key: str | None,
        trigger_type: str | None,
        query_kind: str | None,
        query_value: str | None,
        retry_after_seconds: int,
        cooldown_seconds: int,
        cooldown_until: str | None,
        error_text: str,
    ) -> int:
        try:
            with self._connect() as conn:
                cur = conn.execute(
                    """
                    INSERT INTO rate_limit_events (
                        event_at,
                        source_key,
                        job_key,
                        trigger_type,
                        query_kind,
                        query_value,
                        retry_after_seconds,
                        cooldown_seconds,
                        cooldown_until,
                        error_text
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        event_at,
                        source_key,
                        job_key,
                        trigger_type,
                        query_kind,
                        query_value,
                        retry_after_seconds,
                        cooldown_seconds,
                        cooldown_until,
                        error_text,
                    ),
                )
                return int(cur.lastrowid)
        except sqlite3.OperationalError as exc:
            logger.warning("Failed to persist rate limit event: %s", exc)
            return 0

    def list_rate_limit_events(self, *, limit: int = 20) -> list[dict[str, Any]]:
        try:
            with self._connect() as conn:
                rows = conn.execute(
                    """
                    SELECT
                        id,
                        event_at,
                        source_key,
                        job_key,
                        trigger_type,
                        query_kind,
                        query_value,
                        retry_after_seconds,
                        cooldown_seconds,
                        cooldown_until,
                        error_text
                    FROM rate_limit_events
                    ORDER BY event_at DESC, id DESC
                    LIMIT ?
                    """,
                    (limit,),
                ).fetchall()
            return [dict(row) for row in rows]
        except sqlite3.OperationalError as exc:
            logger.warning("Failed to read rate limit events: %s", exc)
            return []

    def count_rate_limit_events_since(self, since_iso: str) -> int:
        try:
            with self._connect() as conn:
                row = conn.execute(
                    "SELECT COUNT(*) AS event_count FROM rate_limit_events WHERE event_at >= ?",
                    (since_iso,),
                ).fetchone()
            return int(row["event_count"] or 0) if row else 0
        except sqlite3.OperationalError as exc:
            logger.warning("Failed to count rate limit events: %s", exc)
            return 0

    def list_db_sources(self) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT
                    source_key,
                    COUNT(DISTINCT world_id) AS world_count,
                    MAX(fetched_at) AS latest_fetched_at
                FROM world_snapshots
                WHERE source_key NOT LIKE 'history:%'
                GROUP BY source_key
                ORDER BY latest_fetched_at DESC, source_key ASC
                """
            ).fetchall()
        return [dict(row) for row in rows]

    def count_latest_worlds(self, source_key: str | None = None) -> int:
        if source_key is None:
            query = """
                SELECT COUNT(DISTINCT world_id) AS count
                FROM world_snapshots
                WHERE source_key NOT LIKE 'history:%'
            """
            params: tuple[object, ...] = ()
        else:
            query = """
                SELECT COUNT(DISTINCT world_id) AS count
                FROM world_snapshots
                WHERE source_key = ?
            """
            params = (source_key,)
        with self._connect() as conn:
            row = conn.execute(query, params).fetchone()
        return int(row["count"] if row and row["count"] is not None else 0)

    def has_data(self) -> bool:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT 1 FROM world_snapshots WHERE source_key NOT LIKE 'history:%' LIMIT 1"
            ).fetchone()
        return row is not None

    def load_latest_worlds(self, source_key: str | None = None) -> list[dict[str, Any]]:
        if source_key is None:
            query = """
                SELECT * FROM (
                    SELECT
                        world_id,
                        source_key,
                        fetched_at,
                        raw_json,
                        ROW_NUMBER() OVER (
                            PARTITION BY source_key, world_id
                            ORDER BY fetched_at DESC, id DESC
                        ) AS rn
                    FROM world_snapshots
                    WHERE source_key NOT LIKE 'history:%'
                )
                WHERE rn = 1
            """
            params: tuple[object, ...] = ()
        else:
            query = """
                SELECT * FROM (
                    SELECT
                        world_id,
                        source_key,
                        fetched_at,
                        raw_json,
                        ROW_NUMBER() OVER (
                            PARTITION BY world_id
                            ORDER BY fetched_at DESC, id DESC
                        ) AS rn
                    FROM world_snapshots
                    WHERE source_key = ?
                )
                WHERE rn = 1
            """
            params = (source_key,)
        with self._connect() as conn:
            rows = conn.execute(query, params).fetchall()
        items = []
        for row in rows:
            payload = json.loads(row["raw_json"])
            payload["fetched_at"] = row["fetched_at"]
            payload["_db_source_key"] = row["source_key"]
            items.append(payload)
        return items

    def load_run_worlds(self, run_id: int) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT world_id, source_key, fetched_at, raw_json
                FROM world_snapshots
                WHERE run_id = ?
                ORDER BY world_id ASC, id ASC
                """,
                (run_id,),
            ).fetchall()
        items = []
        for row in rows:
            payload = json.loads(row["raw_json"])
            payload["fetched_at"] = row["fetched_at"]
            payload["_db_source_key"] = row["source_key"]
            items.append(payload)
        return items

    def load_worlds_by_authors(self, author_ids: set[str]) -> list[dict[str, Any]]:
        if not author_ids:
            return []
        placeholders = ",".join("?" * len(author_ids))
        query = f"""
            SELECT * FROM (
                SELECT
                    world_id,
                    source_key,
                    fetched_at,
                    raw_json,
                    ROW_NUMBER() OVER (
                        PARTITION BY world_id
                        ORDER BY fetched_at DESC, id DESC
                    ) AS rn
                FROM world_snapshots
                WHERE author_id IN ({placeholders})
                  AND source_key NOT LIKE 'history:%'
            )
            WHERE rn = 1
        """
        with self._connect() as conn:
            rows = conn.execute(query, tuple(author_ids)).fetchall()
        items = []
        for row in rows:
            payload = json.loads(row["raw_json"])
            payload["fetched_at"] = row["fetched_at"]
            payload["_db_source_key"] = row["source_key"]
            items.append(payload)
        return items

    def delete_runs_before(self, source_key: str, keep_run_ids: set[int]) -> int:
        if not keep_run_ids:
            return 0
        placeholders = ",".join("?" * len(keep_run_ids))
        with self._connect() as conn:
            conn.execute(
                f"""
                DELETE FROM run_query_hits
                WHERE run_query_id IN (
                    SELECT id FROM run_queries
                    WHERE run_id IN (
                        SELECT id FROM sync_runs WHERE source_key=? AND id NOT IN ({placeholders})
                    )
                )
                """,
                (source_key, *keep_run_ids),
            )
            conn.execute(
                f"DELETE FROM run_queries WHERE run_id IN (SELECT id FROM sync_runs WHERE source_key=? AND id NOT IN ({placeholders}))",
                (source_key, *keep_run_ids),
            )
            cur = conn.execute(
                f"DELETE FROM world_snapshots WHERE source_key=? AND run_id NOT IN ({placeholders})",
                (source_key, *keep_run_ids),
            )
            deleted = cur.rowcount
            conn.execute(
                f"DELETE FROM sync_runs WHERE source_key=? AND id NOT IN ({placeholders})",
                (source_key, *keep_run_ids),
            )
        return deleted

    def load_history_points(
        self,
        world_id: str | None = None,
        source_key: str | None = None,
    ) -> dict[str, list[dict[str, Any]]]:
        query = """
            SELECT
                world_id,
                source_key,
                fetched_at,
                raw_json
            FROM world_snapshots
            WHERE (? IS NULL OR world_id = ?)
              AND (? IS NULL OR source_key = ?)
            ORDER BY world_id ASC, fetched_at ASC, id ASC
        """
        with self._connect() as conn:
            rows = conn.execute(query, (world_id, world_id, source_key, source_key)).fetchall()

        history: dict[str, list[dict[str, Any]]] = {}
        for row in rows:
            payload = json.loads(row["raw_json"])
            history.setdefault(row["world_id"], []).append(
                {
                    "source_key": row["source_key"],
                    "fetched_at": row["fetched_at"],
                    "name": payload.get("name"),
                    "created_at": payload.get("created_at"),
                    "updated_at": payload.get("updated_at"),
                    "publication_date": payload.get("publication_date"),
                    "labs_publication_date": payload.get("labs_publication_date"),
                    "visits": payload.get("visits"),
                    "favorites": payload.get("favorites"),
                    "heat": payload.get("heat"),
                    "popularity": payload.get("popularity"),
                }
            )
        return history

    def list_daily_stats(self) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT source_key, date, total_worlds, new_worlds_today
                FROM daily_stats
                ORDER BY date DESC, source_key ASC
                """
            ).fetchall()
        return [dict(row) for row in rows]

    def count_distinct_authors(self, source_key: str | None = None) -> int:
        if source_key is None:
            query = """
                SELECT COUNT(DISTINCT author_id) AS count
                FROM world_snapshots
                WHERE source_key NOT LIKE 'history:%'
                  AND author_id IS NOT NULL
                  AND TRIM(author_id) != ''
            """
            params: tuple[object, ...] = ()
        else:
            query = """
                SELECT COUNT(DISTINCT author_id) AS count
                FROM world_snapshots
                WHERE source_key = ?
                  AND author_id IS NOT NULL
                  AND TRIM(author_id) != ''
            """
            params = (source_key,)
        with self._connect() as conn:
            row = conn.execute(query, params).fetchone()
        return int(row["count"] if row and row["count"] is not None else 0)

    def list_groups(self, *, limit: int = 100) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT
                    g.group_id,
                    g.name,
                    g.region,
                    g.category,
                    g.description,
                    g.managed_status,
                    g.external_links_json,
                    g.last_synced_at,
                    COUNT(gwm.world_id) AS world_count
                FROM groups g
                LEFT JOIN group_world_memberships gwm ON gwm.group_id = g.group_id
                GROUP BY
                    g.group_id, g.name, g.region, g.category, g.description,
                    g.managed_status, g.external_links_json, g.last_synced_at
                ORDER BY g.last_synced_at DESC, g.group_id ASC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        items = []
        for row in rows:
            item = dict(row)
            item["external_links"] = json.loads(item.pop("external_links_json") or "[]")
            items.append(item)
        return items

    def get_group(self, group_id: str) -> dict[str, Any] | None:
        rows = [row for row in self.list_groups(limit=1000) if row["group_id"] == group_id]
        return rows[0] if rows else None

    def upsert_group(
        self,
        *,
        group_id: str,
        name: str,
        region: str | None,
        category: str | None,
        description: str | None,
        managed_status: str | None,
        external_links: list[str] | None,
        last_synced_at: str | None,
    ) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO groups (
                    group_id, name, region, category, description,
                    managed_status, external_links_json, last_synced_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(group_id) DO UPDATE SET
                    name = excluded.name,
                    region = excluded.region,
                    category = excluded.category,
                    description = excluded.description,
                    managed_status = excluded.managed_status,
                    external_links_json = excluded.external_links_json,
                    last_synced_at = excluded.last_synced_at
                """,
                (
                    group_id,
                    name,
                    region,
                    category,
                    description,
                    managed_status,
                    json.dumps(external_links or [], ensure_ascii=False),
                    last_synced_at,
                ),
            )

    def delete_group(self, group_id: str) -> None:
        with self._connect() as conn:
            conn.execute("DELETE FROM managed_groups WHERE group_id = ?", (group_id,))
            conn.execute("DELETE FROM group_world_memberships WHERE group_id = ?", (group_id,))
            conn.execute("DELETE FROM scheduled_posts WHERE group_id = ?", (group_id,))
            conn.execute("DELETE FROM groups WHERE group_id = ?", (group_id,))

    def list_group_world_memberships(
        self,
        *,
        group_id: str | None = None,
        limit: int = 200,
    ) -> list[dict[str, Any]]:
        query = """
            SELECT
                gwm.group_id,
                gwm.world_id,
                gwm.membership_role,
                gwm.linked_at,
                gwm.source_key,
                g.name AS group_name,
                g.region AS group_region,
                g.category AS group_category
            FROM group_world_memberships gwm
            LEFT JOIN groups g ON g.group_id = gwm.group_id
            WHERE (? IS NULL OR gwm.group_id = ?)
            ORDER BY gwm.linked_at DESC, gwm.group_id ASC, gwm.world_id ASC
            LIMIT ?
        """
        with self._connect() as conn:
            rows = conn.execute(query, (group_id, group_id, limit)).fetchall()
        return [dict(row) for row in rows]

    def get_group_world_membership(self, *, group_id: str, world_id: str) -> dict[str, Any] | None:
        rows = [
            row
            for row in self.list_group_world_memberships(group_id=group_id, limit=1000)
            if row["world_id"] == world_id
        ]
        return rows[0] if rows else None

    def upsert_group_world_membership(
        self,
        *,
        group_id: str,
        world_id: str,
        membership_role: str | None,
        linked_at: str,
        source_key: str | None,
    ) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO group_world_memberships (
                    group_id, world_id, membership_role, linked_at, source_key
                ) VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(group_id, world_id) DO UPDATE SET
                    membership_role = excluded.membership_role,
                    linked_at = excluded.linked_at,
                    source_key = excluded.source_key
                """,
                (group_id, world_id, membership_role, linked_at, source_key),
            )

    def delete_group_world_membership(self, *, group_id: str, world_id: str) -> None:
        with self._connect() as conn:
            conn.execute(
                "DELETE FROM group_world_memberships WHERE group_id = ? AND world_id = ?",
                (group_id, world_id),
            )

    def list_managed_groups(self) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT
                    mg.group_id,
                    mg.workspace_key,
                    mg.posting_enabled,
                    mg.notes,
                    mg.updated_at,
                    g.name,
                    g.region,
                    g.category,
                    g.managed_status
                FROM managed_groups mg
                LEFT JOIN groups g ON g.group_id = mg.group_id
                ORDER BY mg.updated_at DESC, mg.group_id ASC
                """
            ).fetchall()
        return [dict(row) for row in rows]

    def get_managed_group(self, group_id: str) -> dict[str, Any] | None:
        rows = [row for row in self.list_managed_groups() if row["group_id"] == group_id]
        return rows[0] if rows else None

    def upsert_managed_group(
        self,
        *,
        group_id: str,
        workspace_key: str | None,
        posting_enabled: bool,
        notes: str | None,
        updated_at: str,
    ) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO managed_groups (group_id, workspace_key, posting_enabled, notes, updated_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(group_id) DO UPDATE SET
                    workspace_key = excluded.workspace_key,
                    posting_enabled = excluded.posting_enabled,
                    notes = excluded.notes,
                    updated_at = excluded.updated_at
                """,
                (group_id, workspace_key, 1 if posting_enabled else 0, notes, updated_at),
            )

    def delete_managed_group(self, group_id: str) -> None:
        with self._connect() as conn:
            conn.execute("DELETE FROM managed_groups WHERE group_id = ?", (group_id,))

    def list_scheduled_posts(self, *, limit: int = 20) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT
                    sp.id,
                    sp.group_id,
                    sp.content_type,
                    sp.status,
                    sp.scheduled_for,
                    sp.payload_json,
                    sp.created_at,
                    sp.updated_at,
                    sp.delivered_at,
                    g.name AS group_name
                FROM scheduled_posts sp
                LEFT JOIN groups g ON g.group_id = sp.group_id
                ORDER BY sp.scheduled_for ASC, sp.id ASC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        items = []
        for row in rows:
            item = dict(row)
            item["payload"] = json.loads(item.pop("payload_json") or "{}")
            items.append(item)
        return items

    def get_scheduled_post(self, post_id: int) -> dict[str, Any] | None:
        rows = [row for row in self.list_scheduled_posts(limit=1000) if int(row["id"]) == int(post_id)]
        return rows[0] if rows else None

    def upsert_scheduled_post(
        self,
        *,
        post_id: int | None,
        group_id: str,
        content_type: str,
        status: str,
        scheduled_for: str,
        payload: dict[str, Any],
        created_at: str,
        updated_at: str,
        delivered_at: str | None,
    ) -> int:
        with self._connect() as conn:
            if post_id is None:
                cur = conn.execute(
                    """
                    INSERT INTO scheduled_posts (
                        group_id, content_type, status, scheduled_for,
                        payload_json, created_at, updated_at, delivered_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        group_id,
                        content_type,
                        status,
                        scheduled_for,
                        json.dumps(payload, ensure_ascii=False),
                        created_at,
                        updated_at,
                        delivered_at,
                    ),
                )
                return int(cur.lastrowid)
            conn.execute(
                """
                UPDATE scheduled_posts
                SET group_id = ?, content_type = ?, status = ?, scheduled_for = ?,
                    payload_json = ?, updated_at = ?, delivered_at = ?
                WHERE id = ?
                """,
                (
                    group_id,
                    content_type,
                    status,
                    scheduled_for,
                    json.dumps(payload, ensure_ascii=False),
                    updated_at,
                    delivered_at,
                    post_id,
                ),
            )
        return int(post_id)

    def delete_scheduled_post(self, post_id: int) -> None:
        with self._connect() as conn:
            conn.execute("DELETE FROM scheduled_posts WHERE id = ?", (post_id,))

    def count_groups(self) -> int:
        with self._connect() as conn:
            row = conn.execute("SELECT COUNT(*) AS count FROM groups").fetchone()
        return int(row["count"] if row and row["count"] is not None else 0)

    def count_managed_groups(self) -> int:
        with self._connect() as conn:
            row = conn.execute("SELECT COUNT(*) AS count FROM managed_groups").fetchone()
        return int(row["count"] if row and row["count"] is not None else 0)

    def count_scheduled_posts(self, status: str | None = None) -> int:
        if status:
            query = "SELECT COUNT(*) AS count FROM scheduled_posts WHERE status = ?"
            params: tuple[object, ...] = (status,)
        else:
            query = "SELECT COUNT(*) AS count FROM scheduled_posts"
            params = ()
        with self._connect() as conn:
            row = conn.execute(query, params).fetchone()
        return int(row["count"] if row and row["count"] is not None else 0)

    def list_runs(self, *, limit: int = 20, job_key: str | None = None) -> list[dict[str, Any]]:
        query = """
            SELECT
                id, source_key, job_key, trigger_type, query_label, status,
                started_at, finished_at, world_count, error_text
            FROM sync_runs
            WHERE (? IS NULL OR job_key = ?)
            ORDER BY started_at DESC, id DESC
            LIMIT ?
        """
        with self._connect() as conn:
            rows = conn.execute(query, (job_key, job_key, limit)).fetchall()
        return [dict(row) for row in rows]

    def get_latest_run_for_job(self, job_key: str) -> dict[str, Any] | None:
        runs = self.list_runs(limit=1, job_key=job_key)
        return runs[0] if runs else None

    def upsert_analysis_cache(
        self,
        *,
        scope_key: str,
        scope_type: str,
        updated_at: str,
        payload: dict[str, Any],
        source_run_id: int | None = None,
    ) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO analysis_cache (scope_key, scope_type, updated_at, source_run_id, payload_json)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(scope_key) DO UPDATE SET
                    scope_type = excluded.scope_type,
                    updated_at = excluded.updated_at,
                    source_run_id = excluded.source_run_id,
                    payload_json = excluded.payload_json
                """,
                (
                    scope_key,
                    scope_type,
                    updated_at,
                    source_run_id,
                    json.dumps(payload, ensure_ascii=False),
                ),
            )

    def get_analysis_cache(self, scope_key: str) -> dict[str, Any] | None:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT scope_key, scope_type, updated_at, source_run_id, payload_json
                FROM analysis_cache
                WHERE scope_key = ?
                """,
                (scope_key,),
            ).fetchone()
        if row is None:
            return None
        return {
            "scope_key": row["scope_key"],
            "scope_type": row["scope_type"],
            "updated_at": row["updated_at"],
            "source_run_id": row["source_run_id"],
            "payload": json.loads(row["payload_json"]),
        }

    def upsert_topics(self, topics: list[dict[str, Any]]) -> None:
        rows = [
            (
                item["topic_key"],
                item["label"],
                item.get("description"),
                item.get("color"),
                item.get("sort_order", 0),
                1 if item.get("is_active", True) else 0,
            )
            for item in topics
        ]
        if not rows:
            return
        with self._connect() as conn:
            conn.executemany(
                """
                INSERT INTO topics (topic_key, label, description, color, sort_order, is_active)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(topic_key) DO UPDATE SET
                    label = excluded.label,
                    description = excluded.description,
                    color = excluded.color,
                    sort_order = excluded.sort_order,
                    is_active = excluded.is_active
                """,
                rows,
            )

    def replace_topic_rules(self, topic_key: str, rules: list[dict[str, Any]]) -> None:
        with self._connect() as conn:
            conn.execute("DELETE FROM topic_rules WHERE topic_key = ?", (topic_key,))
            conn.executemany(
                """
                INSERT INTO topic_rules (topic_key, rule_type, rule_value, sort_order, is_active)
                VALUES (?, ?, ?, ?, ?)
                """,
                [
                    (
                        topic_key,
                        item["rule_type"],
                        item["rule_value"],
                        item.get("sort_order", 0),
                        1 if item.get("is_active", True) else 0,
                    )
                    for item in rules
                ],
            )

    def list_topics(self) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT
                    t.topic_key,
                    t.label,
                    t.description,
                    t.color,
                    t.sort_order,
                    t.is_active,
                    COUNT(tm.world_id) AS world_count,
                    MAX(tm.last_seen_at) AS last_seen_at
                FROM topics t
                LEFT JOIN topic_memberships tm ON tm.topic_key = t.topic_key
                GROUP BY t.topic_key, t.label, t.description, t.color, t.sort_order, t.is_active
                ORDER BY t.sort_order ASC, t.topic_key ASC
                """
            ).fetchall()
        return [dict(row) for row in rows]

    def get_topic(self, topic_key: str) -> dict[str, Any] | None:
        rows = [row for row in self.list_topics() if row["topic_key"] == topic_key]
        return rows[0] if rows else None

    def delete_topic(self, topic_key: str) -> None:
        with self._connect() as conn:
            conn.execute("DELETE FROM topic_memberships WHERE topic_key = ?", (topic_key,))
            conn.execute("DELETE FROM topic_rules WHERE topic_key = ?", (topic_key,))
            conn.execute("DELETE FROM topics WHERE topic_key = ?", (topic_key,))

    def list_topic_rules(self, topic_key: str) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT topic_key, rule_type, rule_value, sort_order, is_active
                FROM topic_rules
                WHERE topic_key = ?
                ORDER BY sort_order ASC, id ASC
                """,
                (topic_key,),
            ).fetchall()
        return [dict(row) for row in rows]

    def list_topic_memberships(self, topic_key: str) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT topic_key, world_id, first_seen_at, last_seen_at, matched_by
                FROM topic_memberships
                WHERE topic_key = ?
                ORDER BY last_seen_at DESC, world_id ASC
                """,
                (topic_key,),
            ).fetchall()
        return [dict(row) for row in rows]

    def get_existing_topic_memberships(self, topic_key: str) -> dict[str, dict[str, Any]]:
        return {row["world_id"]: row for row in self.list_topic_memberships(topic_key)}

    def replace_topic_memberships(self, topic_key: str, memberships: list[dict[str, Any]]) -> None:
        with self._connect() as conn:
            conn.execute("DELETE FROM topic_memberships WHERE topic_key = ?", (topic_key,))
            conn.executemany(
                """
                INSERT INTO topic_memberships (
                    topic_key, world_id, first_seen_at, last_seen_at, matched_by
                ) VALUES (?, ?, ?, ?, ?)
                """,
                [
                    (
                        topic_key,
                        item["world_id"],
                        item["first_seen_at"],
                        item["last_seen_at"],
                        item.get("matched_by"),
                    )
                    for item in memberships
                ],
            )

    def purge_source(self, source_key: str) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                DELETE FROM run_query_hits
                WHERE run_query_id IN (
                    SELECT id FROM run_queries
                    WHERE run_id IN (SELECT id FROM sync_runs WHERE source_key = ?)
                )
                """,
                (source_key,),
            )
            conn.execute(
                """
                DELETE FROM run_queries
                WHERE run_id IN (SELECT id FROM sync_runs WHERE source_key = ?)
                """,
                (source_key,),
            )
            conn.execute("DELETE FROM world_snapshots WHERE source_key = ?", (source_key,))
            conn.execute("DELETE FROM daily_stats WHERE source_key = ?", (source_key,))
            conn.execute("DELETE FROM sync_runs WHERE source_key = ?", (source_key,))

    def purge_daily_stats(self, source_key: str) -> None:
        with self._connect() as conn:
            conn.execute("DELETE FROM daily_stats WHERE source_key = ?", (source_key,))

    def delete_world_snapshots(self, source_key: str, world_id: str) -> None:
        with self._connect() as conn:
            conn.execute(
                "DELETE FROM world_snapshots WHERE source_key = ? AND world_id = ?",
                (source_key, world_id),
            )
