from __future__ import annotations

import os
from pathlib import Path

from flask import Flask, jsonify, request, send_from_directory

from world_info.scraper.scraper import VRChatRateLimitError

from .scheduler import AutoSyncScheduler
from .service import WorldInfoService


def create_app(service: WorldInfoService | None = None) -> Flask:
    service = service or WorldInfoService()
    frontend_dir = str(service.frontend_dir)
    schedule_config_path = service.app_root / "config" / "auto_sync_schedule.json"
    scheduler = AutoSyncScheduler(service, schedule_config_path)
    scheduler.start()
    app = Flask(__name__, static_folder=frontend_dir, static_url_path="")
    app.config["JSON_AS_ASCII"] = False

    def error(message: str, status: int = 400):
        return jsonify({"error": message}), status

    def active_rate_limit_response(bypass=False):
        if bypass or os.getenv("WORLD_INFO_BYPASS_RATE_LIMIT", "").strip() == "1":
            return None
        state = scheduler.get_rate_limit_state()
        if not state.get("active"):
            return None
        remaining = int(state.get("remaining_seconds") or 0)
        minutes = max((remaining + 59) // 60, 1) if remaining > 0 else 1
        return jsonify(
            {
                "error": f"VRChat cooldown active. Wait about {minutes} minute(s) before the next crawl.",
                "cooldown_until": state.get("cooldown_until"),
                "remaining_seconds": remaining,
                "last_error": state.get("last_message"),
            }
        ), 429

    def record_rate_limit_and_respond(
        *,
        exc: VRChatRateLimitError,
        source_key: str | None,
        job_key: str | None,
        trigger_type: str | None,
        query_kind: str | None = None,
        query_value: str | None = None,
    ):
        info = service.record_rate_limit_event(
            error=exc,
            source_key=source_key,
            job_key=job_key,
            trigger_type=trigger_type,
            query_kind=query_kind,
            query_value=query_value,
        )
        scheduler.record_rate_limit(
            job_key=job_key,
            retry_after_seconds=info["retry_after_seconds"],
            cooldown_seconds=info["cooldown_seconds"],
            cooldown_until=info["cooldown_until"],
            message=info["message"],
        )
        return jsonify(
            {
                "error": info["message"],
                "cooldown_until": info["cooldown_until"],
                "cooldown_seconds": info["cooldown_seconds"],
                "retry_after_seconds": info["retry_after_seconds"],
            }
        ), 429

    def parse_limit(raw_value: str | None, default: int = 50, maximum: int = 200) -> int:
        if raw_value in (None, ""):
            return default
        try:
            limit = int(raw_value)
        except ValueError as exc:
            raise ValueError("limit must be a positive integer") from exc
        if limit <= 0:
            raise ValueError("limit must be a positive integer")
        return min(limit, maximum)

    def parse_bool(value, default: bool = False) -> bool:
        if value in (None, ""):
            return default
        if isinstance(value, bool):
            return value
        return str(value).strip().casefold() in {"1", "true", "yes", "y", "on"}

    @app.after_request
    def add_cors_headers(response):
        response.headers["Access-Control-Allow-Origin"] = "*"
        response.headers["Access-Control-Allow-Headers"] = "Content-Type"
        response.headers["Access-Control-Allow-Methods"] = "GET,POST,PUT,DELETE,OPTIONS"
        return response

    @app.get("/")
    def index():
        return send_from_directory(frontend_dir, "index.html")

    @app.get("/api/v1/health")
    def health():
        return jsonify(
            {
                "status": "ok",
                "database": service._display_path(service.storage.db_path),
            }
        )

    @app.post("/api/v1/auth/status")
    def auth_status():
        payload = request.get_json(silent=True) or {}
        try:
            result = service.check_auth_status(
                cookie=payload.get("cookie"),
                username=payload.get("username"),
                password=payload.get("password"),
            )
        except Exception as exc:
            return error(str(exc), 500)
        status_code = 200 if result.get("status") == "ok" else 207 if result.get("status") == "warning" else 200
        return jsonify(result), status_code

    @app.post("/api/v1/auth/login")
    def auth_login():
        payload = request.get_json(silent=True) or {}
        try:
            result = service.login_with_vrchat(
                username=str(payload.get("username", "")),
                password=str(payload.get("password", "")),
            )
        except ValueError as exc:
            return error(str(exc))
        except Exception as exc:
            return error(str(exc), 500)
        return jsonify(result), 200

    @app.post("/api/v1/auth/verify-2fa")
    def auth_verify_2fa():
        payload = request.get_json(silent=True) or {}
        try:
            result = service.verify_vrchat_2fa(
                code=str(payload.get("code", "")),
                method=str(payload.get("method", "")),
                auth_cookie=str(payload.get("auth_cookie", "")),
            )
        except ValueError as exc:
            return error(str(exc))
        except Exception as exc:
            return error(str(exc), 500)
        return jsonify(result), 200

    @app.post("/api/v1/auth/persist")
    def auth_persist():
        payload = request.get_json(silent=True) or {}
        try:
            result = service.persist_server_auth(
                cookie=payload.get("cookie"),
                username=payload.get("username"),
                password=payload.get("password"),
            )
        except ValueError as exc:
            return error(str(exc))
        except Exception as exc:
            return error(str(exc), 500)
        return jsonify(result), 200

    @app.delete("/api/v1/auth/persist")
    def auth_clear_persisted():
        try:
            result = service.clear_server_auth()
        except Exception as exc:
            return error(str(exc), 500)
        return jsonify(result), 200

    @app.get("/api/v1/sources")
    def sources():
        items = service.list_sources()
        preferred_keys = ("db:job:starriver", "db:job:taiwan", "db:job:Ch", "db:job:racing")
        default_source = next(
            (
                item["key"]
                for preferred_key in preferred_keys
                for item in items
                if item["key"] == preferred_key and item["count"]
            ),
            None,
        )
        if default_source is None:
            default_source = next(
                (
                    item["key"]
                    for item in items
                    if item["origin"] == "db" and item["key"] != "db:all" and item["count"]
                ),
                None,
            )
        if default_source is None:
            default_source = next((item["key"] for item in items if item["available"] and item["count"]), None)
        if default_source is None:
            default_source = next((item["key"] for item in items if item["available"]), "legacy-raw")
        return jsonify({"items": items, "default_source": default_source})

    @app.get("/api/v1/topics")
    def topics():
        include_inactive = request.args.get("include_inactive", "0") == "1"
        return jsonify({"items": service.list_topics(include_inactive=include_inactive)})

    @app.post("/api/v1/topics")
    def create_topic():
        payload = request.get_json(silent=True) or {}
        try:
            item = service.upsert_topic(
                topic_key=str(payload.get("topic_key", "")),
                label=str(payload.get("label", "")),
                description=payload.get("description"),
                topic_type=str(payload.get("topic_type", "view")),
                color=payload.get("color"),
                sort_order=payload.get("sort_order"),
                is_active=payload.get("is_active", True),
                rules=payload.get("rules") if isinstance(payload.get("rules"), list) else [],
            )
        except ValueError as exc:
            return error(str(exc))
        except Exception as exc:
            return error(str(exc), 500)
        return jsonify(item), 201

    @app.put("/api/v1/topics/<topic_key>")
    def update_topic(topic_key: str):
        payload = request.get_json(silent=True) or {}
        try:
            if "is_active" in payload and len(payload.keys()) == 1:
                item = service.set_topic_active(topic_key, parse_bool(payload.get("is_active")))
            else:
                item = service.upsert_topic(
                    topic_key=topic_key,
                    label=str(payload.get("label", "")),
                    description=payload.get("description"),
                    topic_type=str(payload.get("topic_type", "view")),
                    color=payload.get("color"),
                    sort_order=payload.get("sort_order"),
                    is_active=payload.get("is_active", True),
                    rules=payload.get("rules") if isinstance(payload.get("rules"), list) else [],
                )
        except KeyError:
            return error(f"Unknown topic: {topic_key}", 404)
        except ValueError as exc:
            return error(str(exc))
        except Exception as exc:
            return error(str(exc), 500)
        return jsonify(item), 200

    @app.delete("/api/v1/topics/<topic_key>")
    def delete_topic(topic_key: str):
        try:
            item = service.delete_topic(topic_key)
        except KeyError:
            return error(f"Unknown topic: {topic_key}", 404)
        except Exception as exc:
            return error(str(exc), 500)
        return jsonify(item), 200

    @app.get("/api/v1/topics/<topic_key>")
    def topic(topic_key: str):
        try:
            item = service.get_topic(topic_key)
        except KeyError:
            return error(f"Unknown topic: {topic_key}", 404)
        return jsonify(item)

    @app.get("/api/v1/topics/<topic_key>/worlds")
    def topic_worlds(topic_key: str):
        query = request.args.get("q")
        tag = request.args.get("tag")
        sort = request.args.get("sort", "new_hot")
        direction = request.args.get("direction", "desc")
        try:
            topic_info = service.get_topic(topic_key)
            items = service.load_topic_worlds(
                topic_key,
                query=query,
                tag=tag,
                sort=sort,
                direction=direction,
            )
        except KeyError:
            return error(f"Unknown topic: {topic_key}", 404)
        return jsonify(
            {
                "topic": topic_info,
                "count": len(items),
                "tags": service.collect_tags(items),
                "items": items,
            }
        )

    @app.get("/api/v1/worlds")
    def worlds():
        source = request.args.get("source", "db:all")
        query = request.args.get("q")
        tag = request.args.get("tag")
        sort = request.args.get("sort", "new_hot")
        direction = request.args.get("direction", "desc")
        dedupe = request.args.get("dedupe", "1") != "0"

        try:
            items = service.load_worlds(
                source,
                query=query,
                tag=tag,
                sort=sort,
                direction=direction,
                dedupe=dedupe,
            )
        except KeyError:
            return error(f"Unknown source: {source}", 404)

        return jsonify(
            {
                "source": source,
                "count": len(items),
                "tags": service.collect_tags(items),
                "items": items,
            }
        )

    @app.put("/api/v1/worlds/<world_id>")
    def update_world(world_id: str):
        payload = request.get_json(silent=True) or {}
        source = str(payload.get("source", "")).strip()
        if not source:
            return error("source is required")
        try:
            result = service.update_world_record(
                source=source,
                world_id=world_id,
                changes=payload,
            )
        except KeyError:
            return error(f"Unknown world in source {source}: {world_id}", 404)
        except ValueError as exc:
            return error(str(exc))
        except Exception as exc:
            return error(str(exc), 500)
        return jsonify(result), 200

    @app.delete("/api/v1/worlds/<world_id>")
    def delete_world(world_id: str):
        source = str(request.args.get("source", "")).strip()
        if not source:
            return error("source is required")
        try:
            result = service.delete_world_record(source=source, world_id=world_id)
        except ValueError as exc:
            return error(str(exc))
        except Exception as exc:
            return error(str(exc), 500)
        return jsonify(result), 200

    @app.get("/api/v1/history")
    def history():
        source = str(request.args.get("source", "")).strip() or None
        return jsonify({"items": service.load_history_summary(source=source)})

    @app.get("/api/v1/history/<world_id>")
    def history_for_world(world_id: str):
        source = str(request.args.get("source", "")).strip() or None
        items = service.load_history(world_id, source=source).get(world_id, [])
        return jsonify({"world_id": world_id, "count": len(items), "items": items})

    @app.get("/api/v1/insights")
    def insights():
        source = str(request.args.get("source", "")).strip() or None
        topic_key = str(request.args.get("topic", "")).strip() or None
        limit = parse_limit(request.args.get("limit"), default=12, maximum=50)
        if not source and not topic_key:
            source = "db:all"
        try:
            payload = service.load_collection_insights(source=source, topic_key=topic_key, limit=limit)
        except KeyError as exc:
            return error(str(exc), 404)
        return jsonify(payload)

    @app.get("/api/v1/communities/summary")
    def communities_summary():
        try:
            payload = service.load_communities_workspace()
        except Exception as exc:
            return error(str(exc), 500)
        return jsonify(payload)

    @app.get("/api/v1/groups")
    def groups():
        limit = parse_limit(request.args.get("limit"), default=100, maximum=500)
        try:
            payload = service.list_groups(limit=limit)
        except Exception as exc:
            return error(str(exc), 500)
        return jsonify(payload)

    @app.post("/api/v1/groups")
    def create_group():
        payload = request.get_json(silent=True) or {}
        try:
            item = service.upsert_group(
                group_id=payload.get("group_id"),
                name=payload.get("name"),
                region=payload.get("region"),
                category=payload.get("category"),
                description=payload.get("description"),
                managed_status=payload.get("managed_status"),
                external_links=payload.get("external_links"),
                last_synced_at=payload.get("last_synced_at"),
            )
        except ValueError as exc:
            return error(str(exc))
        except Exception as exc:
            return error(str(exc), 500)
        return jsonify(item), 201

    @app.put("/api/v1/groups/<group_id>")
    def update_group(group_id: str):
        payload = request.get_json(silent=True) or {}
        try:
            item = service.upsert_group(
                group_id=group_id,
                name=payload.get("name"),
                region=payload.get("region"),
                category=payload.get("category"),
                description=payload.get("description"),
                managed_status=payload.get("managed_status"),
                external_links=payload.get("external_links"),
                last_synced_at=payload.get("last_synced_at"),
            )
        except ValueError as exc:
            return error(str(exc))
        except Exception as exc:
            return error(str(exc), 500)
        return jsonify(item), 200

    @app.delete("/api/v1/groups/<group_id>")
    def delete_group(group_id: str):
        try:
            result = service.delete_group(group_id)
        except KeyError:
            return error(f"Unknown group: {group_id}", 404)
        except ValueError as exc:
            return error(str(exc))
        except Exception as exc:
            return error(str(exc), 500)
        return jsonify(result), 200

    @app.get("/api/v1/group-world-memberships")
    def group_world_memberships():
        limit = parse_limit(request.args.get("limit"), default=100, maximum=500)
        group_id = request.args.get("group_id")
        try:
            payload = service.list_group_world_memberships(group_id=group_id, limit=limit)
        except Exception as exc:
            return error(str(exc), 500)
        return jsonify(payload)

    @app.post("/api/v1/group-world-memberships")
    def create_group_world_membership():
        payload = request.get_json(silent=True) or {}
        try:
            item = service.upsert_group_world_membership(
                group_id=payload.get("group_id"),
                world_id=payload.get("world_id"),
                membership_role=payload.get("membership_role"),
                source_key=payload.get("source_key"),
            )
        except KeyError as exc:
            message = str(exc)
            if "group" in message.casefold():
                return error(f"Unknown group: {payload.get('group_id')}", 404)
            return error(f"Unknown world: {payload.get('world_id')}", 404)
        except ValueError as exc:
            return error(str(exc))
        except Exception as exc:
            return error(str(exc), 500)
        return jsonify(item), 201

    @app.put("/api/v1/group-world-memberships/<group_id>/<world_id>")
    def update_group_world_membership(group_id: str, world_id: str):
        payload = request.get_json(silent=True) or {}
        try:
            item = service.upsert_group_world_membership(
                group_id=group_id,
                world_id=world_id,
                membership_role=payload.get("membership_role"),
                source_key=payload.get("source_key"),
            )
        except KeyError as exc:
            message = str(exc)
            if "group" in message.casefold():
                return error(f"Unknown group: {group_id}", 404)
            return error(f"Unknown world: {world_id}", 404)
        except ValueError as exc:
            return error(str(exc))
        except Exception as exc:
            return error(str(exc), 500)
        return jsonify(item), 200

    @app.delete("/api/v1/group-world-memberships/<group_id>/<world_id>")
    def delete_group_world_membership(group_id: str, world_id: str):
        try:
            result = service.delete_group_world_membership(group_id=group_id, world_id=world_id)
        except KeyError:
            return error(f"Unknown membership: {group_id}/{world_id}", 404)
        except ValueError as exc:
            return error(str(exc))
        except Exception as exc:
            return error(str(exc), 500)
        return jsonify(result), 200

    @app.get("/api/v1/managed-groups")
    def managed_groups():
        try:
            payload = service.list_managed_groups()
        except Exception as exc:
            return error(str(exc), 500)
        return jsonify(payload)

    @app.post("/api/v1/managed-groups")
    def create_managed_group():
        payload = request.get_json(silent=True) or {}
        try:
            item = service.upsert_managed_group(
                group_id=payload.get("group_id"),
                workspace_key=payload.get("workspace_key"),
                posting_enabled=payload.get("posting_enabled"),
                notes=payload.get("notes"),
            )
        except KeyError:
            return error(f"Unknown group: {payload.get('group_id')}", 404)
        except ValueError as exc:
            return error(str(exc))
        except Exception as exc:
            return error(str(exc), 500)
        return jsonify(item), 201

    @app.put("/api/v1/managed-groups/<group_id>")
    def update_managed_group(group_id: str):
        payload = request.get_json(silent=True) or {}
        try:
            item = service.upsert_managed_group(
                group_id=group_id,
                workspace_key=payload.get("workspace_key"),
                posting_enabled=payload.get("posting_enabled"),
                notes=payload.get("notes"),
            )
        except KeyError:
            return error(f"Unknown group: {group_id}", 404)
        except ValueError as exc:
            return error(str(exc))
        except Exception as exc:
            return error(str(exc), 500)
        return jsonify(item), 200

    @app.delete("/api/v1/managed-groups/<group_id>")
    def delete_managed_group(group_id: str):
        try:
            result = service.delete_managed_group(group_id)
        except KeyError:
            return error(f"Unknown managed group: {group_id}", 404)
        except ValueError as exc:
            return error(str(exc))
        except Exception as exc:
            return error(str(exc), 500)
        return jsonify(result), 200

    @app.get("/api/v1/scheduled-posts")
    def scheduled_posts():
        limit = parse_limit(request.args.get("limit"), default=50, maximum=500)
        try:
            payload = service.list_scheduled_posts(limit=limit)
        except Exception as exc:
            return error(str(exc), 500)
        return jsonify(payload)

    @app.post("/api/v1/scheduled-posts")
    def create_scheduled_post():
        payload = request.get_json(silent=True) or {}
        try:
            item = service.upsert_scheduled_post(
                group_id=payload.get("group_id"),
                content_type=payload.get("content_type"),
                status=payload.get("status"),
                scheduled_for=payload.get("scheduled_for"),
                payload=payload.get("payload"),
                delivered_at=payload.get("delivered_at"),
            )
        except KeyError:
            return error(f"Unknown group: {payload.get('group_id')}", 404)
        except ValueError as exc:
            return error(str(exc))
        except Exception as exc:
            return error(str(exc), 500)
        return jsonify(item), 201

    @app.put("/api/v1/scheduled-posts/<int:post_id>")
    def update_scheduled_post(post_id: int):
        payload = request.get_json(silent=True) or {}
        try:
            item = service.upsert_scheduled_post(
                post_id=post_id,
                group_id=payload.get("group_id"),
                content_type=payload.get("content_type"),
                status=payload.get("status"),
                scheduled_for=payload.get("scheduled_for"),
                payload=payload.get("payload"),
                delivered_at=payload.get("delivered_at"),
            )
        except KeyError as exc:
            message = str(exc)
            if "scheduled post" in message:
                return error(f"Unknown scheduled post: {post_id}", 404)
            return error(f"Unknown group: {payload.get('group_id')}", 404)
        except ValueError as exc:
            return error(str(exc))
        except Exception as exc:
            return error(str(exc), 500)
        return jsonify(item), 200

    @app.delete("/api/v1/scheduled-posts/<int:post_id>")
    def delete_scheduled_post(post_id: int):
        try:
            result = service.delete_scheduled_post(post_id)
        except KeyError:
            return error(f"Unknown scheduled post: {post_id}", 404)
        except ValueError as exc:
            return error(str(exc))
        except Exception as exc:
            return error(str(exc), 500)
        return jsonify(result), 200

    @app.get("/api/v1/analytics/daily-stats")
    def daily_stats():
        return jsonify({"items": service.load_daily_stats()})

    @app.get("/api/v1/events")
    def events():
        limit = parse_limit(request.args.get("limit"), default=50, maximum=200)
        recency_days = parse_limit(request.args.get("days"), default=7, maximum=30)
        return jsonify(service.list_event_feed(limit=limit, recency_days=recency_days))

    @app.get("/api/v1/jobs")
    def jobs():
        return jsonify({"items": service.list_jobs()})

    @app.get("/api/v1/jobs/diagnostics")
    def job_diagnostics():
        return jsonify({"items": service.list_job_diagnostics()})

    @app.get("/api/v1/jobs/<job_key>/blacklist")
    def job_blacklist(job_key: str):
        try:
            result = service.list_job_blacklist(job_key)
        except KeyError:
            return error(f"Unknown job: {job_key}", 404)
        except ValueError as exc:
            return error(str(exc))
        return jsonify(result), 200

    @app.post("/api/v1/jobs/<job_key>/blacklist")
    def add_job_blacklist(job_key: str):
        payload = request.get_json(silent=True) or {}
        try:
            result = service.add_job_blacklist_entry(
                job_key=job_key,
                world_id=str(payload.get("world_id", "")),
            )
        except KeyError:
            return error(f"Unknown job: {job_key}", 404)
        except ValueError as exc:
            return error(str(exc))
        except Exception as exc:
            return error(str(exc), 500)
        return jsonify(result), 201

    @app.delete("/api/v1/jobs/<job_key>/blacklist/<world_id>")
    def remove_job_blacklist(job_key: str, world_id: str):
        try:
            result = service.remove_job_blacklist_entry(job_key=job_key, world_id=world_id)
        except KeyError:
            return error(f"Unknown job: {job_key}", 404)
        except ValueError as exc:
            return error(str(exc))
        except Exception as exc:
            return error(str(exc), 500)
        return jsonify(result), 200

    @app.get("/api/v1/jobs/<job_key>/creator-whitelist")
    def job_creator_whitelist(job_key: str):
        try:
            result = service.list_job_creator_whitelist(job_key)
        except KeyError:
            return error(f"Unknown job: {job_key}", 404)
        except ValueError as exc:
            return error(str(exc))
        return jsonify(result), 200

    @app.post("/api/v1/jobs/<job_key>/creator-whitelist")
    def add_job_creator_whitelist(job_key: str):
        payload = request.get_json(silent=True) or {}
        try:
            result = service.add_job_creator_whitelist_entry(
                job_key=job_key,
                user_id=str(payload.get("user_id", "")),
            )
        except KeyError:
            return error(f"Unknown job: {job_key}", 404)
        except ValueError as exc:
            return error(str(exc))
        except Exception as exc:
            return error(str(exc), 500)
        return jsonify(result), 201

    @app.delete("/api/v1/jobs/<job_key>/creator-whitelist/<user_id>")
    def remove_job_creator_whitelist(job_key: str, user_id: str):
        try:
            result = service.remove_job_creator_whitelist_entry(job_key=job_key, user_id=user_id)
        except KeyError:
            return error(f"Unknown job: {job_key}", 404)
        except ValueError as exc:
            return error(str(exc))
        except Exception as exc:
            return error(str(exc), 500)
        return jsonify(result), 200

    @app.get("/api/v1/jobs/<job_key>/pending")
    def job_pending_worlds(job_key: str):
        try:
            worlds = service.load_pending_worlds(job_key)
        except KeyError:
            return error(f"Unknown job: {job_key}", 404)
        except Exception as exc:
            return error(str(exc), 500)
        return jsonify({"job_key": job_key, "items": worlds}), 200

    @app.get("/api/v1/jobs/<job_key>/creator-blacklist")
    def job_creator_blacklist(job_key: str):
        try:
            result = service.list_job_creator_blacklist(job_key)
        except KeyError:
            return error(f"Unknown job: {job_key}", 404)
        except ValueError as exc:
            return error(str(exc))
        return jsonify(result), 200

    @app.post("/api/v1/jobs/<job_key>/creator-blacklist")
    def add_job_creator_blacklist(job_key: str):
        payload = request.get_json(silent=True) or {}
        try:
            result = service.add_job_creator_blacklist_entry(
                job_key=job_key,
                user_id=str(payload.get("user_id", "")),
            )
        except KeyError:
            return error(f"Unknown job: {job_key}", 404)
        except ValueError as exc:
            return error(str(exc))
        except Exception as exc:
            return error(str(exc), 500)
        return jsonify(result), 201

    @app.delete("/api/v1/jobs/<job_key>/creator-blacklist/<user_id>")
    def remove_job_creator_blacklist(job_key: str, user_id: str):
        try:
            result = service.remove_job_creator_blacklist_entry(job_key=job_key, user_id=user_id)
        except KeyError:
            return error(f"Unknown job: {job_key}", 404)
        except ValueError as exc:
            return error(str(exc))
        except Exception as exc:
            return error(str(exc), 500)
        return jsonify(result), 200

    @app.post("/api/v1/import/legacy")
    def import_legacy():
        try:
            result = service.import_legacy_data()
        except Exception as exc:
            return error(str(exc), 500)
        return jsonify(result), 201

    @app.post("/api/v1/jobs/<job_key>/run")
    def run_job(job_key: str):
        payload = request.get_json(silent=True) or {}
        bypass_rate_limit = bool(payload.get("bypass_rate_limit"))
        blocked = active_rate_limit_response(bypass=bypass_rate_limit)
        if blocked is not None:
            return blocked
        try:
            result = service.run_job(
                job_key,
                cookie=payload.get("cookie"),
                username=payload.get("username"),
                password=payload.get("password"),
                trigger_type="job_manual",
            )
        except KeyError:
            return error(f"Unknown job: {job_key}", 404)
        except VRChatRateLimitError as exc:
            return record_rate_limit_and_respond(
                exc=exc,
                source_key=f"job:{job_key}",
                job_key=job_key,
                trigger_type="job_manual",
            )
        except ValueError as exc:
            return error(str(exc), 409)
        except Exception as exc:
            return error(str(exc), 500)
        return jsonify(result), 201

    @app.post("/api/v1/jobs")
    def create_job():
        payload = request.get_json(silent=True) or {}
        try:
            result = service.create_job_with_topic(
                job_key=str(payload.get("job_key", "")),
                label=str(payload.get("label", "")),
                job_type=str(payload.get("job_type", "")),
                keywords=payload.get("keywords") if isinstance(payload.get("keywords"), list) else str(payload.get("keywords", "")).split(","),
                user_id=payload.get("user_id"),
                limit=payload.get("limit"),
                limit_per_keyword=payload.get("limit_per_keyword"),
                search=payload.get("search"),
                tags=payload.get("tags"),
                notags=payload.get("notags"),
                sort=payload.get("sort"),
                order=payload.get("order"),
                featured=payload.get("featured"),
                active=payload.get("active"),
                release_status=payload.get("release_status"),
                platform=payload.get("platform"),
            )
        except ValueError as exc:
            return error(str(exc))
        except Exception as exc:
            return error(str(exc), 500)
        return jsonify(result), 201

    @app.put("/api/v1/jobs/<job_key>")
    def update_job(job_key: str):
        payload = request.get_json(silent=True) or {}
        try:
            result = service.update_job_with_topic(
                job_key=job_key,
                label=str(payload.get("label", "")),
                job_type=str(payload.get("job_type", "")),
                keywords=payload.get("keywords") if isinstance(payload.get("keywords"), list) else str(payload.get("keywords", "")).split(","),
                user_id=payload.get("user_id"),
                limit=payload.get("limit"),
                limit_per_keyword=payload.get("limit_per_keyword"),
                search=payload.get("search"),
                tags=payload.get("tags"),
                notags=payload.get("notags"),
                sort=payload.get("sort"),
                order=payload.get("order"),
                featured=payload.get("featured"),
                active=payload.get("active"),
                release_status=payload.get("release_status"),
                platform=payload.get("platform"),
            )
        except KeyError:
            return error(f"Unknown job: {job_key}", 404)
        except ValueError as exc:
            return error(str(exc))
        except Exception as exc:
            return error(str(exc), 500)
        return jsonify(result), 200

    @app.delete("/api/v1/jobs/<job_key>")
    def delete_job(job_key: str):
        delete_topic = request.args.get("delete_topic", "1") != "0"
        try:
            result = service.delete_job(job_key, delete_topic=delete_topic)
            scheduler.remove_job(job_key)
        except KeyError:
            return error(f"Unknown job: {job_key}", 404)
        except Exception as exc:
            return error(str(exc), 500)
        return jsonify(result), 200

    @app.get("/api/v1/runs")
    def runs():
        limit = parse_limit(request.args.get("limit"), default=12, maximum=50)
        return jsonify({"items": service.list_runs(limit=limit)})

    @app.get("/api/v1/query-analytics")
    def query_analytics():
        limit = parse_limit(request.args.get("limit"), default=12, maximum=24)
        return jsonify(service.list_query_analytics(limit_runs=limit))

    @app.get("/api/v1/rate-limits")
    def rate_limits():
        limit = parse_limit(request.args.get("limit"), default=20, maximum=100)
        return jsonify(service.list_rate_limit_events(limit=limit))

    @app.get("/api/v1/review/self-check")
    def self_check():
        result = service.run_self_check()
        status = 200 if result["status"] == "ok" else 207
        return jsonify(result), status

    @app.post("/api/v1/search/keyword")
    def search_keyword():
        payload = request.get_json(silent=True) or {}
        keyword = str(payload.get("keyword", "")).strip()
        if not keyword:
            return error("keyword is required")
        blocked = active_rate_limit_response()
        if blocked is not None:
            return blocked
        try:
            limit = parse_limit(str(payload.get("limit", 50)))
            result = service.search_keyword(
                keyword=keyword,
                limit=limit,
                cookie=payload.get("cookie"),
                username=payload.get("username"),
                password=payload.get("password"),
            )
        except VRChatRateLimitError as exc:
            return record_rate_limit_and_respond(
                exc=exc,
                source_key=f"manual:keyword:{keyword}",
                job_key=None,
                trigger_type="manual",
                query_kind="keyword",
                query_value=keyword,
            )
        except ValueError as exc:
            return error(str(exc))
        except Exception as exc:
            return error(str(exc), 500)
        return jsonify(result), 201

    @app.post("/api/v1/search/user")
    def search_user():
        payload = request.get_json(silent=True) or {}
        user_id = str(payload.get("user_id", "")).strip()
        if not user_id:
            return error("user_id is required")
        blocked = active_rate_limit_response()
        if blocked is not None:
            return blocked
        try:
            limit = parse_limit(str(payload.get("limit", 50)))
            result = service.search_user(
                user_id=user_id,
                limit=limit,
                cookie=payload.get("cookie"),
                username=payload.get("username"),
                password=payload.get("password"),
            )
        except VRChatRateLimitError as exc:
            return record_rate_limit_and_respond(
                exc=exc,
                source_key=f"manual:user:{user_id}",
                job_key=None,
                trigger_type="manual",
                query_kind="user",
                query_value=user_id,
            )
        except ValueError as exc:
            return error(str(exc))
        except Exception as exc:
            return error(str(exc), 500)
        return jsonify(result), 201

    @app.post("/api/v1/search/worlds")
    def search_worlds():
        payload = request.get_json(silent=True) or {}
        blocked = active_rate_limit_response()
        if blocked is not None:
            return blocked
        try:
            limit = parse_limit(str(payload.get("limit", 50)))
            result = service.search_worlds(
                search=payload.get("search"),
                tags=payload.get("tags"),
                notags=payload.get("notags"),
                sort=str(payload.get("sort", "popularity")),
                order=str(payload.get("order", "descending")),
                featured=payload.get("featured"),
                active=payload.get("active"),
                release_status=payload.get("release_status"),
                platform=payload.get("platform"),
                limit=limit,
                source_name=payload.get("source_name"),
                cookie=payload.get("cookie"),
                username=payload.get("username"),
                password=payload.get("password"),
            )
        except VRChatRateLimitError as exc:
            label = str(payload.get("source_name", "") or payload.get("search", "") or "world-search").strip()
            return record_rate_limit_and_respond(
                exc=exc,
                source_key=f"manual:world_search:{label}",
                job_key=None,
                trigger_type="manual",
                query_kind="world_search",
                query_value=label,
            )
        except ValueError as exc:
            return error(str(exc))
        except Exception as exc:
            return error(str(exc), 500)
        return jsonify(result), 201

    @app.post("/api/v1/search/fixed")
    def search_fixed():
        payload = request.get_json(silent=True) or {}
        raw_keywords = payload.get("keywords", [])
        if isinstance(raw_keywords, str):
            keywords = [item.strip() for item in raw_keywords.split(",") if item.strip()]
        else:
            keywords = [str(item).strip() for item in raw_keywords if str(item).strip()]
        if not keywords:
            return error("keywords are required")

        raw_blacklist = payload.get("blacklist", [])
        if isinstance(raw_blacklist, str):
            blacklist = {item.strip() for item in raw_blacklist.split(",") if item.strip()}
        else:
            blacklist = {str(item).strip() for item in raw_blacklist if str(item).strip()}

        blocked = active_rate_limit_response()
        if blocked is not None:
            return blocked

        try:
            limit = parse_limit(str(payload.get("limit_per_keyword", 50)))
            result = service.search_fixed_keywords(
                keywords=keywords,
                blacklist=blacklist,
                limit_per_keyword=limit,
                source_name=str(payload.get("source_name", "fixed-keywords")),
                cookie=payload.get("cookie"),
                username=payload.get("username"),
                password=payload.get("password"),
            )
        except VRChatRateLimitError as exc:
            label = str(payload.get("source_name", "fixed-keywords"))
            return record_rate_limit_and_respond(
                exc=exc,
                source_key=f"manual:fixed:{label}",
                job_key=None,
                trigger_type="manual",
                query_kind="fixed_keywords",
                query_value=", ".join(keywords),
            )
        except ValueError as exc:
            return error(str(exc))
        except Exception as exc:
            return error(str(exc), 500)
        return jsonify(result), 201

    @app.get("/api/v1/auto-sync/status")
    def auto_sync_status():
        return jsonify(
            {
                "jobs": scheduler.get_status(),
                "rate_limit": service.list_rate_limit_events(limit=10).get("summary", {}),
            }
        ), 200

    @app.put("/api/v1/auto-sync/<job_key>/interval")
    def set_auto_sync_interval(job_key: str):
        payload = request.get_json(silent=True) or {}
        interval = str(payload.get("interval", "disabled"))
        try:
            scheduler.set_interval(job_key, interval)
        except ValueError as exc:
            return error(str(exc))
        return jsonify({"job_key": job_key, "interval": interval}), 200

    @app.post("/api/v1/auto-sync/<job_key>/run-now")
    def auto_sync_run_now(job_key: str):
        blocked = active_rate_limit_response()
        if blocked is not None:
            return blocked
        try:
            result = service.run_job(
                job_key,
                trigger_type="auto_manual",
            )
            scheduler._record_run(job_key)
        except KeyError:
            return error(f"Unknown job: {job_key}", 404)
        except VRChatRateLimitError as exc:
            return record_rate_limit_and_respond(
                exc=exc,
                source_key=f"job:{job_key}",
                job_key=job_key,
                trigger_type="auto_manual",
            )
        except Exception as exc:
            return error(str(exc), 500)
        return jsonify(result), 200

    @app.post("/api/v1/auto-sync/<job_key>/record-run")
    def auto_sync_record_run(job_key: str):
        try:
            scheduler.record_run(job_key)
        except Exception as exc:
            return error(str(exc), 500)
        return jsonify({"job_key": job_key, "status": "recorded"}), 200

    @app.get("/api/v1/dashboard/<job_key>")
    def dashboard(job_key: str):
        try:
            result = service.get_dashboard(job_key)
        except KeyError:
            return error(f"Unknown job: {job_key}", 404)
        except Exception as exc:
            return error(str(exc), 500)
        return jsonify(result), 200

    @app.get("/api/v1/dashboard")
    def scope_dashboard():
        source = str(request.args.get("source", "")).strip() or None
        topic_key = str(request.args.get("topic", "")).strip() or None
        if not source and not topic_key:
            source = "db:all"
        try:
            result = service.get_scope_dashboard(source=source, topic_key=topic_key)
        except KeyError as exc:
            return error(str(exc), 404)
        except Exception as exc:
            return error(str(exc), 500)
        return jsonify(result), 200

    @app.get("/api/v1/graph")
    def graph():
        source = request.args.get("source", "db:all")
        edge_types_raw = request.args.get("edges", "author,tag")
        edge_types = [e.strip() for e in edge_types_raw.split(",") if e.strip()]
        try:
            min_shared_tags = max(1, int(request.args.get("min_shared_tags", "2")))
            max_nodes = min(500, max(10, int(request.args.get("max_nodes", "300"))))
        except ValueError as exc:
            return error(str(exc))
        exclude_system_tags = request.args.get("exclude_system_tags", "1") != "0"
        try:
            result = service.build_world_graph(
                source=source,
                edge_types=edge_types,
                min_shared_tags=min_shared_tags,
                exclude_system_tags=exclude_system_tags,
                max_nodes=max_nodes,
            )
        except KeyError:
            return error(f"Unknown source: {source}", 404)
        except Exception as exc:
            return error(str(exc), 500)
        return jsonify(result)

    @app.get("/<path:path>")
    def static_files(path: str):
        file_path = Path(frontend_dir) / path
        if file_path.exists():
            return send_from_directory(frontend_dir, path)
        return send_from_directory(frontend_dir, "index.html")

    return app


app = create_app() if os.getenv("WORLD_INFO_WEB_EAGER_APP") == "1" else None


def main() -> None:
    port = int(os.getenv("WORLD_INFO_WEB_PORT", "5080"))
    current_app = app or create_app()
    current_app.run(host="127.0.0.1", port=port, debug=False)


if __name__ == "__main__":
    main()
