# world_info_web

Parallel new-system workspace for `world_info`. The legacy Tkinter workflow under
`world_info/` stays intact, while this package exposes an API-first web app.
The current primary model is:

- sync jobs write normalized snapshots into SQLite
- topics map worlds into reusable thematic views such as `personal`, `racing`, `taiwan`
- frontend reuses the same table/detail/chart layer across all topics

## Run backend

```bash
python -m world_info_web.backend.app
```

Default port: `5080`

`5060` is intentionally no longer used because Chromium-based browsers may block it with
`ERR_UNSAFE_PORT`.

You can override it with:

```bash
WORLD_INFO_WEB_PORT=5081 python -m world_info_web.backend.app
```

## Quick launcher

For Windows testing, you can double-click:

```bat
start_world_info_web.bat
```

It will:

- start the backend in a separate console window
- wait for `GET /api/v1/health`
- open the browser to `http://127.0.0.1:5080`

If you want to suppress browser auto-open:

```bash
set WORLD_INFO_WEB_OPEN_BROWSER=0
start_world_info_web.bat
```

## App shape

- `GET /api/v1/health`
- `GET /api/v1/sources`
- `GET /api/v1/topics`
- `GET /api/v1/topics/<topic_key>`
- `GET /api/v1/topics/<topic_key>/worlds`
- `GET /api/v1/worlds`
- `GET /api/v1/history`
- `GET /api/v1/history/<world_id>`
- `GET /api/v1/jobs`
- `POST /api/v1/import/legacy`
- `POST /api/v1/jobs/<job_key>/run`
- `GET /api/v1/runs`
- `POST /api/v1/search/keyword`
- `POST /api/v1/search/user`
- `POST /api/v1/search/fixed`
- `GET /api/v1/analytics/daily-stats`
- `GET /api/v1/review/self-check`

## Notes

- Legacy source files are loaded read-only from `world_info/scraper/` and `analytics/`.
- New sync results are written to `world_info_web/data/world_info.sqlite3`.
- Browser UI no longer asks for Cookie values; if authentication is needed, keep it in local `world_info/scraper/headers.json`.
- `GET /api/v1/review/self-check` returns `200` when checks pass and `207` when
  warnings are found.
- Creator search now uses the worlds API with a `userId` filter instead of scraping the website with Playwright.
- Named sync jobs are configured in `world_info_web/config/sync_jobs.json`.
- Topic definitions and matching rules are configured in `world_info_web/config/topics.json`.
- `POST /api/v1/import/legacy` imports legacy JSON, workbook, history, and daily stats into SQLite.

## Architecture

- Product and data architecture draft: `world_info_web/docs/architecture.zh-TW.md`
