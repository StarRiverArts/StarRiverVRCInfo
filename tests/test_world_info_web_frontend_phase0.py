from pathlib import Path
import re


REPO_ROOT = Path(__file__).resolve().parents[1]
FRONTEND_DIR = REPO_ROOT / "world_info_web" / "frontend"


def test_debug_page_exposes_panel_and_visibility_sections():
    html = (FRONTEND_DIR / "index.html").read_text(encoding="utf-8")

    assert 'id="debug-panel-status-body"' in html
    assert 'id="debug-visibility-body"' in html
    assert "Panel Status" in html
    assert "Visibility Snapshot" in html


def test_phase0_removed_broken_record_workspace_form_closer():
    html = (FRONTEND_DIR / "index.html").read_text(encoding="utf-8")

    assert "Record Workspace" in html
    assert "</form>" not in html.split("Record Workspace", 1)[1].split("</section>", 1)[0]
    assert html.count("<form") == html.count("</form>")


def test_app_uses_single_fetch_wrapper_and_positive_page_gating():
    script = (FRONTEND_DIR / "app.js").read_text(encoding="utf-8")

    assert script.count("await fetch(") == 1
    assert 'const visible = state.page !== "dashboard" || sections.includes(state.dashboardSection);' not in script
    assert 'const visible = state.page !== "monitor" || sections.includes(state.monitorSection);' not in script
    assert 'const visible = state.page !== "discover" || sections.includes(state.discoverSection);' not in script
    assert 'const visible = state.page !== "communities" || element.dataset.communitiesSection === state.communitiesSection;' not in script
    assert 'const visible = state.page !== "operations" || element.dataset.operationsSection === state.operationsSection;' not in script
    assert "debug-panel-status-body" in script
    assert "debug-visibility-body" in script
    assert re.search(r'panel:render:error', script)


def test_render_page_is_split_between_visibility_and_work():
    script = (FRONTEND_DIR / "app.js").read_text(encoding="utf-8")

    assert re.search(
        r"function renderPage\(\)\s*\{\s*renderPageVisibility\(\);\s*schedulePageWork\(\);\s*\}",
        script,
    )
    assert "function renderPageVisibility()" in script
    assert "function schedulePageWork()" in script
    visibility_block = re.search(
        r"function renderPageVisibility\(\)\s*\{(?P<body>.*?)\n\}\n\nfunction schedulePageWork",
        script,
        re.S,
    )
    assert visibility_block
    assert "loadDashboard(" not in visibility_block.group("body")
    assert "loadAutoSyncSchedule(" not in visibility_block.group("body")
    assert "loadCommunitiesWorkspace(" not in visibility_block.group("body")


def test_source_selection_prefers_current_dropdown_value():
    script = (FRONTEND_DIR / "app.js").read_text(encoding="utf-8")

    assert 'const selectedSource = $("source-select")?.value || "";' in script
    assert 'const source = preferredSource || selectedSource || state.source || "db:all";' in script
    assert 'loadCollection(selectedSource);' in script
    assert 'loadScopeOverview(selectedSource);' in script


def test_dashboard_visits_and_favorites_use_total_metrics():
    script = (FRONTEND_DIR / "app.js").read_text(encoding="utf-8")
    html = (FRONTEND_DIR / "index.html").read_text(encoding="utf-8")

    assert "total_visits: toNumber(summary.total_visits)," in script
    assert "total_favorites: toNumber(summary.total_favorites)," in script
    assert '$("dash-total-visits").textContent = stats.total_visits != null ? numberFormat.format(stats.total_visits) : "--";' in script
    assert '$("dash-total-favorites").textContent = stats.total_favorites != null ? numberFormat.format(stats.total_favorites) : "--";' in script
    assert "<span>Visits</span><strong id=\"dash-total-visits\">--</strong>" in html
    assert "<span>Favorites</span><strong id=\"dash-total-favorites\">--</strong>" in html


def test_history_requests_and_compare_scope_follow_current_db_source():
    script = (FRONTEND_DIR / "app.js").read_text(encoding="utf-8")

    assert "function buildWorldHistoryUrl(worldId)" in script
    assert 'params.set("source", selection.source);' in script
    assert 'return currentScopeKey();' in script
    assert 'fetchJson(buildWorldHistoryUrl(worldId))' in script
    assert "function truncateCompareLabelSafe(value, maxLength = 24)" in script
    assert "truncateCompareLabelSafe(row.world.name || row.world.id, 16)" in script
    assert "truncateCompareLabelSafe(row.world.name || row.world.id, 28)" in script
    assert 'label.textContent = "Show";' in script
    assert 'item.textContent = item.textContent.replace("??", " -> ");' in script
