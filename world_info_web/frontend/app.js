const state = {
  page: "dashboard",
  dashboardSection: "briefing",
  monitorSection: "events",
  operationsSection: "sync",
  discoverSection: "new",
  communitiesSection: "directory",
  compareWindow: "1d",
  compareHistoryCache: {},
  compareHistoryScope: null,
  historyTrendMode: "daily",
  historyPoints: [],
  historyWorldId: null,
  notificationPollHandle: null,
  source: null,
  activeTopic: null,
  topics: [],
  worlds: [],
  loadedCollectionScope: null,
  collectionLoadingScope: null,
  collectionInsights: null,
  briefing: null,
  communitiesData: null,
  starriverPerformance: null,
  jobs: [],
  selectedWorld: null,
  compareVisibleWorldIds: {},
  authStatus: null,
  authPending: null,
  taiwanBlacklist: [],
  taiwanCreatorWhitelist: [],
  taiwanCreatorBlacklist: [],
  autoSync: {
    enabled: true,
    jobKey: "starriver",
    running: false,
  },
  auth: {
    cookie: "",
    username: "",
    password: "",
  },
  notifications: {
    enabled: false,
    primed: false,
    latestRunIds: {},
    latestDiffRunIds: {},
  },
  events: {
    items: [],
    filter: "all",
    days: 7,
    loadedAt: null,
    pendingRefresh: false,
  },
  uiSettings: {
    enableHourlyHistoryAll: true,
  },
  debug: {
    requestLog: [],
    sequence: 0,
    panels: {},
    lifecycle: [],
    visibilitySnapshot: null,
    lastRequestError: "",
    lastRenderError: "",
    currentPage: "dashboard",
    currentSection: "briefing",
    rendering: false,
  },
};

const AUTH_STORAGE_KEY = "world_info_web_auth";
const AUTO_SYNC_STORAGE_KEY = "world_info_web_auto_sync";
const NOTIFICATION_STORAGE_KEY = "world_info_web_notifications";
const UI_SETTINGS_STORAGE_KEY = "world_info_web_ui_settings";

const numberFormat = new Intl.NumberFormat("zh-TW");
const compactNumberFormat = new Intl.NumberFormat("zh-TW", {
  notation: "compact",
  maximumFractionDigits: 1,
});
const HISTORY_TIME_ZONE = "Asia/Taipei";
const HISTORY_TIME_ZONE_LABEL = "UTC+8";
const INACTIVE_VISITS_THRESHOLD = 1000;
const INACTIVE_PUBLISHED_DAYS = 30;
const PAUSED_UPDATE_DAYS = 365;
const COMPARE_PREFETCH_LIMIT = 16;
const DEBUG_REQUEST_LIMIT = 120;
const DEBUG_LIFECYCLE_LIMIT = 120;

const PANEL_REGISTRY = {
  scopeSummary: { label: "Scope Summary", page: "discover" },
  collectionInsights: { label: "Collection Insights", page: "discover" },
  dashboardBriefing: { label: "Dashboard Briefing", page: "dashboard", section: "briefing" },
  dashboardHealth: { label: "Dashboard Health", page: "dashboard", section: "health" },
  monitorEvents: { label: "Monitor Events", page: "monitor", section: "events" },
  communitiesWorkspace: { label: "Communities Workspace", page: "communities" },
  reviewQueue: { label: "Review Queue", page: "review" },
  autoSyncStatus: { label: "Auto Sync Status", page: "operations", section: "scheduler" },
  analytics: { label: "Daily Analytics", page: "operations", section: "records" },
  jobs: { label: "Job Registry", page: "operations", section: "sync" },
  recentRuns: { label: "Recent Runs", page: "operations", section: "sync" },
  queryAnalytics: { label: "Query Analytics", page: "operations", section: "records" },
  rateLimits: { label: "Rate Limits", page: "operations", section: "diagnostics" },
  topics: { label: "Topics", page: "operations", section: "views" },
  diagnostics: { label: "Diagnostics", page: "operations", section: "diagnostics" },
};

function $(id) {
  return document.getElementById(id);
}

function parseTokenList(value) {
  return String(value || "")
    .split(/\s+/)
    .map((item) => item.trim())
    .filter(Boolean);
}

function currentSectionForPage(page = state.page) {
  if (page === "dashboard") {
    return state.dashboardSection;
  }
  if (page === "monitor") {
    return state.monitorSection;
  }
  if (page === "discover") {
    return state.discoverSection;
  }
  if (page === "communities") {
    return state.communitiesSection;
  }
  if (page === "operations") {
    return state.operationsSection;
  }
  return "";
}

function panelMetadata(panelKey) {
  return PANEL_REGISTRY[panelKey] || {
    label: panelKey,
    page: state.page,
    section: currentSectionForPage(),
  };
}

function rerenderDebugPageIfVisible() {
  if (state.page === "debug" && !state.debug.rendering) {
    renderDebugPage();
  }
}

function ensureDebugPanel(panelKey) {
  if (!panelKey) {
    return null;
  }
  const meta = panelMetadata(panelKey);
  if (!state.debug.panels[panelKey]) {
    state.debug.panels[panelKey] = {
      key: panelKey,
      label: meta.label || panelKey,
      page: meta.page || state.page,
      section: meta.section || "",
      status: "idle",
      lastRequestUrl: "",
      lastSuccessAt: "",
      lastErrorAt: "",
      errorMessage: "",
    };
  }
  return state.debug.panels[panelKey];
}

function setPanelStatus(panelKey, updates = {}) {
  const panel = ensureDebugPanel(panelKey);
  if (!panel) {
    return null;
  }
  Object.assign(panel, updates);
  rerenderDebugPageIfVisible();
  return panel;
}

function recordDebugLifecycle(event, details = {}) {
  const items = state.debug.lifecycle || [];
  items.unshift({
    time: new Date().toISOString(),
    event,
    page: details.page || state.page,
    section: details.section ?? currentSectionForPage(details.page || state.page),
    detail: details.detail || "",
  });
  if (items.length > DEBUG_LIFECYCLE_LIMIT) {
    items.length = DEBUG_LIFECYCLE_LIMIT;
  }
  state.debug.lifecycle = items;
}

function markPanelLoading(panelKey, updates = {}) {
  setPanelStatus(panelKey, {
    status: "loading",
    page: updates.page || state.page,
    section: updates.section ?? currentSectionForPage(updates.page || state.page),
    errorMessage: "",
    ...updates,
  });
  recordDebugLifecycle("panel:load:start", {
    page: updates.page || state.page,
    section: updates.section ?? currentSectionForPage(updates.page || state.page),
    detail: panelKey,
  });
}

function markPanelSuccess(panelKey, updates = {}) {
  setPanelStatus(panelKey, {
    status: updates.status || "ready",
    page: updates.page || state.page,
    section: updates.section ?? currentSectionForPage(updates.page || state.page),
    lastSuccessAt: new Date().toISOString(),
    errorMessage: "",
    ...updates,
  });
  recordDebugLifecycle("panel:load:success", {
    page: updates.page || state.page,
    section: updates.section ?? currentSectionForPage(updates.page || state.page),
    detail: panelKey,
  });
}

function markPanelError(panelKey, error, updates = {}) {
  const message = error instanceof Error ? error.message : String(error || "Unknown error");
  setPanelStatus(panelKey, {
    status: "error",
    page: updates.page || state.page,
    section: updates.section ?? currentSectionForPage(updates.page || state.page),
    lastErrorAt: new Date().toISOString(),
    errorMessage: message,
    ...updates,
  });
  recordDebugLifecycle("panel:load:error", {
    page: updates.page || state.page,
    section: updates.section ?? currentSectionForPage(updates.page || state.page),
    detail: `${panelKey}: ${message}`,
  });
}

function recordPanelRenderError(panelKey, error, updates = {}) {
  const message = error instanceof Error ? error.message : String(error || "Unknown render error");
  state.debug.lastRenderError = message;
  setPanelStatus(panelKey, {
    status: "error",
    page: updates.page || state.page,
    section: updates.section ?? currentSectionForPage(updates.page || state.page),
    lastErrorAt: new Date().toISOString(),
    errorMessage: message,
    ...updates,
  });
  recordDebugLifecycle("panel:render:error", {
    page: updates.page || state.page,
    section: updates.section ?? currentSectionForPage(updates.page || state.page),
    detail: `${panelKey}: ${message}`,
  });
}

function withPanelRender(panelKey, renderFn, onError = null, updates = {}) {
  try {
    renderFn();
    return true;
  } catch (error) {
    recordPanelRenderError(panelKey, error, updates);
    if (typeof onError === "function") {
      onError(error);
    }
    return false;
  }
}

function buildPanelStateMarkup(title, message, tone = "error") {
  return `
    <div class="panel-state panel-state-${escapeHtml(tone)}">
      <strong>${escapeHtml(title)}</strong>
      <p>${escapeHtml(message)}</p>
    </div>
  `;
}

function renderListPanelState(targetId, title, message, tone = "error") {
  const target = $(targetId);
  if (!target) {
    return;
  }
  target.innerHTML = buildPanelStateMarkup(title, message, tone);
}

function renderStatsPanelState(targetId, title, message, tone = "error") {
  const target = $(targetId);
  if (!target) {
    return;
  }
  target.innerHTML = `
    <article class="history-stat panel-stat panel-stat-${escapeHtml(tone)}">
      <span>${escapeHtml(title)}</span>
      <strong>${escapeHtml(message)}</strong>
    </article>
  `;
}

function renderSummaryError(message) {
  const grid = $("summary-grid");
  if (!grid) {
    return;
  }
  grid.innerHTML = `
    <article class="summary-card summary-card-error">
      <span>Summary Error</span>
      <strong>Unavailable</strong>
      <small>${escapeHtml(message)}</small>
    </article>
  `;
}

function renderCollectionInsightsError(message, label = "current collection") {
  $("growth-caption").textContent = label;
  $("rising-now-caption").textContent = label;
  $("new-hot-caption").textContent = label;
  $("worth-watching-caption").textContent = label;
  $("dormant-revival-caption").textContent = label;
  $("creator-momentum-caption").textContent = label;
  $("authors-caption").textContent = label;
  renderListPanelState("growth-list", "Collection Insights", message);
  renderListPanelState("rising-now-list", "Collection Insights", message);
  renderListPanelState("new-hot-list", "Collection Insights", message);
  renderListPanelState("worth-watching-list", "Collection Insights", message);
  renderListPanelState("dormant-revival-list", "Collection Insights", message);
  renderListPanelState("creator-momentum-list", "Collection Insights", message);
  renderListPanelState("authors-list", "Collection Insights", message);
  renderStatsPanelState("anomalies-summary", "Collection Insights", "error");
  renderListPanelState("anomalies-list", "Anomaly Watch", message);
  renderStatsPanelState("updates-summary", "Collection Insights", "error");
  renderListPanelState("updates-list", "Update Effectiveness", message);
  $("signal-caption").textContent = `${label} / heat-popularity relationships`;
  renderStatsPanelState("signal-summary", "Signal Analysis", "error");
  renderListPanelState("signal-correlation-list", "Signal Analysis", message);
  renderListPanelState("signal-chart-grid", "Signal Analysis", message);
  renderListPanelState("signal-leaderboards", "Signal Analysis", message);
}

function renderPerformanceError(message, label = "current collection") {
  $("performance-caption").textContent = label;
  renderStatsPanelState("performance-summary", "Performance", "error");
  renderListPanelState("performance-list", "Performance", message);
}

function renderReviewError(message) {
  $("review-status").textContent = "ERROR";
  $("review-list").innerHTML = `<li>${escapeHtml(message)}</li>`;
}

function renderAnalyticsError(message) {
  renderListPanelState("analytics-list", "Daily Analytics", message);
}

function renderJobsError(message) {
  renderListPanelState("jobs-list", "Job Registry", message);
}

function renderRunsError(message) {
  renderListPanelState("runs-list", "Recent Runs", message);
}

function renderQueryAnalyticsError(message) {
  renderStatsPanelState("query-analytics-summary", "Query Analytics", "error");
  renderListPanelState("query-analytics-list", "Query Analytics", message);
}

function renderRateLimitsError(message) {
  $("rate-limit-caption").textContent = "recent 429 events";
  renderStatsPanelState("rate-limit-summary", "Rate Limits", "error");
  renderListPanelState("rate-limit-list", "Rate Limits", message);
}

function renderTopicsError(message) {
  renderListPanelState("topics-list", "Topics", message);
  renderListPanelState("topics-admin-list", "Topic Manager", message);
}

function renderDiagnosticsError(message) {
  renderListPanelState("job-diagnostics-list", "Diagnostics", message);
  renderListPanelState("source-diff-list", "Source Diff", message);
}

function renderAutoSyncScheduleError(message) {
  $("auto-sync-caption").textContent = "Server-side scheduler";
  renderListPanelState("auto-sync-job-list", "Auto Sync", message);
}

function describeDebugElement(element) {
  const classes = [...element.classList].filter((name) => name !== "hidden");
  const parts = [
    element.id ? `#${element.id}` : "",
    classes.length ? `.${classes[0]}` : element.tagName.toLowerCase(),
  ].filter(Boolean);
  const pageTokens = parseTokenList(element.dataset.pages).join(",");
  const sectionTokens = [
    element.dataset.dashboardSection ? `dashboard:${element.dataset.dashboardSection}` : "",
    element.dataset.monitorSection ? `monitor:${element.dataset.monitorSection}` : "",
    element.dataset.discoverSection ? `discover:${element.dataset.discoverSection}` : "",
    element.dataset.communitiesSection ? `communities:${element.dataset.communitiesSection}` : "",
    element.dataset.operationsSection ? `operations:${element.dataset.operationsSection}` : "",
  ]
    .filter(Boolean)
    .join(" | ");
  return `${parts.join("")} [${pageTokens}]${sectionTokens ? ` ${sectionTokens}` : ""}`;
}

function isElementVisibleForCurrentState(element) {
  const pages = parseTokenList(element.dataset.pages);
  if (!pages.includes(state.page)) {
    return false;
  }
  if (state.page === "dashboard" && element.dataset.dashboardSection) {
    return parseTokenList(element.dataset.dashboardSection).includes(state.dashboardSection);
  }
  if (state.page === "monitor" && element.dataset.monitorSection) {
    return parseTokenList(element.dataset.monitorSection).includes(state.monitorSection);
  }
  if (state.page === "discover" && element.dataset.discoverSection) {
    return parseTokenList(element.dataset.discoverSection).includes(state.discoverSection);
  }
  if (state.page === "communities" && element.dataset.communitiesSection) {
    return parseTokenList(element.dataset.communitiesSection).includes(state.communitiesSection);
  }
  if (state.page === "operations" && element.dataset.operationsSection) {
    return parseTokenList(element.dataset.operationsSection).includes(state.operationsSection);
  }
  return true;
}

function captureVisibilitySnapshot() {
  const sections = [...document.querySelectorAll("[data-pages]")];
  const expectedVisible = sections
    .filter((element) => isElementVisibleForCurrentState(element))
    .map((element) => describeDebugElement(element));
  const actualVisible = sections
    .filter((element) => !element.classList.contains("hidden"))
    .map((element) => describeDebugElement(element));
  return {
    page: state.page,
    section: currentSectionForPage(),
    expectedVisible,
    actualVisible,
    leakedVisible: actualVisible.filter((label) => !expectedVisible.includes(label)),
    hiddenExpected: expectedVisible.filter((label) => !actualVisible.includes(label)),
  };
}

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;");
}

function splitCsv(value) {
  return String(value || "")
    .split(",")
    .map((item) => item.trim())
    .filter(Boolean);
}

function toNumber(value) {
  if (value == null || value === "") {
    return 0;
  }
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : 0;
}

function hasMetric(value) {
  return value != null && value !== "";
}

function formatMetric(value) {
  return hasMetric(value) ? numberFormat.format(value) : "-";
}

function formatTrendPercent(value) {
  if (value == null || value === "") {
    return "-";
  }
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return "-";
  }
  return `${Math.round(numeric * 1000) / 10}%`;
}

function compareKeyForWorld(world) {
  return String(world?.id || world?.name || world?.author_name || world?.author_id || "unknown");
}

function isCompareWorldVisible(world) {
  const key = compareKeyForWorld(world);
  if (!Object.keys(state.compareVisibleWorldIds).length) {
    return true;
  }
  return state.compareVisibleWorldIds[key] !== false;
}

function toggleCompareWorldVisibility(worldKey, visible) {
  state.compareVisibleWorldIds = {
    ...state.compareVisibleWorldIds,
    [worldKey]: visible,
  };
}

function parseDate(value) {
  if (!value) {
    return null;
  }
  const parsed = new Date(value);
  return Number.isNaN(parsed.getTime()) ? null : parsed;
}

function formatDateTime(value) {
  const parsed = parseDate(value);
  if (!parsed) {
    return "-";
  }
  return new Intl.DateTimeFormat("zh-TW", {
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
  }).format(parsed);
}

function formatDateLabel(value) {
  const parsed = parseDate(value);
  if (!parsed) {
    return "-";
  }
  return new Intl.DateTimeFormat("zh-TW", {
    month: "2-digit",
    day: "2-digit",
  }).format(parsed);
}

function formatDateTimeUtc8(value) {
  const parsed = parseDate(value);
  if (!parsed) {
    return null;
  }
  return new Intl.DateTimeFormat("zh-TW", {
    timeZone: HISTORY_TIME_ZONE,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
  }).format(parsed);
}

function getUtc8DayParts(value) {
  const parsed = parseDate(value);
  if (!parsed) {
    return null;
  }
  const parts = new Intl.DateTimeFormat("en-US", {
    timeZone: HISTORY_TIME_ZONE,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).formatToParts(parsed);
  const map = Object.fromEntries(parts.filter((item) => item.type !== "literal").map((item) => [item.type, item.value]));
  if (!map.year || !map.month || !map.day) {
    return null;
  }
  return {
    year: Number(map.year),
    month: Number(map.month),
    day: Number(map.day),
  };
}

function getUtc8HourParts(value) {
  const parsed = parseDate(value);
  if (!parsed) {
    return null;
  }
  const parts = new Intl.DateTimeFormat("en-US", {
    timeZone: HISTORY_TIME_ZONE,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    hourCycle: "h23",
  }).formatToParts(parsed);
  const map = Object.fromEntries(parts.filter((item) => item.type !== "literal").map((item) => [item.type, item.value]));
  if (!map.year || !map.month || !map.day || !map.hour) {
    return null;
  }
  return {
    year: Number(map.year),
    month: Number(map.month),
    day: Number(map.day),
    hour: Number(map.hour),
  };
}

function getUtc8DayKey(value) {
  const parts = getUtc8DayParts(value);
  if (!parts) {
    return null;
  }
  return `${parts.year.toString().padStart(4, "0")}-${parts.month.toString().padStart(2, "0")}-${parts.day.toString().padStart(2, "0")}`;
}

function getUtc8DayStamp(value) {
  const parts = getUtc8DayParts(value);
  if (!parts) {
    return null;
  }
  return Date.UTC(parts.year, parts.month - 1, parts.day);
}

function formatDateLabelUtc8(value) {
  const parsed = parseDate(value);
  if (!parsed) {
    return "-";
  }
  return new Intl.DateTimeFormat("zh-TW", {
    timeZone: HISTORY_TIME_ZONE,
    month: "2-digit",
    day: "2-digit",
  }).format(parsed);
}

function formatHourLabelUtc8(value) {
  const parsed = parseDate(value);
  if (!parsed) {
    return "-";
  }
  return new Intl.DateTimeFormat("zh-TW", {
    timeZone: HISTORY_TIME_ZONE,
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    hourCycle: "h23",
  }).format(parsed);
}

function buildDayAxisTicks(points, maxTicks = 8) {
  if (!points.length) {
    return [];
  }
  const dayStamps = points
    .map((point) => point.chart_day_stamp ?? getUtc8DayStamp(point.iso_time))
    .filter((value) => Number.isFinite(value));
  if (!dayStamps.length) {
    return [];
  }
  const firstDay = Math.min(...dayStamps);
  const lastDay = Math.max(...dayStamps);
  const totalDays = Math.max(Math.round((lastDay - firstDay) / 86400000), 0);
  const stepDays = Math.max(1, Math.ceil((totalDays + 1) / maxTicks));
  const ticks = [];
  for (let offset = 0; offset <= totalDays; offset += stepDays) {
    ticks.push(firstDay + offset * 86400000);
  }
  const lastTick = ticks[ticks.length - 1];
  if (!lastTick || lastTick !== lastDay) {
    ticks.push(lastDay);
  }
  return ticks;
}

function getUtc8HourKey(value) {
  const parts = getUtc8HourParts(value);
  if (!parts) {
    return null;
  }
  return `${parts.year.toString().padStart(4, "0")}-${parts.month.toString().padStart(2, "0")}-${parts.day.toString().padStart(2, "0")} ${parts.hour.toString().padStart(2, "0")}:00`;
}

function getUtc8HourStamp(value) {
  const parts = getUtc8HourParts(value);
  if (!parts) {
    return null;
  }
  return Date.UTC(parts.year, parts.month - 1, parts.day, parts.hour);
}

function buildHourAxisTicks(points, maxTicks = 8) {
  if (!points.length) {
    return [];
  }
  const hourStamps = points
    .map((point) => point.chart_hour_stamp ?? getUtc8HourStamp(point.iso_time))
    .filter((value) => Number.isFinite(value));
  if (!hourStamps.length) {
    return [];
  }
  const firstHour = Math.min(...hourStamps);
  const lastHour = Math.max(...hourStamps);
  const totalHours = Math.max(Math.round((lastHour - firstHour) / 3600000), 0);
  const stepHours = Math.max(1, Math.ceil((totalHours + 1) / maxTicks));
  const ticks = [];
  for (let offset = 0; offset <= totalHours; offset += stepHours) {
    ticks.push(firstHour + offset * 3600000);
  }
  const lastTick = ticks[ticks.length - 1];
  if (!lastTick || lastTick !== lastHour) {
    ticks.push(lastHour);
  }
  return ticks;
}

function formatAxisValue(value) {
  if (value >= 1000) {
    return compactNumberFormat.format(value);
  }
  return numberFormat.format(Math.round(value * 10) / 10);
}

function formatDelta(value) {
  if (value == null) {
    return "-";
  }
  return `${value >= 0 ? "+" : ""}${numberFormat.format(value)}`;
}

function formatPercent(value) {
  if (value == null) {
    return "-";
  }
  return `${Math.round(value * 100) / 100}%`;
}

function daysBetween(later, earlier) {
  if (!later || !earlier) {
    return null;
  }
  return Math.floor((later.getTime() - earlier.getTime()) / 86400000);
}

function getWorldSignals(world) {
  const now = new Date();
  const fetched = parseDate(world.fetched_at);
  const publication = parseDate(world.publication_date);
  const updated = parseDate(world.updated_at);
  const visits = toNumber(world.visits);
  const insight = state.collectionInsights?.world_insights?.[world.id] || {};
  const fetchedWithinWeek = fetched ? daysBetween(now, fetched) <= 7 : false;
  const isNew = Boolean(fetchedWithinWeek && publication && daysBetween(now, publication) <= 7);
  const isRecentlyUpdated = Boolean(
    fetchedWithinWeek &&
    updated &&
    daysBetween(now, updated) <= 30 &&
    !isNew
  );
  const daysSincePublication = publication ? daysBetween(now, publication) : null;
  const daysSinceUpdate = updated ? daysBetween(now, updated) : null;
  const isInactive = Boolean(
    daysSincePublication != null &&
    daysSincePublication >= INACTIVE_PUBLISHED_DAYS &&
    visits < INACTIVE_VISITS_THRESHOLD
  );
  const isPausedUpdate = Boolean(daysSinceUpdate != null && daysSinceUpdate > PAUSED_UPDATE_DAYS);
  return {
    isNew,
    isRecentlyUpdated,
    isInactive,
    isPausedUpdate,
    insightTags: Array.isArray(insight.tags) ? insight.tags : [],
  };
}

function renderWorldSignalBadges(signals) {
  const items = [];
  if (signals.isNew) items.push({ label: "NEW", className: "world-badge-new" });
  if (signals.insightTags.includes("ACTIVE")) items.push({ label: "ACTIVE", className: "world-badge-active" });
  if (signals.insightTags.includes("REVIVE")) items.push({ label: "REVIVE", className: "world-badge-revive" });
  if (signals.insightTags.includes("LOVED WORLD")) items.push({ label: "LOVED", className: "world-badge-loved" });
  if (signals.insightTags.includes("STEADY FLOW")) items.push({ label: "STEADY FLOW", className: "world-badge-steady" });
  if (signals.insightTags.includes("SILENCE UPDATE")) items.push({ label: "SILENCE UPDATE", className: "world-badge-silence" });
  if (signals.insightTags.includes("INACTIVE") || signals.isInactive) items.push({ label: "INACTIVE", className: "world-badge-inactive" });
  if (signals.isRecentlyUpdated) items.push({ label: "RECENT UPDATE", className: "world-badge-updated" });
  if (signals.isPausedUpdate) items.push({ label: "PAUSED UPDATE", className: "world-badge-paused" });
  return items.slice(0, 3).map((item) => `<span class="world-badge ${item.className}">${item.label}</span>`).join("");
}

function setTopicMode(label) {
  $("topic-mode").value = label;
}

function currentScopeSelection(preferredSource = null) {
  if (state.activeTopic) {
    return {
      scopeKey: `topic:${state.activeTopic}`,
      topicKey: state.activeTopic,
      label: state.activeTopic,
    };
  }
  const selectedSource = $("source-select")?.value || "";
  const source = preferredSource || selectedSource || state.source || "db:all";
  return {
    scopeKey: `source:${source}`,
    source,
    label: source,
  };
}

function currentScopeKey(preferredSource = null) {
  return currentScopeSelection(preferredSource).scopeKey;
}

function buildWorldHistoryUrl(worldId) {
  const params = new URLSearchParams();
  const selection = currentScopeSelection();
  if (!state.activeTopic && selection.source) {
    params.set("source", selection.source);
  }
  const query = params.toString();
  return query
    ? `/api/v1/history/${encodeURIComponent(worldId)}?${query}`
    : `/api/v1/history/${encodeURIComponent(worldId)}`;
}

function collectionMatchesCurrentScope(preferredSource = null) {
  return state.loadedCollectionScope === currentScopeKey(preferredSource);
}

function renderDiscoverPlaceholder(message) {
  $("table-caption").textContent = message;
  $("world-table-body").innerHTML = `
    <tr>
      <td colspan="10">${escapeHtml(message)}</td>
    </tr>
  `;
  if ($("world-detail")) {
    $("world-detail").textContent = message;
  }
  $("history-world-label").textContent = "No selection";
  $("history-focus-grid").innerHTML = "";
  $("history-chart").innerHTML = "";
  renderHistoryModeControls();
  renderEditor(null);
  renderWorldCompare([]);
}

async function ensureDiscoverCollection(preferredSource = null) {
  const scopeKey = currentScopeKey(preferredSource);
  if (state.loadedCollectionScope === scopeKey || state.collectionLoadingScope === scopeKey) {
    return;
  }
  renderDiscoverPlaceholder("Loading collection...");
  try {
    await loadCollection(preferredSource);
  } catch (error) {
    renderDiscoverPlaceholder(error.message || "Failed to load collection.");
  }
}

function getAuthPayload() {
  return {
    cookie: state.auth.cookie.trim() || undefined,
    username: state.auth.username.trim() || undefined,
    password: state.auth.password || undefined,
  };
}

function saveAuthState() {
  window.localStorage.setItem(AUTH_STORAGE_KEY, JSON.stringify(state.auth));
  renderAuthStatus();
}

function loadAuthState() {
  try {
    const raw = window.localStorage.getItem(AUTH_STORAGE_KEY);
    if (!raw) {
      return;
    }
    const payload = JSON.parse(raw);
    state.auth.cookie = String(payload.cookie || "");
    state.auth.username = String(payload.username || "");
    state.auth.password = String(payload.password || "");
  } catch {
    state.auth.cookie = "";
    state.auth.username = "";
    state.auth.password = "";
  }
}

function saveAutoSyncState() {
  window.localStorage.setItem(AUTO_SYNC_STORAGE_KEY, JSON.stringify(state.autoSync));
  renderAutoSyncControls();
}

function loadAutoSyncState() {
  try {
    const raw = window.localStorage.getItem(AUTO_SYNC_STORAGE_KEY);
    if (!raw) {
      return;
    }
    const payload = JSON.parse(raw);
    state.autoSync.enabled = payload.enabled !== false;
    state.autoSync.jobKey = String(payload.jobKey || "starriver");
  } catch {
    state.autoSync.enabled = true;
    state.autoSync.jobKey = "starriver";
  }
}

function saveNotificationState() {
  window.localStorage.setItem(NOTIFICATION_STORAGE_KEY, JSON.stringify(state.notifications));
  renderNotificationStatus();
}

function loadNotificationState() {
  try {
    const raw = window.localStorage.getItem(NOTIFICATION_STORAGE_KEY);
    if (!raw) {
      renderNotificationStatus();
      return;
    }
    const payload = JSON.parse(raw);
    state.notifications.enabled = Boolean(payload.enabled);
    state.notifications.primed = Boolean(payload.primed);
    state.notifications.latestRunIds = payload.latestRunIds || {};
    state.notifications.latestDiffRunIds = payload.latestDiffRunIds || {};
  } catch {
    state.notifications.enabled = false;
    state.notifications.primed = false;
    state.notifications.latestRunIds = {};
    state.notifications.latestDiffRunIds = {};
  }
  renderNotificationStatus();
}

function saveUiSettings() {
  window.localStorage.setItem(UI_SETTINGS_STORAGE_KEY, JSON.stringify(state.uiSettings));
}

function loadUiSettings() {
  try {
    const raw = window.localStorage.getItem(UI_SETTINGS_STORAGE_KEY);
    if (!raw) {
      return;
    }
    const payload = JSON.parse(raw);
    state.uiSettings.enableHourlyHistoryAll = payload.enableHourlyHistoryAll !== false;
  } catch {
    state.uiSettings.enableHourlyHistoryAll = true;
  }
}

function renderUiSettings() {
  const toggle = $("hourly-history-all-toggle");
  if (!toggle) {
    return;
  }
  toggle.checked = state.uiSettings.enableHourlyHistoryAll;
}

function notificationsSupported() {
  return "Notification" in window;
}

function notificationsAllowed() {
  return notificationsSupported() && Notification.permission === "granted" && state.notifications.enabled;
}

function renderNotificationStatus() {
  const label = $("notification-status");
  const button = $("notification-toggle-button");
  if (!label || !button) {
    return;
  }
  if (!notificationsSupported()) {
    label.textContent = "This browser does not support notifications.";
    button.disabled = true;
    button.textContent = "Unsupported";
    return;
  }
  const permission = Notification.permission;
  if (permission === "granted" && state.notifications.enabled) {
    label.textContent = "Browser notifications are on for sync results and notable world spikes.";
    button.disabled = false;
    button.textContent = "Disable Notifications";
    return;
  }
  if (permission === "granted") {
    label.textContent = "Browser permission is granted, but local notifications are muted.";
    button.disabled = false;
    button.textContent = "Enable Notifications";
    return;
  }
  if (permission === "denied") {
    label.textContent = "Browser notifications are blocked. Re-enable them in site settings.";
    button.disabled = true;
    button.textContent = "Blocked";
    return;
  }
  label.textContent = "Browser notifications are off.";
  button.disabled = false;
  button.textContent = "Enable Notifications";
}

function emitBrowserNotification(title, options = {}) {
  if (!notificationsAllowed()) {
    return;
  }
  try {
    const notification = new Notification(title, {
      silent: false,
      ...options,
    });
    if (options?.url) {
      notification.onclick = () => {
        window.focus();
        window.location.hash = "";
      };
    }
  } catch {
    // Ignore notification delivery failures.
  }
}

function renderAuthStatus() {
  const fallbackStatus = state.auth.cookie.trim()
    ? "cookie loaded"
    : state.auth.username.trim() && state.auth.password
      ? "basic auth loaded"
      : "no auth saved";
  const status = state.authStatus || {
    status: null,
    mode: null,
    label: fallbackStatus,
    detail: "Stored in this browser only.",
  };
  $("auth-status-label").textContent = status.label;
  $("auth-status-detail").textContent = status.detail;
  $("auth-cookie").value = state.auth.cookie;
  $("auth-username").value = state.auth.username;
  $("auth-password").value = state.auth.password;
  const panel = $("auth-2fa-panel");
  if (state.authPending) {
    $("auth-2fa-method").innerHTML = state.authPending.methods
      .map((method) => `<option value="${escapeHtml(method)}">${escapeHtml(method)}</option>`)
      .join("");
    panel.classList.remove("hidden");
  } else {
    panel.classList.add("hidden");
  }
}

function renderPage() {
  renderPageVisibility();
  schedulePageWork();
}

function renderPageVisibility() {
  const previousPage = state.debug.currentPage;
  const previousSection = state.debug.currentSection;
  document.body.dataset.page = state.page;
  document.body.dataset.dashboardSection = state.dashboardSection;
  document.body.dataset.discoverSection = state.discoverSection;
  document.body.dataset.operationsSection = state.operationsSection;
  document.body.dataset.monitorSection = state.monitorSection;
  document.body.dataset.communitiesSection = state.communitiesSection;
  recordDebugLifecycle("page:render", {
    page: state.page,
    section: currentSectionForPage(),
    detail: previousPage === state.page
      ? `render ${state.page}/${currentSectionForPage()}`
      : `transition ${previousPage || "-"}:${previousSection || "-"} -> ${state.page}:${currentSectionForPage()}`,
  });
  state.debug.currentPage = state.page;
  state.debug.currentSection = currentSectionForPage();
  const discoverOverviewSections = new Set(["new", "potential", "regional", "search"]);
  document.querySelectorAll("[data-pages]").forEach((element) => {
    element.classList.toggle("hidden", !isElementVisibleForCurrentState(element));
  });
  document.querySelectorAll("[data-page-tab]").forEach((button) => {
    button.classList.toggle("is-active", button.dataset.pageTab === state.page);
  });
  document.querySelectorAll("[data-discover-only]").forEach((element) => {
    element.classList.toggle("hidden", state.page !== "discover");
  });
  document.querySelectorAll("[data-discover-filters]").forEach((element) => {
    const visible = state.page === "discover" && discoverOverviewSections.has(state.discoverSection);
    element.classList.toggle("hidden", !visible);
  });
  const summaryGrid = $("summary-grid");
  if (summaryGrid) {
    const visible = state.page === "discover" && discoverOverviewSections.has(state.discoverSection);
    summaryGrid.classList.toggle("hidden", !visible);
  }
  const toolbar = document.querySelector(".toolbar.card");
  if (toolbar) {
    const showToolbar = state.page === "dashboard"
      || state.page === "monitor"
      || state.page === "discover"
      || (state.page === "operations" && state.operationsSection === "records");
    toolbar.classList.toggle("hidden", !showToolbar);
  }
  document.querySelectorAll("[data-dashboard-tab]").forEach((button) => {
    button.classList.toggle("is-active", button.dataset.dashboardTab === state.dashboardSection);
  });
  document.querySelectorAll("[data-monitor-tab]").forEach((button) => {
    button.classList.toggle("is-active", button.dataset.monitorTab === state.monitorSection);
  });
  document.querySelectorAll("[data-discover-tab]").forEach((button) => {
    button.classList.toggle("is-active", button.dataset.discoverTab === state.discoverSection);
  });
  document.querySelectorAll("[data-communities-tab]").forEach((button) => {
    button.classList.toggle("is-active", button.dataset.communitiesTab === state.communitiesSection);
  });
  document.querySelectorAll("[data-operations-tab]").forEach((button) => {
    button.classList.toggle("is-active", button.dataset.operationsTab === state.operationsSection);
  });
  const isConfigPage = state.page === "operations" || state.page === "review";
  document.querySelector(".layout").classList.toggle("layout--sync", isConfigPage);
  state.debug.visibilitySnapshot = captureVisibilitySnapshot();
}

function schedulePageWork() {
  if (state.page === "dashboard") {
    loadDashboard();
    if (state.collectionInsights) {
      renderPerformance(state.collectionInsights);
    }
    if (state.dashboardSection === "health") {
      loadCommunitiesWorkspace();
    }
  }
  if (state.page === "discover") {
    if (!collectionMatchesCurrentScope()) {
      ensureDiscoverCollection();
    } else {
      renderWorldCompare(state.worlds);
    }
  }
  if (state.page === "monitor" && !state.events.loadedAt) {
    loadEventFeed();
  }
  if (state.page === "monitor") {
    if (state.collectionInsights) {
      renderPerformance(state.collectionInsights);
    }
    if (state.monitorSection === "groups") {
      loadCommunitiesWorkspace();
    }
  }
  if (state.page === "communities") {
    loadCommunitiesWorkspace();
  }
  if (state.page === "debug") {
    renderDebugPage();
  }
  if (state.page === "operations") {
    loadAutoSyncSchedule();
  }
}

function renderAutoSyncControls() {
  const select = $("auto-sync-job");
  const jobs = state.jobs || [];
  const options = jobs.length
    ? jobs.map((job) => `<option value="${escapeHtml(job.job_key)}">${escapeHtml(job.label)}</option>`).join("")
    : `<option value="starriver">starriver</option>`;
  select.innerHTML = options;
  const availableJobKeys = [...select.options].map((option) => option.value);
  if (!availableJobKeys.includes(state.autoSync.jobKey) && availableJobKeys.length) {
    state.autoSync.jobKey = availableJobKeys.includes("starriver") ? "starriver" : availableJobKeys[0];
  }
  select.value = state.autoSync.jobKey;
  $("auto-sync-enabled").checked = state.autoSync.enabled;
  const selectedLabel = jobs.find((job) => job.job_key === state.autoSync.jobKey)?.label || state.autoSync.jobKey;
  $("auto-sync-status").textContent = state.autoSync.enabled
    ? `target: ${selectedLabel}`
    : "disabled";
}

function compareWindowLabel(value) {
  if (value === "1d") {
    return "24h";
  }
  if (value === "7d") {
    return "7d";
  }
  if (value === "30d") {
    return "30d";
  }
  return "all";
}

function compareWindowMs(value) {
  if (value === "1d") {
    return 24 * 60 * 60 * 1000;
  }
  if (value === "7d") {
    return 7 * 24 * 60 * 60 * 1000;
  }
  if (value === "30d") {
    return 30 * 24 * 60 * 60 * 1000;
  }
  return null;
}

function applyDiscoverSectionDefaults(section) {
  const sortSelect = $("sort-select");
  const directionSelect = $("direction-select");
  if (!sortSelect || !directionSelect) {
    return;
  }
  if (section === "new") {
    sortSelect.value = "new_hot";
    directionSelect.value = "desc";
    return;
  }
  if (section === "potential") {
    sortSelect.value = "worth_watching";
    directionSelect.value = "desc";
    return;
  }
  if (section === "regional") {
    sortSelect.value = "momentum";
    directionSelect.value = "desc";
    return;
  }
  if (section === "search") {
    sortSelect.value = "breakout";
    directionSelect.value = "desc";
    return;
  }
  if (section === "compare" || section === "history" || section === "signals") {
    sortSelect.value = "momentum";
    directionSelect.value = "desc";
  }
}

function renderSyncStatus(message, tone = "warn") {
  const banner = $("sync-status-banner");
  if (!message) {
    banner.textContent = "";
    banner.className = "status-banner hidden";
    return;
  }
  banner.textContent = message;
  banner.className = `status-banner ${tone}`;
}

function setHelperStatus(id, message, tone = "") {
  const element = $(id);
  if (!element) {
    return;
  }
  element.textContent = message || "";
  element.dataset.tone = tone || "";
}

function resetGroupForm() {
  $("group-form")?.reset();
  if ($("group-managed-status")) {
    $("group-managed-status").value = "observed";
  }
  setHelperStatus("group-form-status", "");
}

function resetManagedGroupForm() {
  $("managed-group-form")?.reset();
  if ($("managed-posting-enabled")) {
    $("managed-posting-enabled").checked = false;
  }
  setHelperStatus("managed-group-status", "");
}

function resetScheduledPostForm() {
  $("scheduled-post-form")?.reset();
  if ($("scheduled-post-id")) {
    $("scheduled-post-id").value = "";
  }
  if ($("scheduled-post-content-type")) {
    $("scheduled-post-content-type").value = "announcement";
  }
  if ($("scheduled-post-status")) {
    $("scheduled-post-status").value = "pending";
  }
  setHelperStatus("scheduled-post-status-text", "");
}

function resetGroupWorldForm() {
  $("group-world-form")?.reset();
  setHelperStatus("group-world-status", "");
}

function fillGroupForm(item) {
  $("group-id").value = item?.group_id || "";
  $("group-name").value = item?.name || "";
  $("group-region").value = item?.region || "";
  $("group-category").value = item?.category || "";
  $("group-managed-status").value = item?.managed_status || "observed";
  $("group-description").value = item?.description || "";
  $("group-external-links").value = Array.isArray(item?.external_links) ? item.external_links.join(", ") : "";
  setHelperStatus("group-form-status", item?.group_id ? `Editing ${item.group_id}` : "");
}

function fillManagedGroupForm(item) {
  $("managed-group-id").value = item?.group_id || "";
  $("managed-workspace-key").value = item?.workspace_key || "";
  $("managed-posting-enabled").checked = Boolean(item?.posting_enabled);
  $("managed-notes").value = item?.notes || "";
  setHelperStatus("managed-group-status", item?.group_id ? `Editing ${item.group_id}` : "");
}

function fillScheduledPostForm(item) {
  $("scheduled-post-id").value = item?.id || "";
  $("scheduled-post-group-id").value = item?.group_id || "";
  $("scheduled-post-content-type").value = item?.content_type || "announcement";
  $("scheduled-post-status").value = item?.status || "pending";
  $("scheduled-post-scheduled-for").value = item?.scheduled_for || "";
  $("scheduled-post-payload").value = JSON.stringify(item?.payload || {}, null, 2);
  setHelperStatus("scheduled-post-status-text", item?.id ? `Editing scheduled post #${item.id}` : "");
}

function fillGroupWorldForm(item) {
  $("group-world-group-id").value = item?.group_id || "";
  $("group-world-world-id").value = item?.world_id || "";
  $("group-world-role").value = item?.membership_role || "member";
  $("group-world-source-key").value = item?.source_key || "";
  setHelperStatus(
    "group-world-status",
    item?.group_id && item?.world_id ? `Editing ${item.group_id} / ${item.world_id}` : "",
  );
}

function renderGroupActionButtons(item) {
  return `
    <div class="run-item-actions">
      <button class="button button-secondary" type="button" data-community-action="edit-group" data-group-id="${escapeHtml(item.group_id || "")}">Edit</button>
      <button class="button button-secondary" type="button" data-community-action="delete-group" data-group-id="${escapeHtml(item.group_id || "")}">Delete</button>
    </div>
  `;
}

function renderManagedGroupActionButtons(item) {
  return `
    <div class="run-item-actions">
      <button class="button button-secondary" type="button" data-community-action="edit-managed-group" data-group-id="${escapeHtml(item.group_id || "")}">Edit</button>
      <button class="button button-secondary" type="button" data-community-action="delete-managed-group" data-group-id="${escapeHtml(item.group_id || "")}">Delete</button>
    </div>
  `;
}

function renderScheduledPostActionButtons(item) {
  return `
    <div class="run-item-actions">
      <button class="button button-secondary" type="button" data-community-action="edit-scheduled-post" data-post-id="${escapeHtml(String(item.id || ""))}">Edit</button>
      <button class="button button-secondary" type="button" data-community-action="delete-scheduled-post" data-post-id="${escapeHtml(String(item.id || ""))}">Delete</button>
    </div>
  `;
}

function renderGroupWorldActionButtons(item) {
  return `
    <div class="run-item-actions">
      <button class="button button-secondary" type="button" data-community-action="edit-group-world" data-group-id="${escapeHtml(item.group_id || "")}" data-world-id="${escapeHtml(item.world_id || "")}">Edit</button>
      <button class="button button-secondary" type="button" data-community-action="delete-group-world" data-group-id="${escapeHtml(item.group_id || "")}" data-world-id="${escapeHtml(item.world_id || "")}">Delete</button>
    </div>
  `;
}

function renderCommunitiesWorkspace(payload) {
  const summary = payload?.summary || {};
  $("communities-summary").innerHTML = [
    { label: "Groups", value: summary.group_count ?? 0 },
    { label: "Managed", value: summary.managed_group_count ?? 0 },
    { label: "Queued Posts", value: summary.scheduled_post_count ?? 0 },
    { label: "Tracked Creators", value: summary.tracked_creator_count ?? 0 },
  ]
    .map(
      (item) => `
        <article class="history-stat">
          <span>${escapeHtml(item.label)}</span>
          <strong>${escapeHtml(numberFormat.format(item.value || 0))}</strong>
        </article>
      `,
    )
    .join("");

  const directoryItems = payload?.directory?.items || [];
  $("communities-directory-list").innerHTML = directoryItems.length
    ? directoryItems
        .map(
          (item) => `
            <article class="run-item">
              <header>
                <strong>${escapeHtml(item.name || item.group_id || "-")}</strong>
                <span>${escapeHtml(item.managed_status || "observed")}</span>
              </header>
              <p>${escapeHtml(item.region || "-")} / ${escapeHtml(item.category || "-")}</p>
              <p>${escapeHtml(item.description || "No description yet.")}</p>
              <p>${numberFormat.format(item.world_count || 0)} worlds tracked</p>
              <p>${escapeHtml((item.external_links || []).join(" · ") || "No external links")}</p>
              ${renderGroupActionButtons(item)}
            </article>
          `,
        )
        .join("")
    : `<div class="detail-empty">No group entities saved yet.</div>`;

  $("communities-growth-summary").innerHTML = `
    <article class="history-stat">
      <span>Status</span>
      <strong>${escapeHtml(payload?.growth?.status || "pending")}</strong>
    </article>
    <article class="history-stat">
      <span>Tracked Views</span>
      <strong>${escapeHtml(numberFormat.format(summary.tracked_view_count || 0))}</strong>
    </article>
    <article class="history-stat">
      <span>Saved Views</span>
      <strong>${escapeHtml(numberFormat.format(summary.saved_view_count || 0))}</strong>
    </article>
  `;

  $("communities-worlds-summary").innerHTML = `
    <article class="history-stat">
      <span>Status</span>
      <strong>${escapeHtml(payload?.worlds?.status || "pending")}</strong>
    </article>
    <article class="history-stat">
      <span>Linked Worlds</span>
      <strong>${escapeHtml(numberFormat.format(payload?.worlds?.count || 0))}</strong>
    </article>
    <article class="history-stat">
      <span>Linked Groups</span>
      <strong>${escapeHtml(numberFormat.format(payload?.worlds?.linked_group_count || 0))}</strong>
    </article>
  `;

  const worldItems = payload?.worlds?.items || [];
  $("communities-worlds-list").innerHTML = worldItems.length
    ? worldItems
        .map(
          (item) => `
            <article class="run-item">
              <header>
                <strong>${escapeHtml(item.world_name || item.world_id || "-")}</strong>
                <span>${escapeHtml(item.membership_role || "member")}</span>
              </header>
              <p>${escapeHtml(item.group_name || item.group_id || "-")} / ${escapeHtml(item.author_name || item.author_id || "-")}</p>
              <p>visits ${escapeHtml(formatMetric(item.visits))} / favorites ${escapeHtml(formatMetric(item.favorites))} / 7d ${escapeHtml(formatDelta(item.visits_delta_7d))}</p>
              ${renderGroupWorldActionButtons(item)}
            </article>
          `,
        )
        .join("")
    : `<div class="detail-empty">No group-world memberships yet.</div>`;

  const groupedWorlds = payload?.worlds?.groups || [];
  $("communities-world-groups").innerHTML = groupedWorlds.length
    ? groupedWorlds
        .map(
          (item) => `
            <article class="run-item">
              <header>
                <strong>${escapeHtml(item.group_name || item.group_id || "-")}</strong>
                <span>${escapeHtml(numberFormat.format(item.world_count || 0))} worlds</span>
              </header>
              <p>${(item.top_worlds || [])
                .map((world) => `${world.world_name || world.world_id} (${formatMetric(world.visits)})`)
                .join(" / ")}</p>
            </article>
          `,
        )
        .join("")
    : `<div class="detail-empty">No group slices yet.</div>`;

  const managedItems = payload?.publishing?.managed_groups || [];
  $("communities-managed-group-list").innerHTML = managedItems.length
    ? managedItems
        .map(
          (item) => `
            <article class="run-item">
              <header>
                <strong>${escapeHtml(item.name || item.group_id || "-")}</strong>
                <span>${item.posting_enabled ? "publishing on" : "publishing off"}</span>
              </header>
              <p>${escapeHtml(item.workspace_key || "No workspace key")}</p>
              <p>${escapeHtml(item.notes || "No notes yet.")}</p>
              <p>${escapeHtml(item.updated_at || "-")}</p>
              ${renderManagedGroupActionButtons(item)}
            </article>
          `,
        )
        .join("")
    : `<div class="detail-empty">No managed groups yet.</div>`;

  const publishingItems = payload?.publishing?.items || [];
  $("communities-publishing-list").innerHTML = publishingItems.length
    ? publishingItems
        .map(
          (item) => `
            <article class="run-item">
              <header>
                <strong>${escapeHtml(item.group_name || item.group_id || "-")}</strong>
                <span>${escapeHtml(item.status || "-")}</span>
              </header>
              <p>${escapeHtml(item.content_type || "post")}</p>
              <p>${escapeHtml(item.scheduled_for || "-")}</p>
              <p>${escapeHtml(JSON.stringify(item.payload || {}))}</p>
              ${renderScheduledPostActionButtons(item)}
            </article>
          `,
        )
        .join("")
    : `<div class="detail-empty">No scheduled posts yet.</div>`;

  $("communities-publishing-summary").innerHTML = `
    <article class="history-stat">
      <span>Status</span>
      <strong>${escapeHtml(payload?.publishing?.status || "pending")}</strong>
    </article>
    <article class="history-stat">
      <span>Pending</span>
      <strong>${escapeHtml(numberFormat.format(summary.scheduled_post_pending_count || 0))}</strong>
    </article>
    <article class="history-stat">
      <span>Managed Groups</span>
      <strong>${escapeHtml(numberFormat.format(summary.managed_group_count || 0))}</strong>
    </article>
  `;

  renderGroupMonitor(payload);
  renderDashboardHealth(payload, state.collectionInsights);
}

function renderGroupMonitor(payload) {
  const summary = payload?.summary || {};
  const directoryItems = payload?.worlds?.groups || payload?.directory?.items || [];
  const managedItems = payload?.publishing?.managed_groups || [];
  $("monitor-groups-summary").innerHTML = [
    { label: "Groups", value: numberFormat.format(summary.group_count || 0) },
    { label: "Managed", value: numberFormat.format(summary.managed_group_count || 0) },
    { label: "Queued Posts", value: numberFormat.format(summary.scheduled_post_count || 0) },
    { label: "Pending Posts", value: numberFormat.format(summary.scheduled_post_pending_count || 0) },
  ]
    .map(
      (item) => `
        <article class="history-stat">
          <span>${escapeHtml(item.label)}</span>
          <strong>${escapeHtml(item.value)}</strong>
        </article>
      `,
    )
    .join("");

  $("monitor-groups-directory").innerHTML = directoryItems.length
    ? directoryItems
        .slice(0, 4)
        .map(
          (item) => `
            <article class="run-item">
              <header>
                <strong>${escapeHtml(item.group_name || item.name || item.group_id || "-")}</strong>
                <span>${escapeHtml(item.world_count == null ? (item.managed_status || "observed") : `${item.world_count} worlds`)}</span>
              </header>
              <p>${escapeHtml(item.region || item.group_region || "-")} / ${escapeHtml(item.category || item.group_category || "-")}</p>
              <p>${
                Array.isArray(item.top_worlds) && item.top_worlds.length
                  ? escapeHtml(item.top_worlds.map((world) => world.world_name || world.world_id).join(" / "))
                  : escapeHtml(item.description || "No description yet.")
              }</p>
            </article>
          `,
        )
        .join("")
    : `<div class="detail-empty">No groups defined yet.</div>`;

  $("monitor-groups-publishing").innerHTML = [
    { label: "Publishing", value: payload?.publishing?.status || "pending" },
    { label: "Saved Views", value: numberFormat.format(summary.saved_view_count || 0) },
    { label: "Tracked Views", value: numberFormat.format(summary.tracked_view_count || 0) },
    { label: "Tracked Creators", value: numberFormat.format(summary.tracked_creator_count || 0) },
  ]
    .map(
      (item) => `
        <article class="history-stat">
          <span>${escapeHtml(item.label)}</span>
          <strong>${escapeHtml(item.value)}</strong>
        </article>
      `,
    )
    .join("");

  $("monitor-groups-managed").innerHTML = managedItems.length
    ? managedItems
        .slice(0, 4)
        .map(
          (item) => `
            <article class="run-item">
              <header>
                <strong>${escapeHtml(item.name || item.group_id || "-")}</strong>
                <span>${item.posting_enabled ? "publishing on" : "publishing off"}</span>
              </header>
              <p>${escapeHtml(item.workspace_key || "No workspace key")}</p>
              <p>${escapeHtml(item.notes || "No notes yet.")}</p>
            </article>
          `,
        )
        .join("")
    : `<div class="detail-empty">No managed groups configured yet.</div>`;
}

function renderDashboardHealth(communitiesPayload, insightsPayload) {
  const summary = insightsPayload?.summary || {};
  const performance = insightsPayload?.performance?.summary || {};
  $("dash-health-summary").innerHTML = [
    { label: "Scope", value: insightsPayload?.label || state.source || "db:all" },
    { label: "Worlds", value: numberFormat.format(summary.world_count || 0) },
    { label: "Recent Updates", value: numberFormat.format(performance.tracked_recent_updates || 0) },
    { label: "Silent Updates", value: numberFormat.format(performance.silent_updates || 0) },
    { label: "Avg Visits / Day", value: performance.avg_visits_per_day == null ? "-" : formatMetric(performance.avg_visits_per_day) },
  ]
    .map(
      (item) => `
        <article class="history-stat">
          <span>${escapeHtml(item.label)}</span>
          <strong>${escapeHtml(item.value)}</strong>
        </article>
      `,
    )
    .join("");

  const communitySummary = communitiesPayload?.summary || {};
  $("dash-community-health").innerHTML = [
    { label: "Groups", value: numberFormat.format(communitySummary.group_count || 0) },
    { label: "Managed", value: numberFormat.format(communitySummary.managed_group_count || 0) },
    { label: "Queued", value: numberFormat.format(communitySummary.scheduled_post_count || 0) },
    { label: "Pending", value: numberFormat.format(communitySummary.scheduled_post_pending_count || 0) },
  ]
    .map(
      (item) => `
        <article class="history-stat">
          <span>${escapeHtml(item.label)}</span>
          <strong>${escapeHtml(item.value)}</strong>
        </article>
      `,
    )
    .join("");

  const notes = [
    communitiesPayload?.growth?.status || "growth status pending",
    communitiesPayload?.worlds?.status || "group worlds status pending",
    communitiesPayload?.publishing?.status || "publishing status pending",
  ];
  $("dash-community-notes").innerHTML = notes
    .map(
      (item) => `
        <article class="run-item">
          <p>${escapeHtml(item)}</p>
        </article>
      `,
    )
    .join("");
}

async function submitGroupForm(event) {
  event.preventDefault();
  const button = $("group-form")?.querySelector("button[type='submit']");
  const originalLabel = button?.textContent || "Save Group";
  const payload = {
    group_id: $("group-id").value.trim(),
    name: $("group-name").value.trim(),
    region: $("group-region").value.trim(),
    category: $("group-category").value.trim(),
    managed_status: $("group-managed-status").value.trim(),
    description: $("group-description").value.trim(),
    external_links: splitCsv($("group-external-links").value),
  };
  if (button) {
    button.disabled = true;
    button.textContent = "Saving...";
  }
  try {
    await fetchJson("/api/v1/groups", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    resetGroupForm();
    setHelperStatus("group-form-status", `Saved ${payload.group_id}.`, "ok");
    await loadCommunitiesWorkspace(true);
  } catch (error) {
    setHelperStatus("group-form-status", error.message, "warn");
  } finally {
    if (button) {
      button.disabled = false;
      button.textContent = originalLabel;
    }
  }
}

async function submitManagedGroupForm(event) {
  event.preventDefault();
  const button = $("managed-group-form")?.querySelector("button[type='submit']");
  const originalLabel = button?.textContent || "Save Managed Group";
  const groupId = $("managed-group-id").value.trim();
  const payload = {
    group_id: groupId,
    workspace_key: $("managed-workspace-key").value.trim(),
    posting_enabled: $("managed-posting-enabled").checked,
    notes: $("managed-notes").value.trim(),
  };
  if (button) {
    button.disabled = true;
    button.textContent = "Saving...";
  }
  try {
    await fetchJson("/api/v1/managed-groups", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    resetManagedGroupForm();
    setHelperStatus("managed-group-status", `Saved managed group ${groupId}.`, "ok");
    await loadCommunitiesWorkspace(true);
  } catch (error) {
    setHelperStatus("managed-group-status", error.message, "warn");
  } finally {
    if (button) {
      button.disabled = false;
      button.textContent = originalLabel;
    }
  }
}

async function submitScheduledPostForm(event) {
  event.preventDefault();
  const button = $("scheduled-post-form")?.querySelector("button[type='submit']");
  const originalLabel = button?.textContent || "Queue Post";
  const postId = $("scheduled-post-id").value.trim();
  const payloadText = $("scheduled-post-payload").value.trim();
  let payloadObject = {};
  if (payloadText) {
    try {
      payloadObject = JSON.parse(payloadText);
    } catch {
      setHelperStatus("scheduled-post-status-text", "Payload must be valid JSON.", "warn");
      return;
    }
    if (typeof payloadObject !== "object" || Array.isArray(payloadObject) || payloadObject == null) {
      setHelperStatus("scheduled-post-status-text", "Payload must be a JSON object.", "warn");
      return;
    }
  }
  const payload = {
    group_id: $("scheduled-post-group-id").value.trim(),
    content_type: $("scheduled-post-content-type").value.trim(),
    status: $("scheduled-post-status").value.trim(),
    scheduled_for: $("scheduled-post-scheduled-for").value.trim(),
    payload: payloadObject,
  };
  if (button) {
    button.disabled = true;
    button.textContent = postId ? "Updating..." : "Queueing...";
  }
  try {
    const endpoint = postId ? `/api/v1/scheduled-posts/${encodeURIComponent(postId)}` : "/api/v1/scheduled-posts";
    const method = postId ? "PUT" : "POST";
    await fetchJson(endpoint, {
      method,
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    resetScheduledPostForm();
    setHelperStatus("scheduled-post-status-text", postId ? `Updated post #${postId}.` : "Queued post.", "ok");
    await loadCommunitiesWorkspace(true);
  } catch (error) {
    setHelperStatus("scheduled-post-status-text", error.message, "warn");
  } finally {
    if (button) {
      button.disabled = false;
      button.textContent = originalLabel;
    }
  }
}

async function submitGroupWorldForm(event) {
  event.preventDefault();
  const button = $("group-world-form")?.querySelector("button[type='submit']");
  const originalLabel = button?.textContent || "Link World";
  const groupId = $("group-world-group-id").value.trim();
  const worldId = $("group-world-world-id").value.trim();
  const payload = {
    group_id: groupId,
    world_id: worldId,
    membership_role: $("group-world-role").value.trim(),
    source_key: $("group-world-source-key").value.trim(),
  };
  if (button) {
    button.disabled = true;
    button.textContent = "Saving...";
  }
  try {
    await fetchJson("/api/v1/group-world-memberships", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    resetGroupWorldForm();
    setHelperStatus("group-world-status", `Linked ${groupId} / ${worldId}.`, "ok");
    await loadCommunitiesWorkspace(true);
  } catch (error) {
    setHelperStatus("group-world-status", error.message, "warn");
  } finally {
    if (button) {
      button.disabled = false;
      button.textContent = originalLabel;
    }
  }
}

async function handleCommunitiesAction(event) {
  const button = event.target.closest("[data-community-action]");
  if (!button) {
    return;
  }
  const action = button.dataset.communityAction;
  const payload = state.communitiesData || {};
  const directoryItems = payload?.directory?.items || [];
  const managedItems = payload?.publishing?.managed_groups || [];
  const scheduledItems = payload?.publishing?.items || [];
  const groupWorldItems = payload?.worlds?.items || [];
  if (action === "edit-group") {
    const item = directoryItems.find((entry) => entry.group_id === button.dataset.groupId);
    fillGroupForm(item);
    return;
  }
  if (action === "delete-group") {
    const groupId = button.dataset.groupId;
    if (!window.confirm(`Delete group ${groupId}? This also removes managed state and scheduled posts.`)) {
      return;
    }
    try {
      await fetchJson(`/api/v1/groups/${encodeURIComponent(groupId)}`, { method: "DELETE" });
      setHelperStatus("group-form-status", `Deleted ${groupId}.`, "ok");
      await loadCommunitiesWorkspace(true);
    } catch (error) {
      setHelperStatus("group-form-status", error.message, "warn");
    }
    return;
  }
  if (action === "edit-managed-group") {
    const item = managedItems.find((entry) => entry.group_id === button.dataset.groupId);
    fillManagedGroupForm(item);
    return;
  }
  if (action === "delete-managed-group") {
    const groupId = button.dataset.groupId;
    if (!window.confirm(`Delete managed group ${groupId}?`)) {
      return;
    }
    try {
      await fetchJson(`/api/v1/managed-groups/${encodeURIComponent(groupId)}`, { method: "DELETE" });
      setHelperStatus("managed-group-status", `Deleted managed group ${groupId}.`, "ok");
      await loadCommunitiesWorkspace(true);
    } catch (error) {
      setHelperStatus("managed-group-status", error.message, "warn");
    }
    return;
  }
  if (action === "edit-scheduled-post") {
    const postId = Number(button.dataset.postId || 0);
    const item = scheduledItems.find((entry) => Number(entry.id) === postId);
    fillScheduledPostForm(item);
    return;
  }
  if (action === "delete-scheduled-post") {
    const postId = button.dataset.postId;
    if (!window.confirm(`Delete scheduled post #${postId}?`)) {
      return;
    }
    try {
      await fetchJson(`/api/v1/scheduled-posts/${encodeURIComponent(postId)}`, { method: "DELETE" });
      setHelperStatus("scheduled-post-status-text", `Deleted scheduled post #${postId}.`, "ok");
      await loadCommunitiesWorkspace(true);
    } catch (error) {
      setHelperStatus("scheduled-post-status-text", error.message, "warn");
    }
  }
  if (action === "edit-group-world") {
    const item = groupWorldItems.find(
      (entry) => entry.group_id === button.dataset.groupId && entry.world_id === button.dataset.worldId,
    );
    fillGroupWorldForm(item);
    return;
  }
  if (action === "delete-group-world") {
    const groupId = button.dataset.groupId;
    const worldId = button.dataset.worldId;
    if (!window.confirm(`Delete membership ${groupId} / ${worldId}?`)) {
      return;
    }
    try {
      await fetchJson(
        `/api/v1/group-world-memberships/${encodeURIComponent(groupId)}/${encodeURIComponent(worldId)}`,
        { method: "DELETE" },
      );
      setHelperStatus("group-world-status", `Deleted ${groupId} / ${worldId}.`, "ok");
      await loadCommunitiesWorkspace(true);
    } catch (error) {
      setHelperStatus("group-world-status", error.message, "warn");
    }
  }
}

async function loadCommunitiesWorkspace(force = false) {
  markPanelLoading("communitiesWorkspace", {
    page: state.page,
    section: currentSectionForPage(),
  });
  if (!force && state.communitiesData) {
    const rendered = withPanelRender(
      "communitiesWorkspace",
      () => renderCommunitiesWorkspace(state.communitiesData),
      (error) => renderListPanelState("communities-directory-list", "Communities Workspace", error.message),
      { page: state.page, section: currentSectionForPage() },
    );
    if (rendered) {
      markPanelSuccess("communitiesWorkspace", {
        page: state.page,
        section: currentSectionForPage(),
        status: "ready",
      });
    }
    return;
  }
  try {
    const { data } = await fetchJson("/api/v1/communities/summary", undefined, {
      panelKey: "communitiesWorkspace",
      page: state.page,
      section: currentSectionForPage(),
    });
    state.communitiesData = data;
    const rendered = withPanelRender(
      "communitiesWorkspace",
      () => renderCommunitiesWorkspace(data),
      (error) => renderListPanelState("communities-directory-list", "Communities Workspace", error.message),
      { page: state.page, section: currentSectionForPage() },
    );
    if (rendered) {
      const summary = data?.summary || {};
      const itemCount = toNumber(summary.group_count) + toNumber(summary.managed_group_count);
      markPanelSuccess("communitiesWorkspace", {
        page: state.page,
        section: currentSectionForPage(),
        status: itemCount > 0 ? "ready" : "empty",
      });
    }
  } catch (error) {
    markPanelError("communitiesWorkspace", error, {
      page: state.page,
      section: currentSectionForPage(),
    });
    renderListPanelState("communities-directory-list", "Communities Workspace", error.message);
  }
}

async function refreshAuthStatusCheck() {
  const payload = getAuthPayload();
  if (!payload.cookie && !payload.username && !payload.password) {
    state.authStatus = {
      status: null,
      mode: null,
      label: "no auth saved",
      detail: "No auth configured. Use a VRChat Cookie for stable counters and session checks.",
    };
    renderAuthStatus();
    return;
  }
  state.authStatus = {
    status: null,
    mode: null,
    label: "checking auth",
    detail: "Verifying current auth state...",
  };
  renderAuthStatus();
  try {
    const { data } = await fetchJson("/api/v1/auth/status", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    state.authStatus = {
      status: data.status || null,
      mode: data.mode || null,
      label: `${data.mode || "auth"}: ${data.status || "unknown"}`,
      detail: data.message || "Auth status checked.",
    };
  } catch (error) {
    state.authStatus = {
      status: null,
      mode: null,
      label: "auth check failed",
      detail: error.message,
    };
  }
  renderAuthStatus();
}

async function persistServerAuth() {
  const payload = getAuthPayload();
  return fetchJson("/api/v1/auth/persist", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
}

async function ensureServerAuthPersisted(reason = "page open") {
  const payload = getAuthPayload();
  if (!payload.cookie && !(payload.username && payload.password)) {
    return false;
  }
  try {
    const { data } = await persistServerAuth();
    const authStatus = data.auth_status || {};
    state.authStatus = {
      status: authStatus.status || "saved",
      mode: data.mode || authStatus.mode || null,
      label: `server auth ${data.status || "saved"}`,
      detail: authStatus.message || `Auth persisted for scheduler (${reason}).`,
    };
    renderAuthStatus();
    return true;
  } catch (error) {
    state.authStatus = {
      status: "error",
      mode: null,
      label: "server auth save failed",
      detail: error.message,
    };
    renderAuthStatus();
    return false;
  }
}

async function clearServerAuth() {
  return fetchJson("/api/v1/auth/persist", {
    method: "DELETE",
  });
}

async function loginWithPassword() {
  const username = $("auth-username").value.trim();
  const password = $("auth-password").value;
  if (!username || !password) {
    window.alert("Username and password are required.");
    return;
  }
  state.authStatus = {
    status: "pending",
    mode: "login",
    label: "logging in",
    detail: "Requesting VRChat session...",
  };
  renderAuthStatus();
  const { data } = await fetchJson("/api/v1/auth/login", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ username, password }),
  });
  if (data.status === "requires_2fa") {
    state.authPending = {
      auth_cookie: data.auth_cookie,
      methods: data.methods || ["totp"],
    };
    state.authStatus = {
      status: "requires_2fa",
      mode: "cookie",
      label: "2FA required",
      detail: "Enter the verification code from VRChat.",
    };
    renderAuthStatus();
    return;
  }
  state.auth.cookie = data.cookie || "";
  state.auth.username = username;
  state.auth.password = "";
  state.authPending = null;
  saveAuthState();
  await persistServerAuth();
  state.authStatus = {
    status: "ok",
    mode: "cookie",
    label: "session saved",
    detail: data.message || "VRChat login successful.",
  };
  renderAuthStatus();
  await refreshAuthStatusCheck();
}

async function verifyTwoFactor() {
  if (!state.authPending) {
    return;
  }
  const method = $("auth-2fa-method").value;
  const code = $("auth-2fa-code").value.trim();
  const { data } = await fetchJson("/api/v1/auth/verify-2fa", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      method,
      code,
      auth_cookie: state.authPending.auth_cookie,
    }),
  });
  state.auth.cookie = data.cookie || "";
  state.auth.password = "";
  state.authPending = null;
  $("auth-2fa-code").value = "";
  saveAuthState();
  await persistServerAuth();
  state.authStatus = {
    status: "ok",
    mode: "cookie",
    label: "session saved",
    detail: data.message || "2FA verified.",
  };
  renderAuthStatus();
  await refreshAuthStatusCheck();
  await maybeRunAutoSync("login");
}

async function fetchJson(url, options, debugMeta = {}) {
  const method = String(options?.method || "GET").toUpperCase();
  const startedAt = performance.now();
  const startedIso = new Date().toISOString();
  let response = null;
  let data = null;
  let contentType = "";
  let requestError = null;
  if (debugMeta.panelKey) {
    setPanelStatus(debugMeta.panelKey, {
      page: debugMeta.page || state.page,
      section: debugMeta.section ?? currentSectionForPage(debugMeta.page || state.page),
      lastRequestUrl: url,
    });
  }
  try {
    response = await fetch(url, options);
    contentType = response.headers.get("content-type") || "";
    const raw = await response.text();
    if (raw.trim()) {
      const looksJson = contentType.includes("application/json") || /^[\[{]/.test(raw.trim());
      if (!looksJson) {
        throw new Error(`Expected JSON but received ${contentType || "non-JSON response"} from ${url}`);
      }
      data = JSON.parse(raw);
    } else {
      data = {};
    }
    if (!response.ok && response.status !== 207) {
      throw new Error(data.error || `Request failed: ${response.status}`);
    }
    return { response, data };
  } catch (error) {
    requestError = error;
    throw error;
  } finally {
    if (requestError) {
      state.debug.lastRequestError = requestError.message || String(requestError);
      if (debugMeta.panelKey) {
        markPanelError(debugMeta.panelKey, requestError, {
          page: debugMeta.page || state.page,
          section: debugMeta.section ?? currentSectionForPage(debugMeta.page || state.page),
          lastRequestUrl: url,
        });
      }
    }
    recordDebugRequest({
      time: startedIso,
      method,
      url,
      panelKey: debugMeta.panelKey || "",
      status: response?.status || null,
      ok: Boolean(response && (response.ok || response.status === 207) && !requestError),
      durationMs: Math.round((performance.now() - startedAt) * 10) / 10,
      contentType,
      error: requestError?.message || "",
    });
  }
}

function recordDebugRequest(entry) {
  const requestLog = state.debug.requestLog || [];
  state.debug.sequence += 1;
  requestLog.unshift({
    id: state.debug.sequence,
    ...entry,
  });
  if (requestLog.length > DEBUG_REQUEST_LIMIT) {
    requestLog.length = DEBUG_REQUEST_LIMIT;
  }
  state.debug.requestLog = requestLog;
  if (state.page === "debug") {
    renderDebugPage();
  }
}

function renderDebugPage() {
  state.debug.rendering = true;
  try {
    const items = state.debug.requestLog || [];
    const panels = Object.values(state.debug.panels || {});
    const count = items.length;
    const avgDuration = count
      ? Math.round((items.reduce((sum, item) => sum + toNumber(item.durationMs), 0) / count) * 10) / 10
      : null;
    const slowest = count
      ? Math.max(...items.map((item) => toNumber(item.durationMs)))
      : null;
    const requestErrorCount = items.filter((item) => !item.ok).length;
    const panelErrorCount = panels.filter((item) => item.status === "error").length;
    const latestError = items.find((item) => item.error)?.error || state.debug.lastRequestError || "-";
    const latestRenderError = state.debug.lastRenderError || "-";
    const snapshot = state.debug.visibilitySnapshot || captureVisibilitySnapshot();
    const lifecycleItems = state.debug.lifecycle || [];
    $("debug-caption").textContent = `${count} requests / ${panels.length} panels`;
    $("debug-summary").innerHTML = [
      { label: "Current Page", value: `${state.page}${currentSectionForPage() ? ` / ${currentSectionForPage()}` : ""}` },
      { label: "Requests", value: numberFormat.format(count) },
      { label: "Avg", value: avgDuration == null ? "-" : `${avgDuration} ms` },
      { label: "Slowest", value: slowest == null ? "-" : `${slowest} ms` },
      { label: "Request Errors", value: numberFormat.format(requestErrorCount) },
      { label: "Panel Errors", value: numberFormat.format(panelErrorCount) },
      { label: "Last Request Error", value: latestError },
      { label: "Last Render Error", value: latestRenderError },
    ]
      .map(
        (item) => `
          <article class="history-stat">
            <span>${escapeHtml(item.label)}</span>
            <strong>${escapeHtml(item.value)}</strong>
          </article>
        `,
      )
      .join("");
    $("debug-panel-status-body").innerHTML = panels.length
      ? panels
          .sort((left, right) => left.label.localeCompare(right.label, "en"))
          .map(
            (panel) => `
              <tr title="${escapeHtml(panel.errorMessage || "")}">
                <td>${escapeHtml(panel.label || panel.key)}</td>
                <td>${escapeHtml(panel.page || "-")}</td>
                <td>${escapeHtml(panel.section || "-")}</td>
                <td>${escapeHtml(panel.status || "idle")}</td>
                <td>${escapeHtml(panel.lastRequestUrl || "-")}</td>
                <td>${escapeHtml(panel.errorMessage || "-")}</td>
                <td>${escapeHtml(panel.lastSuccessAt ? formatDateTime(panel.lastSuccessAt) : "-")}</td>
              </tr>
            `,
          )
          .join("")
      : `<tr><td colspan="7">No panel state recorded yet.</td></tr>`;
    $("debug-visibility-body").innerHTML = `
      <article class="debug-visibility-card">
        <strong>Expected Visible (${numberFormat.format(snapshot.expectedVisible.length)})</strong>
        <div class="debug-list">
          ${snapshot.expectedVisible.length
            ? snapshot.expectedVisible.map((item) => `<div>${escapeHtml(item)}</div>`).join("")
            : "<div>-</div>"}
        </div>
      </article>
      <article class="debug-visibility-card">
        <strong>Actual Visible (${numberFormat.format(snapshot.actualVisible.length)})</strong>
        <div class="debug-list">
          ${snapshot.actualVisible.length
            ? snapshot.actualVisible.map((item) => `<div>${escapeHtml(item)}</div>`).join("")
            : "<div>-</div>"}
        </div>
      </article>
      <article class="debug-visibility-card">
        <strong>Leakage (${numberFormat.format(snapshot.leakedVisible.length)})</strong>
        <div class="debug-list">
          ${snapshot.leakedVisible.length
            ? snapshot.leakedVisible.map((item) => `<div>${escapeHtml(item)}</div>`).join("")
            : "<div>No page leakage detected.</div>"}
        </div>
      </article>
      <article class="debug-visibility-card">
        <strong>Lifecycle (${numberFormat.format(lifecycleItems.length)})</strong>
        <div class="debug-list">
          ${lifecycleItems.length
            ? lifecycleItems.slice(0, 12).map((item) => `<div>${escapeHtml(formatDateTime(item.time))} | ${escapeHtml(item.event)} | ${escapeHtml(item.page)} ${item.section ? `/${escapeHtml(item.section)}` : ""}${item.detail ? ` | ${escapeHtml(item.detail)}` : ""}</div>`).join("")
            : "<div>No lifecycle events yet.</div>"}
        </div>
      </article>
    `;
    $("debug-request-body").innerHTML = items.length
      ? items
          .map(
            (item) => `
              <tr title="${escapeHtml(item.error || "")}">
                <td>${escapeHtml(formatDateTime(item.time))}</td>
                <td>${escapeHtml(item.method || "GET")}</td>
                <td>
                  <strong>${escapeHtml(item.url || "-")}</strong>
                  ${item.panelKey ? `<div class="helper-copy">panel: ${escapeHtml(item.panelKey)}</div>` : ""}
                  ${item.error ? `<div class="helper-copy">${escapeHtml(item.error)}</div>` : ""}
                </td>
                <td>${escapeHtml(item.status == null ? "ERR" : String(item.status))}</td>
                <td>${escapeHtml(`${item.durationMs ?? 0} ms`)}</td>
                <td>${escapeHtml(item.contentType || "-")}</td>
              </tr>
            `,
          )
          .join("")
      : `<tr><td colspan="6">No request timings yet.</td></tr>`;
  } finally {
    state.debug.rendering = false;
  }
}

function setHealthIndicator(label, tone = "") {
  const indicator = $("health-indicator");
  if (!indicator) {
    return;
  }
  indicator.textContent = label;
  indicator.dataset.tone = tone;
}

function isSameLocalDay(value) {
  const parsed = parseDate(value);
  if (!parsed) {
    return false;
  }
  const now = new Date();
  return (
    parsed.getFullYear() === now.getFullYear() &&
    parsed.getMonth() === now.getMonth() &&
    parsed.getDate() === now.getDate()
  );
}

async function runNamedJob(jobKey) {
  await ensureServerAuthPersisted("manual job run");
  const bypassCheckbox = $(`bypass-rate-limit-${jobKey}`);
  const bypassRateLimit = bypassCheckbox ? bypassCheckbox.checked : false;
  const payload = getAuthPayload();
  payload.bypass_rate_limit = bypassRateLimit;
  const { data } = await fetchJson(`/api/v1/jobs/${encodeURIComponent(jobKey)}/run`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  renderSyncStatus((data.warnings || []).join(" "), data.warnings?.length ? "warn" : "ok");
  state.activeTopic = null;
  await refreshCurrentScopeData({
    preferredSource: data.source,
    refreshSources: true,
    refreshAncillary: true,
  });
  return data;
}

async function runAutoSyncJobNow(jobKey) {
  await ensureServerAuthPersisted("auto sync Run Now");
  const { data } = await fetchJson(`/api/v1/auto-sync/${encodeURIComponent(jobKey)}/run-now`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(getAuthPayload()),
  });
  renderSyncStatus((data.warnings || []).join(" "), data.warnings?.length ? "warn" : "ok");
  state.activeTopic = null;
  await refreshCurrentScopeData({
    preferredSource: data.source,
    refreshSources: true,
    refreshAncillary: true,
  });
  return data;
}

async function recordAutoSyncRun(jobKey) {
  try {
    await fetchJson(`/api/v1/auto-sync/${encodeURIComponent(jobKey)}/record-run`, {
      method: "POST",
    });
  } catch (error) {
    console.warn(`Failed to record auto sync run for ${jobKey}: ${error.message}`);
  }
}

async function maybeRunAutoSync(reason = "open") {
  if (!state.autoSync.enabled || state.autoSync.running) {
    return;
  }
  if (state.authStatus?.status !== "ok") {
    return;
  }
  if (!state.jobs.length) {
    return;
  }
  const job = state.jobs.find((item) => item.job_key === state.autoSync.jobKey);
  if (!job || !job.ready) {
    renderAutoSyncControls();
    return;
  }
  const latestRun = job.latest_run;
  if (latestRun?.status === "running") {
    return;
  }
  const hasTodayRecord = Boolean(
    latestRun &&
      latestRun.status === "completed" &&
      (latestRun.world_count || 0) > 0 &&
      isSameLocalDay(latestRun.started_at),
  );
  if (hasTodayRecord) {
    return;
  }
  state.autoSync.running = true;
  renderSyncStatus(`Auto sync: running ${job.label} because no record exists for today (${reason}).`, "ok");
  try {
    await runNamedJob(job.job_key);
  } catch (error) {
    renderSyncStatus(`Auto sync failed for ${job.label}: ${error.message}`, "warn");
  } finally {
    state.autoSync.running = false;
    renderAutoSyncControls();
  }
}

function findComparisonPoint(points, windowKey) {
  if (!points.length) {
    return null;
  }
  if (windowKey === "all") {
    return points[0];
  }
  const latest = points[points.length - 1];
  const latestTime = parseDate(latest.iso_time);
  if (!latestTime) {
    return points.length > 1 ? points[0] : null;
  }
  const targetTime = latestTime.getTime() - compareWindowMs(windowKey);
  let candidate = null;
  for (const point of points) {
    const stamp = parseDate(point.iso_time);
    if (!stamp) {
      continue;
    }
    if (stamp.getTime() <= targetTime) {
      candidate = point;
    }
  }
  return candidate;
}

function calculateWorldMetrics(world) {
  const metrics = { ...(world.metrics || {}) };
  const visits = toNumber(world.visits);
  const favorites = toNumber(world.favorites);
  const publication = parseDate(world.publication_date);
  const labs = parseDate(world.labs_publication_date);
  const updated = parseDate(world.updated_at);
  const fetched = parseDate(world.fetched_at) || new Date();

  if (metrics.favorite_rate == null && visits > 0) {
    metrics.favorite_rate = Math.round((favorites / visits) * 10000) / 100;
  }
  if (metrics.visits_per_day == null && publication) {
    const publishedDays = Math.max(Math.floor((fetched - publication) / 86400000), 1);
    metrics.visits_per_day = Math.round((visits / publishedDays) * 100) / 100;
  }
  if (metrics.days_since_update == null && updated) {
    metrics.days_since_update = Math.max(Math.floor((fetched - updated) / 86400000), 0);
  }
  if (metrics.labs_to_publication_days == null && publication && labs) {
    metrics.labs_to_publication_days = Math.floor((publication - labs) / 86400000);
  }
  metrics.days_since_publication = publication
    ? Math.max(Math.floor((fetched - publication) / 86400000), 0)
    : null;

  return metrics;
}

function normalisePortalLinks(portalLinks) {
  return (Array.isArray(portalLinks) ? portalLinks : [])
    .map((item) => String(item || "").trim())
    .filter(Boolean);
}

function portalLinkHref(value) {
  return value.startsWith("wrld_") ? `https://vrchat.com/home/world/${value}` : value;
}

function renderPortalLinkMarkup(portalLinks, { limit = null, compact = false } = {}) {
  const items = normalisePortalLinks(portalLinks);
  if (!items.length) {
    return "";
  }
  const visible = Number.isInteger(limit) && limit > 0 ? items.slice(0, limit) : items;
  const extraCount = Number.isInteger(limit) && limit > 0 ? Math.max(items.length - visible.length, 0) : 0;
  const chipClass = compact ? "diff-chip diff-chip-compact" : "diff-chip";
  return [
    ...visible.map(
      (value) => `<a href="${escapeHtml(portalLinkHref(value))}" target="_blank" rel="noreferrer" class="${chipClass}">${escapeHtml(value)}</a>`,
    ),
    extraCount ? `<span class="${chipClass}">+${extraCount}</span>` : "",
  ]
    .filter(Boolean)
    .join("");
}

function renderSummary(summary) {
  const cards = [
    { label: "Worlds", value: summary.world_count ?? summary.worlds ?? 0 },
    { label: "Visits", value: numberFormat.format(summary.total_visits ?? 0) },
    { label: "Favorites", value: numberFormat.format(summary.total_favorites ?? 0) },
    { label: "Creators", value: numberFormat.format(summary.tracked_creators ?? 0) },
  ];
  $("summary-grid").innerHTML = cards
    .map(
      (item) => `
        <article class="summary-card">
          <span>${escapeHtml(item.label)}</span>
          <strong>${escapeHtml(item.value)}</strong>
        </article>
      `,
    )
    .join("");
}

function renderTags(tags) {
  $("tag-select").innerHTML = ["all", ...tags]
    .map((tag) => `<option value="${escapeHtml(tag)}">${escapeHtml(tag)}</option>`)
    .join("");
}

function renderTable(worlds) {
  $("table-caption").textContent = `${worlds.length} worlds`;
  $("world-table-body").innerHTML = worlds
    .map(
      (world) => {
        const signals = getWorldSignals(world);
        const badges = renderWorldSignalBadges(signals);
        const portalMarkup = renderPortalLinkMarkup(world.portal_links, { limit: 2, compact: true });
        return `
        <tr data-world-id="${escapeHtml(world.id || "")}">
          <td>
            <div class="world-name-cell">
              <strong>${escapeHtml(world.name || "(unnamed)")}</strong>
              ${badges ? `<div class="world-badge-row">${badges}</div>` : ""}
            </div>
          </td>
          <td>${escapeHtml(world.author_name || world.author_id || "-")}</td>
          <td class="mono">${escapeHtml(world.id || "")}</td>
          <td>${badges || '<span class="world-badge world-badge-muted">-</span>'}</td>
          <td>${escapeHtml(formatMetric(world.visits))}</td>
          <td>${escapeHtml(formatMetric(world.favorites))}</td>
          <td>${escapeHtml(formatMetric(world.heat))}</td>
          <td>${escapeHtml(formatMetric(world.popularity))}</td>
          <td class="portal-cell"><div class="detail-chip-row detail-chip-row-compact">${portalMarkup || '<span class="detail-empty-inline">-</span>'}</div></td>
          <td>${escapeHtml(formatDateTime(world.updated_at || world.fetched_at))}</td>
        </tr>
      `;
      },
    )
    .join("");

  for (const row of $("world-table-body").querySelectorAll("tr")) {
    row.addEventListener("click", () => {
      const selected = state.worlds.find((item) => item.id === row.dataset.worldId);
      if (selected) {
        state.selectedWorld = selected;
        renderDetail(selected);
      }
    });
  }
}

function renderDetail(world) {
  if ($("detail-source")) {
    $("detail-source").textContent = world.source || "-";
  }
  $("history-world-label").textContent = world.name || world.id || "No selection";
  renderHistoryModeControls();
  const metrics = calculateWorldMetrics(world);
  const signals = getWorldSignals(world);
  const badges = renderWorldSignalBadges(signals);
  const portalLinks = normalisePortalLinks(world.portal_links);
  const portalMarkup = renderPortalLinkMarkup(portalLinks, { limit: 12 });
  if ($("world-detail")) {
    $("world-detail").innerHTML = `
    <div class="detail-title-row">
      <div>
        <h3>${escapeHtml(world.name || "(unnamed)")}</h3>
        <p class="detail-author-line">${escapeHtml(world.author_name || world.author_id || "-")}</p>
        <div class="world-badge-row">
          ${badges}
        </div>
      </div>
      ${world.world_url ? `<a href="${escapeHtml(world.world_url)}" target="_blank" rel="noreferrer">VRChat</a>` : ""}
    </div>
    <div class="detail-link-panel">
      <strong>Portal Links ${portalLinks.length ? `(${escapeHtml(String(portalLinks.length))})` : ""}</strong>
      <div class="detail-chip-row">${portalMarkup || '<span class="detail-empty-inline">No portal links recorded.</span>'}</div>
    </div>
    <dl class="detail-grid">
      <div><dt>ID</dt><dd class="mono">${escapeHtml(world.id || "-")}</dd></div>
      <div><dt>Author</dt><dd>${escapeHtml(world.author_name || world.author_id || "-")}</dd></div>
      <div><dt>Visits</dt><dd>${escapeHtml(formatMetric(world.visits))}</dd></div>
      <div><dt>Favorites</dt><dd>${escapeHtml(formatMetric(world.favorites))}</dd></div>
      <div><dt>Heat</dt><dd>${escapeHtml(formatMetric(world.heat))}</dd></div>
      <div><dt>Popularity</dt><dd>${escapeHtml(formatMetric(world.popularity))}</dd></div>
      <div><dt>Capacity</dt><dd>${escapeHtml(formatMetric(world.capacity))}</dd></div>
      <div><dt>Release Status</dt><dd>${escapeHtml(world.release_status || "-")}</dd></div>
      <div><dt>Created</dt><dd>${escapeHtml(formatDateTime(world.created_at))}</dd></div>
      <div><dt>Published</dt><dd>${escapeHtml(formatDateTime(world.publication_date))}</dd></div>
      <div><dt>Updated</dt><dd>${escapeHtml(formatDateTime(world.updated_at))}</dd></div>
      <div><dt>Fetched</dt><dd>${escapeHtml(formatDateTime(world.fetched_at))}</dd></div>
      <div><dt>Favorite Rate</dt><dd>${metrics.favorite_rate == null ? "-" : `${metrics.favorite_rate}%`}</dd></div>
      <div><dt>Visits / Day</dt><dd>${metrics.visits_per_day == null ? "-" : numberFormat.format(metrics.visits_per_day)}</dd></div>
      <div><dt>Days Since Update</dt><dd>${metrics.days_since_update == null ? "-" : numberFormat.format(metrics.days_since_update)}</dd></div>
      <div><dt>Days Since Publication</dt><dd>${metrics.days_since_publication == null ? "-" : numberFormat.format(metrics.days_since_publication)}</dd></div>
      <div><dt>Labs to Public</dt><dd>${metrics.labs_to_publication_days == null ? "-" : numberFormat.format(metrics.labs_to_publication_days)}</dd></div>
      <div><dt>Tags</dt><dd>${escapeHtml((world.tags || []).join(", ") || "-")}</dd></div>
      <div><dt>Portal Count</dt><dd>${portalLinks.length ? numberFormat.format(portalLinks.length) : "-"}</dd></div>
    </dl>
  `;
  }
  renderEditor(world);
  loadWorldHistory(world.id);
}

function renderEditor(world) {
  const canEdit = Boolean(world && state.source && state.source.startsWith("db:") && state.source !== "db:all");
  const editorSourceLabel = $("editor-source-label");
  const editorStatus = $("editor-status");
  const editorSaveButton = $("editor-save-button");
  const editorDeleteButton = $("editor-delete-button");

  const fields = {
    "edit-name": world?.name || "",
    "edit-author-name": world?.author_name || "",
    "edit-visits": hasMetric(world?.visits) ? world.visits : "",
    "edit-favorites": hasMetric(world?.favorites) ? world.favorites : "",
    "edit-heat": hasMetric(world?.heat) ? world.heat : "",
    "edit-popularity": hasMetric(world?.popularity) ? world.popularity : "",
    "edit-updated-at": world?.updated_at || "",
    "edit-publication-date": world?.publication_date || "",
    "edit-release-status": world?.release_status || "",
    "edit-tags": (world?.tags || []).join(", "),
    "edit-portal-links": (world?.portal_links || []).join("\n"),
  };
  $("taiwan-blacklist-selected-button").disabled = !world?.id;
  $("taiwan-creator-whitelist-selected-button").disabled = !world?.author_id;
  $("taiwan-creator-blacklist-selected-button").disabled = !world?.author_id;

  if (!editorSourceLabel || !editorStatus || !editorSaveButton || !editorDeleteButton) {
    return;
  }

  editorSourceLabel.textContent = canEdit ? state.source : "DB source required";
  editorStatus.textContent = canEdit
    ? `Editing ${world.name || world.id}`
    : "Choose a DB-backed source and a world row to edit. db:all cannot be edited directly.";

  Object.entries(fields).forEach(([id, value]) => {
    const field = $(id);
    if (!field) {
      return;
    }
    field.value = value;
    field.disabled = !canEdit;
  });
  editorSaveButton.disabled = !canEdit;
  editorDeleteButton.disabled = !canEdit;
}

function renderTaiwanBlacklist(items) {
  state.taiwanBlacklist = items || [];
  $("taiwan-blacklist-caption").textContent = `${state.taiwanBlacklist.length} entries`;
  $("taiwan-blacklist-list").innerHTML = state.taiwanBlacklist.length
    ? state.taiwanBlacklist
        .map(
          (item) => `
            <article class="run-item blacklist-item">
              <header>
                <strong class="mono">${escapeHtml(item)}</strong>
                <button class="inline-remove-button" type="button" data-blacklist-remove="${escapeHtml(item)}">Remove</button>
              </header>
            </article>
          `,
        )
        .join("")
    : `<div class="detail-empty">No Zh blacklist entries yet.</div>`;

  document.querySelectorAll("[data-blacklist-remove]").forEach((button) => {
    button.addEventListener("click", async () => {
      try {
        const worldId = button.dataset.blacklistRemove;
        await fetchJson(`/api/v1/jobs/taiwan/blacklist/${encodeURIComponent(worldId)}`, {
          method: "DELETE",
        });
        $("taiwan-blacklist-status").textContent = `Removed ${worldId} from Zh blacklist.`;
        await Promise.all([
          loadTaiwanBlacklist(),
          refreshCurrentScopeData({
            preferredSource: state.source,
            refreshAncillary: true,
          }),
        ]);
      } catch (error) {
        $("taiwan-blacklist-status").textContent = error.message;
        window.alert(error.message);
      }
    });
  });
}

async function loadTaiwanBlacklist() {
  const { data } = await fetchJson("/api/v1/jobs/taiwan/blacklist");
  renderTaiwanBlacklist(data.items || []);
}

function renderTaiwanCreatorWhitelist(items) {
  state.taiwanCreatorWhitelist = items || [];
  $("taiwan-creator-whitelist-list").innerHTML = state.taiwanCreatorWhitelist.length
    ? state.taiwanCreatorWhitelist
        .map(
          (item) => `
            <article class="run-item blacklist-item">
              <header>
                <strong class="mono">${escapeHtml(item)}</strong>
                <button class="inline-remove-button" type="button" data-creator-whitelist-remove="${escapeHtml(item)}">Remove</button>
              </header>
            </article>
          `,
        )
        .join("")
    : `<div class="detail-empty">No Zh creator whitelist entries yet.</div>`;

  document.querySelectorAll("[data-creator-whitelist-remove]").forEach((button) => {
    button.addEventListener("click", async () => {
      try {
        const userId = button.dataset.creatorWhitelistRemove;
        await fetchJson(`/api/v1/jobs/taiwan/creator-whitelist/${encodeURIComponent(userId)}`, {
          method: "DELETE",
        });
        $("taiwan-creator-whitelist-status").textContent = `Removed ${userId} from Zh creator whitelist.`;
        await loadTaiwanCreatorWhitelist();
      } catch (error) {
        $("taiwan-creator-whitelist-status").textContent = error.message;
        window.alert(error.message);
      }
    });
  });
}

async function loadTaiwanCreatorWhitelist() {
  const { data } = await fetchJson("/api/v1/jobs/taiwan/creator-whitelist");
  renderTaiwanCreatorWhitelist(data.items || []);
}

function renderTaiwanCreatorBlacklist(items) {
  state.taiwanCreatorBlacklist = items || [];
  $("taiwan-creator-blacklist-list").innerHTML = state.taiwanCreatorBlacklist.length
    ? state.taiwanCreatorBlacklist
        .map(
          (item) => `
            <article class="run-item blacklist-item">
              <header>
                <strong class="mono">${escapeHtml(item)}</strong>
                <button class="inline-remove-button" type="button" data-creator-blacklist-remove="${escapeHtml(item)}">Remove</button>
              </header>
            </article>
          `,
        )
        .join("")
    : `<div class="detail-empty">No Zh user blacklist entries yet.</div>`;

  document.querySelectorAll("[data-creator-blacklist-remove]").forEach((button) => {
    button.addEventListener("click", async () => {
      try {
        const userId = button.dataset.creatorBlacklistRemove;
        await fetchJson(`/api/v1/jobs/taiwan/creator-blacklist/${encodeURIComponent(userId)}`, {
          method: "DELETE",
        });
        $("taiwan-creator-blacklist-status").textContent = `Removed ${userId} from Zh user blacklist.`;
        await loadTaiwanCreatorBlacklist();
      } catch (error) {
        $("taiwan-creator-blacklist-status").textContent = error.message;
        window.alert(error.message);
      }
    });
  });
}

async function loadTaiwanCreatorBlacklist() {
  const { data } = await fetchJson("/api/v1/jobs/taiwan/creator-blacklist");
  renderTaiwanCreatorBlacklist(data.items || []);
}

function buildChartSvg(points, series, title, events = [], { timeMode = "daily" } = {}) {
  const width = 920;
  const height = 300;
  const padLeft = 56;
  const padRight = 18;
  const padTop = 18;
  const padBottom = 42;
  const innerWidth = width - padLeft - padRight;
  const innerHeight = height - padTop - padBottom;
  const maxValue = Math.max(
    1,
    ...series.flatMap((item) => points.map((point) => toNumber(item.getValue(point)))),
  );
  const tickCount = 4;
  const pointTimes = points.map((point) => (
    timeMode === "hourly"
      ? point.chart_hour_stamp ?? getUtc8HourStamp(point.iso_time)
      : point.chart_day_stamp ?? getUtc8DayStamp(point.iso_time)
  ));
  const validTimes = pointTimes.filter((value) => Number.isFinite(value));
  const minTime = validTimes.length ? Math.min(...validTimes) : null;
  const maxTime = validTimes.length ? Math.max(...validTimes) : null;
  const xAtTime = (value) => {
    if (!Number.isFinite(value) || minTime == null || maxTime == null || minTime === maxTime) {
      return padLeft + innerWidth / 2;
    }
    return padLeft + ((value - minTime) / (maxTime - minTime)) * innerWidth;
  };
  const xAt = (index) => {
    const stamp = pointTimes[index];
    return xAtTime(stamp);
  };
  const yAt = (value) => padTop + innerHeight - (toNumber(value) / maxValue) * innerHeight;

  const gridLines = [];
  for (let index = 0; index <= tickCount; index += 1) {
    const value = (maxValue / tickCount) * index;
    const y = yAt(value);
    gridLines.push(`
      <line x1="${padLeft}" y1="${y}" x2="${width - padRight}" y2="${y}" class="chart-grid"></line>
      <text x="${padLeft - 10}" y="${y + 4}" text-anchor="end" class="axis-label">${escapeHtml(formatAxisValue(value))}</text>
    `);
  }

  const xTicks = (timeMode === "hourly" ? buildHourAxisTicks(points) : buildDayAxisTicks(points))
    .map((tick) => {
      const x = xAtTime(tick);
      return `
        <line x1="${x}" y1="${height - padBottom}" x2="${x}" y2="${height - padBottom + 6}" class="axis"></line>
        <text x="${x}" y="${height - 12}" text-anchor="middle" class="axis-label">${escapeHtml(
          timeMode === "hourly"
            ? formatHourLabelUtc8(new Date(tick).toISOString())
            : formatDateLabelUtc8(new Date(tick).toISOString()),
        )}</text>
      `;
    })
    .join("");

  const eventLines = events
    .filter((item) => item.index != null && item.index >= 0 && item.index < points.length)
    .map((item) => {
      const x = xAt(item.index);
      const level = Number.isFinite(item.level) ? item.level : 0;
      const labelY = padTop + 14 + level * 18;
      return `
        <line x1="${x}" y1="${padTop}" x2="${x}" y2="${height - padBottom}" class="event-line"></line>
        <text x="${x + 6}" y="${labelY}" class="event-label">${escapeHtml(item.label)}</text>
      `;
    })
    .join("");

  const seriesRender = series
    .map((item) => {
      const coords = [];
      const dots = [];
      points.forEach((point, index) => {
        const rawValue = item.getValue(point);
        if (rawValue == null) {
          return;
        }
        const x = xAt(index);
        const y = yAt(rawValue);
        coords.push({ x, y, rawValue, point, index });
        dots.push(`
          <circle cx="${x}" cy="${y}" r="4.5" class="dot ${escapeHtml(item.className)}">
            <title>${escapeHtml(`${item.label}: ${formatAxisValue(rawValue)} | ${formatDateTime(point.iso_time)}`)}</title>
          </circle>
        `);
      });
      if (!coords.length) {
        return null;
      }
      const path = coords
        .map((coord, index) => `${index === 0 ? "M" : "L"} ${coord.x} ${coord.y}`)
        .join(" ");
      const baselineY = yAt(0);
      const areaPath = item.area
        ? [
            `M ${coords[0].x} ${baselineY}`,
            ...coords.map((coord) => `L ${coord.x} ${coord.y}`),
            `L ${coords[coords.length - 1].x} ${baselineY}`,
            "Z",
          ].join(" ")
        : "";
      const areaSize = coords.reduce((sum, coord) => sum + Math.abs(toNumber(coord.rawValue)), 0);
      return {
        item,
        path,
        dots: dots.join(""),
        area: item.area ? `<path d="${areaPath}" class="area ${escapeHtml(item.className)}"></path>` : "",
        line: `<path d="${path}" class="line ${escapeHtml(item.className)}"></path>`,
        areaSize,
      };
    })
    .filter(Boolean);

  const areaPaths = seriesRender
    .filter((render) => render.area)
    .sort((left, right) => right.areaSize - left.areaSize)
    .map((render) => render.area)
    .join("");
  const linePaths = seriesRender
    .map((render) => `${render.line}${render.dots}`)
    .join("");

  return `
    <svg viewBox="0 0 ${width} ${height}" aria-label="${escapeHtml(title)}">
      ${gridLines.join("")}
      <line x1="${padLeft}" y1="${padTop}" x2="${padLeft}" y2="${height - padBottom}" class="axis"></line>
      <line x1="${padLeft}" y1="${height - padBottom}" x2="${width - padRight}" y2="${height - padBottom}" class="axis"></line>
      ${eventLines}
      ${areaPaths}
      ${linePaths}
      ${xTicks}
    </svg>
  `;
}

function buildWorldCompareRows(worlds) {
  ensureCompareHistoryCacheScope();
  return Promise.all(
    worlds.map(async (world) => {
      try {
        const points = await fetchWorldHistoryForCompare(world.id);
        if (points.length < 2) {
          return {
            world,
            points,
            deltas: null,
          };
        }
        const latest = points[points.length - 1];
        const previous = points[points.length - 2];
        const latestVisits = toNumber(latest.visits);
        const previousVisits = toNumber(previous.visits);
        const latestFavorites = toNumber(latest.favorites);
        const previousFavorites = toNumber(previous.favorites);
        const visitsDelta = latestVisits - previousVisits;
        const favoritesDelta = latestFavorites - previousFavorites;
        const visitsGrowth = previousVisits > 0 ? (visitsDelta / previousVisits) * 100 : null;
        const favoritesGrowth = previousFavorites > 0 ? (favoritesDelta / previousFavorites) * 100 : null;
        const heatDelta = toNumber(latest.heat) - toNumber(previous.heat);
        const popularityDelta = toNumber(latest.popularity) - toNumber(previous.popularity);
        return {
          world,
          points,
          deltas: {
            visitsDelta,
            favoritesDelta,
            visitsGrowth,
            favoritesGrowth,
            heatDelta,
            popularityDelta,
            latestTime: latest.iso_time,
            previousTime: previous.iso_time,
          },
        };
      } catch {
        return { world, points: [], deltas: null };
      }
    }),
  );
}

function currentCompareScopeKey() {
  return currentScopeKey();
}

function ensureCompareHistoryCacheScope() {
  const scopeKey = currentCompareScopeKey();
  if (state.compareHistoryScope !== scopeKey) {
    state.compareHistoryScope = scopeKey;
    state.compareHistoryCache = {};
  }
}

async function fetchWorldHistoryForCompare(worldId) {
  ensureCompareHistoryCacheScope();
  if (state.compareHistoryCache[worldId]) {
    return state.compareHistoryCache[worldId];
  }
  const { data } = await fetchJson(buildWorldHistoryUrl(worldId));
  const points = data.items || [];
  state.compareHistoryCache[worldId] = points;
  return points;
}

function compareCandidateScore(world, trend = {}) {
  const breakoutScore = toNumber(trend?.breakout_score);
  const newHotScore = toNumber(trend?.new_hot_score);
  const momentumScore = toNumber(trend?.momentum_score);
  const worthWatchingScore = toNumber(trend?.worth_watching_score);
  const updateScore = toNumber(trend?.update_effectiveness_score);
  if (breakoutScore || newHotScore || momentumScore || worthWatchingScore || updateScore) {
    if (state.compareWindow === "30d" || state.compareWindow === "all") {
      return momentumScore * 1.1 + worthWatchingScore * 0.5 + updateScore * 0.35 + breakoutScore * 0.2;
    }
    if (state.compareWindow === "1d") {
      return newHotScore * 1.2 + breakoutScore + worthWatchingScore * 0.35 + updateScore * 0.45;
    }
    return momentumScore + newHotScore * 0.8 + worthWatchingScore * 0.4 + updateScore * 0.3;
  }
  const visits = toNumber(world?.visits);
  const favorites = toNumber(world?.favorites);
  const visitsDelta7d = Math.abs(toNumber(trend?.visits_delta_7d));
  const visitsDelta30d = Math.abs(toNumber(trend?.visits_delta_30d));
  const favoritesDelta7d = Math.abs(toNumber(trend?.favorites_delta_7d));
  const sinceUpdateDelta = Math.abs(toNumber(trend?.since_update_visits_delta));
  if (state.compareWindow === "30d" || state.compareWindow === "all") {
    return visitsDelta30d + favoritesDelta7d * 8 + visitsDelta7d * 0.35 + sinceUpdateDelta * 0.25 + visits * 0.002;
  }
  if (state.compareWindow === "1d") {
    return visitsDelta7d + favoritesDelta7d * 10 + visitsDelta30d * 0.2 + visits * 0.003 + favorites * 0.2;
  }
  return visitsDelta7d + favoritesDelta7d * 8 + visitsDelta30d * 0.3 + sinceUpdateDelta * 0.2 + visits * 0.002;
}

function pickCompareCandidateWorlds(worlds) {
  const worldInsights = state.collectionInsights?.world_insights || {};
  return [...worlds]
    .map((world) => ({
      world,
      score: compareCandidateScore(world, worldInsights[world.id] || {}),
    }))
    .sort((left, right) => right.score - left.score || toNumber(right.world.visits) - toNumber(left.world.visits))
    .slice(0, COMPARE_PREFETCH_LIMIT)
    .map((item) => item.world);
}

function buildSinceUpdatedDelta(points, world) {
  if (!points.length) {
    return null;
  }
  const updatedAt = parseDate(world.updated_at);
  const latest = points[points.length - 1];
  const latestTime = parseDate(latest.iso_time);
  if (!updatedAt || !latestTime) {
    return null;
  }

  let baseline = null;
  for (const point of points) {
    const stamp = parseDate(point.iso_time);
    if (!stamp) {
      continue;
    }
    if (stamp.getTime() <= updatedAt.getTime()) {
      baseline = point;
    }
  }

  if (!baseline) {
    baseline = points.find((point) => {
      const stamp = parseDate(point.iso_time);
      return stamp && stamp.getTime() >= updatedAt.getTime();
    }) || null;
  }

  if (!baseline || baseline === latest) {
    return null;
  }

  const baselineVisits = toNumber(baseline.visits);
  const baselineFavorites = toNumber(baseline.favorites);
  const latestVisits = toNumber(latest.visits);
  const latestFavorites = toNumber(latest.favorites);
  const elapsedDays = Math.max((latestTime.getTime() - updatedAt.getTime()) / 86400000, 0);
  const visitsDelta = latestVisits - baselineVisits;
  const favoritesDelta = latestFavorites - baselineFavorites;

  return {
    updatedDaysAgo: Math.max(Math.floor((Date.now() - updatedAt.getTime()) / 86400000), 0),
    visitsDelta,
    favoritesDelta,
    visitsPerDay: elapsedDays > 0 ? visitsDelta / elapsedDays : null,
    favoritesPerDay: elapsedDays > 0 ? favoritesDelta / elapsedDays : null,
    baselineTime: baseline.iso_time,
    latestTime: latest.iso_time,
  };
}

function trimComparePointsToWindow(points) {
  if (!points.length) {
    return [];
  }
  if (state.compareWindow === "1d") {
    return aggregateHistoryPointsByUtc8Hour(points, 24);
  }
  const dailyPoints = aggregateHistoryPointsByUtc8Day(points);
  if (state.compareWindow === "all") {
    return dailyPoints;
  }
  const dayWindow = state.compareWindow === "7d" ? 7 : state.compareWindow === "30d" ? 30 : 1;
  const latestStamp = dailyPoints[dailyPoints.length - 1]?.chart_day_stamp;
  if (!Number.isFinite(latestStamp)) {
    return dailyPoints;
  }
  const cutoff = latestStamp - Math.max(dayWindow - 1, 0) * 86400000;
  return dailyPoints.filter((point) => (point.chart_day_stamp || 0) >= cutoff);
}

function buildInstantTrafficSeries(points) {
  if (!points.length) {
    return [];
  }
  const deltas = [];
  for (let index = 1; index < points.length; index += 1) {
    const previous = points[index - 1];
    const current = points[index];
    deltas.push({
      iso_time: current.iso_time,
      chart_day_stamp: current.chart_day_stamp,
      chart_hour_stamp: current.chart_hour_stamp,
      delta_visits: toNumber(current.visits) - toNumber(previous.visits),
    });
  }
  return deltas;
}

function truncateCompareLabel(value, maxLength = 24) {
  const text = String(value || "").trim();
  if (!text) {
    return "-";
  }
  return text.length > maxLength ? `${text.slice(0, Math.max(maxLength - 1, 1))}…` : text;
}

function truncateCompareLabelSafe(value, maxLength = 24) {
  const text = String(value || "").trim();
  if (!text) {
    return "-";
  }
  if (text.length <= maxLength) {
    return text;
  }
  const sliceLength = Math.max(maxLength - 3, 1);
  return `${text.slice(0, sliceLength)}...`;
}

function buildCompareTrafficSvg(
  seriesRows,
  {
    timeMode = "daily",
    metricKey = "visits",
    peakKey = "peakVisits",
    latestKey = "latestVisits",
    allowNegative = false,
    ariaLabel = "traffic comparison",
  } = {},
) {
  const width = 920;
  const height = 320;
  const padLeft = 56;
  const padRight = 18;
  const padTop = 18;
  const padBottom = 42;
  const innerWidth = width - padLeft - padRight;
  const innerHeight = height - padTop - padBottom;
  const allTimes = seriesRows.flatMap((row) => row.points.map((point) => (
    timeMode === "hourly"
      ? point.chart_hour_stamp ?? getUtc8HourStamp(point.iso_time)
      : point.chart_day_stamp ?? getUtc8DayStamp(point.iso_time)
  ))).filter((value) => Number.isFinite(value));
  const allValues = seriesRows.flatMap((row) => row.points.map((point) => toNumber(point[metricKey])));
  if (!allTimes.length || !allValues.length) {
    return "";
  }
  const minTime = Math.min(...allTimes);
  const maxTime = Math.max(...allTimes);
  const minValue = allowNegative ? Math.min(0, ...allValues) : 0;
  const maxValue = allowNegative ? Math.max(0, 1, ...allValues) : Math.max(1, ...allValues);
  const valueSpan = Math.max(maxValue - minValue, 1);
  const xAtTime = (value) => {
    if (!Number.isFinite(value) || minTime === maxTime) {
      return padLeft + innerWidth / 2;
    }
    return padLeft + ((value - minTime) / (maxTime - minTime)) * innerWidth;
  };
  const yAt = (value) => padTop + innerHeight - ((toNumber(value) - minValue) / valueSpan) * innerHeight;
  const tickCount = 5;
  const gridLines = [];
  for (let index = 0; index <= tickCount; index += 1) {
    const value = minValue + (valueSpan / tickCount) * index;
    const y = yAt(value);
    gridLines.push(`
      <line x1="${padLeft}" y1="${y}" x2="${width - padRight}" y2="${y}" class="chart-grid"></line>
      <text x="${padLeft - 10}" y="${y + 4}" text-anchor="end" class="axis-label">${escapeHtml(formatAxisValue(value))}</text>
    `);
  }
  const zeroLine = `
    <line x1="${padLeft}" y1="${yAt(0)}" x2="${width - padRight}" y2="${yAt(0)}" class="axis"></line>
  `;
  const tickSeed = [...new Set(allTimes)]
    .sort((left, right) => left - right)
    .map((stamp) => (
      timeMode === "hourly"
        ? { iso_time: new Date(stamp).toISOString(), chart_hour_stamp: stamp }
        : { iso_time: new Date(stamp).toISOString(), chart_day_stamp: stamp }
    ));
  const xTicks = (timeMode === "hourly" ? buildHourAxisTicks(tickSeed) : buildDayAxisTicks(tickSeed))
    .map((tick) => {
      const x = xAtTime(tick);
      return `
        <line x1="${x}" y1="${height - padBottom}" x2="${x}" y2="${height - padBottom + 6}" class="axis"></line>
        <text x="${x}" y="${height - 12}" text-anchor="middle" class="axis-label">${escapeHtml(
          timeMode === "hourly"
            ? formatHourLabelUtc8(new Date(tick).toISOString())
            : formatDateLabelUtc8(new Date(tick).toISOString()),
        )}</text>
      `;
    })
    .join("");
  const paths = seriesRows
    .map((row, index) => {
      const color = paletteColorForKey(row.world.id || row.world.name || row.world.author_name || "flow");
      const coords = row.points
        .map((point) => {
          const stamp = timeMode === "hourly"
            ? point.chart_hour_stamp ?? getUtc8HourStamp(point.iso_time)
            : point.chart_day_stamp ?? getUtc8DayStamp(point.iso_time);
          return Number.isFinite(stamp)
            ? { x: xAtTime(stamp), y: yAt(point[metricKey]), point }
            : null;
        })
        .filter(Boolean);
      if (!coords.length) {
        return "";
      }
      const path = coords.map((coord, index) => `${index === 0 ? "M" : "L"} ${coord.x} ${coord.y}`).join(" ");
      const lastCoord = coords[coords.length - 1];
      const label = index < 8
        ? `<text x="${lastCoord.x + 6}" y="${lastCoord.y - 6}" class="compare-line-label" fill="${escapeHtml(color)}">${escapeHtml(truncateCompareLabelSafe(row.world.name || row.world.id, 16))}</text>`
        : "";
      return `
        <path d="${path}" fill="none" stroke="${escapeHtml(color)}" stroke-width="2" stroke-opacity="0.7">
          <title>${escapeHtml(`${row.world.name || row.world.id} / peak ${formatAxisValue(row[peakKey])} / latest ${formatAxisValue(row[latestKey])}`)}</title>
        </path>
        <circle cx="${lastCoord.x}" cy="${lastCoord.y}" r="3.5" fill="${escapeHtml(color)}">
          <title>${escapeHtml(`${row.world.name || row.world.id} / peak ${formatAxisValue(row[peakKey])} / latest ${formatAxisValue(row[latestKey])}`)}</title>
        </circle>
        ${label}
      `;
    })
    .join("");
  return `
    <svg viewBox="0 0 ${width} ${height}" aria-label="${escapeHtml(ariaLabel)}">
      ${gridLines.join("")}
      <line x1="${padLeft}" y1="${padTop}" x2="${padLeft}" y2="${height - padBottom}" class="axis"></line>
      ${zeroLine}
      ${paths}
      ${xTicks}
    </svg>
  `;
}

function buildCompareTrafficLegend(seriesRows) {
  return `
    <div class="compare-flow-legend">
      ${seriesRows
        .map((row) => {
          const color = paletteColorForKey(row.world.id || row.world.name || row.world.author_name || "flow");
          const peakValue = row.peakDelta != null ? row.peakDelta : row.peakVisits;
          const latestValue = row.latestDelta != null ? row.latestDelta : row.latestVisits;
          return `
            <span class="compare-flow-chip" style="--flow-color:${escapeHtml(color)}">
              <i></i>
              <strong>${escapeHtml(truncateCompareLabelSafe(row.world.name || row.world.id, 28))}</strong>
              <small>peak ${escapeHtml(formatAxisValue(peakValue))} / latest ${escapeHtml(formatAxisValue(latestValue))}</small>
            </span>
          `;
        })
        .join("")}
    </div>
  `;
}

function buildCompareTableRows(seriesRows, { peakKey, latestKey }) {
  if (!seriesRows.length) {
    return `<tr><td colspan="5"><div class="detail-empty-inline">No worlds with history for this view.</div></td></tr>`;
  }
  return seriesRows
    .map((row) => {
      const world = row.world || {};
      return `
        <tr>
          <td>
            <label class="compare-row-toggle compare-table-toggle">
              <input type="checkbox" data-compare-visible="${escapeHtml(compareKeyForWorld(world))}" ${isCompareWorldVisible(world) ? "checked" : ""}>
              <span>Show</span>
            </label>
          </td>
          <td>${escapeHtml(world.name || world.id || "(unnamed)")}</td>
          <td>${escapeHtml(world.author_name || world.author_id || "-")}</td>
          <td>${escapeHtml(formatAxisValue(toNumber(row[peakKey])))}</td>
          <td>${escapeHtml(formatAxisValue(toNumber(row[latestKey])))}</td>
        </tr>
      `;
    })
    .join("");
}





async function renderWorldCompare(worlds) {
  if (!worlds.length) {
    $("compare-caption").textContent = "no worlds";
    $("compare-flow-caption").textContent = `top ${COMPARE_PREFETCH_LIMIT} growth candidates`;
    $("compare-flow-summary").textContent = "";
    $("compare-flow-chart").innerHTML = `<div class="detail-empty">No visit history yet.</div>`;
    $("compare-cumulative-caption").textContent = `top ${COMPARE_PREFETCH_LIMIT} by peak visits`;
    $("compare-cumulative-summary").textContent = "";
    $("compare-cumulative-chart").innerHTML = `<div class="detail-empty">No cumulative visit history yet.</div>`;
    $("compare-traffic-table-caption").textContent = "no worlds";
    $("compare-traffic-table-body").innerHTML = buildCompareTableRows([], { peakKey: "peakDelta", latestKey: "latestDelta" });
    $("compare-cumulative-table-caption").textContent = "no worlds";
    $("compare-cumulative-table-body").innerHTML = buildCompareTableRows([], { peakKey: "peakVisits", latestKey: "latestVisits" });
    $("compare-grid").innerHTML = `<div class="detail-empty">No worlds to compare.</div>`;
    return;
  }
  const windowLabel = compareWindowLabel(state.compareWindow);
  const candidateWorlds = pickCompareCandidateWorlds(worlds);
  $("compare-caption").textContent = `loading ${windowLabel} comparison from top ${candidateWorlds.length} growth candidates`;
  const rows = await buildWorldCompareRows(candidateWorlds);
  const compareTimeMode = state.compareWindow === "1d" ? "hourly" : "daily";
  const trafficSeries = rows
    .map((item) => {
      const trimmed = trimComparePointsToWindow(item.points || []);
      const instantPoints = buildInstantTrafficSeries(trimmed);
      if (!instantPoints.length) {
        return null;
      }
      return {
        world: item.world,
        points: instantPoints,
        peakDelta: Math.max(...instantPoints.map((point) => toNumber(point.delta_visits))),
        latestDelta: toNumber(instantPoints[instantPoints.length - 1]?.delta_visits),
      };
    })
    .filter((item) => item && item.peakDelta > 0)
    .sort((left, right) => right.peakDelta - left.peakDelta || right.latestDelta - left.latestDelta)
    .slice(0, 30);

  const cumulativeSeries = rows
    .map((item) => {
      const trimmed = trimComparePointsToWindow(item.points || []);
      if (!trimmed.length) {
        return null;
      }
      const visitPoints = trimmed.map((point) => ({
        ...point,
        visits: toNumber(point.visits),
      }));
      return {
        world: item.world,
        points: visitPoints,
        peakVisits: Math.max(...visitPoints.map((point) => point.visits)),
        latestVisits: visitPoints[visitPoints.length - 1]?.visits ?? 0,
      };
    })
    .filter((item) => item && item.peakVisits > 0)
    .sort((left, right) => right.peakVisits - left.peakVisits || right.latestVisits - left.latestVisits)
    .slice(0, 30);

  if (!Object.keys(state.compareVisibleWorldIds).length) {
    trafficSeries.forEach((row) => {
      state.compareVisibleWorldIds[compareKeyForWorld(row.world)] = true;
    });
  }

  const visibleTrafficSeries = trafficSeries.filter((row) => isCompareWorldVisible(row.world));
  const visibleCumulativeSeries = cumulativeSeries.filter((row) => isCompareWorldVisible(row.world));
  const visibleTotalVisits = visibleCumulativeSeries.reduce((sum, row) => sum + toNumber(row.latestVisits), 0);
  const compareSummaryText = visibleCumulativeSeries.length
    ? `Visible ${visibleCumulativeSeries.length}/${cumulativeSeries.length} worlds / Total visits ${numberFormat.format(visibleTotalVisits)}`
    : "No worlds selected for display.";
  const compareSummary = visibleCumulativeSeries.length
    ? `Visible ${visibleCumulativeSeries.length}/${cumulativeSeries.length} worlds · Total visits ${numberFormat.format(visibleTotalVisits)}`
    : `No worlds selected for display.`;
  const ranked = rows
    .map((item) => {
      if (!item.points.length) {
        return item;
      }
      const latest = item.points[item.points.length - 1];
      const baseline = findComparisonPoint(item.points, state.compareWindow);
      if (!baseline || baseline === latest) {
        return { ...item, deltas: null };
      }
      const latestVisits = toNumber(latest.visits);
      const previousVisits = toNumber(baseline.visits);
      const latestFavorites = toNumber(latest.favorites);
      const previousFavorites = toNumber(baseline.favorites);
      return {
        ...item,
        deltas: {
          visitsDelta: latestVisits - previousVisits,
          favoritesDelta: latestFavorites - previousFavorites,
          visitsGrowth: previousVisits > 0 ? ((latestVisits - previousVisits) / previousVisits) * 100 : null,
          favoritesGrowth: previousFavorites > 0 ? ((latestFavorites - previousFavorites) / previousFavorites) * 100 : null,
          heatDelta: toNumber(latest.heat) - toNumber(baseline.heat),
          popularityDelta: toNumber(latest.popularity) - toNumber(baseline.popularity),
          latestTime: latest.iso_time,
          previousTime: baseline.iso_time,
        },
        sinceUpdated: buildSinceUpdatedDelta(item.points, item.world),
      };
    })
    .filter((item) => item.deltas)
    .sort((left, right) => {
      const leftScore = Math.abs(left.deltas.visitsDelta) + Math.abs(left.deltas.favoritesDelta) * 4;
      const rightScore = Math.abs(right.deltas.visitsDelta) + Math.abs(right.deltas.favoritesDelta) * 4;
      return rightScore - leftScore;
    });

  $("compare-flow-caption").textContent = trafficSeries.length
    ? `${trafficSeries.length} of ${candidateWorlds.length} prefetched worlds / ${windowLabel} / ${compareTimeMode === "hourly" ? "hourly" : "daily"} instant delta`
    : `no visit history found for ${windowLabel}`;
  $("compare-flow-summary").textContent = compareSummaryText;
  $("compare-flow-chart").innerHTML = visibleTrafficSeries.length
    ? `
        ${buildCompareTrafficSvg(visibleTrafficSeries, {
          timeMode: compareTimeMode,
          metricKey: "delta_visits",
          peakKey: "peakDelta",
          latestKey: "latestDelta",
          allowNegative: true,
          ariaLabel: "instant traffic delta comparison",
        })}
        ${buildCompareTrafficLegend(visibleTrafficSeries)}
      `
    : `<div class="detail-empty">${compareSummaryText}</div>`;

  $("compare-cumulative-caption").textContent = cumulativeSeries.length
    ? `${cumulativeSeries.length} of ${candidateWorlds.length} prefetched worlds / ${windowLabel} / ${compareTimeMode === "hourly" ? "hourly" : "daily"} cumulative visits`
    : `no visit history found for ${windowLabel}`;
  $("compare-cumulative-summary").textContent = compareSummaryText;
  $("compare-cumulative-chart").innerHTML = visibleCumulativeSeries.length
    ? `
        ${buildCompareTrafficSvg(visibleCumulativeSeries, {
          timeMode: compareTimeMode,
          metricKey: "visits",
          peakKey: "peakVisits",
          latestKey: "latestVisits",
          ariaLabel: "cumulative visits comparison",
        })}
        ${buildCompareTrafficLegend(visibleCumulativeSeries)}
      `
    : `<div class="detail-empty">${compareSummaryText}</div>`;
  $("compare-traffic-table-caption").textContent = trafficSeries.length
    ? `${visibleTrafficSeries.length}/${trafficSeries.length} visible worlds`
    : "no instant traffic rows";
  $("compare-traffic-table-body").innerHTML = buildCompareTableRows(trafficSeries, {
    peakKey: "peakDelta",
    latestKey: "latestDelta",
  });
  $("compare-cumulative-table-caption").textContent = cumulativeSeries.length
    ? `${visibleCumulativeSeries.length}/${cumulativeSeries.length} visible worlds`
    : "no cumulative rows";
  $("compare-cumulative-table-body").innerHTML = buildCompareTableRows(cumulativeSeries, {
    peakKey: "peakVisits",
    latestKey: "latestVisits",
  });

  $("compare-caption").textContent = `${ranked.length} worlds compared against ${windowLabel} ago from top ${candidateWorlds.length} growth candidates`;
  $("compare-grid").innerHTML = ranked.length
    ? ranked
        .map(({ world, deltas, sinceUpdated }) => {
          const visitsTone = deltas.visitsDelta > 0 ? "metric-up" : deltas.visitsDelta < 0 ? "metric-down" : "metric-flat";
          const favoritesTone = deltas.favoritesDelta > 0 ? "metric-up" : deltas.favoritesDelta < 0 ? "metric-down" : "metric-flat";
          const heatTone = deltas.heatDelta > 0 ? "metric-up" : deltas.heatDelta < 0 ? "metric-down" : "metric-flat";
          const popularityTone = deltas.popularityDelta > 0 ? "metric-up" : deltas.popularityDelta < 0 ? "metric-down" : "metric-flat";
          const updateTone = sinceUpdated
            ? sinceUpdated.visitsDelta > 0
              ? "metric-up"
              : sinceUpdated.visitsDelta < 0
                ? "metric-down"
                : "metric-flat"
            : "metric-flat";
          return `
            <article class="compare-row">
              <div class="compare-main">
                <label class="compare-row-toggle">
                  <input type="checkbox" data-compare-visible="${escapeHtml(compareKeyForWorld(world))}" ${isCompareWorldVisible(world) ? "checked" : ""}>
                  <span>顯示</span>
                </label>
                <strong>${escapeHtml(world.name || world.id || "(unnamed)")}</strong>
                <span class="mono">${escapeHtml(world.id || "-")}</span>
                <span class="compare-sub">${escapeHtml(formatDateTime(deltas.previousTime))} → ${escapeHtml(formatDateTime(deltas.latestTime))}</span>
              </div>
              <div class="compare-metric">
                <span>Visits</span>
                <strong class="${visitsTone}">${escapeHtml(formatDelta(deltas.visitsDelta))}</strong>
                <small>${escapeHtml(formatPercent(deltas.visitsGrowth))}</small>
              </div>
              <div class="compare-metric">
                <span>Favorites</span>
                <strong class="${favoritesTone}">${escapeHtml(formatDelta(deltas.favoritesDelta))}</strong>
                <small>${escapeHtml(formatPercent(deltas.favoritesGrowth))}</small>
              </div>
              <div class="compare-metric">
                <span>Heat</span>
                <strong class="${heatTone}">${escapeHtml(formatDelta(deltas.heatDelta))}</strong>
                <small>latest ${escapeHtml(formatMetric(world.heat))}</small>
              </div>
              <div class="compare-metric">
                <span>Popularity</span>
                <strong class="${popularityTone}">${escapeHtml(formatDelta(deltas.popularityDelta))}</strong>
                <small>latest ${escapeHtml(formatMetric(world.popularity))}</small>
              </div>
              <div class="compare-metric compare-metric-update">
                <span>Since Updated</span>
                <strong class="${updateTone}">${escapeHtml(sinceUpdated ? formatDelta(sinceUpdated.visitsDelta) : "-")}</strong>
                <small>
                  ${escapeHtml(
                    sinceUpdated
                      ? `updated ${numberFormat.format(sinceUpdated.updatedDaysAgo)}d ago | fav ${formatDelta(sinceUpdated.favoritesDelta)}`
                      : "no update baseline",
                  )}
                </small>
              </div>
            </article>
          `;
        })
        .join("")
    : `<div class="detail-empty">Not enough history points yet for change comparison.</div>`;

  for (const label of document.querySelectorAll("#compare-grid .compare-row-toggle span")) {
    label.textContent = "Show";
  }
  for (const item of document.querySelectorAll("#compare-grid .compare-sub")) {
    item.textContent = item.textContent.replace("??", " -> ");
  }

  for (const checkbox of document.querySelectorAll("[data-compare-visible]")) {
    checkbox.addEventListener("change", () => {
      toggleCompareWorldVisibility(checkbox.dataset.compareVisible, checkbox.checked);
      renderWorldCompare(state.worlds);
    });
  }
}

function buildTimelineEvents(points, { timeMode = "daily" } = {}) {
  if (!points.length) {
    return [];
  }
  const latest = points[points.length - 1];
  const firstStamp = parseDate(points[0].iso_time);
  const lastStamp = parseDate(latest.iso_time);
  const events = [];
  const sourceFields = [
    { key: "created_at", label: "Created", value: latest.created_at },
    { key: "publication_date", label: "Published", value: latest.publication_date },
    { key: "updated_at", label: "Updated", value: latest.updated_at },
  ];
  sourceFields.forEach((field) => {
    const target = parseDate(field.value);
    if (!target) {
      return;
    }
    if (timeMode === "hourly" && firstStamp && lastStamp) {
      if (target.getTime() < firstStamp.getTime() || target.getTime() > lastStamp.getTime()) {
        return;
      }
    }
    let nearestIndex = null;
    let nearestDistance = Number.POSITIVE_INFINITY;
    points.forEach((point, index) => {
      const stamp = parseDate(point.iso_time);
      if (!stamp) {
        return;
      }
      const distance = Math.abs(stamp - target);
      if (distance < nearestDistance) {
        nearestDistance = distance;
        nearestIndex = index;
      }
    });
    if (nearestIndex != null) {
      events.push({ index: nearestIndex, label: field.label, level: events.length });
    }
  });
  return events.filter((item, index, list) => list.findIndex((other) => other.index === item.index && other.label === item.label) === index);
}

function aggregateHistoryPointsByUtc8Day(points) {
  if (!points.length) {
    return [];
  }
  const grouped = new Map();
  for (const point of points) {
    const dayKey = getUtc8DayKey(point.iso_time);
    const dayStamp = getUtc8DayStamp(point.iso_time);
    if (!dayKey || !Number.isFinite(dayStamp)) {
      continue;
    }
    grouped.set(dayKey, {
      ...point,
      chart_day_key: dayKey,
      chart_day_stamp: dayStamp,
      raw_sample_count: (grouped.get(dayKey)?.raw_sample_count || 0) + 1,
      last_sample_iso_time: point.iso_time,
    });
  }
  return [...grouped.values()].sort((left, right) => (left.chart_day_stamp || 0) - (right.chart_day_stamp || 0));
}

function aggregateHistoryPointsByUtc8Hour(points, windowHours = 48) {
  if (!points.length) {
    return [];
  }
  const validPoints = points.filter((point) => Number.isFinite(getUtc8HourStamp(point.iso_time)));
  if (!validPoints.length) {
    return [];
  }
  const latestStamp = Math.max(...validPoints.map((point) => getUtc8HourStamp(point.iso_time)));
  const cutoffStamp = latestStamp - Math.max(windowHours - 1, 0) * 3600000;
  const grouped = new Map();
  for (const point of validPoints) {
    const hourKey = getUtc8HourKey(point.iso_time);
    const hourStamp = getUtc8HourStamp(point.iso_time);
    if (!hourKey || !Number.isFinite(hourStamp) || hourStamp < cutoffStamp) {
      continue;
    }
    grouped.set(hourKey, {
      ...point,
      chart_hour_key: hourKey,
      chart_hour_stamp: hourStamp,
      raw_sample_count: (grouped.get(hourKey)?.raw_sample_count || 0) + 1,
      last_sample_iso_time: point.iso_time,
    });
  }
  return [...grouped.values()].sort((left, right) => (left.chart_hour_stamp || 0) - (right.chart_hour_stamp || 0));
}

function buildUpdateEffectCards(points, { timeMode = "daily" } = {}) {
  if (!points.length) {
    return [];
  }
  const latest = points[points.length - 1];
  const updatedAt = parseDate(latest.updated_at);
  if (!updatedAt) {
    return [];
  }
  const postUpdate = points.filter((point) => {
    const stamp = parseDate(point.iso_time);
    return stamp && stamp >= updatedAt;
  });
  if (postUpdate.length < 2) {
    return [
      { label: "Update Window", value: "No post-update samples", detail: formatDateTimeUtc8(latest.updated_at) || "-" },
    ];
  }
  const first = postUpdate[0];
  const last = postUpdate[postUpdate.length - 1];
  const days = Math.max((parseDate(last.iso_time) - parseDate(first.iso_time)) / 86400000, 0);
  const hours = Math.max((parseDate(last.iso_time) - parseDate(first.iso_time)) / 3600000, 0);
  const visitsGain = toNumber(last.visits) - toNumber(first.visits);
  const favoritesGain = toNumber(last.favorites) - toNumber(first.favorites);
  return [
    {
      label: "Updated At",
      value: formatDateTimeUtc8(latest.updated_at) || "-",
      detail: timeMode === "hourly" ? `${postUpdate.length} hourly samples` : `${postUpdate.length} days after update`,
    },
    {
      label: "Visits Since Update",
      value: formatDelta(visitsGain),
      detail: timeMode === "hourly"
        ? hours > 0
          ? `${Math.round((visitsGain / hours) * 100) / 100} / hour`
          : "same-hour window"
        : days > 0
          ? `${Math.round((visitsGain / days) * 100) / 100} / day`
          : "same-day window",
    },
    {
      label: "Favorites Since Update",
      value: formatDelta(favoritesGain),
      detail: timeMode === "hourly"
        ? hours > 0
          ? `${Math.round((favoritesGain / hours) * 100) / 100} / hour`
          : "same-hour window"
        : days > 0
          ? `${Math.round((favoritesGain / days) * 100) / 100} / day`
          : "same-day window",
    },
  ];
}

function buildHistoryStats(points, { timeMode = "daily" } = {}) {
  if (!points.length) {
    return [];
  }
  const first = points[0];
  const latest = points[points.length - 1];
  const latestVisits = toNumber(latest.visits);
  const latestFavorites = toNumber(latest.favorites);
  const favoriteRate = latestVisits > 0
    ? Math.round((latestFavorites / latestVisits) * 10000) / 100
    : null;
  const visitsDelta = latestVisits - toNumber(first.visits);
  const favoritesDelta = latestFavorites - toNumber(first.favorites);
  const latestStamp = parseDate(latest.iso_time);
  const updated = parseDate(latest.updated_at);
  const publication = parseDate(latest.publication_date);
  const firstStamp = parseDate(first.iso_time);
  const daysSinceUpdate = latestStamp && updated
    ? Math.max(Math.floor((latestStamp - updated) / 86400000), 0)
    : null;
  const daysSincePublication = latestStamp && publication
    ? Math.max(Math.floor((latestStamp - publication) / 86400000), 0)
    : null;
  const visitsPerDay = daysSincePublication && daysSincePublication > 0
    ? Math.round((latestVisits / daysSincePublication) * 100) / 100
    : null;
  const elapsedHours = latestStamp && firstStamp
    ? Math.max((latestStamp - firstStamp) / 3600000, 0)
    : 0;
  const visitsPerHour = elapsedHours > 0
    ? Math.round((visitsDelta / elapsedHours) * 100) / 100
    : null;

  return [
    { label: timeMode === "hourly" ? "Hours Tracked" : "Days Tracked", value: numberFormat.format(points.length) },
    { label: "Latest Heat", value: numberFormat.format(toNumber(latest.heat)) },
    { label: "Latest Popularity", value: numberFormat.format(toNumber(latest.popularity)) },
    { label: "Favorite Rate", value: favoriteRate == null ? "-" : `${favoriteRate}%` },
    { label: "Visits Delta", value: `${visitsDelta >= 0 ? "+" : ""}${numberFormat.format(visitsDelta)}` },
    { label: "Favorites Delta", value: `${favoritesDelta >= 0 ? "+" : ""}${numberFormat.format(favoritesDelta)}` },
    { label: "Published", value: formatDateTimeUtc8(latest.publication_date) || "-" },
    { label: "Days Since Update", value: daysSinceUpdate == null ? "-" : numberFormat.format(daysSinceUpdate) },
    {
      label: timeMode === "hourly" ? "Visits / Hour" : "Visits / Day",
      value: timeMode === "hourly"
        ? (visitsPerHour == null ? "-" : numberFormat.format(visitsPerHour))
        : (visitsPerDay == null ? "-" : numberFormat.format(visitsPerDay)),
    },
  ];
}

function buildHistoryFocusCards(points) {
  if (!points.length) {
    return [];
  }
  const latest = points[points.length - 1];
  const previous = points.length > 1 ? points[points.length - 2] : null;
  const visits = toNumber(latest.visits);
  const favorites = toNumber(latest.favorites);
  const heat = toNumber(latest.heat);
  const popularity = toNumber(latest.popularity);

  const visitDelta = previous ? visits - toNumber(previous.visits) : null;
  const favoriteDelta = previous ? favorites - toNumber(previous.favorites) : null;
  const heatDelta = previous ? heat - toNumber(previous.heat) : null;
  const popularityDelta = previous ? popularity - toNumber(previous.popularity) : null;

  return [
    {
      label: "Visits",
      value: numberFormat.format(visits),
      detail: visitDelta == null ? "latest sample" : `${visitDelta >= 0 ? "+" : ""}${numberFormat.format(visitDelta)} vs prev`,
    },
    {
      label: "Favorites",
      value: numberFormat.format(favorites),
      detail: favoriteDelta == null ? "latest sample" : `${favoriteDelta >= 0 ? "+" : ""}${numberFormat.format(favoriteDelta)} vs prev`,
    },
    {
      label: "Heat",
      value: numberFormat.format(heat),
      detail: heatDelta == null ? "latest sample" : `${heatDelta >= 0 ? "+" : ""}${numberFormat.format(heatDelta)} vs prev`,
    },
    {
      label: "Popularity",
      value: numberFormat.format(popularity),
      detail: popularityDelta == null ? "latest sample" : `${popularityDelta >= 0 ? "+" : ""}${numberFormat.format(popularityDelta)} vs prev`,
    },
  ];
}

function shouldShowHourlyHistoryToggle() {
  return Boolean(state.uiSettings.enableHourlyHistoryAll || state.collectionInsights?.performance?.enabled);
}

function renderHistoryModeControls() {
  const controls = $("history-mode-controls");
  if (!controls) {
    return;
  }
  const visible = shouldShowHourlyHistoryToggle();
  if (!visible) {
    state.historyTrendMode = "daily";
  }
  controls.classList.toggle("hidden", !visible);
  $("history-mode-daily")?.classList.toggle("is-active", state.historyTrendMode === "daily");
  $("history-mode-48h")?.classList.toggle("is-active", state.historyTrendMode === "48h");
}

function renderChart(points) {
  if (!points.length) {
    $("history-focus-grid").innerHTML = "";
    $("history-chart").innerHTML = `<div class="detail-empty">No history data yet.</div>`;
    return;
  }

  const timeMode = shouldShowHourlyHistoryToggle() && state.historyTrendMode === "48h" ? "hourly" : "daily";
  const chartPoints = timeMode === "hourly"
    ? aggregateHistoryPointsByUtc8Hour(points, 48)
    : aggregateHistoryPointsByUtc8Day(points);
  if (!chartPoints.length) {
    $("history-focus-grid").innerHTML = "";
    $("history-chart").innerHTML = `<div class="detail-empty">No ${timeMode === "hourly" ? "48-hour" : "history"} data yet.</div>`;
    return;
  }

  const focusCards = buildHistoryFocusCards(chartPoints);
  const stats = buildHistoryStats(chartPoints, { timeMode });
  const updateEffectCards = buildUpdateEffectCards(chartPoints, { timeMode });
  const events = buildTimelineEvents(chartPoints, { timeMode });
  const trafficSvg = buildChartSvg(
    chartPoints,
    [
      { label: "Visits", className: "visits", area: true, getValue: (point) => point.visits },
      { label: "Favorites", className: "favorites", area: true, getValue: (point) => point.favorites },
    ],
    "traffic chart",
    events,
    { timeMode },
  );
  const signalSvg = buildChartSvg(
    chartPoints,
    [
      { label: "Heat", className: "heat", getValue: (point) => point.heat },
      { label: "Popularity", className: "popularity", getValue: (point) => point.popularity },
      {
        label: "Favorite Rate",
        className: "favorite-rate",
        getValue: (point) => {
          const visits = toNumber(point.visits);
          return visits > 0 ? Math.round((toNumber(point.favorites) / visits) * 10000) / 100 : 0;
        },
      },
    ],
    "signal chart",
    events,
    { timeMode },
  );

  $("history-focus-grid").innerHTML = focusCards
    .map(
      (item) => `
        <article class="history-focus-card">
          <span>${escapeHtml(item.label)}</span>
          <strong>${escapeHtml(item.value)}</strong>
          <small>${escapeHtml(item.detail)}</small>
        </article>
      `,
    )
    .join("");

  $("history-chart").innerHTML = `
    <div class="history-stats">
      ${stats
        .map(
          (item) => `
            <article class="history-stat">
              <span>${escapeHtml(item.label)}</span>
              <strong>${escapeHtml(item.value)}</strong>
            </article>
          `,
        )
        .join("")}
    </div>
    <div class="history-stats">
      ${updateEffectCards
        .map(
          (item) => `
            <article class="history-stat">
              <span>${escapeHtml(item.label)}</span>
              <strong>${escapeHtml(item.value)}</strong>
            </article>
          `,
        )
        .join("")}
    </div>
    <section class="chart-panel">
      <div class="chart-head">
        <h4>Traffic</h4>
        <span>${escapeHtml(
          timeMode === "hourly"
            ? formatHourLabelUtc8(chartPoints[0]?.iso_time)
            : formatDateLabelUtc8(chartPoints[0]?.iso_time),
        )} to ${escapeHtml(
          timeMode === "hourly"
            ? formatHourLabelUtc8(chartPoints[chartPoints.length - 1]?.iso_time)
            : formatDateLabelUtc8(chartPoints[chartPoints.length - 1]?.iso_time),
        )} / ${HISTORY_TIME_ZONE_LABEL} ${timeMode === "hourly" ? "hourly" : "daily"}</span>
      </div>
      ${trafficSvg}
      <div class="chart-legend">
        <span><i class="legend-swatch visits"></i>Visits</span>
        <span><i class="legend-swatch favorites"></i>Favorites</span>
        <span>Dashed lines: Created / Published / Updated</span>
      </div>
    </section>
    <section class="chart-panel">
      <div class="chart-head">
        <h4>Signals</h4>
        <span>Heat / Popularity / Favorite Rate / ${HISTORY_TIME_ZONE_LABEL} ${timeMode === "hourly" ? "hourly" : "daily"}</span>
      </div>
      ${signalSvg}
      <div class="chart-legend">
        <span><i class="legend-swatch heat"></i>Heat</span>
        <span><i class="legend-swatch popularity"></i>Popularity</span>
        <span><i class="legend-swatch favorite-rate"></i>Favorite Rate</span>
      </div>
    </section>
  `;
}

async function loadWorldHistory(worldId) {
  state.historyWorldId = worldId;
  const { data } = await fetchJson(buildWorldHistoryUrl(worldId));
  if (state.historyWorldId !== worldId) {
    return;
  }
  state.historyPoints = data.items || [];
  renderChart(state.historyPoints);
}

function renderReview(result, statusCode) {
  $("review-status").textContent = `${result.status.toUpperCase()} / ${statusCode}`;
  const warnings = result.warnings || [];
  $("review-list").innerHTML = warnings.length
    ? warnings.map((item) => `<li>${escapeHtml(item)}</li>`).join("")
    : "<li>No warnings.</li>";
}

function renderAnalytics(items) {
  $("analytics-list").innerHTML = items
    .slice(0, 8)
    .map(
      (item) => `
        <article class="analytics-item">
          <header>
            <strong>${escapeHtml(item.source)}</strong>
            <span>${escapeHtml(item.origin)}</span>
          </header>
          <p>${escapeHtml((item.latest || {}).date || "no data")}</p>
          <p>total: ${numberFormat.format((item.latest || {}).total_worlds || 0)}</p>
          <p>new today: ${numberFormat.format((item.latest || {}).new_worlds_today || 0)}</p>
        </article>
      `,
    )
    .join("");
}

function renderGrowthLeaderboard(items, label = "current collection") {
  $("growth-caption").textContent = label;
  $("growth-list").innerHTML = (items || []).length
    ? items
        .map(
          (item) => `
            <article class="run-item">
              <header>
                <strong>${escapeHtml(item.name || item.id)}</strong>
                <span>${escapeHtml(formatDelta(item.visits_delta_7d))} / 7d</span>
              </header>
              <p>${escapeHtml(item.author_name || item.author_id || "-")}</p>
              <p>30d ${escapeHtml(formatDelta(item.visits_delta_30d))} / fav 7d ${escapeHtml(formatDelta(item.favorites_delta_7d))} / rate ${escapeHtml(item.favorite_rate == null ? "-" : `${item.favorite_rate}%`)}</p>
              <p>growth ${escapeHtml(formatTrendPercent(item.visits_growth_7d))} / score ${escapeHtml(formatMetric(item.momentum_score))}</p>
              <p>${escapeHtml(item.discovery_reason || "steady growth candidate")}</p>
            </article>
          `,
        )
        .join("")
    : `<div class="detail-empty">No growth data yet.</div>`;
}

function renderRisingNowLeaderboard(items, label = "current collection") {
  $("rising-now-caption").textContent = label;
  $("rising-now-list").innerHTML = (items || []).length
    ? items
        .map(
          (item) => `
            <article class="run-item">
              <header>
                <strong>${escapeHtml(item.name || item.id)}</strong>
                <span>${escapeHtml(formatDelta(item.visits_delta_1d))} / 1d</span>
              </header>
              <p>${escapeHtml(item.author_name || item.author_id || "-")}</p>
              <p>7d ${escapeHtml(formatDelta(item.visits_delta_7d))} / fav 1d ${escapeHtml(formatDelta(item.favorites_delta_1d))} / growth ${escapeHtml(formatTrendPercent(item.visits_growth_1d))}</p>
              <p>${escapeHtml(item.days_since_publication == null ? "-" : `${item.days_since_publication}d old`)} / score ${escapeHtml(formatMetric(item.rising_now_score))}</p>
              <p>${escapeHtml(item.discovery_reason || "short-term breakout candidate")}</p>
            </article>
          `,
        )
        .join("")
    : `<div class="detail-empty">No rising worlds right now.</div>`;
}

function renderNewHotLeaderboard(items, label = "current collection") {
  $("new-hot-caption").textContent = label;
  $("new-hot-list").innerHTML = (items || []).length
    ? items
        .map(
          (item) => `
            <article class="run-item">
              <header>
                <strong>${escapeHtml(item.name || item.id)}</strong>
                <span>${escapeHtml(item.days_since_publication == null ? "-" : `${item.days_since_publication}d old`)}</span>
              </header>
              <p>${escapeHtml(item.author_name || item.author_id || "-")}</p>
              <p>1d ${escapeHtml(formatDelta(item.visits_delta_1d))} / 7d ${escapeHtml(formatDelta(item.visits_delta_7d))} / ${escapeHtml(formatMetric(item.publication_visits_per_day))}/day</p>
              <p>growth ${escapeHtml(formatTrendPercent(item.visits_growth_1d))} / score ${escapeHtml(formatMetric(item.new_hot_score || item.breakout_score))}</p>
              <p>${escapeHtml(item.discovery_reason || "recent breakout candidate")}</p>
            </article>
          `,
        )
        .join("")
    : `<div class="detail-empty">No new breakout worlds yet.</div>`;
}

function renderWorthWatchingLeaderboard(items, label = "current collection") {
  $("worth-watching-caption").textContent = label;
  $("worth-watching-list").innerHTML = (items || []).length
    ? items
        .map(
          (item) => `
            <article class="run-item">
              <header>
                <strong>${escapeHtml(item.name || item.id)}</strong>
                <span>${escapeHtml(item.favorite_rate == null ? "-" : `${item.favorite_rate}% rate`)}</span>
              </header>
              <p>${escapeHtml(item.author_name || item.author_id || "-")}</p>
              <p>heat ${escapeHtml(formatMetric(item.heat))} / popularity ${escapeHtml(formatMetric(item.popularity))} / fav 7d ${escapeHtml(formatDelta(item.favorites_delta_7d))}</p>
              <p>7d ${escapeHtml(formatDelta(item.visits_delta_7d))} / update velocity ${escapeHtml(formatMetric(item.since_update_visits_per_day))}/day / score ${escapeHtml(formatMetric(item.worth_watching_score))}</p>
              <p>${escapeHtml(item.discovery_reason || "worth watching candidate")}</p>
            </article>
          `,
        )
        .join("")
    : `<div class="detail-empty">No standout watch-list worlds yet.</div>`;
}

function renderDormantRevivalLeaderboard(items, label = "current collection") {
  $("dormant-revival-caption").textContent = label;
  $("dormant-revival-list").innerHTML = (items || []).length
    ? items
        .map(
          (item) => `
            <article class="run-item">
              <header>
                <strong>${escapeHtml(item.name || item.id)}</strong>
                <span>${escapeHtml(item.update_effect_tag || "REVIVAL")}</span>
              </header>
              <p>${escapeHtml(item.author_name || item.author_id || "-")}</p>
              <p>7d ${escapeHtml(formatDelta(item.visits_delta_7d))} / 1d ${escapeHtml(formatDelta(item.visits_delta_1d))} / post-update ${escapeHtml(formatDelta(item.since_update_visits_delta))}</p>
              <p>age ${escapeHtml(item.days_since_publication == null ? "-" : `${item.days_since_publication}d`)} / gap ${escapeHtml(item.update_gap_days == null ? "-" : `${item.update_gap_days}d`)} / score ${escapeHtml(formatMetric(item.dormant_revival_score))}</p>
              <p>${escapeHtml(item.discovery_reason || "older world showing renewed traction")}</p>
            </article>
          `,
        )
        .join("")
    : `<div class="detail-empty">No dormant revival signals yet.</div>`;
}

function renderCreatorMomentum(items, label = "current collection") {
  $("creator-momentum-caption").textContent = label;
  $("creator-momentum-list").innerHTML = (items || []).length
    ? items
        .map(
          (item) => `
            <article class="run-item">
              <header>
                <strong>${escapeHtml(item.author_name || item.author_id || "Unknown")}</strong>
                <span>${escapeHtml(formatMetric(item.creator_momentum_score))}</span>
              </header>
              <p>7d ${escapeHtml(formatDelta(item.recent_visits_delta_7d))} / 30d ${escapeHtml(formatDelta(item.recent_visits_delta_30d))} / active ${numberFormat.format(item.active_worlds_30d || 0)}</p>
              <p>breakout ${numberFormat.format(item.breakout_worlds || 0)} / rising ${numberFormat.format(item.rising_worlds || 0)} / watch ${numberFormat.format(item.worth_watching_worlds || 0)}</p>
              <p>avg rate ${escapeHtml(item.average_favorite_rate == null ? "-" : `${item.average_favorite_rate}%`)} / lead ${escapeHtml(item.lead_world_name || "-")}</p>
            </article>
          `,
        )
        .join("")
    : `<div class="detail-empty">No creator momentum data yet.</div>`;
}

function renderAuthorData(items, label = "current collection") {
  $("authors-caption").textContent = label;
  $("authors-list").innerHTML = (items || []).length
    ? items
        .map(
          (item) => `
            <article class="run-item">
              <header>
                <strong>${escapeHtml(item.author_name || item.author_id || "Unknown")}</strong>
                <span>${numberFormat.format(item.world_count || 0)} worlds</span>
              </header>
              <p>${escapeHtml(formatMetric(item.total_visits))} visits / ${escapeHtml(formatMetric(item.total_favorites))} favorites</p>
              <p>30d ${escapeHtml(formatDelta(item.recent_visits_delta_30d))} / active ${numberFormat.format(item.active_worlds_30d || 0)} / avg rate ${escapeHtml(item.average_favorite_rate == null ? "-" : `${item.average_favorite_rate}%`)}</p>
              <p>top ${escapeHtml(item.top_world_name || "-")} / top share ${escapeHtml(item.top_world_share == null ? "-" : `${item.top_world_share}%`)}</p>
            </article>
          `,
        )
        .join("")
    : `<div class="detail-empty">No author data yet.</div>`;
}

function renderAnomalyWatch(payload, label = "current collection") {
  $("anomalies-caption").textContent = label;
  const summary = payload?.summary || {};
  const items = payload?.items || [];
  $("anomalies-summary").innerHTML = [
    { label: "Tracked", value: numberFormat.format(summary.tracked_anomalies || 0) },
    { label: "Ratio >= 2x", value: numberFormat.format(summary.high_ratio || 0) },
    { label: "Strong 1D", value: numberFormat.format(summary.strong_1d || 0) },
    { label: "Avg Ratio", value: summary.avg_ratio == null ? "-" : `${Math.round(Number(summary.avg_ratio) * 100) / 100}x` },
  ]
    .map(
      (item) => `
        <article class="history-stat">
          <span>${escapeHtml(item.label)}</span>
          <strong>${escapeHtml(item.value)}</strong>
        </article>
      `,
    )
    .join("");
  $("anomalies-list").innerHTML = items.length
    ? items
        .map(
          (item) => `
            <article class="run-item">
              <header>
                <strong>${escapeHtml(item.name || item.id)}</strong>
                <span>${escapeHtml(item.anomaly_ratio == null ? "-" : `${Math.round(Number(item.anomaly_ratio) * 100) / 100}x`)}</span>
              </header>
              <p>${escapeHtml(item.author_name || "-")}</p>
              <p>1d ${escapeHtml(formatDelta(item.visits_delta_1d))} / 7d ${escapeHtml(formatDelta(item.visits_delta_7d))} / prev 7d ${escapeHtml(formatDelta(item.visits_delta_prev_7d))}</p>
              <p>fav 7d ${escapeHtml(formatDelta(item.favorites_delta_7d))} / score ${escapeHtml(formatMetric(item.anomaly_score))}</p>
            </article>
          `,
        )
        .join("")
    : `<div class="detail-empty">No notable anomalies yet.</div>`;
}

function renderUpdateEffectiveness(payload, label = "current collection") {
  $("updates-caption").textContent = label;
  const summary = payload?.summary || {};
  const items = payload?.items || [];
  $("updates-summary").innerHTML = [
    { label: "Recent Updates", value: numberFormat.format(summary.tracked_recent_updates || 0) },
    { label: "Active", value: numberFormat.format(summary.active_updates || 0) },
    { label: "Steady", value: numberFormat.format(summary.steady_updates || 0) },
    { label: "Silent", value: numberFormat.format(summary.silent_updates || 0) },
    { label: "Avg Visits / Day", value: summary.avg_visits_per_day == null ? "-" : formatMetric(summary.avg_visits_per_day) },
  ]
    .map(
      (item) => `
        <article class="history-stat">
          <span>${escapeHtml(item.label)}</span>
          <strong>${escapeHtml(item.value)}</strong>
        </article>
      `,
    )
    .join("");
  $("updates-list").innerHTML = items.length
    ? items
        .map(
          (item) => `
            <article class="run-item">
              <header>
                <strong>${escapeHtml(item.name || item.id)}</strong>
                <span>${escapeHtml(item.update_effect_tag || "-")}</span>
              </header>
              <p>${escapeHtml(item.author_name || "-")} / updated ${escapeHtml(formatDateTime(item.updated_at))}</p>
              <p>since update ${escapeHtml(formatDelta(item.since_update_visits_delta))} visits / ${escapeHtml(formatDelta(item.since_update_favorites_delta))} favorites</p>
              <p>1d ${escapeHtml(formatDelta(item.visits_delta_1d))} / 7d ${escapeHtml(formatDelta(item.visits_delta_7d))} / velocity ${escapeHtml(formatMetric(item.since_update_visits_per_day))}/day</p>
            </article>
          `,
        )
        .join("")
    : `<div class="detail-empty">No recent update effectiveness data yet.</div>`;
}

function formatSignalMetric(metric, value) {
  if (value == null) {
    return "-";
  }
  if (metric === "favorite_rate") {
    return `${Math.round(Number(value) * 100) / 100}%`;
  }
  if (metric === "signal_score" || metric === "signal_percentile" || metric === "confidence") {
    return `${Math.round(Number(value) * 10) / 10}`;
  }
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return "-";
  }
  return Number.isInteger(numeric)
    ? numberFormat.format(numeric)
    : numberFormat.format(Math.round(numeric * 100) / 100);
}

function formatCorrelation(coefficient) {
  if (coefficient == null) {
    return "n/a";
  }
  return `${coefficient >= 0 ? "+" : ""}${coefficient.toFixed(3)}`;
}

function correlationTone(coefficient) {
  if (coefficient == null) {
    return "metric-flat";
  }
  if (coefficient > 0.2) {
    return "metric-up";
  }
  if (coefficient < -0.2) {
    return "metric-down";
  }
  return "metric-flat";
}

const GRAPH_TEN_COLOR_PALETTE = [
  "#1f77b4",
  "#ff7f0e",
  "#2ca02c",
  "#d62728",
  "#9467bd",
  "#8c564b",
  "#e377c2",
  "#7f7f7f",
  "#bcbd22",
  "#17becf",
];

function paletteColorForKey(key, palette = GRAPH_TEN_COLOR_PALETTE) {
  return palette[hashString(key) % palette.length];
}

function signalAccentColor(metric) {
  if (metric === "heat") {
    return "var(--heat)";
  }
  if (metric === "popularity") {
    return "var(--popularity)";
  }
  return "var(--accent)";
}

function signalPointColor(point, chart, index) {
  return paletteColorForKey(point?.id || point?.name || `${chart?.key || "signal"}:${index}`);
}

function buildSignalScatterSvg(chart) {
  const points = chart?.points || [];
  if (!points.length) {
    return `<div class="detail-empty">No ${escapeHtml(chart?.title || "signal")} data yet.</div>`;
  }
  const width = 520;
  const height = 280;
  const padding = { top: 18, right: 16, bottom: 38, left: 58 };
  const plotWidth = width - padding.left - padding.right;
  const plotHeight = height - padding.top - padding.bottom;
  const xValues = points.map((point) => Number(point.x)).filter(Number.isFinite);
  const yValues = points.map((point) => Number(point.y)).filter(Number.isFinite);
  const xMin = Math.min(...xValues, 0);
  const xMax = Math.max(...xValues, 1);
  const yRawMin = Math.min(...yValues, 0);
  const yRawMax = Math.max(...yValues, 1);
  const useLogY = ["visits", "favorites"].includes(chart.y_metric) && yRawMax > 100;
  const projectY = (value) => useLogY ? Math.log1p(Math.max(0, value)) : value;
  const yMin = projectY(yRawMin);
  const yMax = Math.max(projectY(yRawMax), yMin + 1);
  const xSpan = Math.max(xMax - xMin, 1);
  const ySpan = Math.max(yMax - yMin, 1);
  const ticks = 4;
  const gridLines = [];
  const circles = [];

  for (let index = 0; index <= ticks; index += 1) {
    const y = padding.top + (plotHeight * index) / ticks;
    const ratio = 1 - index / ticks;
    const rawValue = useLogY
      ? Math.expm1(yMin + ySpan * ratio)
      : yRawMin + (yRawMax - yRawMin) * ratio;
    gridLines.push(`
      <line class="chart-grid" x1="${padding.left}" y1="${y}" x2="${width - padding.right}" y2="${y}"></line>
      <text x="${padding.left - 8}" y="${y + 4}" text-anchor="end" class="chart-label">${escapeHtml(formatSignalMetric(chart.y_metric, rawValue))}</text>
    `);
  }
  for (let index = 0; index <= ticks; index += 1) {
    const x = padding.left + (plotWidth * index) / ticks;
    const rawValue = xMin + (xSpan * index) / ticks;
    gridLines.push(`
      <line class="chart-grid" x1="${x}" y1="${padding.top}" x2="${x}" y2="${height - padding.bottom}"></line>
      <text x="${x}" y="${height - padding.bottom + 18}" text-anchor="middle" class="chart-label">${escapeHtml(formatSignalMetric(chart.x_metric, rawValue))}</text>
    `);
  }

  const maxVisits = Math.max(...points.map((point) => toNumber(point.visits)), 1);
  for (const point of points) {
    const cx = padding.left + ((Number(point.x) - xMin) / xSpan) * plotWidth;
    const cy = padding.top + (1 - ((projectY(Number(point.y)) - yMin) / ySpan)) * plotHeight;
    const radius = 3 + Math.pow(Math.max(0, toNumber(point.visits)) / maxVisits, 0.45) * 7;
    const pointColor = signalPointColor(point, chart, circles.length);
    circles.push(`
      <circle
        cx="${cx}"
        cy="${cy}"
        r="${radius}"
        fill="${pointColor}"
        fill-opacity="0.68"
        stroke="${signalAccentColor(chart.x_metric)}"
        stroke-width="1"
      >
        <title>${escapeHtml(point.name || point.id)} | ${escapeHtml(point.author_name || "-")}
${escapeHtml(chart.x_label)}: ${escapeHtml(formatSignalMetric(chart.x_metric, point.x))}
${escapeHtml(chart.y_label)}: ${escapeHtml(formatSignalMetric(chart.y_metric, point.y))}
Visits: ${escapeHtml(formatSignalMetric("visits", point.visits))}
Favorites: ${escapeHtml(formatSignalMetric("favorites", point.favorites))}
Favorite Rate: ${escapeHtml(formatSignalMetric("favorite_rate", point.favorite_rate))}</title>
      </circle>
    `);
  }

  return `
    <svg viewBox="0 0 ${width} ${height}" role="img" aria-label="${escapeHtml(chart.title)} scatter plot">
      ${gridLines.join("")}
      <line class="axis" x1="${padding.left}" y1="${height - padding.bottom}" x2="${width - padding.right}" y2="${height - padding.bottom}"></line>
      <line class="axis" x1="${padding.left}" y1="${padding.top}" x2="${padding.left}" y2="${height - padding.bottom}"></line>
      ${circles.join("")}
      <text x="${width / 2}" y="${height - 6}" text-anchor="middle" class="chart-label">${escapeHtml(chart.x_label)}</text>
      <text x="18" y="${height / 2}" text-anchor="middle" transform="rotate(-90 18 ${height / 2})" class="chart-label">${escapeHtml(chart.y_label)}${useLogY ? " (log)" : ""}</text>
    </svg>
  `;
}

function renderSignalAnalysis(signals, label = "current collection") {
  $("signal-caption").textContent = `${label} / heat-popularity relationships`;
  if (!signals) {
    $("signal-summary").innerHTML = "";
    $("signal-correlation-list").innerHTML = `<div class="detail-empty">No signal analysis yet.</div>`;
    $("signal-chart-grid").innerHTML = "";
    $("signal-leaderboards").innerHTML = "";
    return;
  }

  const summary = signals.summary || {};
  const correlations = signals.correlations || [];
  const charts = signals.charts || [];
  const boards = signals.leaderboards || {};

  const summaryCards = [
    { label: "Worlds", value: numberFormat.format(summary.world_count || 0) },
    { label: "Heat Coverage", value: `${numberFormat.format(summary.heat_count || 0)} worlds` },
    { label: "Popularity Coverage", value: `${numberFormat.format(summary.popularity_count || 0)} worlds` },
    { label: "Avg Heat", value: formatSignalMetric("heat", summary.avg_heat) },
    { label: "Avg Popularity", value: formatSignalMetric("popularity", summary.avg_popularity) },
    { label: "Avg Favorite Rate", value: formatSignalMetric("favorite_rate", summary.avg_favorite_rate) },
  ];
  $("signal-summary").innerHTML = summaryCards
    .map((item) => `
      <article class="history-stat">
        <span>${escapeHtml(item.label)}</span>
        <strong>${escapeHtml(item.value)}</strong>
      </article>
    `)
    .join("");

  $("signal-correlation-list").innerHTML = correlations.length
    ? correlations
        .map((item) => `
          <article class="signal-correlation-item">
            <header>
              <strong>${escapeHtml(item.label)}</strong>
              <span>${numberFormat.format(item.sample_size || 0)} worlds</span>
            </header>
            <p class="${correlationTone(item.coefficient)}">${escapeHtml(formatCorrelation(item.coefficient))}</p>
            <p>${escapeHtml(item.strength || "insufficient")} relationship</p>
          </article>
        `)
        .join("")
    : `<div class="detail-empty">Not enough signal samples for correlation yet.</div>`;

  $("signal-chart-grid").innerHTML = charts.length
    ? charts
        .map((chart) => `
          <section class="chart-panel signal-chart-panel">
            <div class="chart-head">
              <h4>${escapeHtml(chart.title)}</h4>
              <span>${numberFormat.format(chart.sample_size || 0)} worlds</span>
            </div>
            ${buildSignalScatterSvg(chart)}
          </section>
        `)
        .join("")
    : `<div class="detail-empty">No signal charts yet.</div>`;

  const leaderboardConfigs = [
    {
      key: "heat_leaders",
      title: "Heat Leaders",
      subtitle: "Highest current heat",
      line: (item) => `heat ${formatSignalMetric("heat", item.heat)} / visits ${formatSignalMetric("visits", item.visits)} / rate ${formatSignalMetric("favorite_rate", item.favorite_rate)}`,
    },
    {
      key: "popularity_leaders",
      title: "Popularity Leaders",
      subtitle: "Highest current popularity",
      line: (item) => `popularity ${formatSignalMetric("popularity", item.popularity)} / visits ${formatSignalMetric("visits", item.visits)} / rate ${formatSignalMetric("favorite_rate", item.favorite_rate)}`,
    },
    {
      key: "signal_efficiency",
      title: "Signal Efficiency",
      subtitle: "Bucket percentile weighted by visit confidence",
      line: (item) => `score ${formatSignalMetric("signal_score", item.signal_efficiency_score)} / heat pct ${item.heat_percentile == null ? "-" : `${formatSignalMetric("signal_percentile", item.heat_percentile)}%`} / pop pct ${item.popularity_percentile == null ? "-" : `${formatSignalMetric("signal_percentile", item.popularity_percentile)}%`} / conf ${item.confidence_weight == null ? "-" : `${formatSignalMetric("confidence", item.confidence_weight)}%`} / ${escapeHtml(item.visit_bucket || "-")} / visits ${formatSignalMetric("visits", item.visits)}`,
    },
  ];
  $("signal-leaderboards").innerHTML = leaderboardConfigs
    .map((config) => {
      const items = boards[config.key] || [];
      return `
        <section class="signal-board">
          <div class="chart-head">
            <h4>${escapeHtml(config.title)}</h4>
            <span>${escapeHtml(config.subtitle)}</span>
          </div>
          <div class="runs-list">
            ${
              items.length
                ? items
                    .map((item) => `
                      <article class="run-item">
                        <header>
                          <strong>${escapeHtml(item.name || item.id)}</strong>
                          <span>${escapeHtml(item.author_name || item.author_id || "-")}</span>
                        </header>
                        <p>${escapeHtml(config.line(item))}</p>
                      </article>
                    `)
                    .join("")
                : `<div class="detail-empty">No data yet.</div>`
            }
          </div>
        </section>
      `;
    })
    .join("");
}

function renderPerformance(payload) {
  state.starriverPerformance = payload || null;
  $("performance-caption").textContent = payload?.label || state.source || "db:all";
  const items = payload?.performance?.items || [];
  const summary = payload?.performance?.summary || {};
  $("performance-summary").innerHTML = [
    { label: "Recent Updates", value: numberFormat.format(summary.tracked_recent_updates || items.length) },
    { label: "Active Updates", value: numberFormat.format(summary.active_updates || 0) },
    { label: "Steady Updates", value: numberFormat.format(summary.steady_updates || 0) },
    { label: "Silent Updates", value: numberFormat.format(summary.silent_updates || 0) },
    { label: "Avg Visits / Day", value: summary.avg_visits_per_day == null ? "-" : formatMetric(summary.avg_visits_per_day) },
  ]
    .map(
      (item) => `
        <article class="history-stat">
          <span>${escapeHtml(item.label)}</span>
          <strong>${escapeHtml(item.value)}</strong>
        </article>
      `,
    )
    .join("");
  $("performance-list").innerHTML = items.length
    ? items
        .map(
          (item) => `
            <article class="run-item">
              <header>
                <strong>${escapeHtml(item.name || item.id)}</strong>
                <span>${escapeHtml(item.update_effect_tag || "-")}</span>
              </header>
              <p>updated ${escapeHtml(formatDateTime(item.updated_at))} / ${numberFormat.format(item.days_since_update || 0)}d ago</p>
              <p>visits ${escapeHtml(formatDelta(item.since_update_visits_delta))} / favorites ${escapeHtml(formatDelta(item.since_update_favorites_delta))} / gap ${escapeHtml(item.update_gap_days == null ? "-" : `${item.update_gap_days}d`)}</p>
              <p>1d ${escapeHtml(formatDelta(item.visits_delta_1d))} / 7d ${escapeHtml(formatDelta(item.visits_delta_7d))} / velocity ${escapeHtml(formatMetric(item.since_update_visits_per_day))}/day</p>
            </article>
          `,
        )
        .join("")
    : `<div class="detail-empty">No update performance data yet.</div>`;
}

function renderJobs(items) {
  state.jobs = items;
  renderAutoSyncControls();
  populateReviewJobSelect([]);
  $("jobs-list").innerHTML = items
    .map(
      (item) => `
        <article class="job-item">
          <header>
            <strong>${escapeHtml(item.label)}</strong>
            <span class="${item.ready ? "status-ok" : "status-warn"}">${item.ready ? "ready" : "blocked"}</span>
          </header>
          <p>${escapeHtml(item.type)} / ${escapeHtml(item.source)}</p>
          <p>${escapeHtml(item.reason || "config ok")}</p>
          <div class="auth-actions">
            <button class="button ${item.ready ? "" : "subtle"}" type="button" data-job-run="${escapeHtml(item.job_key)}" ${item.ready ? "" : "disabled"}>Run Job</button>
            <button class="button subtle" type="button" data-job-edit="${escapeHtml(item.job_key)}">Edit</button>
            <button class="button subtle" type="button" data-job-delete="${escapeHtml(item.job_key)}">Delete Job</button>
          </div>
        </article>
      `,
    )
    .join("");

  for (const button of document.querySelectorAll("[data-job-run]")) {
    button.addEventListener("click", async () => {
      const jobKey = button.dataset.jobRun;
      const original = button.textContent;
      button.disabled = true;
      button.textContent = "Running...";
      try {
        await runNamedJob(jobKey);
      } catch (error) {
        window.alert(error.message);
      } finally {
        button.disabled = false;
        button.textContent = original;
      }
    });
  }

  for (const button of document.querySelectorAll("[data-job-delete]")) {
    button.addEventListener("click", async () => {
      const jobKey = button.dataset.jobDelete;
      if (!window.confirm(`Delete job ${jobKey} and its matching topic? Stored run/world history is kept.`)) {
        return;
      }
      try {
        await fetchJson(`/api/v1/jobs/${encodeURIComponent(jobKey)}?delete_topic=1`, { method: "DELETE" });
        if (state.activeTopic === jobKey) {
          state.activeTopic = null;
        }
        await refreshCurrentScopeData({
          refreshSources: true,
          refreshAncillary: true,
          refreshAutoSync: true,
        });
      } catch (error) {
        window.alert(error.message);
      }
    });
  }

  for (const button of document.querySelectorAll("[data-job-edit]")) {
    button.addEventListener("click", () => {
      const jobKey = button.dataset.jobEdit;
      const job = state.jobs.find((item) => item.job_key === jobKey);
      if (job) {
        beginJobEdit(job);
      }
    });
  }
}

function renderJobCreateMode() {
  const jobType = $("job-create-type").value;
  $("job-create-keywords-wrap").classList.toggle("hidden", jobType !== "keywords");
  $("job-create-user-wrap").classList.toggle("hidden", jobType !== "user");
  const isWorldSearch = jobType === "world_search";
  $("job-create-world-search-wrap").classList.toggle("hidden", !isWorldSearch);
  $("job-create-tags-wrap").classList.toggle("hidden", !isWorldSearch);
  $("job-create-notags-wrap").classList.toggle("hidden", !isWorldSearch);
  $("job-create-sort-wrap").classList.toggle("hidden", !isWorldSearch);
  $("job-create-active-wrap").classList.toggle("hidden", !isWorldSearch);
  $("job-create-featured-wrap").classList.toggle("hidden", !isWorldSearch);
}

function resetJobEditor() {
  $("job-edit-mode").value = "create";
  $("job-create-form").reset();
  $("job-create-key").disabled = false;
  $("job-create-limit").value = "50";
  $("job-create-button").textContent = "Add Job + Topic";
  $("job-edit-cancel-button").classList.add("hidden");
  $("job-create-status").textContent = "Creates a new sync job and a matching topic that follows its DB source.";
  $("job-create-type").value = "keywords";
  renderJobCreateMode();
}

function beginJobEdit(job) {
  $("job-edit-mode").value = job.job_key;
  $("job-create-label").value = job.label || "";
  $("job-create-key").value = job.job_key || "";
  $("job-create-key").disabled = true;
  $("job-create-type").value = job.type || "keywords";
  $("job-create-limit").value = String(job.type === "keywords" ? (job.limit_per_keyword || 50) : (job.limit || 50));
  $("job-create-keywords").value = (job.keywords || []).join(", ");
  $("job-create-user-id").value = job.user_id || "";
  $("job-create-search").value = job.search || "";
  $("job-create-tags").value = (job.tags || []).join(", ");
  $("job-create-notags").value = (job.notags || []).join(", ");
  $("job-create-sort").value = job.sort || "popularity";
  $("job-create-active").value = job.active ? "true" : "false";
  $("job-create-featured").value = job.featured == null ? "" : String(Boolean(job.featured));
  $("job-create-button").textContent = "Update Job";
  $("job-edit-cancel-button").classList.remove("hidden");
  $("job-create-status").textContent = `Editing ${job.label} (${job.job_key}).`;
  renderJobCreateMode();
}

function renderRuns(items) {
  $("runs-list").innerHTML = items
    .map(
      (item) => `
        <article class="run-item">
          <header>
            <strong>${escapeHtml(item.label)}</strong>
            <span>${escapeHtml(item.status)}</span>
          </header>
          <p>${escapeHtml(formatDateTime(item.started_at))}</p>
          <p>${numberFormat.format(item.world_count || 0)} worlds</p>
          ${item.error_text ? `<p class="metric-down">${escapeHtml(item.error_text)}</p>` : ""}
        </article>
      `,
    )
    .join("");
}

function renderRateLimits(payload) {
  const summary = payload?.summary || {};
  const items = payload?.items || [];
  const remainingSeconds = Number(summary.active_cooldown_remaining_seconds || 0);
  const activeUntil = summary.active_cooldown_until ? formatDateTime(summary.active_cooldown_until) : "-";
  $("rate-limit-caption").textContent = remainingSeconds > 0
    ? `cooldown active until ${activeUntil}`
    : "recent 429 events";
  $("rate-limit-summary").innerHTML = `
    <article class="history-stat"><span>429 in 24h</span><strong>${numberFormat.format(summary.count_24h || 0)}</strong></article>
    <article class="history-stat"><span>Cooldown</span><strong>${remainingSeconds > 0 ? `${numberFormat.format(Math.ceil(remainingSeconds / 60))} min` : "inactive"}</strong></article>
    <article class="history-stat"><span>Retry-After</span><strong>${summary.latest_retry_after_seconds ? `${numberFormat.format(summary.latest_retry_after_seconds)}s` : "-"}</strong></article>
  `;
  $("rate-limit-list").innerHTML = `
    <article class="run-item">
      <header><strong>Strategy</strong><span>${remainingSeconds > 0 ? "cooldown" : "monitor"}</span></header>
      <p>${escapeHtml(summary.strategy_hint || "No recent 429 events recorded.")}</p>
    </article>
    ${items.length
      ? items.map((item) => `
        <article class="run-item">
          <header>
            <strong>${escapeHtml(item.job_key || item.query_value || item.source_key || "rate limit")}</strong>
            <span>${escapeHtml(item.trigger_type || "manual")}</span>
          </header>
          <p>${escapeHtml(formatDateTime(item.event_at))}</p>
          <p>retry-after ${escapeHtml(item.retry_after_seconds ? `${item.retry_after_seconds}s` : "-")} / cooldown ${escapeHtml(item.cooldown_seconds ? `${Math.ceil(item.cooldown_seconds / 60)} min` : "-")}</p>
          <p>${escapeHtml(item.error_text || "")}</p>
        </article>
      `).join("")
      : `<div class="detail-empty">No 429 events recorded yet.</div>`}
  `;
}

function renderQueryAnalytics(payload) {
  const summary = payload?.summary || {};
  const items = payload?.items || [];
  $("query-analytics-summary").innerHTML = `
    <article class="history-stat"><span>Runs</span><strong>${numberFormat.format(summary.run_count || 0)}</strong></article>
    <article class="history-stat"><span>Tracked Queries</span><strong>${numberFormat.format(summary.query_count || 0)}</strong></article>
    <article class="history-stat"><span>Tracked Hits</span><strong>${numberFormat.format(summary.tracked_world_hits || 0)}</strong></article>
    <article class="history-stat"><span>New Worlds</span><strong>${numberFormat.format(summary.new_world_hits || 0)}</strong></article>
  `;
  $("query-analytics-list").innerHTML = items.length
    ? items
        .map((run) => {
          const queries = run.queries || [];
          const queryBody = queries.length
            ? queries
                .map((query) => {
                  const topics = (query.top_topics || []).length
                    ? query.top_topics
                        .map((topic) => `<span class="diff-chip">${escapeHtml(topic.label)} ${numberFormat.format(topic.count || 0)}</span>`)
                        .join("")
                    : `<span class="diff-chip">no topic match</span>`;
                  const samples = (query.sample_hits || []).length
                    ? query.sample_hits
                        .map((hit) => {
                          const topicText = (hit.topics || []).length
                            ? hit.topics.map((topic) => topic.label || topic.topic_key).join(", ")
                            : "unmatched";
                          return `<li>${escapeHtml(hit.world_name || hit.world_id)} <small>${escapeHtml(topicText)}</small>${hit.is_new_global ? " <strong>new</strong>" : ""}</li>`;
                        })
                        .join("")
                    : `<li>No kept worlds.</li>`;
                  return `
                    <div class="job-item">
                      <header>
                        <strong>${escapeHtml(query.query_label || query.query_value || "-")}</strong>
                        <span>${escapeHtml(query.query_kind || "query")}</span>
                      </header>
                      ${query.legacy_inferred ? `<p>Legacy run: query label inferred from current config; no per-world attribution was stored at that time.</p>` : ""}
                      <p>${numberFormat.format(query.kept_count || 0)} kept / ${numberFormat.format(query.result_count || 0)} raw / ${numberFormat.format(query.new_world_count || 0)} new</p>
                      <div class="event-delta-row">${topics}</div>
                      <ul>${samples}</ul>
                    </div>
                  `;
                })
                .join("")
            : `<div class="detail-empty">No per-query tracking for this older run.</div>`;
          return `
            <article class="run-item">
              <header>
                <strong>${escapeHtml(run.label || run.source || "run")}</strong>
                <span>${escapeHtml(run.tracking_status || run.status || "-")}</span>
              </header>
              <p>${escapeHtml(formatDateTime(run.started_at))} / ${numberFormat.format(run.world_count || 0)} worlds</p>
              <p>${escapeHtml(run.query_label || "no query label")} / ${numberFormat.format(run.tracked_query_count || 0)} tracked queries</p>
              ${queryBody}
            </article>
          `;
        })
        .join("")
    : `<div class="detail-empty">No query analytics yet.</div>`;
}

function eventTypeLabel(type) {
  if (type === "traffic_spike") {
    return "Traffic Spike";
  }
  if (type === "new_upload") {
    return "New Upload";
  }
  if (type === "new_update") {
    return "New Update";
  }
  return type || "event";
}

function renderEventFeedMeta() {
  const summary = $("events-summary");
  const caption = $("events-caption");
  const reloadButton = $("events-reload-button");
  if (!summary || !caption || !reloadButton) {
    return;
  }
  const items = (state.events.items || []).filter((item) => state.events.filter === "all" || item.type === state.events.filter);
  const counts = {
    total: items.length,
    spikes: items.filter((item) => item.type === "traffic_spike").length,
    uploads: items.filter((item) => item.type === "new_upload").length,
    updates: items.filter((item) => item.type === "new_update").length,
  };
  const loadedLabel = state.events.loadedAt ? formatDateTime(state.events.loadedAt) : "-";
  const refreshHint = state.events.pendingRefresh ? " / new sync detected, press Reload" : "";
  caption.textContent = `${numberFormat.format(counts.total)} events / ${numberFormat.format(state.events.days || 7)} day window / loaded ${loadedLabel}${refreshHint}`;
  reloadButton.textContent = state.events.pendingRefresh ? "Reload New Events" : "Reload";
  summary.innerHTML = `
    <article class="history-stat"><span>Total</span><strong>${numberFormat.format(counts.total)}</strong></article>
    <article class="history-stat"><span>Traffic Spike</span><strong>${numberFormat.format(counts.spikes)}</strong></article>
    <article class="history-stat"><span>Uploads / Updates</span><strong>${numberFormat.format(counts.uploads + counts.updates)}</strong></article>
  `;
}

function renderEventFeed() {
  const target = $("events-list");
  if (!target) {
    return;
  }
  renderEventFeedMeta();
  const items = (state.events.items || []).filter((item) => state.events.filter === "all" || item.type === state.events.filter);
  target.innerHTML = items.length
    ? items
        .map(
          (item) => `
            <article class="run-item event-item">
              <header>
                <strong>${escapeHtml(item.name || item.world_id || item.label)}</strong>
                <span class="event-badge event-badge-${escapeHtml(item.type || "generic")}">${escapeHtml(eventTypeLabel(item.type))}</span>
              </header>
              <p>${escapeHtml(item.label || item.source || "-")} / ${escapeHtml(item.author_name || "unknown author")}</p>
              <p>${escapeHtml(formatDateTime(item.occurred_at || item.detected_at))}</p>
              <p>${escapeHtml(item.summary || "")}</p>
              <div class="event-delta-row">
                ${item.delta && item.delta.visits_delta ? `<span class="diff-chip">${escapeHtml(`visits ${formatDelta(item.delta.visits_delta)}`)}</span>` : ""}
                ${item.delta && item.delta.favorites_delta ? `<span class="diff-chip">${escapeHtml(`fav ${formatDelta(item.delta.favorites_delta)}`)}</span>` : ""}
                ${item.delta && item.delta.heat_delta ? `<span class="diff-chip">${escapeHtml(`heat ${formatDelta(item.delta.heat_delta)}`)}</span>` : ""}
                ${item.delta && item.delta.popularity_delta ? `<span class="diff-chip">${escapeHtml(`pop ${formatDelta(item.delta.popularity_delta)}`)}</span>` : ""}
              </div>
              <div class="auth-actions">
                ${item.world_url ? `<a class="button subtle" href="${escapeHtml(item.world_url)}" target="_blank" rel="noreferrer">Open World</a>` : ""}
                ${item.source && item.world_id ? `<button class="button subtle" type="button" data-event-source="${escapeHtml(item.source)}" data-event-world="${escapeHtml(item.world_id)}">Open In Explore</button>` : ""}
              </div>
            </article>
          `,
        )
        .join("")
    : `<div class="detail-empty">No recent events in this window.</div>`;

  for (const button of document.querySelectorAll("[data-event-source][data-event-world]")) {
    button.addEventListener("click", async () => {
      const source = button.dataset.eventSource;
      const worldId = button.dataset.eventWorld;
      if (!source || !worldId) {
        return;
      }
      state.activeTopic = null;
      await loadSources(source);
      await loadCollection(source);
      const matched = (state.worlds || []).find((item) => item.id === worldId);
      if (matched) {
        state.selectedWorld = matched;
        renderDetail(matched);
      }
      state.page = "discover";
      renderPage();
    });
  }
}

async function loadEventFeed(force = false) {
  const target = $("events-list");
  if (!target) {
    return;
  }
  markPanelLoading("monitorEvents", {
    page: "monitor",
    section: "events",
  });
  if (!force && state.events.loadedAt) {
    if (withPanelRender("monitorEvents", () => renderEventFeed(), (error) => {
      target.innerHTML = buildPanelStateMarkup("Monitor Events", error.message);
    }, { page: "monitor", section: "events" })) {
      markPanelSuccess("monitorEvents", {
        page: "monitor",
        section: "events",
        status: (state.events.items || []).length ? "ready" : "empty",
      });
    }
    return;
  }
  target.innerHTML = buildPanelStateMarkup("Monitor Events", "Loading event feed...", "loading");
  const params = new URLSearchParams({
    limit: "60",
    days: String(state.events.days || 7),
  });
  try {
    const { data } = await fetchJson(`/api/v1/events?${params.toString()}`, undefined, {
      panelKey: "monitorEvents",
      page: "monitor",
      section: "events",
    });
    state.events.items = data.items || [];
    state.events.loadedAt = data.generated_at || new Date().toISOString();
    state.events.pendingRefresh = false;
    if (withPanelRender("monitorEvents", () => renderEventFeed(), (error) => {
      target.innerHTML = buildPanelStateMarkup("Monitor Events", error.message);
    }, { page: "monitor", section: "events" })) {
      markPanelSuccess("monitorEvents", {
        page: "monitor",
        section: "events",
        status: (data.items || []).length ? "ready" : "empty",
      });
    }
  } catch (error) {
    markPanelError("monitorEvents", error, {
      page: "monitor",
      section: "events",
    });
    target.innerHTML = buildPanelStateMarkup("Monitor Events", error.message);
  }
}

function findDiagnosticsForJob(items, jobKey) {
  return (items || []).find((item) => item.job_key === jobKey) || null;
}

function computeWorldSpike(item) {
  const latestVisits = toNumber(item?.latest?.visits);
  const previousVisits = toNumber(item?.previous?.visits);
  const latestFavorites = toNumber(item?.latest?.favorites);
  const previousFavorites = toNumber(item?.previous?.favorites);
  const visitsDelta = toNumber(item?.visits_delta);
  const favoritesDelta = toNumber(item?.favorites_delta);
  const visitsGrowth = previousVisits > 0 ? visitsDelta / previousVisits : null;
  const favoritesGrowth = previousFavorites > 0 ? favoritesDelta / previousFavorites : null;
  return {
    visitsDelta,
    favoritesDelta,
    visitsGrowth,
    favoritesGrowth,
    score:
      (visitsGrowth != null ? visitsGrowth * 100 : visitsDelta > 0 ? 25 : 0)
      + Math.max(0, favoritesDelta) * 6
      + Math.max(0, toNumber(item?.heat_delta)) * 4
      + Math.max(0, toNumber(item?.popularity_delta)) * 4,
  };
}

function selectNotableWorldChanges(diff) {
  const candidates = (diff?.changed_worlds || [])
    .map((item) => ({ ...item, spike: computeWorldSpike(item) }))
    .filter((item) => {
      const { visitsDelta, favoritesDelta, visitsGrowth, favoritesGrowth } = item.spike;
      return (
        (visitsDelta >= 25 && visitsGrowth != null && visitsGrowth >= 0.45)
        || (visitsDelta >= 60)
        || (favoritesDelta >= 6 && favoritesGrowth != null && favoritesGrowth >= 0.35)
        || (toNumber(item.heat_delta) >= 3 && visitsDelta >= 15)
        || (toNumber(item.popularity_delta) >= 3 && visitsDelta >= 15)
      );
    })
    .sort((left, right) => right.spike.score - left.spike.score);
  return candidates.slice(0, 3);
}

function describeNotableWorld(item) {
  const parts = [];
  if (toNumber(item.visits_delta) > 0) {
    parts.push(`visits ${formatDelta(item.visits_delta)}`);
  }
  if (toNumber(item.favorites_delta) > 0) {
    parts.push(`fav ${formatDelta(item.favorites_delta)}`);
  }
  if (toNumber(item.heat_delta) > 0) {
    parts.push(`heat ${formatDelta(item.heat_delta)}`);
  }
  if (toNumber(item.popularity_delta) > 0) {
    parts.push(`pop ${formatDelta(item.popularity_delta)}`);
  }
  return `${item.name || item.id}: ${parts.join(" / ") || "notable movement"}`;
}

function notifyForRun(run, diagnosticsItems) {
  if (!run?.job_key) {
    return;
  }
  if (run.status === "failed") {
    emitBrowserNotification(`Sync failed: ${run.label}`, {
      body: run.error_text || "Check Auto Sync or Recent Runs for details.",
      tag: `sync-failed-${run.id}`,
    });
    return;
  }
  if (run.status !== "completed") {
    return;
  }

  const diagnostics = findDiagnosticsForJob(diagnosticsItems, run.job_key);
  const diff = diagnostics?.source_diff || null;
  const notableWorlds = diff?.status === "ok" ? selectNotableWorldChanges(diff) : [];
  const summaryLine = notableWorlds.length
    ? `${numberFormat.format(run.world_count || 0)} worlds. ${notableWorlds.length} notable change(s).`
    : `${numberFormat.format(run.world_count || 0)} worlds synced successfully.`;
  emitBrowserNotification(`Sync completed: ${run.label}`, {
    body: summaryLine,
    tag: `sync-completed-${run.id}`,
  });

  if (diff?.status === "ok" && diff.latest_run?.id === run.id && notableWorlds.length) {
    emitBrowserNotification(`Notable world changes: ${run.label}`, {
      body: notableWorlds.map(describeNotableWorld).join(" | "),
      tag: `sync-spikes-${run.id}`,
    });
  }
}

function processRunNotifications(runs, diagnosticsItems) {
  const latestRunsByJob = {};
  for (const run of runs || []) {
    if (!run.job_key || latestRunsByJob[run.job_key]) {
      continue;
    }
    latestRunsByJob[run.job_key] = run;
  }

  if (!state.notifications.primed) {
    for (const [jobKey, run] of Object.entries(latestRunsByJob)) {
      state.notifications.latestRunIds[jobKey] = run.id;
      const diff = findDiagnosticsForJob(diagnosticsItems, jobKey)?.source_diff;
      if (diff?.latest_run?.id) {
        state.notifications.latestDiffRunIds[jobKey] = diff.latest_run.id;
      }
    }
    state.notifications.primed = true;
    saveNotificationState();
    return;
  }

  let changed = false;
  for (const [jobKey, run] of Object.entries(latestRunsByJob)) {
    const previousId = Number(state.notifications.latestRunIds[jobKey] || 0);
    if (Number(run.id) > previousId) {
      notifyForRun(run, diagnosticsItems);
      state.notifications.latestRunIds[jobKey] = run.id;
      const diff = findDiagnosticsForJob(diagnosticsItems, jobKey)?.source_diff;
      if (diff?.latest_run?.id) {
        state.notifications.latestDiffRunIds[jobKey] = diff.latest_run.id;
      }
      changed = true;
    }
  }
  if (changed) {
    saveNotificationState();
    if (state.events.loadedAt) {
      state.events.pendingRefresh = true;
      renderEventFeedMeta();
    }
  }
}

function renderJobDiagnostics(items) {
  $("job-diagnostics-list").innerHTML = (items || []).length
    ? items
        .map((item) => {
          const keywordsLine = item.keyword_count
            ? `${numberFormat.format(item.keyword_count)} keywords: ${(item.keywords || []).slice(0, 4).join(", ")}${item.keyword_count > 4 ? "..." : ""}`
            : item.type === "user"
              ? `creator job / limit ${numberFormat.format(item.limit || 0)}`
              : item.type === "world_search"
                ? `world search / ${(item.tags || []).length ? `tags: ${(item.tags || []).join(", ")}` : `sort ${item.sort || "popularity"}`}${item.active ? " / active" : ""}`
                : "no keywords";
          const latestRunLine = item.latest_completed_run
            ? `${formatDateTime(item.latest_completed_run.started_at)} / ${numberFormat.format(item.latest_completed_run.world_count || 0)} worlds`
            : "no completed run yet";
          const runButtonDisabled = item.rate_limit_active ? "disabled" : "";
          const runButtonText = item.rate_limit_active ? "限速中" : "運行";
          return `
            <article class="diagnostic-item">
              <header>
                <strong>${escapeHtml(item.label)}</strong>
                <span class="${item.ready ? "status-ok" : "status-warn"}">${item.ready ? "ready" : "blocked"}</span>
              </header>
              <p>${escapeHtml(item.source)} / ${escapeHtml(item.type)}</p>
              <div class="diagnostic-grid">
                <span>${numberFormat.format(item.current_world_count || 0)} worlds</span>
                <span>${numberFormat.format(item.current_creator_count || 0)} creators</span>
                <span>${numberFormat.format(item.creator_whitelist_count || 0)} whitelist</span>
                <span>${numberFormat.format(item.creator_blacklist_count || 0)} creator blacklist</span>
              </div>
              <p>${escapeHtml(keywordsLine)}</p>
              <p>${escapeHtml(item.reason || latestRunLine)}</p>
              <div class="job-run-controls">
                <label class="bypass-toggle">
                  <input type="checkbox" id="bypass-rate-limit-${item.job_key}" ${item.rate_limit_active ? "" : "disabled"}>
                  <span>強制執行 (繞過限速)</span>
                </label>
                <button class="button run-job-button" data-job-key="${item.job_key}" ${runButtonDisabled}>
                  ${runButtonText}
                </button>
              </div>
            </article>
          `;
        })
        .join("")
    : `<div class="detail-empty">No job diagnostics yet.</div>`;

  // Add event listeners for run job buttons
  for (const button of document.querySelectorAll(".run-job-button")) {
    button.addEventListener("click", async () => {
      const jobKey = button.dataset.jobKey;
      const original = button.textContent;
      button.disabled = true;
      button.textContent = "Running...";
      try {
        await runNamedJob(jobKey);
      } catch (error) {
        window.alert(error.message);
      } finally {
        button.disabled = false;
        button.textContent = original;
      }
    });
  }
}

function renderSourceDiffs(items) {
  const diffs = (items || [])
    .map((item) => ({
      job_key: item.job_key,
      label: item.label,
      source_diff: item.source_diff || null,
    }))
    .filter((item) => item.source_diff);
  $("source-diff-list").innerHTML = diffs.length
    ? diffs
        .map((item) => {
          const diff = item.source_diff;
          if (diff.status !== "ok") {
            return `
              <article class="source-diff-item">
                <header>
                  <strong>${escapeHtml(item.label)}</strong>
                  <span>${escapeHtml(diff.status)}</span>
                </header>
                <p>${escapeHtml(diff.message || "Need more history.")}</p>
              </article>
            `;
          }
          const changedWorlds = (diff.changed_worlds || []).slice(0, 3);
          const addedWorlds = (diff.added_worlds || []).slice(0, 2);
          const removedWorlds = (diff.removed_worlds || []).slice(0, 2);
          return `
            <article class="source-diff-item">
              <header>
                <strong>${escapeHtml(item.label)}</strong>
                <span>${escapeHtml(formatDateTime(diff.latest_run?.started_at))}</span>
              </header>
              <div class="diagnostic-grid">
                <span>+${numberFormat.format(diff.added_count || 0)} added</span>
                <span>-${numberFormat.format(diff.removed_count || 0)} removed</span>
                <span>${numberFormat.format(diff.changed_count || 0)} changed</span>
                <span>${escapeHtml(formatDateTime(diff.previous_run?.started_at))}</span>
              </div>
              <div class="diff-world-list">
                ${addedWorlds.map((world) => `<span class="diff-chip diff-chip-added">+ ${escapeHtml(world.name || world.id)}</span>`).join("")}
                ${removedWorlds.map((world) => `<span class="diff-chip diff-chip-removed">- ${escapeHtml(world.name || world.id)}</span>`).join("")}
              </div>
              <div class="diff-world-list">
                ${changedWorlds.map((world) => `<span class="diff-chip">~ ${escapeHtml(world.name || world.id)} (${escapeHtml(formatDelta(world.visits_delta || 0))})</span>`).join("")}
              </div>
            </article>
          `;
        })
        .join("")
    : `<div class="detail-empty">No source diff yet.</div>`;
}

function renderTopics(items) {
  const activeItems = (items || []).filter((item) => item.is_active !== false);
  state.topics = activeItems;
  const groups = [
    { topicType: "job", label: "Tracked Views" },
    { topicType: "view", label: "Saved Views" },
  ];
  $("topics-list").innerHTML = groups
    .map((group) => {
      const groupItems = activeItems.filter((item) => (item.topic_type || "job") === group.topicType);
      if (!groupItems.length) {
        return "";
      }
      return `
        <section class="topic-section">
          <div class="topic-section-head">
            <strong>${escapeHtml(group.label)}</strong>
            <span>${numberFormat.format(groupItems.length)} topics</span>
          </div>
          <div class="topic-section-grid">
            ${groupItems
              .map(
                (item) => `
                  <article class="topic-card" data-topic-key="${escapeHtml(item.topic_key)}" style="--topic-accent:${escapeHtml(item.color || "#0f766e")}">
                    <header>
                      <strong>${escapeHtml(item.label)}</strong>
                      <span class="topic-type-badge">${escapeHtml(item.topic_type || "job")}</span>
                    </header>
                    <p>${escapeHtml(item.description || "")}</p>
                    <p>${numberFormat.format(item.summary.world_count || 0)} worlds / updated 30d: ${numberFormat.format(item.summary.updated_worlds_30d || 0)}</p>
                  </article>
                `,
              )
              .join("")}
          </div>
        </section>
      `;
    })
    .join("");

  for (const card of document.querySelectorAll("[data-topic-key]")) {
    card.addEventListener("click", async () => {
      state.activeTopic = card.dataset.topicKey;
      await loadCollection();
    });
  }
}

function renderTopicManager(items) {
  const target = $("topics-admin-list");
  if (!target) {
    return;
  }
  target.innerHTML = (items || []).length
    ? items
        .map((item) => {
          const rules = (item.rules || [])
            .map((rule) => `${rule.rule_type}: ${rule.rule_value}`)
            .join(" / ");
          return `
            <article class="job-item topic-admin-item">
              <header>
                <strong>${escapeHtml(item.label)}</strong>
                <span class="${item.is_active ? "status-ok" : "status-warn"}">${item.is_active ? "visible" : "hidden"}</span>
              </header>
              <p class="mono">${escapeHtml(item.topic_key)}</p>
              <p>${escapeHtml(rules || "no rules")}</p>
              <div class="auth-actions">
                <button class="button subtle" type="button" data-topic-toggle="${escapeHtml(item.topic_key)}" data-topic-active="${item.is_active ? "0" : "1"}">${item.is_active ? "Hide" : "Show"}</button>
                <button class="button subtle" type="button" data-topic-delete="${escapeHtml(item.topic_key)}">Delete</button>
              </div>
            </article>
          `;
        })
        .join("")
    : `<div class="detail-empty">No topics configured.</div>`;

  for (const button of document.querySelectorAll("[data-topic-toggle]")) {
    button.addEventListener("click", async () => {
      const topicKey = button.dataset.topicToggle;
      const isActive = button.dataset.topicActive === "1";
      await fetchJson(`/api/v1/topics/${encodeURIComponent(topicKey)}`, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ is_active: isActive }),
      });
      await refreshAncillaryPanels();
      if (state.activeTopic === topicKey && !isActive) {
        state.activeTopic = null;
        if (state.page === "discover") {
          await loadCollection();
        } else {
          await loadScopeOverview();
        }
      }
    });
  }

  for (const button of document.querySelectorAll("[data-topic-delete]")) {
    button.addEventListener("click", async () => {
      const topicKey = button.dataset.topicDelete;
      if (!window.confirm(`Delete topic ${topicKey}? This does not delete jobs or stored world data.`)) {
        return;
      }
      await fetchJson(`/api/v1/topics/${encodeURIComponent(topicKey)}`, { method: "DELETE" });
      if (state.activeTopic === topicKey) {
        state.activeTopic = null;
      }
      await refreshCurrentScopeData({
        refreshAncillary: true,
      });
    });
  }
}

async function loadSources(preferredSource = null) {
  const { data } = await fetchJson("/api/v1/sources");
  $("source-select").innerHTML = data.items
    .map((item) => {
      const optionLabel = item.key === "db:all"
        ? `db:all (${item.count})`
        : `${item.label} (${item.count})`;
      return `<option value="${escapeHtml(item.key)}">${escapeHtml(optionLabel)}</option>`;
    })
    .join("");
  state.source = preferredSource || data.default_source;
  if ([...$("source-select").options].some((item) => item.value === state.source)) {
    $("source-select").value = state.source;
  } else {
    $("source-select").value = data.default_source;
    state.source = data.default_source;
  }
  populateGraphSourceSelect(data.items);
  populateReviewJobSelect(data.items);
}

async function loadCollection(preferredSource = null) {
  const requestedScopeKey = currentScopeKey(preferredSource);
  state.collectionLoadingScope = requestedScopeKey;
  const sort = $("sort-select").value;
  const direction = $("direction-select").value;
  const q = $("query-input").value.trim();
  const tag = $("tag-select").value;

  try {
    if (state.activeTopic) {
      const params = new URLSearchParams({ q, tag, sort, direction });
      const { data } = await fetchJson(`/api/v1/topics/${encodeURIComponent(state.activeTopic)}/worlds?${params.toString()}`);
      state.worlds = data.items || [];
      state.source = data.topic.label;
      state.collectionInsights = await loadCollectionInsights({ topicKey: state.activeTopic, label: data.topic.label });
      state.briefing = buildDashboardPayloadFromInsights(state.collectionInsights);
      setTopicMode(data.topic.label);
      $("current-source-label").textContent = data.topic.label;
      renderTags(data.tags || []);
      renderSummary(data.topic.summary || state.collectionInsights?.summary || { world_count: data.count || 0 });
      renderTable(state.worlds);
    } else {
      const source = preferredSource || $("source-select").value;
      const params = new URLSearchParams({ source, q, tag, sort, direction });
      const { data } = await fetchJson(`/api/v1/worlds?${params.toString()}`);
      state.source = source;
      state.worlds = data.items || [];
      state.collectionInsights = await loadCollectionInsights({ source, label: source });
      state.briefing = buildDashboardPayloadFromInsights(state.collectionInsights);
      setTopicMode("all sources");
      $("current-source-label").textContent = source;
      renderTags(data.tags || []);
      renderSummary(state.collectionInsights?.summary || {
        world_count: data.count,
        total_visits: state.worlds.reduce((sum, item) => sum + toNumber(item.visits), 0),
        total_favorites: state.worlds.reduce((sum, item) => sum + toNumber(item.favorites), 0),
        tracked_creators: new Set(state.worlds.map((item) => item.author_id).filter(Boolean)).size,
      });
      renderTable(state.worlds);
    }

    state.loadedCollectionScope = requestedScopeKey;
    if (state.worlds.length) {
      state.selectedWorld = state.worlds[0];
      renderDetail(state.selectedWorld);
    } else {
      state.selectedWorld = null;
      state.historyWorldId = null;
      state.historyPoints = [];
      renderDiscoverPlaceholder("No world selected.");
      renderGrowthLeaderboard([]);
      renderRisingNowLeaderboard([]);
      renderNewHotLeaderboard([]);
      renderWorthWatchingLeaderboard([]);
      renderDormantRevivalLeaderboard([]);
      renderCreatorMomentum([]);
      renderAuthorData([]);
      renderAnomalyWatch(null);
      renderUpdateEffectiveness(null);
      renderSignalAnalysis(null);
    }
    if (state.page === "dashboard" || state.page === "monitor") {
      await loadDashboard();
    }
    if (state.page === "discover") {
      await renderWorldCompare(state.worlds);
    }
  } finally {
    if (state.collectionLoadingScope === requestedScopeKey) {
      state.collectionLoadingScope = null;
    }
  }
}

async function loadCollectionInsights({ source = null, topicKey = null, label = "current collection" } = {}) {
  const params = new URLSearchParams({ limit: "12" });
  if (topicKey) {
    params.set("topic", topicKey);
  } else if (source) {
    params.set("source", source);
  }
  markPanelLoading("collectionInsights", {
    page: state.page,
    section: currentSectionForPage(),
  });
  try {
    const { data } = await fetchJson(`/api/v1/insights?${params.toString()}`, undefined, {
      panelKey: "collectionInsights",
      page: state.page,
      section: currentSectionForPage(),
    });
    const rendered = withPanelRender(
      "collectionInsights",
      () => {
        renderGrowthLeaderboard(data.growth_leaderboard || [], data.label || label);
        renderRisingNowLeaderboard(data.rising_now_leaderboard || [], data.label || label);
        renderNewHotLeaderboard(data.new_hot_leaderboard || [], data.label || label);
        renderWorthWatchingLeaderboard(data.worth_watching_leaderboard || [], data.label || label);
        renderDormantRevivalLeaderboard(data.dormant_revival_leaderboard || [], data.label || label);
        renderCreatorMomentum(data.creator_momentum || [], data.label || label);
        renderAuthorData(data.authors || [], data.label || label);
        renderAnomalyWatch(data.anomalies || null, data.label || label);
        renderUpdateEffectiveness(data.update_effectiveness || null, data.label || label);
        renderSignalAnalysis(data.signals || null, data.label || label);
        renderPerformance(data);
      },
      (error) => {
        renderCollectionInsightsError(error.message, data?.label || label);
        renderPerformanceError(error.message, data?.label || label);
      },
      { page: state.page, section: currentSectionForPage() },
    );
    if (rendered) {
      const summary = data?.summary || {};
      const itemCount =
        toNumber(summary.world_count)
        + (data?.growth_leaderboard || []).length
        + (data?.rising_now_leaderboard || []).length
        + (data?.new_hot_leaderboard || []).length;
      markPanelSuccess("collectionInsights", {
        page: state.page,
        section: currentSectionForPage(),
        status: itemCount > 0 ? "ready" : "empty",
      });
    }
    return data;
  } catch (error) {
    markPanelError("collectionInsights", error, {
      page: state.page,
      section: currentSectionForPage(),
    });
    renderCollectionInsightsError(error.message, label);
    renderPerformanceError(error.message, label);
    return null;
  }
}

async function loadScopeOverview(preferredSource = null) {
  const selection = currentScopeSelection(preferredSource);
  markPanelLoading("scopeSummary", {
    page: state.page,
    section: currentSectionForPage(),
  });
  if (!state.activeTopic && selection.source) {
    state.source = selection.source;
  }
  if (!collectionMatchesCurrentScope(preferredSource)) {
    state.selectedWorld = null;
    state.historyWorldId = null;
    state.historyPoints = [];
  }
  const data = state.activeTopic
    ? await loadCollectionInsights({ topicKey: state.activeTopic, label: selection.label })
    : await loadCollectionInsights({ source: selection.source, label: selection.label });
  state.collectionInsights = data;
  state.briefing = buildDashboardPayloadFromInsights(data);
  if (state.activeTopic) {
    setTopicMode(data?.label || state.activeTopic);
    $("current-source-label").textContent = data?.label || state.activeTopic;
  } else {
    setTopicMode("all sources");
    $("current-source-label").textContent = selection.source || "db:all";
  }
  renderTags([]);
  if (data?.summary) {
    const rendered = withPanelRender(
      "scopeSummary",
      () => renderSummary(data.summary),
      (error) => renderSummaryError(error.message),
      { page: state.page, section: currentSectionForPage() },
    );
    if (rendered) {
      markPanelSuccess("scopeSummary", {
        page: state.page,
        section: currentSectionForPage(),
        status: toNumber(data.summary.world_count) > 0 ? "ready" : "empty",
      });
    }
  } else {
    const message = state.debug.panels.collectionInsights?.errorMessage || "Insights unavailable for current scope.";
    markPanelError("scopeSummary", new Error(message), {
      page: state.page,
      section: currentSectionForPage(),
    });
    renderSummaryError(message);
  }
  if (state.page === "dashboard" || state.page === "monitor") {
    await loadDashboard();
  }
}

async function refreshCurrentScopeData({
  preferredSource = null,
  refreshSources = false,
  refreshAncillary = false,
  refreshAutoSync = false,
} = {}) {
  const tasks = [];
  if (refreshSources) {
    tasks.push(loadSources(preferredSource || state.source || "db:all"));
  }
  if (refreshAncillary) {
    tasks.push(refreshAncillaryPanels());
  }
  if (refreshAutoSync) {
    tasks.push(loadAutoSyncSchedule());
  }
  if (tasks.length) {
    await Promise.all(tasks);
  }
  if (state.page === "discover") {
    await loadCollection(preferredSource);
    return;
  }
  await loadScopeOverview(preferredSource);
}

async function refreshAncillaryPanels() {
  const health = await fetchJson("/api/v1/health");
  setHealthIndicator(String(health.data.status || "ok").toUpperCase(), "ok");

  const ancillaryPanels = [
    ["reviewQueue", "operations", "diagnostics"],
    ["analytics", "operations", "records"],
    ["jobs", "operations", "sync"],
    ["recentRuns", "operations", "sync"],
    ["queryAnalytics", "operations", "records"],
    ["rateLimits", "operations", "diagnostics"],
    ["topics", "operations", "views"],
    ["diagnostics", "operations", "diagnostics"],
  ];
  for (const [panelKey, page, section] of ancillaryPanels) {
    markPanelLoading(panelKey, { page, section });
  }

  const [
    review,
    analytics,
    jobs,
    runs,
    queryAnalytics,
    rateLimits,
    topics,
    diagnostics,
  ] = await Promise.allSettled([
    fetchJson("/api/v1/review/self-check", undefined, { panelKey: "reviewQueue", page: "operations", section: "diagnostics" }),
    fetchJson("/api/v1/analytics/daily-stats", undefined, { panelKey: "analytics", page: "operations", section: "records" }),
    fetchJson("/api/v1/jobs", undefined, { panelKey: "jobs", page: "operations", section: "sync" }),
    fetchJson("/api/v1/runs?limit=12", undefined, { panelKey: "recentRuns", page: "operations", section: "sync" }),
    fetchJson("/api/v1/query-analytics?limit=12", undefined, { panelKey: "queryAnalytics", page: "operations", section: "records" }),
    fetchJson("/api/v1/rate-limits?limit=12", undefined, { panelKey: "rateLimits", page: "operations", section: "diagnostics" }),
    fetchJson("/api/v1/topics?include_inactive=1", undefined, { panelKey: "topics", page: "operations", section: "views" }),
    fetchJson("/api/v1/jobs/diagnostics", undefined, { panelKey: "diagnostics", page: "operations", section: "diagnostics" }),
  ]);

  if (review.status === "fulfilled") {
    if (withPanelRender("reviewQueue", () => renderReview(review.value.data, review.value.response.status), (error) => renderReviewError(error.message), { page: "operations", section: "diagnostics" })) {
      markPanelSuccess("reviewQueue", { page: "operations", section: "diagnostics", status: "ready" });
    }
  } else {
    renderReviewError(review.reason?.message || "Review self-check failed.");
  }
  if (analytics.status === "fulfilled") {
    if (withPanelRender("analytics", () => renderAnalytics(analytics.value.data.items || []), (error) => renderAnalyticsError(error.message), { page: "operations", section: "records" })) {
      markPanelSuccess("analytics", {
        page: "operations",
        section: "records",
        status: (analytics.value.data.items || []).length ? "ready" : "empty",
      });
    }
  } else {
    renderAnalyticsError(analytics.reason?.message || "Daily analytics failed.");
  }
  if (jobs.status === "fulfilled") {
    if (withPanelRender("jobs", () => renderJobs(jobs.value.data.items || []), (error) => renderJobsError(error.message), { page: "operations", section: "sync" })) {
      markPanelSuccess("jobs", {
        page: "operations",
        section: "sync",
        status: (jobs.value.data.items || []).length ? "ready" : "empty",
      });
    }
  } else {
    renderJobsError(jobs.reason?.message || "Job registry failed.");
  }
  if (runs.status === "fulfilled") {
    if (withPanelRender("recentRuns", () => renderRuns(runs.value.data.items || []), (error) => renderRunsError(error.message), { page: "operations", section: "sync" })) {
      markPanelSuccess("recentRuns", {
        page: "operations",
        section: "sync",
        status: (runs.value.data.items || []).length ? "ready" : "empty",
      });
    }
  } else {
    renderRunsError(runs.reason?.message || "Recent runs failed.");
  }
  if (queryAnalytics.status === "fulfilled") {
    if (withPanelRender("queryAnalytics", () => renderQueryAnalytics(queryAnalytics.value.data || {}), (error) => renderQueryAnalyticsError(error.message), { page: "operations", section: "records" })) {
      markPanelSuccess("queryAnalytics", {
        page: "operations",
        section: "records",
        status: (queryAnalytics.value.data?.items || []).length ? "ready" : "empty",
      });
    }
  } else {
    renderQueryAnalyticsError(queryAnalytics.reason?.message || "Query analytics failed.");
  }
  if (rateLimits.status === "fulfilled") {
    if (withPanelRender("rateLimits", () => renderRateLimits(rateLimits.value.data || {}), (error) => renderRateLimitsError(error.message), { page: "operations", section: "diagnostics" })) {
      markPanelSuccess("rateLimits", { page: "operations", section: "diagnostics", status: "ready" });
    }
  } else {
    renderRateLimitsError(rateLimits.reason?.message || "Rate limit diagnostics failed.");
  }
  if (topics.status === "fulfilled") {
    if (withPanelRender("topics", () => {
      renderTopics(topics.value.data.items || []);
      renderTopicManager(topics.value.data.items || []);
    }, (error) => renderTopicsError(error.message), { page: "operations", section: "views" })) {
      markPanelSuccess("topics", {
        page: "operations",
        section: "views",
        status: (topics.value.data.items || []).length ? "ready" : "empty",
      });
    }
  } else {
    renderTopicsError(topics.reason?.message || "Topics failed.");
  }
  if (diagnostics.status === "fulfilled") {
    if (withPanelRender("diagnostics", () => {
      renderJobDiagnostics(diagnostics.value.data.items || []);
      renderSourceDiffs(diagnostics.value.data.items || []);
    }, (error) => renderDiagnosticsError(error.message), { page: "operations", section: "diagnostics" })) {
      markPanelSuccess("diagnostics", {
        page: "operations",
        section: "diagnostics",
        status: (diagnostics.value.data.items || []).length ? "ready" : "empty",
      });
    }
  } else {
    renderDiagnosticsError(diagnostics.reason?.message || "Diagnostics failed.");
  }
  if (runs.status === "fulfilled" && diagnostics.status === "fulfilled") {
    processRunNotifications(runs.value.data.items || [], diagnostics.value.data.items || []);
  }
}

async function pollRunStatusAndNotifications() {
  try {
    const [runs, diagnostics, rateLimits] = await Promise.all([
      fetchJson("/api/v1/runs?limit=12"),
      fetchJson("/api/v1/jobs/diagnostics"),
      fetchJson("/api/v1/rate-limits?limit=12"),
    ]);
    renderRuns(runs.data.items || []);
    renderJobDiagnostics(diagnostics.data.items || []);
    renderRateLimits(rateLimits.data || {});
    renderSourceDiffs(diagnostics.data.items || []);
    processRunNotifications(runs.data.items || [], diagnostics.data.items || []);
    if (state.page === "operations") {
      await loadAutoSyncSchedule();
    }
  } catch {
    // Background polling should stay quiet.
  }
}

async function submitJsonForm(formId, endpoint, transform) {
  const form = $(formId);
  form.addEventListener("submit", async (event) => {
    event.preventDefault();
    const formData = new FormData(form);
    const payload = transform(formData);
    const button = form.querySelector("button[type='submit']");
    const originalLabel = button.textContent;
    button.disabled = true;
    button.textContent = "Running...";
    try {
      const authPayload = getAuthPayload();
      const { data } = await fetchJson(endpoint, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ ...payload, ...authPayload }),
      });
      renderSyncStatus((data.warnings || []).join(" "), data.warnings?.length ? "warn" : "ok");
      state.activeTopic = null;
      await refreshCurrentScopeData({
        preferredSource: data.source,
        refreshSources: true,
        refreshAncillary: true,
      });
    } catch (error) {
      window.alert(error.message);
    } finally {
      button.disabled = false;
      button.textContent = originalLabel;
    }
  });
}

function attachControls() {
  document.querySelectorAll("[data-page-tab]").forEach((button) => {
    button.addEventListener("click", () => {
      recordDebugLifecycle("page:transition", {
        page: button.dataset.pageTab,
        section: currentSectionForPage(button.dataset.pageTab),
        detail: `${state.page}:${currentSectionForPage()} -> ${button.dataset.pageTab}`,
      });
      state.page = button.dataset.pageTab;
      renderPage();
    });
  });
  document.querySelectorAll("[data-dashboard-tab]").forEach((button) => {
    button.addEventListener("click", () => {
      state.dashboardSection = button.dataset.dashboardTab || "briefing";
      renderPage();
    });
  });
  document.querySelectorAll("[data-monitor-tab]").forEach((button) => {
    button.addEventListener("click", () => {
      state.monitorSection = button.dataset.monitorTab || "events";
      renderPage();
    });
  });
  document.querySelectorAll("[data-discover-tab]").forEach((button) => {
    button.addEventListener("click", () => {
      state.discoverSection = button.dataset.discoverTab || "new";
      applyDiscoverSectionDefaults(state.discoverSection);
      renderPage();
    });
  });
  document.querySelectorAll("[data-communities-tab]").forEach((button) => {
    button.addEventListener("click", () => {
      state.communitiesSection = button.dataset.communitiesTab || "directory";
      renderPage();
    });
  });
  $("group-form")?.addEventListener("submit", submitGroupForm);
  $("managed-group-form")?.addEventListener("submit", submitManagedGroupForm);
  $("scheduled-post-form")?.addEventListener("submit", submitScheduledPostForm);
  $("group-world-form")?.addEventListener("submit", submitGroupWorldForm);
  $("group-form-reset")?.addEventListener("click", resetGroupForm);
  $("managed-group-reset")?.addEventListener("click", resetManagedGroupForm);
  $("scheduled-post-reset")?.addEventListener("click", resetScheduledPostForm);
  $("group-world-reset")?.addEventListener("click", resetGroupWorldForm);
  $("communities-directory-list")?.addEventListener("click", handleCommunitiesAction);
  $("communities-managed-group-list")?.addEventListener("click", handleCommunitiesAction);
  $("communities-publishing-list")?.addEventListener("click", handleCommunitiesAction);
  $("communities-worlds-list")?.addEventListener("click", handleCommunitiesAction);
  document.querySelectorAll("[data-operations-tab]").forEach((button) => {
    button.addEventListener("click", () => {
      state.operationsSection = button.dataset.operationsTab || "sync";
      renderPage();
    });
  });
  $("debug-clear-button")?.addEventListener("click", () => {
    state.debug.requestLog = [];
    state.debug.lifecycle = [];
    state.debug.sequence = 0;
    state.debug.lastRequestError = "";
    state.debug.lastRenderError = "";
    if (state.page === "debug") {
      renderDebugPage();
    }
  });
  document.querySelectorAll("[data-compare-window]").forEach((button) => {
    button.addEventListener("click", () => {
      state.compareWindow = button.dataset.compareWindow;
      document.querySelectorAll("[data-compare-window]").forEach((item) => {
        item.classList.toggle("is-active", item.dataset.compareWindow === state.compareWindow);
      });
      if (state.page === "discover") {
        renderWorldCompare(state.worlds);
      }
    });
  });
  $("auto-sync-job").addEventListener("change", () => {
    state.autoSync.jobKey = $("auto-sync-job").value;
    saveAutoSyncState();
  });
  $("events-filter-select").value = state.events.filter;
  $("events-days-select").value = String(state.events.days);
  $("events-filter-select").addEventListener("change", () => {
    state.events.filter = $("events-filter-select").value;
    renderEventFeed();
  });
  $("events-days-select").addEventListener("change", () => {
    state.events.days = Number($("events-days-select").value || 7);
    state.events.loadedAt = null;
    if (state.page === "monitor") {
      loadEventFeed(true);
    }
  });
  $("events-reload-button").addEventListener("click", () => {
    state.events.loadedAt = null;
    loadEventFeed(true);
  });
  $("auto-sync-enabled").addEventListener("change", () => {
    state.autoSync.enabled = $("auto-sync-enabled").checked;
    saveAutoSyncState();
  });
  $("job-create-type").addEventListener("change", renderJobCreateMode);
  $("auth-form").addEventListener("submit", async (event) => {
    event.preventDefault();
    state.auth.cookie = $("auth-cookie").value;
    state.auth.username = $("auth-username").value;
    state.auth.password = $("auth-password").value;
    saveAuthState();
    try {
      await persistServerAuth();
      await refreshAuthStatusCheck();
      await loadAutoSyncSchedule();
      await maybeRunAutoSync("saved session");
    } catch (error) {
      state.authStatus = {
        status: "error",
        mode: null,
        label: "server auth save failed",
        detail: error.message,
      };
      renderAuthStatus();
      window.alert(error.message);
    }
  });
  $("auth-login-button").addEventListener("click", async () => {
    try {
      await loginWithPassword();
      await maybeRunAutoSync("login");
    } catch (error) {
      state.authStatus = { label: "login failed", detail: error.message };
      renderAuthStatus();
      window.alert(error.message);
    }
  });
  $("auth-check-button").addEventListener("click", () => refreshAuthStatusCheck());
  $("auth-2fa-verify-button").addEventListener("click", async () => {
    try {
      await verifyTwoFactor();
    } catch (error) {
      window.alert(error.message);
    }
  });
  $("auth-2fa-cancel-button").addEventListener("click", () => {
    state.authPending = null;
    renderAuthStatus();
  });
  $("auth-clear-button").addEventListener("click", async () => {
    state.auth.cookie = "";
    state.auth.username = "";
    state.auth.password = "";
    state.authStatus = null;
    state.authPending = null;
    window.localStorage.removeItem(AUTH_STORAGE_KEY);
    try {
      await clearServerAuth();
    } catch (error) {
      window.alert(error.message);
    }
    renderAuthStatus();
    await loadAutoSyncSchedule();
  });
  for (const id of ["sort-select", "direction-select"]) {
    $(id).addEventListener("change", () => {
      if (state.page === "discover") {
        loadCollection();
      }
    });
  }
  $("import-legacy-button").addEventListener("click", async () => {
    const button = $("import-legacy-button");
    const original = button.textContent;
    button.disabled = true;
    button.textContent = "Importing...";
    try {
      state.activeTopic = null;
      await fetchJson("/api/v1/import/legacy", { method: "POST" });
      await refreshCurrentScopeData({
        refreshSources: true,
        refreshAncillary: true,
      });
    } catch (error) {
      window.alert(error.message);
    } finally {
      button.disabled = false;
      button.textContent = original;
    }
  });
  $("job-edit-cancel-button").addEventListener("click", () => {
    resetJobEditor();
  });
  $("job-create-form").addEventListener("submit", async (event) => {
    event.preventDefault();
    const jobType = $("job-create-type").value;
    const editingJobKey = $("job-edit-mode").value !== "create" ? $("job-edit-mode").value : null;
    const payload = {
      label: $("job-create-label").value.trim(),
      job_key: $("job-create-key").value.trim(),
      job_type: jobType,
      limit: Number($("job-create-limit").value || 50),
      limit_per_keyword: Number($("job-create-limit").value || 50),
      keywords: $("job-create-keywords").value,
      user_id: $("job-create-user-id").value.trim(),
      search: $("job-create-search").value.trim(),
      tags: $("job-create-tags").value,
      notags: $("job-create-notags").value,
      sort: $("job-create-sort").value,
      order: "descending",
      active: $("job-create-active").value === "true",
      featured: $("job-create-featured").value,
    };
    try {
      const endpoint = editingJobKey ? `/api/v1/jobs/${encodeURIComponent(editingJobKey)}` : "/api/v1/jobs";
      const method = editingJobKey ? "PUT" : "POST";
      const { data } = await fetchJson(endpoint, {
        method,
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      $("job-create-status").textContent = editingJobKey
        ? `Updated ${data.job.label}.`
        : `Created ${data.job.label} and view ${data.topic.label}.`;
      resetJobEditor();
      await refreshCurrentScopeData({
        refreshSources: true,
        refreshAncillary: true,
      });
    } catch (error) {
      $("job-create-status").textContent = error.message;
      window.alert(error.message);
    }
  });
  $("topic-form").addEventListener("submit", async (event) => {
    event.preventDefault();
    const rules = [];
    const sourceRule = $("topic-create-source").value.trim();
    if (sourceRule) {
      rules.push({ type: "source", value: sourceRule });
    }
    for (const tag of splitCsv($("topic-create-tags").value)) {
      rules.push({ type: "tag", value: tag });
    }
    for (const keyword of splitCsv($("topic-create-keywords").value)) {
      rules.push({ type: "keyword", value: keyword });
    }
    const payload = {
      label: $("topic-create-label").value.trim(),
      topic_key: $("topic-create-key").value.trim(),
      topic_type: $("topic-create-type").value,
      color: $("topic-create-color").value,
      is_active: $("topic-create-active").checked,
      rules,
    };
    try {
      const { data } = await fetchJson("/api/v1/topics", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      $("topic-create-status").textContent = `Saved topic ${data.label}.`;
      $("topic-form").reset();
      $("topic-create-color").value = "#0f766e";
      $("topic-create-active").checked = true;
      await refreshAncillaryPanels();
    } catch (error) {
      $("topic-create-status").textContent = error.message;
      window.alert(error.message);
    }
  });
  $("source-select").addEventListener("change", () => {
    state.activeTopic = null;
    const selectedSource = $("source-select").value;
    if (state.page === "discover") {
      loadCollection(selectedSource);
      return;
    }
    loadScopeOverview(selectedSource);
  });
  $("view-db-all-button")?.addEventListener("click", async () => {
    state.activeTopic = null;
    state.source = "db:all";
    if ([...$("source-select").options].some((item) => item.value === "db:all")) {
      $("source-select").value = "db:all";
    }
    setTopicMode("all sources");
    if (state.page === "discover") {
      await loadCollection("db:all");
      return;
    }
    await loadScopeOverview("db:all");
  });
  $("tag-select").addEventListener("change", () => {
    if (state.page === "discover") {
      loadCollection();
    }
  });
  $("reload-button").addEventListener("click", () => {
    if (state.page === "discover") {
      loadCollection();
      return;
    }
    loadScopeOverview();
  });
  $("query-input").addEventListener("keydown", (event) => {
    if (event.key === "Enter") {
      event.preventDefault();
      if (state.page === "discover") {
        loadCollection();
      }
    }
  });
  $("editor-form")?.addEventListener("submit", async (event) => {
    event.preventDefault();
    if (!state.selectedWorld) {
      return;
    }
    const payload = {
      source: state.source,
      name: $("edit-name")?.value || "",
      author_name: $("edit-author-name")?.value || "",
      visits: $("edit-visits")?.value || "",
      favorites: $("edit-favorites")?.value || "",
      heat: $("edit-heat")?.value || "",
      popularity: $("edit-popularity")?.value || "",
      updated_at: $("edit-updated-at")?.value || "",
      publication_date: $("edit-publication-date")?.value || "",
      release_status: $("edit-release-status")?.value || "",
      tags: $("edit-tags")?.value || "",
      portal_links: $("edit-portal-links")?.value || "",
    };
    try {
      const { data } = await fetchJson(`/api/v1/worlds/${encodeURIComponent(state.selectedWorld.id)}`, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      const portalCount = Number.isFinite(Number(data.portal_links_count))
        ? Number(data.portal_links_count)
        : Array.isArray(data.world?.portal_links) ? data.world.portal_links.length : 0;
      const portalPath = String(data.portal_links_saved_to || "").trim();
      if ($("editor-status")) {
        $("editor-status").textContent = portalCount
          ? `Record saved. ${portalCount} portal link(s) stored in ${portalPath || "world_properties.json"}.`
          : "Record saved.";
      }
      await refreshCurrentScopeData({
        preferredSource: state.source,
        refreshAncillary: true,
      });
      state.selectedWorld = state.worlds.find((item) => item.id === data.world.id) || data.world;
      renderDetail(state.selectedWorld);
    } catch (error) {
      if ($("editor-status")) {
        $("editor-status").textContent = error.message;
      }
      window.alert(error.message);
    }
  });
  $("editor-delete-button")?.addEventListener("click", async () => {
    if (!state.selectedWorld) {
      return;
    }
    if (!window.confirm(`Delete ${state.selectedWorld.name || state.selectedWorld.id} from ${state.source}?`)) {
      return;
    }
    try {
      await fetchJson(`/api/v1/worlds/${encodeURIComponent(state.selectedWorld.id)}?source=${encodeURIComponent(state.source)}`, {
        method: "DELETE",
      });
      if ($("editor-status")) {
        $("editor-status").textContent = "Record deleted.";
      }
      await refreshCurrentScopeData({
        preferredSource: state.source,
        refreshAncillary: true,
      });
    } catch (error) {
      if ($("editor-status")) {
        $("editor-status").textContent = error.message;
      }
      window.alert(error.message);
    }
  });
  $("taiwan-blacklist-selected-button").addEventListener("click", () => {
    if (!state.selectedWorld?.id) {
      return;
    }
    $("taiwan-blacklist-world-id").value = state.selectedWorld.id;
  });
  $("taiwan-blacklist-form").addEventListener("submit", async (event) => {
    event.preventDefault();
    const worldId = $("taiwan-blacklist-world-id").value.trim();
    if (!worldId) {
      $("taiwan-blacklist-status").textContent = "world ID is required";
      return;
    }
    try {
      const { data } = await fetchJson("/api/v1/jobs/taiwan/blacklist", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ world_id: worldId }),
      });
      $("taiwan-blacklist-world-id").value = "";
      $("taiwan-blacklist-status").textContent =
        data.removed_from_db > 0
          ? `Added ${worldId} and removed ${data.removed_from_db} Zh record(s).`
          : `Added ${worldId} to Zh blacklist.`;
      await Promise.all([
        loadTaiwanBlacklist(),
        refreshCurrentScopeData({
          preferredSource: state.source,
          refreshAncillary: true,
        }),
      ]);
    } catch (error) {
      $("taiwan-blacklist-status").textContent = error.message;
      window.alert(error.message);
    }
  });
  $("taiwan-creator-whitelist-selected-button").addEventListener("click", () => {
    if (!state.selectedWorld?.author_id) {
      return;
    }
    $("taiwan-creator-whitelist-user-id").value = state.selectedWorld.author_id;
  });
  $("taiwan-creator-whitelist-form").addEventListener("submit", async (event) => {
    event.preventDefault();
    const userId = $("taiwan-creator-whitelist-user-id").value.trim();
    if (!userId) {
      $("taiwan-creator-whitelist-status").textContent = "user_id is required";
      return;
    }
    try {
      await fetchJson("/api/v1/jobs/taiwan/creator-whitelist", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ user_id: userId }),
      });
      $("taiwan-creator-whitelist-user-id").value = "";
      $("taiwan-creator-whitelist-status").textContent =
        `Added ${userId} to Zh creator whitelist. Run Zh sync again to include this creator's worlds.`;
      await loadTaiwanCreatorWhitelist();
    } catch (error) {
      $("taiwan-creator-whitelist-status").textContent = error.message;
      window.alert(error.message);
    }
  });
  $("taiwan-creator-blacklist-selected-button").addEventListener("click", () => {
    if (!state.selectedWorld?.author_id) {
      return;
    }
    $("taiwan-creator-blacklist-user-id").value = state.selectedWorld.author_id;
  });
  $("taiwan-creator-blacklist-form").addEventListener("submit", async (event) => {
    event.preventDefault();
    const userId = $("taiwan-creator-blacklist-user-id").value.trim();
    if (!userId) {
      $("taiwan-creator-blacklist-status").textContent = "user_id is required";
      return;
    }
    try {
      await fetchJson("/api/v1/jobs/taiwan/creator-blacklist", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ user_id: userId }),
      });
      $("taiwan-creator-blacklist-user-id").value = "";
      $("taiwan-creator-blacklist-status").textContent =
        `Added ${userId} to Zh user blacklist. Run Zh sync again to exclude this creator's worlds.`;
      await loadTaiwanCreatorBlacklist();
    } catch (error) {
      $("taiwan-creator-blacklist-status").textContent = error.message;
      window.alert(error.message);
    }
  });
  $("review-load-btn").addEventListener("click", loadCreatorReview);
  $("graph-load-button").addEventListener("click", loadGraph);
  $("graph-color-mode").addEventListener("change", () => {
    renderGraphLegend();
    if (graphState.data) {
      renderGraph(graphState.data);
    }
  });
  $("graph-highlight-mode").addEventListener("change", () => {
    renderGraphLegend();
    if (graphState.data) {
      renderGraph(graphState.data);
    }
  });
  $("graph-min-tags").addEventListener("input", () => {
    $("graph-min-tags-label").textContent = $("graph-min-tags").value;
  });
  $("hourly-history-all-toggle").addEventListener("change", () => {
    state.uiSettings.enableHourlyHistoryAll = $("hourly-history-all-toggle").checked;
    saveUiSettings();
    renderUiSettings();
    renderHistoryModeControls();
    renderChart(state.historyPoints || []);
  });
  $("history-mode-daily").addEventListener("click", () => {
    state.historyTrendMode = "daily";
    renderHistoryModeControls();
    renderChart(state.historyPoints || []);
  });
  $("history-mode-48h").addEventListener("click", () => {
    state.historyTrendMode = "48h";
    renderHistoryModeControls();
    renderChart(state.historyPoints || []);
  });
  $("notification-toggle-button").addEventListener("click", async () => {
    if (!notificationsSupported()) {
      renderNotificationStatus();
      return;
    }
    if (Notification.permission === "granted") {
      state.notifications.enabled = !state.notifications.enabled;
      saveNotificationState();
      return;
    }
    const permission = await Notification.requestPermission();
    state.notifications.enabled = permission === "granted";
    saveNotificationState();
    if (permission === "granted") {
      emitBrowserNotification("Notifications enabled", {
        body: "You will now receive sync results and notable world change alerts.",
        tag: "notifications-enabled",
      });
    }
  });
}

const graphState = {
  data: null,
  simulation: null,
  hoveredNode: null,
  selectedNode: null,
  dragging: null,
  transform: { x: 0, y: 0, k: 1 },
};

const AUTHOR_GRAPH_COLORS = GRAPH_TEN_COLOR_PALETTE;
const TOPIC_FALLBACK_COLORS = {
  personal: "#155e75",
  starriver: "#155e75",
  racing: "#b45309",
  taiwan: "#0f766e",
};

function hashString(value) {
  const text = String(value || "");
  let hash = 0;
  for (let index = 0; index < text.length; index += 1) {
    hash = (hash * 31 + text.charCodeAt(index)) >>> 0;
  }
  return hash;
}

function getTopicColorMap() {
  const map = new Map();
  for (const topic of state.topics || []) {
    map.set(topic.topic_key, topic.color || TOPIC_FALLBACK_COLORS[topic.topic_key] || "#94a3b8");
  }
  return map;
}

function pickPrimaryTopic(node) {
  const topicOrder = new Map((state.topics || []).map((topic) => [topic.topic_key, topic.sort_order || 0]));
  const keys = [...(node.topic_keys || [])];
  if (!keys.length) {
    return null;
  }
  keys.sort((left, right) => (topicOrder.get(left) ?? 9999) - (topicOrder.get(right) ?? 9999));
  return keys[0];
}

function authorNodeColor(node) {
  const key = node.author_id || node.author_name || node.id;
  return paletteColorForKey(key, AUTHOR_GRAPH_COLORS);
}

function topicNodeColors(node) {
  const topicColorMap = getTopicColorMap();
  const primaryTopic = pickPrimaryTopic(node);
  const topicKeys = [...(node.topic_keys || [])].filter((key) => topicColorMap.has(key));
  const primary = primaryTopic ? (topicColorMap.get(primaryTopic) || TOPIC_FALLBACK_COLORS[primaryTopic] || "#94a3b8") : "#94a3b8";
  const secondaryTopic = topicKeys.find((key) => key !== primaryTopic) || null;
  const secondary = secondaryTopic ? (topicColorMap.get(secondaryTopic) || "#cbd5e1") : null;
  return { primary, secondary, primaryTopic, secondaryTopic };
}

function graphSignal(node) {
  const now = new Date();
  const fetched = parseDate(node.fetched_at);
  const publication = parseDate(node.publication_date);
  const updated = parseDate(node.updated_at);
  const fetchedWithinWeek = fetched ? daysBetween(now, fetched) <= 7 : false;
  const isNew = Boolean(fetchedWithinWeek && publication && daysBetween(now, publication) <= 7);
  const isRecentlyUpdated = Boolean(fetchedWithinWeek && updated && daysBetween(now, updated) <= 30 && !isNew);
  return { isNew, isRecentlyUpdated };
}

function graphNodeStyle(node, colorMode) {
  if (colorMode === "topic") {
    const topicColors = topicNodeColors(node);
    return {
      fill: topicColors.primary,
      stroke: topicColors.secondary || "rgba(255,255,255,0.14)",
      lineWidth: topicColors.secondary ? 3 : 1.5,
    };
  }
  if (colorMode === "relationship") {
    return {
      fill: "#cbd5e1",
      stroke: "rgba(255,255,255,0.18)",
      lineWidth: 1.5,
    };
  }
  return {
    fill: authorNodeColor(node),
    stroke: "rgba(255,255,255,0.16)",
    lineWidth: 1.5,
  };
}

function graphEdgeStyle(edge, colorMode) {
  if (colorMode === "relationship") {
    if (edge.type === "same_author") {
      return { strokeStyle: "rgba(56,189,248,0.72)", lineWidth: 3.2 };
    }
    if (edge.type === "portal_link") {
      return { strokeStyle: "rgba(16,185,129,0.78)", lineWidth: 2.8 };
    }
    return { strokeStyle: "rgba(245,158,11,0.42)", lineWidth: 1.8 };
  }
  if (edge.type === "same_author") {
    return { strokeStyle: "rgba(148,163,184,0.28)", lineWidth: 2.4 };
  }
  if (edge.type === "portal_link") {
    return { strokeStyle: "rgba(16,185,129,0.34)", lineWidth: 2 };
  }
  return { strokeStyle: "rgba(100,116,139,0.16)", lineWidth: 1.2 };
}

function graphAuthorKey(node) {
  return node?.author_id || node?.author_name || null;
}

function resetGraphDetail() {
  $("graph-selected-label").textContent = "No selection";
  $("graph-world-detail").textContent = "Click a node to see details.";
}

function renderGraphLegend() {
  const mode = $("graph-color-mode")?.value || "author";
  const highlightMode = $("graph-highlight-mode")?.value || "both";
  const container = $("graph-legend-content");
  if (!container) return;
  const highlightItems = highlightMode === "none"
    ? `
      <div class="legend-item"><span class="legend-circle legend-circle-selected"></span>Selected node</div>
    `
    : `
      <div class="legend-item"><span class="legend-circle legend-circle-selected"></span>Selected node</div>
      <div class="legend-item"><span class="legend-circle legend-circle-highlight"></span>Highlight match</div>
      <div class="legend-item"><span class="legend-dot legend-dim"></span>Non-matching nodes and edges dimmed</div>
    `;
  if (mode === "relationship") {
    container.innerHTML = `
      <div class="legend-item"><span class="legend-dot legend-rel-author"></span>Same Author edge</div>
      <div class="legend-item"><span class="legend-dot legend-rel-tag"></span>Shared Tag edge</div>
      <div class="legend-item"><span class="legend-dot legend-rel-portal"></span>Portal Link edge</div>
      <div class="legend-item"><span class="legend-circle legend-circle-neutral"></span>Neutral nodes, relationship-focused view</div>
      ${highlightItems}
      <div class="legend-item"><span class="legend-halo legend-halo-new"></span>New world (strong halo)</div>
      <div class="legend-item"><span class="legend-halo legend-halo-updated"></span>Recent update (halo ring)</div>
    `;
    return;
  }
  if (mode === "topic") {
    const items = (state.topics || []).slice(0, 5)
      .map((topic) => `<div class="legend-item"><span class="legend-circle" style="background:${escapeHtml(topic.color || "#94a3b8")}"></span>${escapeHtml(topic.label)}</div>`)
      .join("");
    container.innerHTML = `
      ${items}
      <div class="legend-item"><span class="legend-ring"></span>Second topic = outer ring</div>
      ${highlightItems}
      <div class="legend-item"><span class="legend-halo legend-halo-new"></span>New world (strong halo)</div>
      <div class="legend-item"><span class="legend-halo legend-halo-updated"></span>Recent update (halo ring)</div>
    `;
    return;
  }
  container.innerHTML = `
    <div class="legend-item"><span class="legend-circle" style="background:${AUTHOR_GRAPH_COLORS[0]}"></span>Author palette (10-color cycle)</div>
    <div class="legend-item"><span class="legend-dot legend-author"></span>Edges de-emphasized</div>
    ${highlightItems}
    <div class="legend-item"><span class="legend-halo legend-halo-new"></span>New world (strong halo)</div>
    <div class="legend-item"><span class="legend-halo legend-halo-updated"></span>Recent update (halo ring)</div>
  `;
}

function populateGraphSourceSelect(sources) {
  const select = $("graph-source-select");
  select.innerHTML = sources
    .filter((item) => item.available && item.count > 0)
    .map((item) => `<option value="${escapeHtml(item.key)}">${escapeHtml(item.label)} (${item.count})</option>`)
    .join("");
  const preferred = [...select.options].find((opt) => opt.value === "db:job:taiwan");
  if (preferred) select.value = preferred.value;
}

async function loadGraph() {
  const source = $("graph-source-select").value;
  const edgeTypes = [];
  if ($("graph-edge-author").checked) edgeTypes.push("author");
  if ($("graph-edge-tag").checked) edgeTypes.push("tag");
  if ($("graph-edge-portal").checked) edgeTypes.push("portal");
  const minTags = $("graph-min-tags").value;
  const maxNodes = $("graph-max-nodes").value;
  const button = $("graph-load-button");
  button.disabled = true;
  button.textContent = "Loading...";
  try {
    const params = new URLSearchParams({
      source,
      edges: edgeTypes.join(",") || "author",
      min_shared_tags: minTags,
      max_nodes: maxNodes,
    });
    const { data } = await fetchJson(`/api/v1/graph?${params.toString()}`);
    graphState.data = data;
    graphState.transform = { x: 0, y: 0, k: 1 };
    graphState.selectedNode = null;
    resetGraphDetail();
    const expanded = Number(data.portal_expanded_nodes || 0);
    $("graph-caption").textContent = expanded > 0
      ? `${data.node_count} nodes (${expanded} via portal) · ${data.edge_count} edges`
      : `${data.node_count} nodes · ${data.edge_count} edges`;
    renderGraphLegend();
    renderGraph(data);
  } catch (error) {
    window.alert(error.message);
  } finally {
    button.disabled = false;
    button.textContent = "Load Graph";
  }
}

function renderGraph(data) {
  const canvas = $("graph-canvas");
  if (!canvas) return;
  if (graphState.simulation) {
    graphState.simulation.stop();
    graphState.simulation = null;
  }

  const container = canvas.parentElement;
  const width = Math.max(400, container.clientWidth - 2);
  const height = Math.max(520, Math.round(width * 0.58));
  canvas.width = width;
  canvas.height = height;
  const ctx = canvas.getContext("2d");
  const transform = graphState.transform;

  const nodes = data.nodes.map((n) => ({ ...n }));
  const nodeById = new Map(nodes.map((n) => [n.id, n]));
  const edges = data.edges.map((e, index) => ({
    ...e,
    edgeKey: `${e.type || "edge"}:${typeof e.source === "string" ? e.source : e.source?.id}:${typeof e.target === "string" ? e.target : e.target?.id}:${index}`,
    source: nodeById.get(typeof e.source === "string" ? e.source : e.source?.id) || e.source,
    target: nodeById.get(typeof e.target === "string" ? e.target : e.target?.id) || e.target,
  }));
  const sameAuthorBuckets = new Map();
  const neighborIds = new Map(nodes.map((node) => [node.id, new Set()]));
  for (const node of nodes) {
    const authorKey = graphAuthorKey(node);
    if (!authorKey) continue;
    if (!sameAuthorBuckets.has(authorKey)) {
      sameAuthorBuckets.set(authorKey, new Set());
    }
    sameAuthorBuckets.get(authorKey).add(node.id);
  }
  for (const edge of edges) {
    const sourceId = edge.source?.id;
    const targetId = edge.target?.id;
    if (!sourceId || !targetId) continue;
    neighborIds.get(sourceId)?.add(targetId);
    neighborIds.get(targetId)?.add(sourceId);
  }

  const maxVisits = Math.max(1, ...nodes.map((n) => n.visits || 0));
  const minNodeRadius = 12;
  const maxNodeRadius = 26;
  const haloPadding = 22;
  const nodeRadius = (node) => {
    const visits = Math.max(0, node.visits || 0);
    const ratio = Math.max(0, visits / maxVisits);
    const compressed = Math.pow(ratio, 0.55);
    return minNodeRadius + compressed * (maxNodeRadius - minNodeRadius);
  };
  const colorMode = $("graph-color-mode")?.value || "author";

  function drawSignalHalo(node, radius, signal) {
    if (!signal.isNew && !signal.isRecentlyUpdated) return;
    const outerRadius = radius + haloPadding + (signal.isNew ? 6 : 0);
    const innerRadius = Math.max(radius * 0.65, radius - 3);
    const gradient = ctx.createRadialGradient(node.x, node.y, innerRadius, node.x, node.y, outerRadius);
    const baseColor = signal.isNew ? "52, 211, 153" : "96, 165, 250";
    gradient.addColorStop(0, `rgba(${baseColor}, ${signal.isNew ? 0.28 : 0.22})`);
    gradient.addColorStop(0.55, `rgba(${baseColor}, ${signal.isNew ? 0.12 : 0.1})`);
    gradient.addColorStop(1, `rgba(${baseColor}, 0)`);
    ctx.beginPath();
    ctx.arc(node.x, node.y, outerRadius, 0, Math.PI * 2);
    ctx.fillStyle = gradient;
    ctx.fill();

    ctx.beginPath();
    ctx.arc(node.x, node.y, radius + (signal.isNew ? 8 : 6), 0, Math.PI * 2);
    ctx.strokeStyle = signal.isNew ? "rgba(110, 231, 183, 0.85)" : "rgba(147, 197, 253, 0.75)";
    ctx.lineWidth = signal.isNew ? 2.6 : 2.2;
    ctx.stroke();
  }

  function resolveSelectionHighlight() {
    const selectedId = graphState.selectedNode?.id;
    const highlightMode = $("graph-highlight-mode")?.value || "both";
    if (!selectedId || highlightMode === "none" || !nodeById.has(selectedId)) {
      return { active: false, nodeIds: new Set(), edgeKeys: new Set() };
    }

    const selected = nodeById.get(selectedId);
    const nodeIds = new Set([selectedId]);
    const edgeKeys = new Set();
    const includeAuthor = highlightMode === "author" || highlightMode === "both";
    const includeLinked = highlightMode === "linked" || highlightMode === "both";

    const sameAuthorIds = includeAuthor
      ? new Set(sameAuthorBuckets.get(graphAuthorKey(selected)) || [])
      : new Set();
    const linkedIds = includeLinked
      ? new Set(neighborIds.get(selectedId) || [])
      : new Set();

    for (const nodeId of sameAuthorIds) {
      nodeIds.add(nodeId);
    }
    for (const nodeId of linkedIds) {
      nodeIds.add(nodeId);
    }

    for (const edge of edges) {
      const sourceId = edge.source?.id;
      const targetId = edge.target?.id;
      if (!sourceId || !targetId) continue;
      const linkedMatch = includeLinked && (sourceId === selectedId || targetId === selectedId);
      const authorMatch = includeAuthor
        && edge.type === "same_author"
        && sameAuthorIds.has(sourceId)
        && sameAuthorIds.has(targetId);
      if (linkedMatch || authorMatch) {
        edgeKeys.add(edge.edgeKey);
      }
    }

    return { active: true, nodeIds, edgeKeys };
  }

  function draw() {
    const highlight = resolveSelectionHighlight();
    ctx.save();
    ctx.clearRect(0, 0, width, height);
    ctx.translate(transform.x, transform.y);
    ctx.scale(transform.k, transform.k);

    for (const edge of edges) {
      const src = edge.source;
      const tgt = edge.target;
      if (!src || !tgt || src.x == null || tgt.x == null) continue;
      const edgeStyle = graphEdgeStyle(edge, colorMode);
      const edgeHighlighted = highlight.active && highlight.edgeKeys.has(edge.edgeKey);
      const edgeDimmed = highlight.active && !edgeHighlighted;
      ctx.beginPath();
      ctx.moveTo(src.x, src.y);
      ctx.lineTo(tgt.x, tgt.y);
      ctx.strokeStyle = edgeStyle.strokeStyle;
      ctx.lineWidth = edgeHighlighted ? edgeStyle.lineWidth + 1.2 : edgeStyle.lineWidth;
      ctx.globalAlpha = edgeDimmed ? 0.12 : edgeHighlighted ? 0.98 : 1;
      ctx.stroke();
      ctx.globalAlpha = 1;
    }

    for (const node of nodes) {
      if (node.x == null) continue;
      const r = nodeRadius(node);
      const isSelected = graphState.selectedNode?.id === node.id;
      const isHovered = graphState.hoveredNode?.id === node.id;
      const nodeHighlighted = highlight.active && highlight.nodeIds.has(node.id);
      const nodeDimmed = highlight.active && !nodeHighlighted && !isSelected && !isHovered;
      const style = graphNodeStyle(node, colorMode);
      const signal = graphSignal(node);
      ctx.globalAlpha = nodeDimmed ? 0.18 : 1;
      drawSignalHalo(node, r, signal);
      ctx.beginPath();
      ctx.arc(node.x, node.y, r, 0, Math.PI * 2);
      ctx.globalAlpha = nodeDimmed ? 0.18 : 0.92;
      ctx.fillStyle = isSelected ? "#ffffff" : isHovered ? "#f8fafc" : style.fill;
      ctx.fill();
      ctx.globalAlpha = 1;
      ctx.strokeStyle = isSelected ? "rgba(255,255,255,0.98)" : nodeHighlighted ? "rgba(250, 204, 21, 0.95)" : style.stroke;
      ctx.lineWidth = isSelected
        ? style.lineWidth + 2.2
        : nodeHighlighted
          ? style.lineWidth + 1.6
          : isHovered
            ? style.lineWidth + 1
            : style.lineWidth;
      ctx.stroke();
      ctx.globalAlpha = 1;
    }
    ctx.restore();
  }

  function autoFit() {
    const valid = nodes.filter((n) => n.x != null);
    if (!valid.length) return;
    const pad = 48;
    const minX = Math.min(...valid.map((n) => n.x - nodeRadius(n))) - pad;
    const maxX = Math.max(...valid.map((n) => n.x + nodeRadius(n))) + pad;
    const minY = Math.min(...valid.map((n) => n.y - nodeRadius(n))) - pad;
    const maxY = Math.max(...valid.map((n) => n.y + nodeRadius(n))) + pad;
    const k = Math.min(width / (maxX - minX), height / (maxY - minY));
    transform.k = k;
    transform.x = (width - (minX + maxX) * k) / 2;
    transform.y = (height - (minY + maxY) * k) / 2;
    draw();
  }

  const simulation = d3
    .forceSimulation(nodes)
    .force(
      "link",
      d3
        .forceLink(edges)
        .id((d) => d.id)
        .strength((d) => (d.type === "same_author" ? 0.5 : 0.08))
        .distance((d) => (d.type === "same_author" ? 120 : 180)),
    )
    .force("charge", d3.forceManyBody().strength(-300).distanceMax(600))
    .force("center", d3.forceCenter(width / 2, height / 2).strength(0.04))
    .force("collide", d3.forceCollide().radius((d) => nodeRadius(d) + 10))
    .alphaDecay(0.012);

  graphState.simulation = simulation;
  simulation.on("tick", draw);
  simulation.on("end", () => { draw(); autoFit(); });

  function worldPos(event) {
    const rect = canvas.getBoundingClientRect();
    return {
      x: (event.clientX - rect.left - transform.x) / transform.k,
      y: (event.clientY - rect.top - transform.y) / transform.k,
    };
  }

  function findNode(wx, wy) {
    let closest = null;
    let closestDist = Infinity;
    for (const node of nodes) {
      if (node.x == null) continue;
      const dx = node.x - wx;
      const dy = node.y - wy;
      const dist = Math.sqrt(dx * dx + dy * dy);
      if (dist < nodeRadius(node) + 4 && dist < closestDist) {
        closest = node;
        closestDist = dist;
      }
    }
    return closest;
  }

  let panStart = null;

  canvas.onmousedown = (event) => {
    const pos = worldPos(event);
    const node = findNode(pos.x, pos.y);
    if (node) {
      graphState.dragging = node;
      node.fx = node.x;
      node.fy = node.y;
      simulation.alphaTarget(0.1).restart();
    } else {
      panStart = { x: event.clientX - transform.x, y: event.clientY - transform.y };
    }
  };

  canvas.onmousemove = (event) => {
    const pos = worldPos(event);
    if (graphState.dragging) {
      graphState.dragging.fx = pos.x;
      graphState.dragging.fy = pos.y;
      return;
    }
    if (panStart) {
      transform.x = event.clientX - panStart.x;
      transform.y = event.clientY - panStart.y;
      draw();
      return;
    }
    const node = findNode(pos.x, pos.y);
    if (node !== graphState.hoveredNode) {
      graphState.hoveredNode = node;
      showGraphTooltip(event, node);
      draw();
    } else if (node) {
      showGraphTooltip(event, node);
    }
  };

  canvas.onmouseup = () => {
    if (graphState.dragging) {
      graphState.dragging.fx = null;
      graphState.dragging.fy = null;
      graphState.dragging = null;
      simulation.alphaTarget(0);
    }
    panStart = null;
  };

  canvas.onmouseleave = () => {
    panStart = null;
    if (graphState.dragging) {
      graphState.dragging.fx = null;
      graphState.dragging.fy = null;
      graphState.dragging = null;
      simulation.alphaTarget(0);
    }
    graphState.hoveredNode = null;
    hideGraphTooltip();
    draw();
  };

  canvas.onclick = (event) => {
    const pos = worldPos(event);
    const node = findNode(pos.x, pos.y);
    if (node) {
      graphState.selectedNode = node;
      renderGraphDetail(node);
    } else {
      graphState.selectedNode = null;
      resetGraphDetail();
    }
    draw();
  };

  canvas.onwheel = (event) => {
    event.preventDefault();
    const rect = canvas.getBoundingClientRect();
    const mx = event.clientX - rect.left;
    const my = event.clientY - rect.top;
    const delta = event.deltaY < 0 ? 1.15 : 1 / 1.15;
    const newK = Math.max(0.1, Math.min(10, transform.k * delta));
    transform.x = mx - (mx - transform.x) * (newK / transform.k);
    transform.y = my - (my - transform.y) * (newK / transform.k);
    transform.k = newK;
    draw();
  };
}

function showGraphTooltip(event, node) {
  const tooltip = $("graph-tooltip");
  if (!node) {
    tooltip.classList.add("hidden");
    return;
  }
  tooltip.innerHTML = `
    <strong>${escapeHtml(node.name || node.id)}</strong><br>
    ${escapeHtml(node.author_name || "-")}<br>
    Visits: ${numberFormat.format(node.visits || 0)}
  `;
  const rect = $("graph-canvas").getBoundingClientRect();
  tooltip.style.left = `${event.clientX - rect.left + 14}px`;
  tooltip.style.top = `${event.clientY - rect.top - 10}px`;
  tooltip.classList.remove("hidden");
}

function hideGraphTooltip() {
  const tooltip = $("graph-tooltip");
  if (tooltip) tooltip.classList.add("hidden");
}

function renderGraphDetail(node) {
  $("graph-selected-label").textContent = node.name || node.id;
  const visibleTags = (node.tags || []).filter((t) => !t.startsWith("system_")).slice(0, 10);
  const portalLinks = normalisePortalLinks(node.portal_links);
  const portalMarkup = renderPortalLinkMarkup(portalLinks, { limit: 12 });
  $("graph-world-detail").innerHTML = `
    <div class="detail-link-panel">
      <strong>Portal Links ${portalLinks.length ? `(${escapeHtml(String(portalLinks.length))})` : ""}</strong>
      <div class="detail-chip-row">${portalMarkup || '<span class="detail-empty-inline">No portal links recorded.</span>'}</div>
    </div>
    <dl class="detail-grid">
      <div><dt>Name</dt><dd>${escapeHtml(node.name || node.id)}</dd></div>
      <div><dt>Author</dt><dd>${escapeHtml(node.author_name || node.author_id || "-")}</dd></div>
      <div><dt>Visits</dt><dd>${numberFormat.format(node.visits || 0)}</dd></div>
      <div><dt>Favorites</dt><dd>${numberFormat.format(node.favorites || 0)}</dd></div>
      <div><dt>Status</dt><dd>${escapeHtml(node.release_status || "-")}</dd></div>
      <div><dt>Tags</dt><dd>${escapeHtml(visibleTags.join(", ") || "-")}</dd></div>
      <div><dt>Portal Count</dt><dd>${portalLinks.length ? numberFormat.format(portalLinks.length) : "-"}</dd></div>
    </dl>
    ${node.world_url ? `<a href="${escapeHtml(node.world_url)}" target="_blank" rel="noreferrer" class="button subtle" style="margin-top:8px;display:inline-block">Open in VRChat</a>` : ""}
  `;
}

// ── Creator Review ────────────────────────────────────────────────────────────

let reviewJobKey = "taiwan";

function populateReviewJobSelect(sources) {
  const sel = $("review-job-select");
  sel.innerHTML = "";
  const reviewJobs = (state.jobs || []).filter((job) => job.creator_review_enabled);
  if (reviewJobs.length) {
    for (const job of reviewJobs) {
      const opt = document.createElement("option");
      opt.value = job.job_key;
      opt.textContent = job.label || job.job_key;
      if (job.job_key === "taiwan") opt.selected = true;
      sel.appendChild(opt);
    }
    if (!sel.value && sel.options.length) {
      sel.value = sel.options[0].value;
    }
    return;
  }
  const jobSources = sources.filter(s => s.source && s.source.startsWith("db:job:"));
  if (!jobSources.length) {
    const opt = document.createElement("option");
    opt.value = "taiwan";
    opt.textContent = "Zh (taiwan)";
    sel.appendChild(opt);
    return;
  }
  for (const s of jobSources) {
    const jobKey = s.source.replace("db:job:", "");
    const opt = document.createElement("option");
    opt.value = jobKey;
    opt.textContent = s.label || jobKey;
    if (jobKey === "taiwan") opt.selected = true;
    sel.appendChild(opt);
  }
}

async function loadCreatorReview() {
  reviewJobKey = $("review-job-select").value || "taiwan";
  const countEl = $("review-pending-count");
  const listEl = $("creator-review-list");
  markPanelLoading("reviewQueue", { page: "review" });
  countEl.textContent = "Loading...";
  listEl.innerHTML = "";
  try {
    const { data } = await fetchJson(`/api/v1/jobs/${reviewJobKey}/pending`, undefined, {
      panelKey: "reviewQueue",
      page: "review",
    });
    if (withPanelRender("reviewQueue", () => renderCreatorReview(data.items || []), (error) => {
      countEl.textContent = "Render error";
      listEl.innerHTML = buildPanelStateMarkup("Creator Review", error.message);
    }, { page: "review" })) {
      markPanelSuccess("reviewQueue", {
        page: "review",
        status: (data.items || []).length ? "ready" : "empty",
      });
    }
  } catch (err) {
    markPanelError("reviewQueue", err, { page: "review" });
    countEl.textContent = "Error";
    listEl.innerHTML = buildPanelStateMarkup("Creator Review", err.message);
  }
}

function renderCreatorReview(worlds) {
  const countEl = $("review-pending-count");
  const listEl = $("creator-review-list");
  listEl.innerHTML = "";

  try {
    const byAuthor = new Map();
    for (const world of worlds || []) {
      const key = world.author_id || "__unknown__";
      if (!byAuthor.has(key)) {
        byAuthor.set(key, { author_id: world.author_id, author_name: world.author_name, worlds: [] });
      }
      byAuthor.get(key).worlds.push(world);
    }

    if (!byAuthor.size) {
      countEl.textContent = "0 pending";
      listEl.innerHTML = `<p class="review-empty">All creators reviewed.</p>`;
      return;
    }

    countEl.textContent = `${byAuthor.size} creators / ${worlds.length} worlds`;
    const fragment = document.createDocumentFragment();

    for (const group of byAuthor.values()) {
      const row = document.createElement("article");
      row.className = "review-author-row";
      row.dataset.authorId = group.author_id || "";

      const main = document.createElement("div");
      main.className = "review-author-main";

      const heading = document.createElement("div");
      heading.className = "review-author-heading";

      const authorName = document.createElement("strong");
      authorName.className = "review-author-name";
      authorName.textContent = group.author_name || group.author_id || "Unknown";

      const authorId = document.createElement("code");
      authorId.className = "review-author-id";
      authorId.textContent = group.author_id || "";

      const summary = document.createElement("span");
      summary.className = "review-author-summary";
      summary.textContent = `${group.worlds.length} world${group.worlds.length === 1 ? "" : "s"}`;

      heading.appendChild(authorName);
      heading.appendChild(authorId);
      heading.appendChild(summary);

      const worldList = document.createElement("div");
      worldList.className = "review-world-list";
      for (const world of group.worlds) {
        const item = document.createElement("div");
        item.className = "review-world-item";

        if (world.thumbnail_url) {
          const img = document.createElement("img");
          img.className = "review-world-thumb";
          img.src = world.thumbnail_url;
          img.alt = "";
          img.loading = "lazy";
          item.appendChild(img);
        } else {
          const placeholder = document.createElement("div");
          placeholder.className = "review-world-thumb review-world-thumb-empty";
          item.appendChild(placeholder);
        }

        const info = document.createElement("div");
        info.className = "review-world-info";
        const visibleTags = (world.tags || []).filter((tag) => !tag.startsWith("author_tag_")).slice(0, 8);
        const releaseStatus = String(world.release_status || "").trim();

        const link = document.createElement("a");
        link.className = "review-world-link";
        link.href = world.world_url || "#";
        link.target = "_blank";
        link.rel = "noreferrer";
        link.textContent = world.name || world.id || "(unnamed)";
        info.appendChild(link);

        const worldId = document.createElement("code");
        worldId.className = "review-world-id";
        worldId.textContent = world.id || "";
        info.appendChild(worldId);

        if (world.description) {
          const desc = document.createElement("p");
          desc.className = "review-world-desc";
          desc.textContent = world.description.length > 120
            ? `${world.description.slice(0, 120)}...`
            : world.description;
          info.appendChild(desc);
        }

        const meta = document.createElement("div");
        meta.className = "review-world-meta";
        const parts = [];
        if (world.visits != null) parts.push(`${world.visits.toLocaleString()} visits`);
        if (world.favorites != null) parts.push(`${world.favorites.toLocaleString()} favorites`);
        if (releaseStatus) parts.push(releaseStatus);
        if (world.tags?.length) {
          const shown = world.tags.filter(t => !t.startsWith("author_tag_")).slice(0, 6);
          if (shown.length) parts.push(shown.map(t => `<span class="review-tag">${escapeHtml(t)}</span>`).join(" "));
        }
        meta.innerHTML = parts.join(" / ");
        info.appendChild(meta);

        const tagRow = document.createElement("div");
        tagRow.className = "review-world-tags";
        tagRow.innerHTML = visibleTags.length
          ? visibleTags.map((tag) => `<span class="review-tag">${escapeHtml(tag)}</span>`).join("")
          : `<span class="review-tag review-tag-muted">No visible tags</span>`;
        info.appendChild(tagRow);

        item.appendChild(info);
        worldList.appendChild(item);
      }

      main.appendChild(heading);
      main.appendChild(worldList);

      const actions = document.createElement("div");
      actions.className = "review-actions";

      const approveButton = document.createElement("button");
      approveButton.className = "button review-approve";
      approveButton.type = "button";
      approveButton.textContent = "OK";

      const rejectButton = document.createElement("button");
      rejectButton.className = "button review-reject";
      rejectButton.type = "button";
      rejectButton.textContent = "Blacklist";

      approveButton.addEventListener("click", () => reviewDecision(group.author_id, "whitelist", row));
      rejectButton.addEventListener("click", () => reviewDecision(group.author_id, "blacklist", row));

      actions.appendChild(approveButton);
      actions.appendChild(rejectButton);

      row.appendChild(main);
      row.appendChild(actions);
      fragment.appendChild(row);
    }

    listEl.appendChild(fragment);
  } catch (error) {
    recordPanelRenderError("reviewQueue", error, { page: "review" });
    countEl.textContent = "Render error";
    listEl.innerHTML = buildPanelStateMarkup("Creator Review", error.message);
  }
}

async function reviewDecision(authorId, action, card) {
  if (!authorId) return;
  const url = action === "whitelist"
    ? `/api/v1/jobs/${reviewJobKey}/creator-whitelist`
    : `/api/v1/jobs/${reviewJobKey}/creator-blacklist`;
  card.classList.add("review-pending-action");
  try {
    await fetchJson(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ user_id: authorId }),
    }, {
      panelKey: "reviewQueue",
      page: "review",
    });
    card.classList.remove("review-pending-action");
    card.classList.add("review-done");
    setTimeout(() => card.remove(), 400);
    const countEl = $("review-pending-count");
    const remaining = $("creator-review-list").querySelectorAll(".review-author-row:not(.review-done)").length - 1;
    if (remaining <= 0) {
      $("creator-review-list").innerHTML = `<p class="review-empty">All creators reviewed.</p>`;
      countEl.textContent = "0 pending";
    } else {
      countEl.textContent = countEl.textContent.replace(/^\d+/, remaining);
    }
  } catch (err) {
    card.classList.remove("review-pending-action");
    alert(`Failed: ${err.message}`);
  }
}

// ── Auto Sync ────────────────────────────────────────────────────────────────

// -- Auto Sync --------------------------------------------------------------
const INTERVAL_OPTIONS = [
  { value: "disabled", label: "Disabled" },
  { value: "1h",  label: "Every 1 hour" },
  { value: "3h",  label: "Every 3 hours" },
  { value: "6h",  label: "Every 6 hours" },
  { value: "12h", label: "Every 12 hours" },
  { value: "1d",  label: "Every day" },
  { value: "2d",  label: "Every 2 days" },
  { value: "7d",  label: "Every week" },
];

async function loadAutoSyncSchedule() {
  const caption = $("auto-sync-caption");
  const list = $("auto-sync-job-list");
  markPanelLoading("autoSyncStatus", { page: "operations", section: "scheduler" });
  try {
    const { data } = await fetchJson("/api/v1/auto-sync/status", undefined, {
      panelKey: "autoSyncStatus",
      page: "operations",
      section: "scheduler",
    });
    const rateLimit = data.rate_limit || {};
    if (Number(rateLimit.active_cooldown_remaining_seconds || 0) > 0 && rateLimit.active_cooldown_until) {
      caption.textContent = `Server-side scheduler cooldown until ${formatDateTime(rateLimit.active_cooldown_until)}`;
    } else {
      caption.textContent = "Server-side scheduler checks every minute";
    }
    if (withPanelRender("autoSyncStatus", () => renderAutoSyncSchedule(data.jobs || {}, rateLimit), (error) => {
      renderAutoSyncScheduleError(error.message);
    }, { page: "operations", section: "scheduler" })) {
      markPanelSuccess("autoSyncStatus", {
        page: "operations",
        section: "scheduler",
        status: Object.keys(data.jobs || {}).length ? "ready" : "empty",
      });
    }
  } catch (err) {
    markPanelError("autoSyncStatus", err, { page: "operations", section: "scheduler" });
    renderAutoSyncScheduleError(err.message);
  }
}

function renderAutoSyncSchedule(statusMap, rateLimit = {}) {
  const list = $("auto-sync-job-list");
  list.innerHTML = "";
  const jobs = Object.values(statusMap);
  if (!jobs.length) {
    list.innerHTML = `<p class="dash-empty">No jobs configured.</p>`;
    return;
  }
  for (const job of jobs) {
    const card = document.createElement("div");
    card.className = "auto-sync-job-card";

    const nextText = job.next_run
      ? new Date(job.next_run).toLocaleString("zh-Hant", { month: "short", day: "numeric", hour: "2-digit", minute: "2-digit" })
      : "--";
    const lastText = job.last_auto_run
      ? new Date(job.last_auto_run).toLocaleString("zh-Hant", { month: "short", day: "numeric", hour: "2-digit", minute: "2-digit" })
      : "Never";
    const attemptText = job.last_attempt_at
      ? new Date(job.last_attempt_at).toLocaleString("zh-Hant", { month: "short", day: "numeric", hour: "2-digit", minute: "2-digit" })
      : "--";
    const triggerLabels = {
      auto: "scheduler auto",
      auto_manual: "auto sync Run Now",
      job_manual: "manual job",
      job: "legacy job",
    };
    const triggerText = triggerLabels[job.last_success_trigger] || job.last_success_trigger || "--";
    const overdueClass = job.overdue && !job.running ? " auto-sync-overdue" : "";
    const schedulerFeedback = job.last_error
      ? escapeHtml(job.last_error)
      : Number(rateLimit.active_cooldown_remaining_seconds || 0) > 0
        ? escapeHtml(rateLimit.strategy_hint || "VRChat cooldown active.")
      : job.running
        ? "Scheduler is running this job now."
      : job.last_attempt_at && !job.last_auto_run
        ? "Scheduler attempted this job, but it did not complete."
        : "";

    const selectId = `interval-select-${job.job_key}`;
    const optionsHtml = INTERVAL_OPTIONS.map(o =>
      `<option value="${o.value}"${o.value === job.interval ? " selected" : ""}>${escapeHtml(o.label)}</option>`
    ).join("");

    card.innerHTML = `
      <div class="auto-sync-job-header">
        <strong class="auto-sync-job-label">${escapeHtml(job.label)}</strong>
        <code class="auto-sync-job-key">${escapeHtml(job.job_key)}</code>
      </div>
      <div class="auto-sync-job-body">
        <div class="auto-sync-times${overdueClass}">
          <div class="auto-sync-time-row"><span>Last run</span><strong>${lastText}</strong></div>
          <div class="auto-sync-time-row"><span>Last success</span><strong>${escapeHtml(triggerText)}</strong></div>
          <div class="auto-sync-time-row"><span>Last attempt</span><strong>${attemptText}</strong></div>
          <div class="auto-sync-time-row"><span>Next run</span><strong>${nextText}</strong></div>
        </div>
        <div class="auto-sync-controls">
          <label class="auto-sync-interval-label">
            Interval
            <select id="${selectId}" class="auto-sync-select">
              ${optionsHtml}
            </select>
          </label>
          <button class="button auto-sync-save-btn" type="button" data-job="${escapeHtml(job.job_key)}">Save</button>
          <button class="button subtle auto-sync-now-btn" type="button" data-job="${escapeHtml(job.job_key)}">Run Now</button>
        </div>
        <p class="helper-copy auto-sync-feedback" data-job-feedback="${escapeHtml(job.job_key)}">${schedulerFeedback}</p>
      </div>`;

    card.querySelector(".auto-sync-save-btn").addEventListener("click", async (e) => {
      const button = e.currentTarget;
      const key = e.currentTarget.dataset.job;
      const sel = document.getElementById(`interval-select-${key}`);
      const feedback = card.querySelector("[data-job-feedback]");
      button.disabled = true;
      try {
        await fetchJson(`/api/v1/auto-sync/${key}/interval`, {
          method: "PUT",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ interval: sel.value }),
        });
        feedback.textContent = `Saved interval: ${sel.value}. Matching jobs were rebalanced.`;
        loadAutoSyncSchedule();
      } catch (err) {
        feedback.textContent = err.message;
      } finally {
        button.disabled = false;
      }
    });

    card.querySelector(".auto-sync-now-btn").addEventListener("click", async (e) => {
      const key = e.currentTarget.dataset.job;
      const button = e.currentTarget;
      const feedback = card.querySelector("[data-job-feedback]");
      button.disabled = true;
      button.textContent = "Running...";
      feedback.textContent = `Running ${job.label}...`;
      try {
        const data = await runAutoSyncJobNow(key);
        const runId = data.run_id ? ` (run #${data.run_id})` : "";
        feedback.textContent = `Completed ${job.label}${runId}.`;
        await loadAutoSyncSchedule();
      } catch (err) {
        feedback.textContent = err.message;
      } finally {
        button.disabled = false;
        button.textContent = "Run Now";
      }
    });

    list.appendChild(card);
  }
}

// ── Project Dashboard ─────────────────────────────────────────────────────────

// -- Project Dashboard ------------------------------------------------------
async function loadDashboard() {
  const payload = buildDashboardPayloadFromInsights(state.collectionInsights);
  const panelPage = state.page === "monitor" ? "monitor" : "dashboard";
  const panelSection = state.page === "monitor" ? state.monitorSection : state.dashboardSection;
  const panelKey = panelPage === "monitor"
    ? "scopeSummary"
    : state.dashboardSection === "health" ? "dashboardHealth" : "dashboardBriefing";
  markPanelLoading(panelKey, {
    page: panelPage,
    section: panelSection,
  });
  const rendered = withPanelRender(
    panelKey,
    () => renderDashboard(payload),
    (error) => {
      renderListPanelState("dash-new-worlds", "Dashboard", error.message);
      renderListPanelState("dash-worth-watching", "Dashboard", error.message);
      renderListPanelState("dash-creators", "Dashboard", error.message);
      renderListPanelState("dash-world-grid", "Dashboard", error.message);
      renderListPanelState("dash-movers", "Dashboard", error.message);
    },
    { page: panelPage, section: panelSection },
  );
  if (rendered) {
    const itemCount = toNumber(payload.stats?.world_count) + (payload.worlds || []).length + (payload.top_movers || []).length;
    markPanelSuccess(panelKey, {
      page: panelPage,
      section: panelSection,
      status: itemCount > 0 ? "ready" : "empty",
    });
  }
}

function buildDashboardPayloadFromInsights(insights) {
  const payload = insights || {};
  const summary = payload.summary || {};
  const growthRows = Array.isArray(payload.growth_leaderboard) ? payload.growth_leaderboard : [];
  const briefing = payload.briefing || {};
  const highlightCandidates = [
    ...(Array.isArray(briefing.worth_watching) ? briefing.worth_watching : payload.worth_watching_leaderboard || []),
    ...(Array.isArray(briefing.new_worlds) ? briefing.new_worlds : payload.new_hot_leaderboard || []),
    ...(Array.isArray(briefing.rising_now) ? briefing.rising_now : payload.rising_now_leaderboard || []),
  ];
  const worlds = [];
  const seenIds = new Set();
  for (const item of highlightCandidates) {
    const worldId = String(item?.id || "").trim();
    if (!worldId || seenIds.has(worldId)) {
      continue;
    }
    seenIds.add(worldId);
    worlds.push(item);
    if (worlds.length >= 8) {
      break;
    }
  }
  return {
    label: payload.label || state.source || $("source-select")?.value || "db:all",
    last_sync_at: summary.last_seen_at || payload.generated_at || null,
    stats: {
      world_count: toNumber(summary.world_count),
      total_visits: toNumber(summary.total_visits),
      total_favorites: toNumber(summary.total_favorites),
      tracked_creators: toNumber(summary.tracked_creators),
      new_worlds_14d: toNumber(summary.new_worlds_14d),
      updated_worlds_30d: toNumber(summary.updated_worlds_30d),
    },
    briefing: {
      new_worlds: (Array.isArray(briefing.new_worlds) ? briefing.new_worlds : payload.new_hot_leaderboard || []).slice(0, 3),
      worth_watching: (Array.isArray(briefing.worth_watching) ? briefing.worth_watching : payload.worth_watching_leaderboard || []).slice(0, 3),
      creators: (Array.isArray(payload.creator_momentum) ? payload.creator_momentum : []).slice(0, 3),
    },
    worlds: worlds.slice(0, 8),
    top_movers: growthRows.slice(0, 5).map((item) => ({
      id: item.id,
      name: item.name,
      delta: toNumber(item.visits_delta_7d),
      visits: toNumber(item.visits),
    })),
  };
}

async function openWorldFromDashboard(worldId) {
  if (!worldId) {
    return;
  }
  state.page = "discover";
  state.discoverSection = "search";
  applyDiscoverSectionDefaults(state.discoverSection);
  renderPage();
  await ensureDiscoverCollection();
  const matched = (state.worlds || []).find((item) => item.id === worldId);
  if (matched) {
    state.selectedWorld = matched;
    renderDetail(matched);
  }
}

function renderDashboardBriefItems(targetId, items, buildRow) {
  const target = $(targetId);
  if (!target) {
    return;
  }
  target.innerHTML = "";
  if (!(items || []).length) {
    target.innerHTML = `<div class="dash-empty">No briefing items yet.</div>`;
    return;
  }
  for (const item of items) {
    target.appendChild(buildRow(item));
  }
}

function renderDashboard(data) {
  const stats = data.stats || {};
  const worlds = data.worlds || [];
  const movers = data.top_movers || [];
  const briefing = data.briefing || {};
  $("dash-eyebrow").textContent = data.label || state.source || "db:all";

  $("dash-world-count").textContent = stats.world_count != null ? numberFormat.format(stats.world_count) : "--";
  $("dash-total-visits").textContent = stats.total_visits != null ? numberFormat.format(stats.total_visits) : "--";
  $("dash-total-favorites").textContent = stats.total_favorites != null ? numberFormat.format(stats.total_favorites) : "--";
  $("dash-avg-visits").textContent = stats.tracked_creators != null ? numberFormat.format(stats.tracked_creators) : "--";

  const lastSyncAt = data.last_sync_at || null;
  if (lastSyncAt) {
    const d = new Date(lastSyncAt);
    $("dash-last-sync").textContent = d.toLocaleString("zh-Hant", {
      month: "short",
      day: "numeric",
      hour: "2-digit",
      minute: "2-digit",
    });
  } else {
    $("dash-last-sync").textContent = "--";
  }

  renderDashboardBriefItems("dash-new-worlds", briefing.new_worlds || [], (item) => {
    const row = document.createElement("article");
    row.className = "dash-brief-item dash-brief-item-action";
    row.innerHTML = `
      <div class="dash-brief-main">
        <strong>${escapeHtml(item.name || item.id)}</strong>
        <p>${escapeHtml(item.author_name || item.author_id || "-")}</p>
      </div>
      <div class="dash-brief-meta">
        <span>${escapeHtml(item.days_since_publication == null ? "-" : `${item.days_since_publication}d old`)}</span>
        <span>${escapeHtml(formatDelta(item.visits_delta_7d))} / 7d</span>
      </div>
    `;
    row.addEventListener("click", () => openWorldFromDashboard(item.id));
    return row;
  });

  renderDashboardBriefItems("dash-worth-watching", briefing.worth_watching || [], (item) => {
    const row = document.createElement("article");
    row.className = "dash-brief-item dash-brief-item-action";
    row.innerHTML = `
      <div class="dash-brief-main">
        <strong>${escapeHtml(item.name || item.id)}</strong>
        <p>${escapeHtml(item.discovery_reason || item.author_name || item.author_id || "-")}</p>
      </div>
      <div class="dash-brief-meta">
        <span>${escapeHtml(item.favorite_rate == null ? "-" : `${item.favorite_rate}% rate`)}</span>
        <span>heat ${escapeHtml(formatMetric(item.heat))}</span>
      </div>
    `;
    row.addEventListener("click", () => openWorldFromDashboard(item.id));
    return row;
  });

  renderDashboardBriefItems("dash-creators", briefing.creators || [], (item) => {
    const row = document.createElement("article");
    row.className = "dash-brief-item";
    row.innerHTML = `
      <div class="dash-brief-main">
        <strong>${escapeHtml(item.author_name || item.author_id || "Unknown")}</strong>
        <p>${escapeHtml(item.lead_world_name || "No lead world yet")}</p>
      </div>
      <div class="dash-brief-meta">
        <span>${escapeHtml(formatDelta(item.recent_visits_delta_7d))} / 7d</span>
        <span>${numberFormat.format(item.active_worlds_30d || 0)} active</span>
      </div>
    `;
    return row;
  });

  const grid = $("dash-world-grid");
  grid.innerHTML = "";
  for (const w of worlds) {
    const card = document.createElement("article");
    card.className = "dash-world-card";
    const userTags = (w.tags || [])
      .filter((tag) => !tag.startsWith("system:") && !tag.startsWith("author_tag_"))
      .slice(0, 3);
    const statLine = [
      w.days_since_publication == null ? null : `${w.days_since_publication}d old`,
      w.favorite_rate == null ? null : `${w.favorite_rate}% rate`,
    ].filter(Boolean).join(" / ");
    card.innerHTML = `
      <a href="${escapeHtml(w.world_url || "#")}" target="_blank" rel="noreferrer" class="dash-world-thumb-wrap">
        ${
          w.thumbnail_url
            ? `<img class="dash-world-thumb" src="${escapeHtml(w.thumbnail_url)}" alt="" loading="lazy">`
            : `<div class="dash-world-thumb dash-world-thumb-empty"></div>`
        }
      </a>
      <div class="dash-world-body">
        <a href="${escapeHtml(w.world_url || "#")}" target="_blank" rel="noreferrer" class="dash-world-name">${escapeHtml(w.name || w.id)}</a>
        <div class="dash-world-meta">
          <span class="dash-world-stat">Visits ${numberFormat.format(w.visits ?? 0)}</span>
          <span class="dash-world-stat">Fav ${numberFormat.format(w.favorites ?? 0)}</span>
        </div>
        ${
          userTags.length
            ? `<div class="dash-world-tags">${userTags.map((t) => `<span class="dash-tag">${escapeHtml(t)}</span>`).join("")}</div>`
            : ""
        }
      </div>`;
    const body = card.querySelector(".dash-world-body");
    if (body && statLine) {
      const meta = document.createElement("div");
      meta.className = "dash-world-meta";
      meta.textContent = statLine;
      body.appendChild(meta);
    }
    if (body) {
      const reason = document.createElement("p");
      reason.className = "helper-copy";
      reason.textContent = w.discovery_reason || "watchlist highlight";
      body.appendChild(reason);
    }
    card.addEventListener("click", async (e) => {
      if (e.target.tagName === "A") return;
      await openWorldFromDashboard(w.id);
    });
    grid.appendChild(card);
  }

  const moversEl = $("dash-movers");
  moversEl.innerHTML = "";
  if (!movers.length) {
    moversEl.innerHTML = `<p class="dash-empty">No history data yet.</p>`;
    return;
  }
  for (const m of movers) {
    const row = document.createElement("div");
    row.className = "dash-mover-row";
    const delta = toNumber(m.delta);
    const sign = delta >= 0 ? "+" : "";
    row.innerHTML = `
      <div class="dash-mover-name">${escapeHtml(m.name || m.id)}</div>
      <div class="dash-mover-delta ${delta >= 0 ? "positive" : "negative"}">${sign}${numberFormat.format(delta)}</div>`;
    moversEl.appendChild(row);
  }
}

async function boot() {
  recordDebugLifecycle("boot:start", {
    page: state.page,
    section: currentSectionForPage(),
    detail: "boot sequence start",
  });
  loadAuthState();
  loadAutoSyncState();
  loadNotificationState();
  loadUiSettings();
  renderAuthStatus();
  renderUiSettings();
  renderPage();
  recordDebugLifecycle("boot:attachControls", {
    page: state.page,
    section: currentSectionForPage(),
    detail: "attach controls",
  });
  attachControls();
  renderGraphLegend();
  renderJobCreateMode();
  submitJsonForm("keyword-form", "/api/v1/search/keyword", (formData) => ({
    keyword: formData.get("keyword"),
    limit: Number(formData.get("limit") || 50),
  }));
  submitJsonForm("user-form", "/api/v1/search/user", (formData) => ({
    user_id: formData.get("user_id"),
    limit: Number(formData.get("limit") || 50),
  }));
  submitJsonForm("world-search-form", "/api/v1/search/worlds", (formData) => ({
    source_name: formData.get("source_name"),
    search: formData.get("search"),
    tags: formData.get("tags"),
    notags: formData.get("notags"),
    sort: formData.get("sort"),
    order: "descending",
    featured: formData.get("featured"),
    active: formData.get("active") === "true",
    limit: Number(formData.get("limit") || 50),
  }));
  submitJsonForm("fixed-form", "/api/v1/search/fixed", (formData) => ({
    source_name: formData.get("source_name"),
    keywords: formData.get("keywords"),
    blacklist: formData.get("blacklist"),
    limit_per_keyword: 50,
  }));
  recordDebugLifecycle("boot:loadSources", {
    page: state.page,
    section: currentSectionForPage(),
    detail: "load initial scope + ancillary panels",
  });
  await loadSources("db:all");
  await Promise.all([
    loadScopeOverview("db:all"),
    refreshAncillaryPanels(),
    loadTaiwanBlacklist(),
    loadTaiwanCreatorWhitelist(),
    loadTaiwanCreatorBlacklist(),
  ]);
  await ensureServerAuthPersisted("page open");
  await refreshAuthStatusCheck();
  await loadAutoSyncSchedule();
  await maybeRunAutoSync("page open");
  if (!state.notificationPollHandle) {
    state.notificationPollHandle = window.setInterval(() => {
      pollRunStatusAndNotifications();
    }, 60000);
  }
}

boot().catch((error) => {
  state.debug.lastRenderError = error.message || "Boot failed.";
  recordDebugLifecycle("boot:error", {
    page: state.page,
    section: currentSectionForPage(),
    detail: error.message || "Boot failed.",
  });
  setHealthIndicator("ERROR", "error");
  renderSyncStatus(error.message || "Boot failed.", "warn");
  if (state.page === "debug") {
    renderDebugPage();
  }
});
