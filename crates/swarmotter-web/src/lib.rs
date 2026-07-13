// SPDX-License-Identifier: Apache-2.0

//! Web support for SwarmOtter.
//!
//! Serves a practical, function-over-form Web UI that consumes the same API
//! exposed to external automation (ADR-0004, ADR-0006). The UI is plain HTML +
//! vanilla JS with no heavy framework, prioritizing fast load and complete
//! operational coverage. The torrent list uses a vendored Tabulator grid for
//! standard table sorting, filtering, and refresh behavior without requiring a
//! runtime CDN or frontend build step.
//!
//! The UI assets are embedded at compile time so the daemon serves a single
//! binary with no external static files.

use axum::{
    body::Body,
    http::{header, HeaderValue, Request as HttpRequest, StatusCode},
    middleware::{self, Next},
    response::{Html, IntoResponse, Response},
    routing::get,
    Router,
};

const INDEX_HTML: &str = include_str!("../assets/index.html");
const APP_JS_ENTRY: &str = include_str!("../assets/app.js");
const API_JS: &str = include_str!("../assets/js/api.js");
const STATE_JS: &str = include_str!("../assets/js/state.js");
const TORRENTS_JS: &str = include_str!("../assets/js/torrents.js");
const DETAILS_JS: &str = include_str!("../assets/js/details.js");
const SETTINGS_JS: &str = include_str!("../assets/js/settings.js");
const EVENTS_JS: &str = include_str!("../assets/js/events.js");
const UI_JS: &str = include_str!("../assets/js/ui.js");
#[cfg(test)]
const APP_JS: &str = concat!(
    include_str!("../assets/app.js"),
    include_str!("../assets/js/api.js"),
    include_str!("../assets/js/state.js"),
    include_str!("../assets/js/torrents.js"),
    include_str!("../assets/js/details.js"),
    include_str!("../assets/js/settings.js"),
    include_str!("../assets/js/events.js"),
    include_str!("../assets/js/ui.js"),
);
const SEEDING_POLICY_JS: &str = include_str!("../assets/seeding-policy.js");
const WATCH_HISTORY_JS: &str = include_str!("../assets/watch-history.js");
const THEME_BOOTSTRAP_JS: &str = include_str!("../assets/theme-bootstrap.js");
const STYLE_CSS: &str = include_str!("../assets/style.css");
const TABULATOR_JS: &str = include_str!("../assets/vendor/tabulator/tabulator.min.js");
const TABULATOR_CSS: &str = include_str!("../assets/vendor/tabulator/tabulator_midnight.min.css");
const TABULATOR_LICENSE: &str = include_str!("../assets/vendor/tabulator/LICENSE");
const FAVICON_ICO: &[u8] = include_bytes!("../../../assets/graphics/web/favicon.ico");
const FAVICON_16: &[u8] = include_bytes!("../../../assets/graphics/web/favicon-16x16.png");
const FAVICON_32: &[u8] = include_bytes!("../../../assets/graphics/web/favicon-32x32.png");
const FAVICON_48: &[u8] = include_bytes!("../../../assets/graphics/web/favicon-48x48.png");
const APPLE_TOUCH_ICON: &[u8] = include_bytes!("../../../assets/graphics/web/apple-touch-icon.png");
const ANDROID_CHROME_192: &[u8] =
    include_bytes!("../../../assets/graphics/web/android-chrome-192x192.png");
const ANDROID_CHROME_512: &[u8] =
    include_bytes!("../../../assets/graphics/web/android-chrome-512x512.png");
const MASKABLE_ICON_192: &[u8] =
    include_bytes!("../../../assets/graphics/web/maskable-icon-192x192.png");
const MASKABLE_ICON_512: &[u8] =
    include_bytes!("../../../assets/graphics/web/maskable-icon-512x512.png");
const MSTILE_150: &[u8] = include_bytes!("../../../assets/graphics/web/mstile-150x150.png");
const SITE_WEBMANIFEST: &[u8] = include_bytes!("../../../assets/graphics/web/site.webmanifest");
const HEADER_LOGO: &[u8] =
    include_bytes!("../../../assets/graphics/icons/swarmotter-icon-64x64.png");

/// Build the web UI router, mounted at `/` (excluding `/api`).
pub fn web_router() -> Router {
    Router::new()
        .route("/", get(index))
        .route("/index.html", get(index))
        .route("/app.js", get(app_js))
        .route("/js/api.js", get(api_js))
        .route("/js/state.js", get(state_js))
        .route("/js/torrents.js", get(torrents_js))
        .route("/js/details.js", get(details_js))
        .route("/js/settings.js", get(settings_js))
        .route("/js/events.js", get(events_js))
        .route("/js/ui.js", get(ui_js))
        .route("/seeding-policy.js", get(seeding_policy_js))
        .route("/watch-history.js", get(watch_history_js))
        .route("/theme-bootstrap.js", get(theme_bootstrap_js))
        .route("/style.css", get(style_css))
        .route("/vendor/tabulator/tabulator.min.js", get(tabulator_js))
        .route(
            "/vendor/tabulator/tabulator_midnight.min.css",
            get(tabulator_css),
        )
        .route("/vendor/tabulator/LICENSE", get(tabulator_license))
        .route("/favicon.ico", get(favicon_ico))
        .route("/favicon-16x16.png", get(favicon_16))
        .route("/favicon-32x32.png", get(favicon_32))
        .route("/favicon-48x48.png", get(favicon_48))
        .route("/apple-touch-icon.png", get(apple_touch_icon))
        .route("/android-chrome-192x192.png", get(android_chrome_192))
        .route("/android-chrome-512x512.png", get(android_chrome_512))
        .route("/maskable-icon-192x192.png", get(maskable_icon_192))
        .route("/maskable-icon-512x512.png", get(maskable_icon_512))
        .route("/mstile-150x150.png", get(mstile_150))
        .route("/site.webmanifest", get(site_webmanifest))
        .route("/swarmotter-icon-64x64.png", get(header_logo))
        .layer(middleware::from_fn(add_security_headers))
}

async fn index() -> Response {
    Html(INDEX_HTML).into_response()
}

async fn app_js() -> Response {
    javascript(APP_JS_ENTRY)
}

async fn api_js() -> Response {
    javascript(API_JS)
}

async fn state_js() -> Response {
    javascript(STATE_JS)
}

async fn torrents_js() -> Response {
    javascript(TORRENTS_JS)
}

async fn details_js() -> Response {
    javascript(DETAILS_JS)
}

async fn settings_js() -> Response {
    javascript(SETTINGS_JS)
}

async fn events_js() -> Response {
    javascript(EVENTS_JS)
}

async fn ui_js() -> Response {
    javascript(UI_JS)
}

fn javascript(source: &'static str) -> Response {
    (
        [(
            axum::http::header::CONTENT_TYPE,
            "application/javascript; charset=utf-8",
        )],
        source,
    )
        .into_response()
}

async fn seeding_policy_js() -> Response {
    (
        [(
            axum::http::header::CONTENT_TYPE,
            "application/javascript; charset=utf-8",
        )],
        SEEDING_POLICY_JS,
    )
        .into_response()
}

async fn watch_history_js() -> Response {
    (
        [(
            axum::http::header::CONTENT_TYPE,
            "application/javascript; charset=utf-8",
        )],
        WATCH_HISTORY_JS,
    )
        .into_response()
}

async fn theme_bootstrap_js() -> Response {
    (
        [(
            axum::http::header::CONTENT_TYPE,
            "application/javascript; charset=utf-8",
        )],
        THEME_BOOTSTRAP_JS,
    )
        .into_response()
}

async fn add_security_headers(request: HttpRequest<Body>, next: Next) -> Response {
    let mut response = next.run(request).await;
    let headers = response.headers_mut();
    headers.insert(
        header::CONTENT_SECURITY_POLICY,
        HeaderValue::from_static(
            "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; img-src 'self'; connect-src 'self'; object-src 'none'; base-uri 'none'; frame-ancestors 'none'; form-action 'self'",
        ),
    );
    headers.insert(
        header::X_CONTENT_TYPE_OPTIONS,
        HeaderValue::from_static("nosniff"),
    );
    headers.insert(
        header::REFERRER_POLICY,
        HeaderValue::from_static("no-referrer"),
    );
    headers.insert("x-frame-options", HeaderValue::from_static("DENY"));
    response
}

async fn style_css() -> Response {
    (
        [(header::CONTENT_TYPE, "text/css; charset=utf-8")],
        STYLE_CSS,
    )
        .into_response()
}

async fn tabulator_js() -> Response {
    (
        [(
            axum::http::header::CONTENT_TYPE,
            "application/javascript; charset=utf-8",
        )],
        TABULATOR_JS,
    )
        .into_response()
}

async fn tabulator_css() -> Response {
    (
        [(header::CONTENT_TYPE, "text/css; charset=utf-8")],
        TABULATOR_CSS,
    )
        .into_response()
}

async fn tabulator_license() -> Response {
    (
        [(header::CONTENT_TYPE, "text/plain; charset=utf-8")],
        TABULATOR_LICENSE,
    )
        .into_response()
}

async fn favicon_ico() -> Response {
    static_asset("image/x-icon", FAVICON_ICO)
}

async fn favicon_16() -> Response {
    static_asset("image/png", FAVICON_16)
}

async fn favicon_32() -> Response {
    static_asset("image/png", FAVICON_32)
}

async fn favicon_48() -> Response {
    static_asset("image/png", FAVICON_48)
}

async fn apple_touch_icon() -> Response {
    static_asset("image/png", APPLE_TOUCH_ICON)
}

async fn android_chrome_192() -> Response {
    static_asset("image/png", ANDROID_CHROME_192)
}

async fn android_chrome_512() -> Response {
    static_asset("image/png", ANDROID_CHROME_512)
}

async fn maskable_icon_192() -> Response {
    static_asset("image/png", MASKABLE_ICON_192)
}

async fn maskable_icon_512() -> Response {
    static_asset("image/png", MASKABLE_ICON_512)
}

async fn mstile_150() -> Response {
    static_asset("image/png", MSTILE_150)
}

async fn site_webmanifest() -> Response {
    static_asset("application/manifest+json", SITE_WEBMANIFEST)
}

async fn header_logo() -> Response {
    static_asset("image/png", HEADER_LOGO)
}

fn static_asset(content_type: &'static str, body: &'static [u8]) -> Response {
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, content_type)
        .header(header::CACHE_CONTROL, "public, max-age=86400")
        .body(Body::from(body))
        .expect("static asset response is valid")
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    #[test]
    fn assets_are_nonempty() {
        assert!(!INDEX_HTML.is_empty());
        assert!(!APP_JS.is_empty());
        assert!(!APP_JS_ENTRY.is_empty());
        assert!(!API_JS.is_empty());
        assert!(!STATE_JS.is_empty());
        assert!(!TORRENTS_JS.is_empty());
        assert!(!DETAILS_JS.is_empty());
        assert!(!SETTINGS_JS.is_empty());
        assert!(!EVENTS_JS.is_empty());
        assert!(!UI_JS.is_empty());
        assert!(!THEME_BOOTSTRAP_JS.is_empty());
        assert!(!STYLE_CSS.is_empty());
        assert!(!TABULATOR_JS.is_empty());
        assert!(!TABULATOR_CSS.is_empty());
        assert!(!TABULATOR_LICENSE.is_empty());
        assert!(!FAVICON_ICO.is_empty());
        assert!(!FAVICON_48.is_empty());
        assert!(!SITE_WEBMANIFEST.is_empty());
        assert!(!HEADER_LOGO.is_empty());
    }

    #[test]
    fn web_ui_includes_health_indicator() {
        // Health bars + health label classes must be present in the CSS so
        // the per-torrent health indicator renders consistently across the
        // list and the details view. The JS renderer must also exist.
        for needle in [
            ".torrent-health",
            ".health-bars",
            ".bar.active",
            ".health-excellent",
            ".health-good",
            ".health-fair",
            ".health-poor",
            ".health-critical",
            ".health-stalled",
            ".health-network-blocked",
            ".health-paused",
            ".health-complete",
            ".health-unknown",
        ] {
            assert!(
                STYLE_CSS.contains(needle),
                "style.css is missing health CSS class {needle}"
            );
        }
        for needle in [
            "function renderHealth(",
            "function renderDetailsHealth(",
            "function healthLabelName(",
            "torrent-health${healthClass}",
            "title: \"Health\"",
            "function renderPeerCount(",
            "formatter: cell => renderPeerCount(cell.getRow().getData())",
            "function renderTorrentActions(",
        ] {
            assert!(
                APP_JS.contains(needle) || INDEX_HTML.contains(needle),
                "Web UI is missing health markup {needle}"
            );
        }
    }

    #[test]
    fn web_ui_uses_icon_torrent_actions() {
        for needle in [
            "const TORRENT_ACTIONS",
            "data-act=\"${action.act}\"",
            "class=\"icon-button${danger}\"",
            "aria-label=\"${action.label}\"",
            "title=\"${action.label}\"",
            ".torrent-actions",
            "button.icon-button svg",
        ] {
            assert!(
                APP_JS.contains(needle) || STYLE_CSS.contains(needle),
                "Web UI is missing icon action support {needle}"
            );
        }
        for old_button in [
            "<button data-act=\"pause\">Pause</button>",
            "<button data-act=\"resume\">Resume</button>",
            "<button data-act=\"recheck\">Recheck</button>",
            "<button data-act=\"remove\" class=\"danger\">Remove</button>",
        ] {
            assert!(
                !APP_JS.contains(old_button),
                "Web UI still contains text action button {old_button}"
            );
        }
    }

    #[test]
    fn web_ui_torrent_encryption_setting_is_wired() {
        assert!(
            INDEX_HTML.contains("id=\"cfg-torrent-encryption-mode\""),
            "Web UI is missing torrent encryption mode field"
        );
        for (mode, label) in [
            ("disabled", "Disabled"),
            ("preferred", "Preferred"),
            ("required", "Required"),
        ] {
            assert!(
                INDEX_HTML.contains(&format!("<option value=\"{mode}\">{label}</option>")),
                "Web UI is missing torrent encryption option {mode}"
            );
        }
        assert!(
            APP_JS.contains("setSettingsValue(\"cfg-torrent-encryption-mode\", torrent.encryption_mode || \"preferred\")"),
            "Web UI is missing torrent encryption load wiring"
        );
        assert!(
            APP_JS.contains("encryption_mode: settingsString(\"cfg-torrent-encryption-mode\")"),
            "Web UI is missing torrent encryption save wiring"
        );
    }

    #[test]
    fn web_ui_sends_api_auth_token_when_configured() {
        for needle in [
            "const API_TOKEN_STORAGE_KEY = \"swarmotter.apiToken\";",
            "headers.set(\"x-swarmotter-auth\", token);",
            "async function promptForApiToken(",
            "async function apiFetch(",
            "if (res.status === 401 && retryAuth)",
            "saveApiToken(nextConfig.api.auth_token);",
            "accept: \"text/event-stream\"",
            "function dispatchEventStreamBlock(",
        ] {
            assert!(
                APP_JS.contains(needle),
                "Web UI API auth support is missing {needle}"
            );
        }
    }

    #[test]
    fn web_ui_supports_bulk_torrent_selection() {
        for id in [
            "select-all-torrents-btn",
            "deselect-all-torrents-btn",
            "remove-selected-torrents-btn",
            "selection-summary",
        ] {
            assert!(
                INDEX_HTML.contains(&format!("id=\"{}\"", id)),
                "Torrent selection toolbar is missing field id {id}"
            );
        }
        for needle in [
            "cssClass: \"selection-column\"",
            "aria-label=\"Torrent selection actions\"",
            "Remove Selected",
        ] {
            assert!(
                INDEX_HTML.contains(needle) || APP_JS.contains(needle),
                "Torrent selection markup is missing {needle}"
            );
        }
        for needle in [
            "selectedTorrents: new Map(),",
            "visibleTorrents: [],",
            "torrentTable: null,",
            "bulkRemoveInFlight: false,",
            "new Tabulator(\"#torrent-table\"",
            "function torrentSelectionFormatter(",
            "function renderTorrentSelection(",
            "function updateSelectionControls(",
            "function selectAllVisibleTorrents(",
            "function deselectAllTorrents(",
            "async function removeSelectedTorrents(",
            "Downloaded data will be kept.",
            "api(\"/torrents/remove\"",
            "info_hashes: selected.map(([hash]) => hash)",
            "not_found",
            "state.selectedTorrents.delete(hash);",
            "$(\"#select-all-torrents-btn\").addEventListener(\"click\", selectAllVisibleTorrents);",
            "$(\"#deselect-all-torrents-btn\").addEventListener(\"click\", deselectAllTorrents);",
            "$(\"#remove-selected-torrents-btn\").addEventListener(\"click\", removeSelectedTorrents);",
        ] {
            assert!(APP_JS.contains(needle), "Web UI is missing bulk selection JS {needle}");
        }
        for needle in [
            ".bulk-actions",
            ".selection-summary",
            ".torrent-table",
            ".torrent-select",
            ".tabulator-row.selected",
        ] {
            assert!(
                STYLE_CSS.contains(needle),
                "style.css is missing bulk selection support {needle}"
            );
        }
    }

    #[test]
    fn web_ui_uses_tabulator_for_torrent_table_features() {
        for needle in [
            "/vendor/tabulator/tabulator_midnight.min.css",
            "/vendor/tabulator/tabulator.min.js",
            "id=\"torrent-table\" class=\"torrent-table\"",
            "id=\"clear-torrent-filters-btn\"",
        ] {
            assert!(
                INDEX_HTML.contains(needle),
                "Web UI is missing Tabulator markup {needle}"
            );
        }
        for needle in ["Tabulator v6.5.0", "The MIT License (MIT)"] {
            assert!(
                TABULATOR_JS.contains(needle)
                    || TABULATOR_CSS.contains(needle)
                    || TABULATOR_LICENSE.contains(needle),
                "Vendored Tabulator asset is missing {needle}"
            );
        }
        for needle in [
            "layout: \"fitDataStretch\"",
            "movableColumns: true",
            "initialSort: [{ column: \"name\", dir: \"asc\" }]",
            "headerFilter: \"input\"",
            "headerFilter: \"list\"",
            "headerFilterParams: { valuesLookup: true, clearable: true }",
            "headerFilterFunc: numericHeaderFilter",
            "function parseNumericFilter(",
            "function clearTorrentFilters(",
            "torrentTableReady: Promise.resolve(),",
            "state.torrentTable.on(\"tableBuilt\"",
            "await state.torrentTableReady;",
            "state.torrentTable.replaceData(rows)",
            "state.torrentTable.getRows(\"active\")",
        ] {
            assert!(
                APP_JS.contains(needle),
                "Torrent table is missing Tabulator feature support {needle}"
            );
        }
        for needle in [
            ".tabulator.torrent-table",
            ".tabulator.torrent-table .tabulator-header .tabulator-col .tabulator-header-filter input",
            ".tabulator.torrent-table .tabulator-header .tabulator-col .tabulator-header-filter select",
            ".tabulator.torrent-table .tabulator-row.selected",
        ] {
            assert!(
                STYLE_CSS.contains(needle),
                "style.css is missing Tabulator table styling {needle}"
            );
        }
    }

    #[test]
    fn web_ui_supports_large_library_query_controls() {
        for id in [
            "torrent-state-filter",
            "torrent-health-filter",
            "torrent-performance-filter",
            "torrent-per-page",
            "torrent-prev-page-btn",
            "torrent-next-page-btn",
            "save-torrent-view-btn",
            "load-torrent-view-btn",
            "clear-torrent-view-btn",
            "query-summary",
        ] {
            assert!(
                INDEX_HTML.contains(&format!("id=\"{}\"", id)),
                "Large-library torrent controls are missing field id {id}"
            );
        }
        for needle in [
            "const TORRENT_QUERY_STORAGE_KEY = \"swarmotter.torrentQueryView\";",
            "api(`/torrents/query${queryParams ? `?${queryParams}` : \"\"}`)",
            "function buildTorrentQueryParams(",
            "function saveTorrentQueryView(",
            "function loadTorrentQueryView(",
            "function clearTorrentQueryView(",
            "state.torrentTable.on(\"dataSorted\"",
            "function handleTorrentTableSort(",
            "function renderTorrentQuerySummary(",
            "$(\"#save-torrent-view-btn\").addEventListener(\"click\", saveTorrentQueryView);",
        ] {
            assert!(
                APP_JS.contains(needle),
                "Web UI is missing large-library query support {needle}"
            );
        }
        for needle in [
            ".torrent-query-controls",
            ".torrent-query-field",
            ".torrent-pagination",
            "#query-summary",
        ] {
            assert!(
                STYLE_CSS.contains(needle),
                "style.css is missing large-library query styling {needle}"
            );
        }
    }

    #[test]
    fn web_ui_supports_light_dark_theme_toggle() {
        for needle in [
            "<html lang=\"en\" data-theme=\"dark\">",
            "id=\"theme-toggle\"",
            "class=\"icon-button theme-toggle\"",
            "theme-icon-sun",
            "theme-icon-moon",
            "src=\"/theme-bootstrap.js\"",
        ] {
            assert!(
                INDEX_HTML.contains(needle),
                "Web UI is missing theme toggle markup {needle}"
            );
        }
        for needle in [
            "swarmotter.theme",
            "document.documentElement.dataset.theme = theme;",
        ] {
            assert!(
                THEME_BOOTSTRAP_JS.contains(needle),
                "Theme bootstrap script is missing {needle}"
            );
        }
        for needle in [
            "const THEME_STORAGE_KEY = \"swarmotter.theme\";",
            "const DEFAULT_THEME = THEME_DARK;",
            "function loadThemePreference(",
            "function applyTheme(",
            "function refreshTorrentTableTheme(",
            "function toggleTheme(",
            "document.documentElement.dataset.theme = next;",
            "tableElement.dataset.theme = theme;",
            "state.torrentTable.redraw(true)",
            "window.localStorage.setItem(THEME_STORAGE_KEY, next);",
            "themeToggle.addEventListener(\"click\", toggleTheme);",
        ] {
            assert!(
                APP_JS.contains(needle),
                "Web UI is missing theme toggle behavior {needle}"
            );
        }
        for needle in [
            "[data-theme=\"light\"]",
            "--header-bg",
            "--field-bg",
            "--row-selected-bg",
            ".header-actions",
            "[data-theme=\"dark\"] #theme-toggle .theme-icon-sun",
            "[data-theme=\"light\"] #theme-toggle .theme-icon-moon",
            ".tabulator.torrent-table",
            ".tabulator.torrent-table .tabulator-tableholder .tabulator-table",
            ".tabulator.torrent-table .tabulator-header .tabulator-col .tabulator-header-filter input",
        ] {
            assert!(
                STYLE_CSS.contains(needle),
                "style.css is missing theme styling {needle}"
            );
        }
    }

    #[test]
    fn web_ui_uses_toast_notifications() {
        for needle in [
            "const DEFAULT_TOAST_DISPLAY_MS = 5000",
            "const MAX_VISIBLE_TOASTS = 3",
            "function showToast(",
            "function normalizeToastDurationMs(",
            "function setToastDisplaySeconds(",
            "swarmotter.toastDisplayMs",
            "expectedRemovedTorrents",
            "magnetAddInFlight",
            "duplicate_torrent",
            "showToast(\"Torrent removed\"",
            "showToast(\"Adding magnet\"",
            "showToast(`Added ${added} file",
            "failed++",
            "showToast(\"No files added\"",
            "id=\"toast-region\"",
            "id=\"toast-seconds\"",
            ".toast-region",
            ".toast.success",
            ".toast.error",
        ] {
            assert!(
                APP_JS.contains(needle)
                    || INDEX_HTML.contains(needle)
                    || STYLE_CSS.contains(needle),
                "Web UI is missing toast notification support {needle}"
            );
        }
        for old_message_surface in [
            "alert(",
            "id=\"drop-status\"",
            "id=\"add-magnet-result\"",
            "id=\"add-file-result\"",
            "id=\"save-bw-result\"",
        ] {
            assert!(
                !APP_JS.contains(old_message_surface) && !INDEX_HTML.contains(old_message_surface),
                "Web UI still contains old message surface {old_message_surface}"
            );
        }
    }

    #[test]
    fn web_ui_includes_extended_view_markup() {
        for id in [
            "network-summary",
            "network-config",
            "network-interfaces",
            "network-originality",
            "settings-editor",
            "settings-api",
            "settings-compatibility",
            "settings-autopilot",
            "settings-storage",
            "settings-network",
            "settings-torrent",
            "settings-bandwidth",
            "settings-queue",
            "settings-seeding",
            "settings-dht",
            "settings-pex",
            "settings-watch",
            "settings-watch-list",
            "settings-logging",
            "settings-interface",
            "settings-save-status",
            "watch-config",
            "watch-history",
            "watch-scan-result",
            "log-controls",
            "log-stream",
            "doctor-storage",
            "doctor-summary",
            "doctor-application",
            "doctor-checks",
            "details-controls",
            "details-activity",
            "tracker-add-url",
            "remove-torrent-dialog",
        ] {
            assert!(
                INDEX_HTML.contains(&format!("id=\"{}\"", id)),
                "Web UI is missing placeholder id {id}"
            );
        }

        for needle in [
            "id=\"health-badge\"",
            "data-view=\"doctor\"",
            "id=\"view-doctor\"",
            "class=\"view-grid\"",
            "class=\"settings-layout\"",
            "class=\"settings-header\"",
            "class=\"settings-shell\"",
            "class=\"settings-nav\"",
            "data-settings-target=\"api\"",
            "data-settings-panel=\"api\"",
            "class=\"watch-layout\"",
        ] {
            assert!(
                INDEX_HTML.contains(needle),
                "Web UI is missing markup marker {needle}"
            );
        }
    }

    #[test]
    fn web_ui_exposes_required_torrent_operations() {
        for id in [
            "details-start-btn",
            "details-stop-btn",
            "details-reannounce-btn",
            "details-queue-top-btn",
            "details-queue-up-btn",
            "details-queue-down-btn",
            "details-queue-bottom-btn",
            "details-move-path",
            "details-labels",
            "details-download-limit",
            "details-upload-limit",
            "details-limits-btn",
            "details-ratio-inherit",
            "details-ratio-limit",
            "details-idle-inherit",
            "details-idle-limit",
            "details-seed-forever",
            "details-seeding-save-btn",
            "details-seeding-error",
            "tracker-add-btn",
        ] {
            assert!(
                INDEX_HTML.contains(&format!("id=\"{}\"", id)),
                "Web UI is missing required operation control {id}"
            );
        }
        for needle in [
            "function runDetailsCommand(",
            "\"/queue/move-top\"",
            "\"/queue/move-bottom\"",
            "\"/reannounce\"",
            "\"/move\"",
            "\"/labels\"",
            "\"/limits\"",
            "/seeding`",
            "/trackers/edit",
            "/files/${fi}/rename",
            "function renderDetailsActivity(",
        ] {
            assert!(
                APP_JS.contains(needle) || SEEDING_POLICY_JS.contains(needle),
                "Web UI operation wiring is missing {needle}"
            );
        }
    }

    #[test]
    fn web_ui_renders_and_replaces_seeding_policy_without_optimistic_drift() {
        for needle in [
            "[\"Ratio\", finiteNumber(t.ratio)",
            "[\"Uploaded\", fmtBytes(t.uploaded)]",
            "[\"Seeding status\"",
            "[\"Stored ratio target\"",
            "[\"Effective ratio target\"",
            "[\"Stored idle target\"",
            "[\"Effective idle target\"",
            "ratio_limit: ratio",
            "idle_limit: idle",
            "seed_forever: field(document, \"details-seed-forever\").checked",
            "method: \"PUT\"",
            "Number.isSafeInteger(idle)",
            "errorPanel.textContent = error?.message",
            "await openDetails(hash);",
        ] {
            assert!(
                APP_JS.contains(needle) || SEEDING_POLICY_JS.contains(needle),
                "Seeding policy UI behavior is missing {needle}"
            );
        }
        assert!(INDEX_HTML.contains("0 stops immediately"));
        assert!(INDEX_HTML.contains("Inherit global ratio limit"));
        assert!(INDEX_HTML.contains("Inherit global idle limit"));
    }

    #[test]
    fn executable_seeding_policy_dom_state_harness_passes() {
        let script =
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/seeding-policy.test.js");
        let output = std::process::Command::new("node")
            .arg(script)
            .output()
            .expect("Node.js is required by the Web UI quality gate");
        assert!(
            output.status.success(),
            "seeding policy DOM harness failed:\n{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }

    #[test]
    fn web_ui_renders_stable_watch_outcomes_and_post_action_errors() {
        for needle in [
            "src=\"/watch-history.js\"",
            "<th>Outcome</th>",
            "watchHistoryUi.outcomeLabel(item)",
            "watchHistoryUi.statusKey(item)",
            "watchHistoryUi.detail(item)",
        ] {
            assert!(
                INDEX_HTML.contains(needle) || APP_JS.contains(needle),
                "watch history renderer is missing {needle}"
            );
        }
        for needle in [
            "permanent_failure",
            "transient_failure",
            "item?.post_action_error",
            "Existing torrent retained; success action applied.",
        ] {
            assert!(
                WATCH_HISTORY_JS.contains(needle),
                "watch history outcome module is missing {needle}"
            );
        }
    }

    #[test]
    fn executable_watch_history_renderer_harness_passes() {
        let script =
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/watch-history.test.js");
        let output = std::process::Command::new("node")
            .arg(script)
            .output()
            .expect("Node.js is required by the Web UI quality gate");
        assert!(
            output.status.success(),
            "watch history renderer harness failed:\n{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }

    #[test]
    fn web_ui_renders_and_escapes_tracker_scrape_state() {
        for heading in [
            "<th>Scrape</th>",
            "<th>Last scrape</th>",
            "Scrape counts (S/L/D)",
        ] {
            assert!(
                INDEX_HTML.contains(heading),
                "tracker table is missing {heading}"
            );
        }
        for field in [
            "t.scrape_status",
            "t.last_scrape_error",
            "t.last_scrape",
            "t.scrape_seeders",
            "t.scrape_leechers",
            "t.scrape_downloads",
        ] {
            assert!(
                APP_JS.contains(field),
                "tracker renderer is missing {field}"
            );
        }
        for escaped in [
            "escapeHtml(scrapeDetail)",
            "escapeHtml(fmtUnixSeconds(t.last_scrape))",
            "escapeHtml(scrapeCounts)",
        ] {
            assert!(
                APP_JS.contains(escaped),
                "tracker renderer does not safely escape {escaped}"
            );
        }
    }

    #[test]
    fn web_ui_removal_has_explicit_cancel_keep_and_delete_choices() {
        for value in ["cancel", "keep", "delete"] {
            assert!(
                INDEX_HTML.contains(&format!("value=\"{}\"", value)),
                "Removal dialog is missing {value} choice"
            );
        }
        assert!(APP_JS.contains("const removal = await chooseTorrentRemoval(name);"));
        assert!(APP_JS.contains("if (removal === \"cancel\") return;"));
        assert!(!APP_JS.contains("Remove torrent? Delete data too?"));
    }

    #[test]
    fn web_ui_scopes_removal_observation_to_the_active_query() {
        for needle in [
            "lastTorrentObservationKey: null,",
            "observeTorrentRemovals(torrents, queryParams, query);",
            "const observesCompleteLibrary = !state.torrentQueryState.q",
            "if (!observesCompleteLibrary)",
            "if (observationKey !== state.lastTorrentObservationKey)",
        ] {
            assert!(
                APP_JS.contains(needle),
                "Removal observation is missing {needle}"
            );
        }
    }

    #[test]
    fn web_ui_rejects_stale_torrent_detail_responses() {
        for needle in [
            "function detailsRequestIsCurrent(hash)",
            "function beginDetailsLoad()",
            "if (!detailsRequestIsCurrent(hash)) return;",
            "#details-controls\").classList.add(\"hidden\")",
            "const hash = state.currentHash;",
            "button.dataset.pendingHash = hash;",
            "if (button.dataset.pendingHash === hash)",
        ] {
            assert!(
                APP_JS.contains(needle),
                "Torrent detail stale-response protection is missing {needle}"
            );
        }
        assert!(
            APP_JS
                .matches("if (!detailsRequestIsCurrent(hash)) return;")
                .count()
                >= 6,
            "Every asynchronous torrent detail renderer must reject stale responses"
        );
    }

    #[test]
    fn web_ui_doctor_displays_application_version() {
        for needle in [
            "id=\"doctor-application\"",
            "<h3>Application</h3>",
            "api(\"/version\")",
            "function renderDoctor(report, version = null, storageRoots = null)",
            "[\"Version\", version?.version || \"unknown\"]",
            "[\"Commit\", version?.commit || \"unknown\"]",
            "[\"Target\", version?.target || \"unknown\"]",
        ] {
            assert!(
                APP_JS.contains(needle) || INDEX_HTML.contains(needle),
                "Web UI is missing Doctor version support {needle}"
            );
        }
    }

    #[test]
    fn web_ui_has_new_layout_classes() {
        for needle in [
            ".view-grid",
            ".settings-layout",
            ".settings-header",
            ".settings-shell",
            ".settings-nav",
            ".settings-nav-item",
            ".settings-detail",
            ".settings-panel",
            ".settings-panel[hidden]",
            ".settings-wide",
            ".settings-form-grid",
            ".settings-field",
            ".settings-check",
            ".settings-watch-list",
            ".watch-folder-editor",
            ".settings-actions",
            ".watch-layout",
            ".network-layout",
            ".card-subsection",
            ".health-payload",
            ".storage-root-table",
            "#health-badge",
        ] {
            assert!(
                STYLE_CSS.contains(needle),
                "style.css is missing class {needle}"
            );
        }
    }

    #[test]
    fn web_ui_settings_uses_structured_full_editor() {
        for id in [
            "cfg-api-bind-address",
            "cfg-api-auth-token",
            "cfg-api-require-auth",
            "cfg-api-max-request-body-bytes",
            "cfg-compat-transmission-enabled",
            "cfg-compat-qbittorrent-enabled",
            "cfg-autopilot-mode",
            "cfg-storage-download-dir",
            "cfg-storage-incomplete-dir",
            "cfg-storage-preallocate",
            "cfg-storage-sparse",
            "cfg-storage-minimum-free-space-bytes",
            "cfg-storage-minimum-free-space-percent",
            "cfg-network-mode",
            "cfg-network-required-interface",
            "cfg-network-required-source-ipv4",
            "cfg-network-required-source-ipv6",
            "cfg-network-required-network-namespace",
            "cfg-network-allow-ipv6",
            "cfg-network-fail-closed",
            "cfg-network-validate-route",
            "cfg-network-validate-dns",
            "cfg-torrent-listen-port",
            "cfg-torrent-allow-ipv6",
            "cfg-torrent-utp-enabled",
            "cfg-torrent-utp-prefer-tcp",
            "cfg-torrent-selfish",
            "cfg-bandwidth-global-download",
            "cfg-bandwidth-global-upload",
            "cfg-bandwidth-alt-download",
            "cfg-bandwidth-alt-upload",
            "cfg-bandwidth-max-peers",
            "cfg-bandwidth-max-peers-per-torrent",
            "cfg-bandwidth-alt-enabled",
            "cfg-queue-max-active-downloads",
            "cfg-queue-max-active-metadata-fetches",
            "cfg-queue-max-active-seeds",
            "cfg-queue-auto-start",
            "cfg-seeding-global-ratio-limit",
            "cfg-seeding-global-idle-limit",
            "cfg-dht-enabled",
            "cfg-dht-port",
            "cfg-dht-bootstrap-nodes",
            "cfg-pex-enabled",
            "cfg-pex-max-peers",
            "cfg-logging-level",
            "cfg-logging-json",
            "cfg-logging-file",
            "cfg-logging-file-path",
            "logging-level-options",
            "save-settings-btn",
            "reload-settings-btn",
            "reset-downloads-btn",
            "add-watch-folder-btn",
        ] {
            assert!(
                INDEX_HTML.contains(&format!("id=\"{}\"", id)),
                "Settings editor is missing field id {id}"
            );
        }

        for needle in [
            "function renderSettingsEditor(",
            "activeSettingsPanel: \"api\"",
            "function activateSettingsPanel(",
            "$$(\".settings-nav-item\").forEach",
            "const panel = invalid?.closest(\"[data-settings-panel]\")?.dataset.settingsPanel;",
            "function collectSettingsConfig(",
            "autopilot: {",
            "mode: settingsString(\"cfg-autopilot-mode\")",
            "qbittorrent: {",
            "enabled: settingsField(\"cfg-compat-qbittorrent-enabled\").checked,",
            "minimum_free_space_bytes: settingsInteger(\"cfg-storage-minimum-free-space-bytes\", 0),",
            "minimum_free_space_percent: settingsInteger(\"cfg-storage-minimum-free-space-percent\", 0),",
            "max_active_metadata_fetches: settingsInteger(\"cfg-queue-max-active-metadata-fetches\"),",
            "function renderWatchFolderEditors(",
            "function collectWatchFolderEditors(",
            "method: \"PUT\"",
            "async function replaceSettingsWithRuntimeFallback(",
            "method: \"PATCH\"",
            "error?.status !== 500",
            "Runtime settings applied",
            "api(\"/reset\"",
            "Reset all downloads?",
            "let resetError = null;",
            "state.selectedTorrents.clear();",
            "const remaining = finiteNumber(query?.total);",
            "Reset incomplete",
            "torrents are still listed after reset.",
            "Reset refresh failed",
            "Configuration saved",
        ] {
            assert!(
                APP_JS.contains(needle),
                "Settings editor is missing JS support {needle}"
            );
        }

        for old_surface in [
            "settings-runtime-editor",
            "settings-full-config",
            "full-config-json",
            "save-config-btn",
            "save-bw-btn",
            "bw-dl",
            "bw-ul",
            "bw-alt",
            "config-preview",
        ] {
            assert!(
                !INDEX_HTML.contains(old_surface)
                    && !APP_JS.contains(old_surface)
                    && !STYLE_CSS.contains(old_surface),
                "Settings editor still contains old raw/partial config surface {old_surface}"
            );
        }
    }

    #[test]
    fn web_ui_queue_metadata_fetch_limit_is_wired() {
        assert!(
            INDEX_HTML.contains("id=\"cfg-queue-max-active-metadata-fetches\""),
            "Queue settings editor is missing max_active_metadata_fetches field id"
        );
        assert!(
            INDEX_HTML.contains("<span>Max active metadata fetches</span>"),
            "Queue settings editor is missing max_active_metadata_fetches label"
        );
        assert!(
            APP_JS.contains("setSettingsValue(\"cfg-queue-max-active-metadata-fetches\", queue.max_active_metadata_fetches)"),
            "Queue settings editor is missing max_active_metadata_fetches load wiring"
        );
        assert!(
            APP_JS.contains("max_active_metadata_fetches: settingsInteger(\"cfg-queue-max-active-metadata-fetches\"),"),
            "Queue settings editor is missing max_active_metadata_fetches save wiring"
        );
    }

    #[test]
    fn web_ui_doctor_displays_storage_diagnostics() {
        assert!(
            INDEX_HTML.contains("id=\"doctor-storage\""),
            "Doctor view is missing storage diagnostics card id doctor-storage"
        );
        for needle in [
            "api(\"/storage/roots\")",
            "renderDoctorStorageRoots",
            "function renderDoctor(",
            "#doctor-storage",
            "Storage diagnostics",
            "Minimum free percent",
        ] {
            assert!(
                APP_JS.contains(needle) || INDEX_HTML.contains(needle),
                "Doctor storage diagnostics wiring is incomplete: {needle}"
            );
        }
    }

    #[test]
    fn web_ui_disables_watch_scan_without_configured_folders() {
        assert!(
            INDEX_HTML.contains("id=\"watch-scan-btn\" disabled"),
            "Watch scan button should start disabled until watch status is loaded"
        );
        for needle in [
            "scanButton.disabled = folders.length === 0",
            "No watch folders configured",
        ] {
            assert!(
                APP_JS.contains(needle),
                "Watch view is missing disabled scan button support {needle}"
            );
        }
    }

    #[test]
    fn web_ui_dynamic_data_regions_start_empty() {
        for placeholder in [
            "badge\">unknown",
            "value=\"0\"",
            ">Torrent Details</h2>",
            "Added 1 file",
        ] {
            assert!(
                !INDEX_HTML.contains(placeholder),
                "Web UI contains hardcoded data placeholder {placeholder}"
            );
        }
    }

    #[test]
    fn torrent_details_exposes_terminal_error_diagnostics() {
        for needle in [
            "[\"Last error\", t.error || \"\"]",
            "renderDetailsSummary(t)",
        ] {
            assert!(
                APP_JS.contains(needle),
                "Torrent Details is missing terminal error diagnostics {needle}"
            );
        }
    }

    #[test]
    fn web_ui_renders_health_for_sample_torrent_summary() {
        // Mimic the renderHealth output for a sample summary and assert
        // the produced HTML is a valid container with the right number of
        // bars and the correct label.
        fn render(label: &str, score: u8, bars: u8, reasons: &[&str]) -> String {
            let reasons_str = reasons.join("; ");
            let sr_text = format!("Health: {label}, {score} out of 100");
            let mut bars_html = String::new();
            for i in 0..5 {
                bars_html.push_str(&format!(
                    "<span class=\"bar{}\"></span>",
                    if (i as u8) < bars { " active" } else { "" }
                ));
            }
            format!(
                "<div class=\"torrent-health health-{label}\" title=\"{label} - {score}/100: {reasons_str}\">\
<span class=\"sr-only\">{sr_text}</span>\
<span class=\"health-bars\" aria-hidden=\"true\">{bars_html}</span>\
<span class=\"health-label\">{label}</span>\
</div>"
            )
        }
        let html = render("good", 82, 4, &["all missing pieces are available"]);
        assert!(html.contains("class=\"torrent-health health-good\""));
        assert!(html.contains("Health: good, 82 out of 100"));
        assert!(html.contains("82/100"));
        let active_bars = html.matches("class=\"bar active\"").count();
        assert_eq!(active_bars, 4);
    }

    #[tokio::test]
    async fn web_asset_routes_serve_expected_content_types() {
        let app = web_router();
        for (path, content_type) in [
            ("/favicon.ico", "image/x-icon"),
            ("/site.webmanifest", "application/manifest+json"),
            ("/swarmotter-icon-64x64.png", "image/png"),
            (
                "/vendor/tabulator/tabulator.min.js",
                "application/javascript; charset=utf-8",
            ),
            (
                "/theme-bootstrap.js",
                "application/javascript; charset=utf-8",
            ),
            (
                "/seeding-policy.js",
                "application/javascript; charset=utf-8",
            ),
            ("/watch-history.js", "application/javascript; charset=utf-8"),
            (
                "/vendor/tabulator/tabulator_midnight.min.css",
                "text/css; charset=utf-8",
            ),
            ("/vendor/tabulator/LICENSE", "text/plain; charset=utf-8"),
        ] {
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .uri(path)
                        .body(Body::empty())
                        .expect("request is valid"),
                )
                .await
                .expect("route responds");
            assert_eq!(response.status(), StatusCode::OK);
            assert_eq!(
                response.headers().get(header::CONTENT_TYPE).unwrap(),
                content_type
            );
        }
    }

    #[tokio::test]
    async fn es_module_routes_preserve_security_and_cache_policy() {
        let app = web_router();
        for path in [
            "/app.js",
            "/js/api.js",
            "/js/state.js",
            "/js/torrents.js",
            "/js/details.js",
            "/js/settings.js",
            "/js/events.js",
            "/js/ui.js",
        ] {
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .uri(path)
                        .body(Body::empty())
                        .expect("request is valid"),
                )
                .await
                .expect("module route responds");
            assert_eq!(response.status(), StatusCode::OK, "route {path}");
            assert_eq!(
                response.headers().get(header::CONTENT_TYPE).unwrap(),
                "application/javascript; charset=utf-8",
                "route {path}",
            );
            assert!(
                response
                    .headers()
                    .get(header::CONTENT_SECURITY_POLICY)
                    .and_then(|value| value.to_str().ok())
                    .is_some_and(|value| value.contains("script-src 'self'")),
                "route {path}",
            );
            assert!(
                response.headers().get(header::CACHE_CONTROL).is_none(),
                "module route {path} must retain the entry script's uncached policy",
            );
        }
        assert!(INDEX_HTML.contains("<script type=\"module\" src=\"/app.js\"></script>"));
    }

    #[tokio::test]
    async fn web_assets_set_browser_security_headers() {
        let response = web_router()
            .oneshot(
                Request::builder()
                    .uri("/")
                    .body(Body::empty())
                    .expect("request is valid"),
            )
            .await
            .expect("route responds");
        assert_eq!(response.status(), StatusCode::OK);
        assert!(response
            .headers()
            .get(header::CONTENT_SECURITY_POLICY)
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value.contains("frame-ancestors 'none'")));
        assert_eq!(response.headers().get("x-frame-options").unwrap(), "DENY");
        assert_eq!(
            response
                .headers()
                .get(header::X_CONTENT_TYPE_OPTIONS)
                .unwrap(),
            "nosniff"
        );
    }
}
