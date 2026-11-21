// SPDX-License-Identifier: Apache-2.0

//! OpenAPI 3.1 document and self-contained interactive API explorer.
//!
//! The route catalog is checked against the actual `.route(...)` registrations
//! in `routes.rs` by a unit test. The document is generated from the catalog so
//! the served specification and explorer cannot silently drift from the
//! registered native and compatibility operations.

use serde_json::{json, Map, Value};

const DOCS_HTML: &str = include_str!("../assets/api-docs.html");

struct RouteDoc {
    path: &'static str,
    methods: &'static [&'static str],
    tag: &'static str,
    /// Whether the operation is reached through the daemon control plane.
    gated: bool,
}

const ROUTES: &[RouteDoc] = &[
    RouteDoc {
        path: "/transmission/rpc",
        methods: &["GET", "POST"],
        tag: "Transmission Compatibility",
        gated: true,
    },
    RouteDoc {
        path: "/api/v2/auth/login",
        methods: &["POST"],
        tag: "qBittorrent Compatibility",
        gated: true,
    },
    RouteDoc {
        path: "/api/v2/auth/logout",
        methods: &["POST"],
        tag: "qBittorrent Compatibility",
        gated: true,
    },
    RouteDoc {
        path: "/api/v2/app/version",
        methods: &["GET"],
        tag: "qBittorrent Compatibility",
        gated: true,
    },
    RouteDoc {
        path: "/api/v2/app/webapiVersion",
        methods: &["GET"],
        tag: "qBittorrent Compatibility",
        gated: true,
    },
    RouteDoc {
        path: "/api/v2/torrents/info",
        methods: &["GET"],
        tag: "qBittorrent Compatibility",
        gated: true,
    },
    RouteDoc {
        path: "/api/v2/torrents/categories",
        methods: &["GET"],
        tag: "qBittorrent Compatibility",
        gated: true,
    },
    RouteDoc {
        path: "/api/v2/torrents/add",
        methods: &["POST"],
        tag: "qBittorrent Compatibility",
        gated: true,
    },
    RouteDoc {
        path: "/api/v2/torrents/delete",
        methods: &["POST"],
        tag: "qBittorrent Compatibility",
        gated: true,
    },
    RouteDoc {
        path: "/api/v2/torrents/pause",
        methods: &["POST"],
        tag: "qBittorrent Compatibility",
        gated: true,
    },
    RouteDoc {
        path: "/api/v2/torrents/resume",
        methods: &["POST"],
        tag: "qBittorrent Compatibility",
        gated: true,
    },
    RouteDoc {
        path: "/api/v2/torrents/stop",
        methods: &["POST"],
        tag: "qBittorrent Compatibility",
        gated: true,
    },
    RouteDoc {
        path: "/api/v2/torrents/start",
        methods: &["POST"],
        tag: "qBittorrent Compatibility",
        gated: true,
    },
    RouteDoc {
        path: "/api/v2/torrents/setCategory",
        methods: &["POST"],
        tag: "qBittorrent Compatibility",
        gated: true,
    },
    RouteDoc {
        path: "/api/v2/torrents/recheck",
        methods: &["POST"],
        tag: "qBittorrent Compatibility",
        gated: true,
    },
    RouteDoc {
        path: "/api/v2/torrents/reannounce",
        methods: &["POST"],
        tag: "qBittorrent Compatibility",
        gated: true,
    },
    RouteDoc {
        path: "/api/v2/torrents/setLocation",
        methods: &["POST"],
        tag: "qBittorrent Compatibility",
        gated: true,
    },
    RouteDoc {
        path: "/api/v2/torrents/renameFile",
        methods: &["POST"],
        tag: "qBittorrent Compatibility",
        gated: true,
    },
    RouteDoc {
        path: "/api/v2/torrents/properties",
        methods: &["GET"],
        tag: "qBittorrent Compatibility",
        gated: true,
    },
    RouteDoc {
        path: "/api/v2/torrents/trackers",
        methods: &["GET"],
        tag: "qBittorrent Compatibility",
        gated: true,
    },
    RouteDoc {
        path: "/api/v2/torrents/files",
        methods: &["GET"],
        tag: "qBittorrent Compatibility",
        gated: true,
    },
    RouteDoc {
        path: "/health",
        methods: &["GET"],
        tag: "System",
        gated: false,
    },
    RouteDoc {
        path: "/live",
        methods: &["GET"],
        tag: "System",
        gated: false,
    },
    RouteDoc {
        path: "/api/v1/health",
        methods: &["GET"],
        tag: "System",
        gated: true,
    },
    RouteDoc {
        path: "/api/v1/version",
        methods: &["GET"],
        tag: "System",
        gated: true,
    },
    RouteDoc {
        path: "/api/v1/stats",
        methods: &["GET"],
        tag: "System",
        gated: true,
    },
    RouteDoc {
        path: "/api/v1/storage/roots",
        methods: &["GET"],
        tag: "Configuration",
        gated: true,
    },
    RouteDoc {
        path: "/api/v1/autopilot/status",
        methods: &["GET"],
        tag: "Configuration",
        gated: true,
    },
    RouteDoc {
        path: "/api/v1/profiles",
        methods: &["GET", "PUT"],
        tag: "Configuration",
        gated: true,
    },
    RouteDoc {
        path: "/api/v1/peer-filter",
        methods: &["GET", "PUT"],
        tag: "Configuration",
        gated: true,
    },
    RouteDoc {
        path: "/api/v1/peer-filter/unban",
        methods: &["POST"],
        tag: "Configuration",
        gated: true,
    },
    RouteDoc {
        path: "/api/v1/torrents",
        methods: &["GET"],
        tag: "Torrents",
        gated: true,
    },
    RouteDoc {
        path: "/api/v1/torrents",
        methods: &["POST"],
        tag: "Torrents",
        gated: true,
    },
    RouteDoc {
        path: "/api/v1/torrents/query",
        methods: &["GET"],
        tag: "Torrents",
        gated: true,
    },
    RouteDoc {
        path: "/api/v1/torrents/magnet",
        methods: &["POST"],
        tag: "Torrents",
        gated: true,
    },
    RouteDoc {
        path: "/api/v1/torrents/file",
        methods: &["POST"],
        tag: "Torrents",
        gated: true,
    },
    RouteDoc {
        path: "/api/v1/torrents/bulk",
        methods: &["POST"],
        tag: "Torrents",
        gated: true,
    },
    RouteDoc {
        path: "/api/v1/torrents/create",
        methods: &["POST"],
        tag: "Torrents",
        gated: true,
    },
    RouteDoc {
        path: "/api/v1/torrents/create/roots",
        methods: &["GET"],
        tag: "Torrents",
        gated: true,
    },
    RouteDoc {
        path: "/api/v1/torrents/remove",
        methods: &["POST"],
        tag: "Torrents",
        gated: true,
    },
    RouteDoc {
        path: "/api/v1/torrents/bulk/pause",
        methods: &["POST"],
        tag: "Torrents",
        gated: true,
    },
    RouteDoc {
        path: "/api/v1/torrents/bulk/resume",
        methods: &["POST"],
        tag: "Torrents",
        gated: true,
    },
    RouteDoc {
        path: "/api/v1/torrents/bulk/recheck",
        methods: &["POST"],
        tag: "Torrents",
        gated: true,
    },
    RouteDoc {
        path: "/api/v1/torrents/bulk/restart",
        methods: &["POST"],
        tag: "Torrents",
        gated: true,
    },
    RouteDoc {
        path: "/api/v1/torrents/{hash}/metainfo",
        methods: &["GET"],
        tag: "Torrents",
        gated: true,
    },
    RouteDoc {
        path: "/api/v1/torrents/{hash}/magnet",
        methods: &["GET"],
        tag: "Torrents",
        gated: true,
    },
    RouteDoc {
        path: "/api/v1/torrents/{hash}/annotation",
        methods: &["PUT"],
        tag: "Torrents",
        gated: true,
    },
    RouteDoc {
        path: "/api/v1/torrents/export",
        methods: &["POST"],
        tag: "Torrents",
        gated: true,
    },
    RouteDoc {
        path: "/api/v1/torrents/{hash}",
        methods: &["DELETE", "GET"],
        tag: "Torrents",
        gated: true,
    },
    RouteDoc {
        path: "/api/v1/torrents/{hash}/stats",
        methods: &["GET"],
        tag: "Torrents",
        gated: true,
    },
    RouteDoc {
        path: "/api/v1/torrents/{hash}/policy",
        methods: &["GET", "PUT"],
        tag: "Torrents",
        gated: true,
    },
    RouteDoc {
        path: "/api/v1/torrents/{hash}/storage-preview",
        methods: &["POST"],
        tag: "Torrents",
        gated: true,
    },
    RouteDoc {
        path: "/api/v1/torrents/{hash}/encryption-mode",
        methods: &["PUT"],
        tag: "Torrents",
        gated: true,
    },
    RouteDoc {
        path: "/api/v1/torrents/{hash}/autopilot",
        methods: &["GET", "POST"],
        tag: "Torrents",
        gated: true,
    },
    RouteDoc {
        path: "/api/v1/torrents/{hash}/pause",
        methods: &["POST"],
        tag: "Torrents",
        gated: true,
    },
    RouteDoc {
        path: "/api/v1/torrents/{hash}/resume",
        methods: &["POST"],
        tag: "Torrents",
        gated: true,
    },
    RouteDoc {
        path: "/api/v1/torrents/{hash}/start",
        methods: &["POST"],
        tag: "Torrents",
        gated: true,
    },
    RouteDoc {
        path: "/api/v1/torrents/{hash}/stop",
        methods: &["POST"],
        tag: "Torrents",
        gated: true,
    },
    RouteDoc {
        path: "/api/v1/torrents/{hash}/recheck",
        methods: &["POST"],
        tag: "Torrents",
        gated: true,
    },
    RouteDoc {
        path: "/api/v1/torrents/{hash}/reannounce",
        methods: &["POST"],
        tag: "Torrents",
        gated: true,
    },
    RouteDoc {
        path: "/api/v1/torrents/{hash}/move",
        methods: &["POST"],
        tag: "Torrents",
        gated: true,
    },
    RouteDoc {
        path: "/api/v1/torrents/{hash}/labels",
        methods: &["POST"],
        tag: "Torrents",
        gated: true,
    },
    RouteDoc {
        path: "/api/v1/torrents/{hash}/limits",
        methods: &["POST"],
        tag: "Torrents",
        gated: true,
    },
    RouteDoc {
        path: "/api/v1/torrents/{hash}/seeding",
        methods: &["PUT"],
        tag: "Torrents",
        gated: true,
    },
    RouteDoc {
        path: "/api/v1/torrents/{hash}/files",
        methods: &["GET", "PATCH"],
        tag: "Torrents",
        gated: true,
    },
    RouteDoc {
        path: "/api/v1/torrents/{hash}/files/wanted",
        methods: &["POST"],
        tag: "Torrents",
        gated: true,
    },
    RouteDoc {
        path: "/api/v1/torrents/{hash}/files/priority",
        methods: &["POST"],
        tag: "Torrents",
        gated: true,
    },
    RouteDoc {
        path: "/api/v1/torrents/{hash}/files/{index}/rename",
        methods: &["POST"],
        tag: "Torrents",
        gated: true,
    },
    RouteDoc {
        path: "/api/v1/torrents/{hash}/trackers",
        methods: &["GET", "POST"],
        tag: "Torrents",
        gated: true,
    },
    RouteDoc {
        path: "/api/v1/torrents/{hash}/trackers/{url}",
        methods: &["DELETE"],
        tag: "Torrents",
        gated: true,
    },
    RouteDoc {
        path: "/api/v1/torrents/{hash}/trackers/edit",
        methods: &["POST"],
        tag: "Torrents",
        gated: true,
    },
    RouteDoc {
        path: "/api/v1/torrents/{hash}/peers",
        methods: &["GET"],
        tag: "Torrents",
        gated: true,
    },
    RouteDoc {
        path: "/api/v1/torrents/{hash}/peers/ban",
        methods: &["POST"],
        tag: "Torrents",
        gated: true,
    },
    RouteDoc {
        path: "/api/v1/torrents/{hash}/peers/unban",
        methods: &["POST"],
        tag: "Torrents",
        gated: true,
    },
    RouteDoc {
        path: "/api/v1/torrents/{hash}/queue/move-up",
        methods: &["POST"],
        tag: "Torrents",
        gated: true,
    },
    RouteDoc {
        path: "/api/v1/torrents/{hash}/queue/move-down",
        methods: &["POST"],
        tag: "Torrents",
        gated: true,
    },
    RouteDoc {
        path: "/api/v1/torrents/{hash}/queue/move-top",
        methods: &["POST"],
        tag: "Torrents",
        gated: true,
    },
    RouteDoc {
        path: "/api/v1/torrents/{hash}/queue/move-bottom",
        methods: &["POST"],
        tag: "Torrents",
        gated: true,
    },
    RouteDoc {
        path: "/api/v1/settings",
        methods: &["GET", "PATCH", "PUT"],
        tag: "Configuration",
        gated: true,
    },
    RouteDoc {
        path: "/api/v1/network/health",
        methods: &["GET"],
        tag: "Network",
        gated: true,
    },
    RouteDoc {
        path: "/api/v1/network/port-mapping",
        methods: &["GET"],
        tag: "Network",
        gated: true,
    },
    RouteDoc {
        path: "/api/v1/network/port-mapping/refresh",
        methods: &["POST"],
        tag: "Network",
        gated: true,
    },
    RouteDoc {
        path: "/api/v1/network/port-test",
        methods: &["POST"],
        tag: "Network",
        gated: true,
    },
    RouteDoc {
        path: "/api/v1/network/diagnostics",
        methods: &["GET"],
        tag: "Network",
        gated: true,
    },
    RouteDoc {
        path: "/api/v1/trash",
        methods: &["GET"],
        tag: "Trash",
        gated: true,
    },
    RouteDoc {
        path: "/api/v1/trash/preview",
        methods: &["POST"],
        tag: "Trash",
        gated: true,
    },
    RouteDoc {
        path: "/api/v1/trash/apply",
        methods: &["POST"],
        tag: "Trash",
        gated: true,
    },
    RouteDoc {
        path: "/api/v1/trash/{id}/restore",
        methods: &["POST"],
        tag: "Trash",
        gated: true,
    },
    RouteDoc {
        path: "/api/v1/trash/{id}",
        methods: &["DELETE"],
        tag: "Trash",
        gated: true,
    },
    RouteDoc {
        path: "/api/v1/rss",
        methods: &["GET", "POST"],
        tag: "RSS",
        gated: true,
    },
    RouteDoc {
        path: "/api/v1/rss/{id}",
        methods: &["DELETE", "PUT"],
        tag: "RSS",
        gated: true,
    },
    RouteDoc {
        path: "/api/v1/rss/{id}/preview",
        methods: &["GET"],
        tag: "RSS",
        gated: true,
    },
    RouteDoc {
        path: "/api/v1/rss/{id}/poll",
        methods: &["POST"],
        tag: "RSS",
        gated: true,
    },
    RouteDoc {
        path: "/api/v1/rss/{id}/retry",
        methods: &["POST"],
        tag: "RSS",
        gated: true,
    },
    RouteDoc {
        path: "/api/v1/rss/{id}/decisions",
        methods: &["GET"],
        tag: "RSS",
        gated: true,
    },
    RouteDoc {
        path: "/api/v1/rss/history/completions",
        methods: &["DELETE"],
        tag: "RSS",
        gated: true,
    },
    RouteDoc {
        path: "/api/v1/rss/diagnostics",
        methods: &["GET"],
        tag: "RSS",
        gated: true,
    },
    RouteDoc {
        path: "/api/v1/watch/scan",
        methods: &["POST"],
        tag: "Watch",
        gated: true,
    },
    RouteDoc {
        path: "/api/v1/watch/history",
        methods: &["GET"],
        tag: "Watch",
        gated: true,
    },
    RouteDoc {
        path: "/api/v1/watch/status",
        methods: &["GET"],
        tag: "Watch",
        gated: true,
    },
    RouteDoc {
        path: "/api/v1/logs/recent",
        methods: &["GET"],
        tag: "System",
        gated: true,
    },
    RouteDoc {
        path: "/api/v1/doctor",
        methods: &["GET"],
        tag: "System",
        gated: true,
    },
    RouteDoc {
        path: "/api/v1/reset",
        methods: &["POST"],
        tag: "System",
        gated: true,
    },
    RouteDoc {
        path: "/api/v1/events",
        methods: &["GET"],
        tag: "Events",
        gated: true,
    },
    RouteDoc {
        path: "/api/v1/ws",
        methods: &["GET"],
        tag: "Events",
        gated: true,
    },
    RouteDoc {
        path: "/api/docs",
        methods: &["GET"],
        tag: "System",
        gated: false,
    },
    RouteDoc {
        path: "/api/openapi.json",
        methods: &["GET"],
        tag: "System",
        gated: false,
    },
];

pub fn docs_html() -> &'static str {
    DOCS_HTML
}

pub fn document_json() -> Vec<u8> {
    serde_json::to_vec_pretty(&document()).unwrap_or_else(|_| b"{}".to_vec())
}

pub fn document() -> Value {
    let mut paths = Map::new();
    for route in ROUTES {
        let entry = paths
            .entry(route.path.to_string())
            .or_insert_with(|| Value::Object(Map::new()));
        let Some(object) = entry.as_object_mut() else {
            continue;
        };
        for method in route.methods {
            object.insert((*method).to_ascii_lowercase(), operation(route, method));
        }
    }
    json!({
        "openapi": "3.1.0",
        "info": {
            "title": "SwarmOtter API",
            "version": env!("CARGO_PKG_VERSION"),
            "description": "Native SwarmOtter /api/v1 control plane plus the supported qBittorrent and Transmission compatibility subsets. All torrent data-plane activity is daemon-owned and constrained by the configured network path.",
            "license": { "name": "Apache-2.0", "identifier": "Apache-2.0" }
        },
        "servers": [{ "url": "/", "description": "This daemon" }],
        "tags": [
            { "name": "System", "description": "Health, version, stats, logs, doctor, reset, and public liveness." },
            { "name": "Torrents", "description": "Native torrent intake, lifecycle, provenance, creation, export, and operations." },
            { "name": "Configuration", "description": "Settings, policy profiles, peer admission, storage roots, and autopilot." },
            { "name": "Network", "description": "Containment health, listener reachability, diagnostics, and router mapping." },
            { "name": "RSS", "description": "User-configured feed subscriptions and durable intake decisions." },
            { "name": "Trash", "description": "Dry-run cleanup preview, trash apply, restore, purge, and retention." },
            { "name": "Watch", "description": "Watch-folder scan, history, and status." },
            { "name": "Events", "description": "SSE and WebSocket event delivery." },
            { "name": "qBittorrent Compatibility", "description": "Supported subset of the qBittorrent WebAPI. Unsupported methods are not registered and must not be inferred." },
            { "name": "Transmission Compatibility", "description": "Supported subset of Transmission RPC 17. Unsupported methods return a JSON-RPC method-not-found error." }
        ],
        "paths": paths,
        "components": components(),
        "x-swarmotter-json-format-version": 1,
        "externalDocs": { "description": "Operator API reference", "url": "/docs/api" }
    })
}

fn operation(route: &RouteDoc, method: &str) -> Value {
    let mut op = json!({
        "operationId": operation_id(route.path, method),
        "summary": summary(route.path, method),
        "description": description(route.path, method),
        "tags": [route.tag],
        "security": security(route),
        "x-swarmotter-state-changing": method != "GET" && route.path != "/api/docs" && route.path != "/api/openapi.json",
        "parameters": parameters(route.path),
        "responses": responses(route.path, method),
    });
    if let Some(request_body) = request_body(route.path, method) {
        op["requestBody"] = request_body;
    }
    if route.tag == "qBittorrent Compatibility" {
        op["x-swarmotter-compatibility"] = json!({
            "surface": "qbittorrent",
            "status": "supported-subset",
            "parity_limits": [
                "Only paths and methods listed in this specification are implemented.",
                "Unsupported qBittorrent methods return 404/405 or an explicit compatibility error; support must not be inferred from another client.",
                "SwarmOtter labels and profiles map only the documented category/label fields."
            ]
        });
    }
    if route.tag == "Transmission Compatibility" {
        op["x-swarmotter-supported-rpc-methods"] = json!([
            "session-get",
            "session-set",
            "session-stats",
            "session-close",
            "torrent-get",
            "torrent-add",
            "torrent-remove",
            "torrent-start",
            "torrent-start-now",
            "torrent-stop",
            "torrent-verify",
            "torrent-reannounce",
            "torrent-set",
            "torrent-set-location",
            "torrent-rename-path",
            "queue-move-top",
            "queue-move-up",
            "queue-move-down",
            "queue-move-bottom",
            "free-space",
            "port-test",
            "blocklist-update"
        ]);
        op["x-swarmotter-compatibility"] = json!({
            "surface": "transmission",
            "status": "supported-subset",
            "parity_limits": [
                "Methods not listed in x-swarmotter-supported-rpc-methods return JSON-RPC method-not-found.",
                "Unsupported request fields are ignored or rejected according to the adapter contract; they do not imply full Transmission parity.",
                "Session negotiation uses X-Transmission-Session-Id and the configured compatibility adapter."
            ]
        });
    }
    op
}

fn operation_id(path: &str, method: &str) -> String {
    let mut id = method.to_ascii_lowercase();
    for ch in path.chars() {
        if ch.is_ascii_alphanumeric() {
            id.push(ch.to_ascii_lowercase());
        } else {
            id.push('_');
        }
    }
    id.trim_matches('_').to_string()
}

fn summary(path: &str, method: &str) -> String {
    let action = match method {
        "GET" => "Read",
        "POST" => "Create or act on",
        "PUT" => "Replace",
        "PATCH" => "Update",
        "DELETE" => "Delete",
        _ => "Operate on",
    };
    if path == "/api/docs" {
        "Interactive API explorer".into()
    } else if path == "/api/openapi.json" {
        "Machine-readable OpenAPI document".into()
    } else {
        format!("{action} {path}")
    }
}

fn description(path: &str, method: &str) -> String {
    let mut text = match path {
        "/api/v1/torrents" if method == "GET" => {
            "List every torrent. Prefer /api/v1/torrents/query for large libraries.".to_string()
        }
        "/api/v1/torrents/query" => {
            "Server-side filtering, sorting, grouping, and pagination over the torrent library.".to_string()
        }
        "/api/v1/torrents/file" => {
            "Upload raw application/x-bittorrent bytes. Query options control start behavior, profile, labels, preview, and file selection.".to_string()
        }
        "/api/v1/torrents/create" => {
            "Create v1, pure v2, or hybrid metainfo from a local file or directory and optionally seed it immediately.".to_string()
        }
        "/api/v1/torrents/export" => {
            "Authenticated tar archive of retained original .torrent documents with a per-item manifest. Missing originals are reported, never reconstructed.".to_string()
        }
        "/api/v1/trash/preview" => {
            "Dry-run cleanup preview. No files, queue state, or metainfo are changed.".to_string()
        }
        "/api/v1/trash/apply" => {
            "Revalidate ownership, roots, symlinks, shared paths, and current source state, then move eligible torrents or stale partials into trash.".to_string()
        }
        "/transmission/rpc" => {
            "Transmission-compatible JSON-RPC endpoint. The supported method set is explicit in x-swarmotter-supported-rpc-methods.".to_string()
        }
        _ => format!("Native {method} operation for {path}."),
    };
    if path.starts_with("/api/v2") {
        text.push_str(" Compatibility operations use the configured qBittorrent adapter and its documented parity limits.");
    }
    if path.starts_with("/api/v1") {
        text.push_str(" Native operations use the standard ApiEnvelope response unless documented as a binary/stream response.");
    }
    text
}

fn security(route: &RouteDoc) -> Value {
    if !route.gated || route.path == "/api/docs" || route.path == "/api/openapi.json" {
        return json!([]);
    }
    if route.tag == "qBittorrent Compatibility" {
        if route.path == "/api/v2/auth/login" {
            return json!([]);
        }
        return json!([{ "qbittorrentSession": [] }]);
    }
    if route.tag == "Transmission Compatibility" {
        return json!([{ "transmissionSession": [] }]);
    }
    json!([{ "bearerAuth": [] }])
}

fn parameters(path: &str) -> Value {
    let mut params = Vec::new();
    for segment in path.split('/') {
        if segment.starts_with('{') && segment.ends_with('}') {
            let name = segment.trim_matches('{').trim_matches('}');
            params.push(json!({
                "name": name,
                "in": "path",
                "required": true,
                "schema": { "type": "string" },
                "description": format!("Path parameter {name}")
            }));
        }
    }
    let mut query = |name: &str, description: &str, schema: Value| {
        params.push(json!({
            "name": name, "in": "query", "required": false,
            "description": description, "schema": schema
        }));
    };
    if path == "/api/v1/torrents/query" {
        query("q", "Free-text search", json!({"type":"string"}));
        query(
            "state",
            "Comma-separated state filter",
            json!({"type":"string"}),
        );
        query(
            "health",
            "Comma-separated health filter",
            json!({"type":"string"}),
        );
        query(
            "label",
            "Comma-separated label filter",
            json!({"type":"string"}),
        );
        query("sort", "Sort key", json!({"type":"string"}));
        query(
            "dir",
            "Sort direction",
            json!({"type":"string","enum":["asc","desc"]}),
        );
        query(
            "page",
            "One-based page",
            json!({"type":"integer","minimum":1}),
        );
        query(
            "per_page",
            "Page size (server maximum 500)",
            json!({"type":"integer","minimum":1,"maximum":500}),
        );
        query("group_by", "Optional grouping", json!({"type":"string"}));
    }
    if path == "/api/v1/torrents/{hash}/magnet" {
        query(
            "tracker_policy",
            "Tracker inclusion policy",
            json!({"type":"string","enum":["all","primary_tier","none"]}),
        );
        query(
            "include_private_trackers",
            "Explicit consent for private tracker URLs",
            json!({"type":"boolean"}),
        );
    }
    if path == "/api/v1/rss/{id}/poll" {
        query(
            "import_current",
            "Explicitly import current matches",
            json!({"type":"boolean"}),
        );
        query(
            "reconsider",
            "Re-evaluate stored older listings",
            json!({"type":"boolean"}),
        );
    }
    if path == "/api/v1/rss/{id}/decisions" {
        query(
            "limit",
            "Decision page size",
            json!({"type":"integer","minimum":1,"maximum":500}),
        );
        query(
            "before",
            "Paginate before this decision id",
            json!({"type":"integer"}),
        );
    }
    if path == "/api/v1/logs/recent" {
        query(
            "lines",
            "Number of lines (1..500)",
            json!({"type":"integer","minimum":1,"maximum":500}),
        );
    }
    if path == "/api/v1/torrents/{hash}" || path == "/api/v1/torrents" {
        query(
            "delete_data",
            "Delete payload data with the torrent record",
            json!({"type":"boolean"}),
        );
    }
    Value::Array(params)
}

fn request_body(path: &str, method: &str) -> Option<Value> {
    if method == "GET" || method == "DELETE" {
        return None;
    }
    let json_body = |schema: &str, example: Value| {
        Some(json!({
            "required": true,
            "content": { "application/json": { "schema": { "$ref": format!("#/components/schemas/{schema}") }, "example": example } }
        }))
    };
    match (path, method) {
        ("/api/v1/torrents/magnet", "POST") => json_body(
            "AddMagnetRequest",
            json!({
                "magnet": "magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567&dn=example",
                "paused": true,
                "profile": "archive",
                "labels": ["linux"]
            }),
        ),
        ("/api/v1/torrents/file", "POST") => Some(binary_body(
            "application/x-bittorrent",
            "Raw .torrent bytes",
        )),
        ("/api/v1/torrents", "POST") => Some(json!({
            "required": true,
            "content": {
                "application/json": { "schema": { "$ref": "#/components/schemas/AddMagnetRequest" }, "example": { "magnet": "magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567" } },
                "application/x-bittorrent": { "schema": { "type": "string", "format": "binary" } }
            }
        })),
        ("/api/v1/torrents/bulk", "POST") => json_body(
            "BulkAddRequest",
            json!({
                "torrents": [{ "magnet": "magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567" }],
                "paused": true
            }),
        ),
        ("/api/v1/torrents/create", "POST") => json_body(
            "CreateTorrentRequest",
            json!({
                "source": "/data/downloads/release",
                "name": "release",
                "format": "hybrid",
                "piece_length": 262144,
                "tracker_tiers": [["https://tracker.example/announce"]],
                "webseeds": [],
                "private": false,
                "seed": true
            }),
        ),
        ("/api/v1/torrents/export", "POST") => json_body(
            "ExportArchiveRequest",
            json!({
                "hashes": ["0123456789abcdef0123456789abcdef01234567"]
            }),
        ),
        ("/api/v1/torrents/{hash}/annotation", "PUT") => json_body(
            "AnnotationRequest",
            json!({ "annotation": "operator note" }),
        ),
        ("/api/v1/torrents/{hash}/limits", "POST") => json_body(
            "LimitsRequest",
            json!({ "download_limit": 1048576, "upload_limit": 524288 }),
        ),
        ("/api/v1/trash/preview", "POST") | ("/api/v1/trash/apply", "POST") => json_body(
            "TrashSelectionRequest",
            json!({
                "hashes": ["0123456789abcdef0123456789abcdef01234567"],
                "rule": null,
                "mode": "torrent"
            }),
        ),
        ("/api/v1/trash/{id}/restore", "POST") => {
            json_body("RestoreRequest", json!({ "collision_policy": "fail" }))
        }
        ("/api/v1/rss", "POST") | ("/api/v1/rss/{id}", "PUT") => json_body(
            "RssSubscriptionRequest",
            json!({
                "id": "distro-isos",
                "name": "Distribution releases",
                "url": "https://distributor.example/feed.xml",
                "enabled": true,
                "poll_interval_seconds": 900,
                "profile": "archive",
                "start_behavior": "paused",
                "import_mode": "new_only",
                "rules": { "include": [{ "title": "*ISO*" }], "exclude": [{ "title": "*live*" }] },
                "labels": ["linux"]
            }),
        ),
        ("/transmission/rpc", "POST") => json_body(
            "TransmissionRpcRequest",
            json!({
                "method": "session-get",
                "arguments": {},
                "tag": 1
            }),
        ),
        _ if path.starts_with("/api/v2") => Some(json!({
            "required": false,
            "content": {
                "application/x-www-form-urlencoded": { "schema": { "type": "string" } },
                "multipart/form-data": { "schema": { "type": "object", "additionalProperties": true } }
            }
        })),
        _ => json_body("JsonObject", json!({})),
    }
}

fn binary_body(content_type: &str, description: &str) -> Value {
    json!({
        "required": true,
        "content": { content_type: { "schema": { "type": "string", "format": "binary" }, "description": description } }
    })
}

fn responses(path: &str, method: &str) -> Value {
    let success = if path == "/api/v1/torrents/{hash}/metainfo" {
        json!({
            "200": {
                "description": "Exact retained original .torrent bytes. No reconstruction or fallback.",
                "content": { "application/x-bittorrent": { "schema": { "type": "string", "format": "binary" } } }
            }
        })
    } else if path == "/api/v1/torrents/export" {
        json!({
            "200": {
                "description": "Tar archive with manifest.json and retained original .torrent entries.",
                "content": { "application/x-tar": { "schema": { "type": "string", "format": "binary" } } }
            }
        })
    } else if path == "/api/v1/events" {
        json!({
            "200": {
                "description": "Server-Sent Events stream of the shared event envelope.",
                "content": { "text/event-stream": { "schema": { "type": "string" } } }
            }
        })
    } else if path == "/api/v1/ws" {
        json!({ "101": { "description": "WebSocket upgrade" } })
    } else if path == "/api/docs" {
        json!({
            "200": {
                "description": "Version-matched self-contained API explorer.",
                "content": { "text/html": { "schema": { "type": "string" } } }
            }
        })
    } else if path == "/api/openapi.json" {
        json!({
            "200": {
                "description": "Machine-readable OpenAPI 3.1 document.",
                "content": { "application/json": { "schema": { "type": "object", "additionalProperties": true } } }
            }
        })
    } else if path == "/transmission/rpc" {
        json!({
            "200": {
                "description": "Transmission JSON-RPC result or error object",
                "content": { "application/json": { "schema": { "$ref": "#/components/schemas/TransmissionRpcResponse" }, "example": { "result": { "version": "17 (SwarmOtter)" }, "arguments": {}, "tag": 1 } } }
            }
        })
    } else if path.starts_with("/api/v2") {
        if path == "/api/v2/torrents/info" {
            json!({
                "200": {
                    "description": "qBittorrent-compatible torrent rows",
                    "content": { "application/json": { "schema": { "type": "array", "items": { "type": "object", "additionalProperties": true } } } }
                }
            })
        } else if path == "/api/v2/torrents/categories" {
            json!({
                "200": {
                    "description": "qBittorrent-compatible category object",
                    "content": { "application/json": { "schema": { "type": "object", "additionalProperties": true } } }
                }
            })
        } else if path == "/api/v2/app/version" || path == "/api/v2/app/webapiVersion" {
            json!({
                "200": {
                    "description": "Version string",
                    "content": { "text/plain": { "schema": { "$ref": "#/components/schemas/QbittorrentVersion" } } }
                }
            })
        } else {
            json!({
                "200": {
                    "description": "qBittorrent-compatible acknowledgement",
                    "content": { "text/plain": { "schema": { "type": "string" } } }
                }
            })
        }
    } else {
        json!({
            "200": {
                "description": "Successful response",
                "content": { "application/json": { "schema": { "$ref": "#/components/schemas/Envelope" }, "example": envelope_example() } }
            }
        })
    };
    let mut object = success.as_object().cloned().unwrap_or_default();
    for (status, description) in [
        ("400", "Invalid request or typed validation error"),
        ("401", "Authentication required or invalid"),
        ("403", "Origin or authorization policy rejected the request"),
        ("404", "Resource or compatibility operation not found"),
        ("409", "Conflicting state such as a duplicate torrent"),
        ("500", "Daemon or storage failure"),
    ] {
        object.insert(status.into(), json!({
            "description": description,
            "content": { "application/json": { "schema": { "$ref": "#/components/schemas/ErrorEnvelope" }, "example": error_example() } }
        }));
    }
    let _ = method;
    Value::Object(object)
}

fn envelope_example() -> Value {
    json!({
        "success": true,
        "data": torrent_example(),
        "error": null
    })
}

fn error_example() -> Value {
    json!({
        "success": false,
        "data": null,
        "error": { "code": "not_found", "message": "torrent not found" }
    })
}

fn torrent_example() -> Value {
    json!({
        "info_hash": "0123456789abcdef0123456789abcdef01234567",
        "identity": { "kind": "v1", "v1": "0123456789abcdef0123456789abcdef01234567", "v2": null },
        "name": "example-release",
        "state": "paused",
        "error": null,
        "total_length": 1048576,
        "bytes_completed": 524288,
        "uploaded": 0,
        "downloaded": 524288,
        "piece_count": 16,
        "pieces_have": 8,
        "piece_length": 65536,
        "private": false,
        "labels": ["linux"],
        "comment": "source comment",
        "created_by": "release tool",
        "creation_date": 1700000000,
        "encoding": "UTF-8",
        "annotation": "operator note",
        "annotation_updated_at": 1700000100,
        "annotation_updated_by": "api",
        "download_dir": "/data/downloads",
        "download_limit": 0,
        "upload_limit": 0,
        "rate_down": 0,
        "rate_up": 0,
        "active_peer_workers": 0,
        "known_peers": 0,
        "ratio": 0.0,
        "queue_position": 1,
        "date_added": 1700000000,
        "date_completed": null,
        "health": { "score": 80, "bars": 4, "label": "good", "availability_score": 80, "throughput_score": 80, "peer_score": 80, "stability_score": 80, "discovery_score": 80, "reasons": [] }
    })
}

fn components() -> Value {
    json!({
        "securitySchemes": {
            "bearerAuth": {
                "type": "http",
                "scheme": "bearer",
                "description": "Configured api.auth_token. The X-SwarmOtter-Auth header is also accepted."
            },
            "qbittorrentSession": {
                "type": "apiKey",
                "in": "cookie",
                "name": "SID",
                "description": "qBittorrent compatibility session established by /api/v2/auth/login."
            },
            "transmissionSession": {
                "type": "apiKey",
                "in": "header",
                "name": "X-Transmission-Session-Id",
                "description": "Transmission RPC session id returned by session negotiation."
            }
        },
        "schemas": {
            "Envelope": {
                "type": "object",
                "required": ["success", "data", "error"],
                "properties": {
                    "success": { "type": "boolean" },
                    "data": {},
                    "error": { "anyOf": [{ "type": "null" }, { "$ref": "#/components/schemas/ErrorBody" }] }
                },
                "additionalProperties": true,
                "example": envelope_example()
            },
            "ErrorEnvelope": {
                "type": "object",
                "required": ["success", "data", "error"],
                "properties": {
                    "success": { "const": false },
                    "data": { "type": "null" },
                    "error": { "$ref": "#/components/schemas/ErrorBody" }
                },
                "additionalProperties": true,
                "example": error_example()
            },
            "ErrorBody": {
                "type": "object",
                "required": ["code", "message"],
                "properties": {
                    "code": { "type": "string" },
                    "message": { "type": "string" }
                },
                "additionalProperties": false
            },
            "JsonObject": { "type": "object", "additionalProperties": true },
            "TorrentIdentity": {
                "type": "object",
                "required": ["kind"],
                "properties": {
                    "kind": { "type": "string", "enum": ["v1", "v2", "hybrid", "unknown"] },
                    "v1": { "type": ["string", "null"] },
                    "v2": { "type": ["string", "null"] }
                },
                "additionalProperties": true
            },
            "TorrentHealth": {
                "type": "object",
                "required": ["score", "label"],
                "properties": {
                    "score": { "type": "integer" },
                    "bars": { "type": "integer" },
                    "label": { "type": "string" },
                    "reasons": { "type": "array", "items": { "type": "string" } }
                },
                "additionalProperties": true
            },
            "TorrentSummary": {
                "type": "object",
                "required": ["info_hash", "name", "state", "identity", "total_length", "bytes_completed", "health"],
                "properties": {
                    "info_hash": { "type": "string" },
                    "identity": { "$ref": "#/components/schemas/TorrentIdentity" },
                    "name": { "type": "string" },
                    "state": { "type": "string" },
                    "error": { "type": ["string", "null"] },
                    "total_length": { "type": "integer" },
                    "bytes_completed": { "type": "integer" },
                    "uploaded": { "type": "integer" },
                    "downloaded": { "type": "integer" },
                    "piece_count": { "type": "integer" },
                    "pieces_have": { "type": "integer" },
                    "piece_length": { "type": "integer" },
                    "private": { "type": "boolean" },
                    "labels": { "type": "array", "items": { "type": "string" } },
                    "comment": { "type": ["string", "null"] },
                    "created_by": { "type": ["string", "null"] },
                    "creation_date": { "type": ["integer", "null"] },
                    "encoding": { "type": ["string", "null"] },
                    "annotation": { "type": ["string", "null"] },
                    "annotation_updated_at": { "type": ["integer", "null"] },
                    "annotation_updated_by": { "type": ["string", "null"] },
                    "health": { "$ref": "#/components/schemas/TorrentHealth" }
                },
                "additionalProperties": true,
                "example": torrent_example()
            },
            "TorrentListResponse": {
                "type": "object",
                "required": ["rows", "total", "filtered", "page", "per_page", "page_count"],
                "properties": {
                    "rows": { "type": "array", "items": { "$ref": "#/components/schemas/TorrentSummary" } },
                    "total": { "type": "integer" },
                    "filtered": { "type": "integer" },
                    "page": { "type": "integer" },
                    "per_page": { "type": "integer" },
                    "page_count": { "type": "integer" },
                    "counts": { "type": "object", "additionalProperties": true },
                    "groups": { "type": "array", "items": { "type": "object", "additionalProperties": true } }
                },
                "additionalProperties": true
            },
            "AddMagnetRequest": {
                "type": "object",
                "required": ["magnet"],
                "properties": {
                    "magnet": { "type": "string" },
                    "download_dir": { "type": ["string", "null"] },
                    "incomplete_dir": { "type": ["string", "null"] },
                    "paused": { "type": ["boolean", "null"] },
                    "start_behavior": { "type": ["string", "null"], "enum": ["start", "paused", null] },
                    "profile": { "type": ["string", "null"] },
                    "labels": { "type": ["array", "null"], "items": { "type": "string" } },
                    "preview": { "type": ["boolean", "null"] },
                    "unwanted_file_indices": { "type": ["array", "null"], "items": { "type": "integer" } },
                    "file_exclusion_rules": { "type": ["array", "null"], "items": { "type": "object", "additionalProperties": true } },
                    "partial_file_suffix": { "type": ["string", "null"] }
                },
                "additionalProperties": false
            },
            "BulkAddRequest": { "type": "object", "additionalProperties": true },
            "CreateTorrentRequest": {
                "type": "object",
                "required": ["source"],
                "properties": {
                    "source": { "type": "string" },
                    "name": { "type": ["string", "null"] },
                    "format": { "type": "string", "enum": ["v1", "v2", "hybrid"] },
                    "piece_length": { "type": ["integer", "null"] },
                    "tracker_tiers": { "type": "array", "items": { "type": "array", "items": { "type": "string" } } },
                    "webseeds": { "type": "array", "items": { "type": "string" } },
                    "private": { "type": "boolean" },
                    "seed": { "type": "boolean" }
                },
                "additionalProperties": false
            },
            "ExportArchiveRequest": {
                "type": "object",
                "required": ["hashes"],
                "properties": { "hashes": { "type": "array", "items": { "type": "string" } } },
                "additionalProperties": false
            },
            "AnnotationRequest": {
                "type": "object",
                "properties": { "annotation": { "type": ["string", "null"] } },
                "additionalProperties": false
            },
            "LimitsRequest": {
                "type": "object",
                "properties": { "download_limit": { "type": "integer", "minimum": 0 }, "upload_limit": { "type": "integer", "minimum": 0 } },
                "additionalProperties": false
            },
            "TrashSelectionRequest": {
                "type": "object",
                "properties": {
                    "hashes": { "type": "array", "items": { "type": "string" } },
                    "rule": { "type": ["string", "null"] },
                    "mode": { "type": "string", "enum": ["torrent", "stale_partials"] }
                },
                "additionalProperties": false
            },
            "RestoreRequest": {
                "type": "object",
                "properties": { "collision_policy": { "type": ["string", "null"], "enum": ["fail", null] } },
                "additionalProperties": false
            },
            "RssSubscriptionRequest": {
                "type": "object",
                "required": ["id", "name", "url"],
                "properties": {
                    "id": { "type": "string" },
                    "name": { "type": "string" },
                    "url": { "type": "string" },
                    "enabled": { "type": "boolean" },
                    "poll_interval_seconds": { "type": "integer" },
                    "profile": { "type": ["string", "null"] },
                    "start_behavior": { "type": "string", "enum": ["start", "paused"] },
                    "import_mode": { "type": "string", "enum": ["new_only", "current_matches"] },
                    "rules": { "type": "object", "additionalProperties": true },
                    "labels": { "type": "array", "items": { "type": "string" } }
                },
                "additionalProperties": false
            },
            "TransmissionRpcResponse": {
                "type": "object",
                "properties": {
                    "result": {},
                    "arguments": { "type": "object", "additionalProperties": true },
                    "tag": { "type": ["integer", "null"] },
                    "error": { "type": "object", "additionalProperties": true }
                },
                "additionalProperties": false
            },
            "QbittorrentVersion": { "type": "string" },
            "TransmissionRpcRequest": {
                "type": "object",
                "required": ["method"],
                "properties": {
                    "method": { "type": "string" },
                    "arguments": { "type": "object", "additionalProperties": true },
                    "tag": { "type": ["integer", "null"] }
                },
                "additionalProperties": true
            },
            "Config": { "type": "object", "additionalProperties": true },
            "TorrentCreationResult": { "type": "object", "additionalProperties": true },
            "TrashStatus": { "type": "object", "additionalProperties": true },
            "RssSubscriptionStatus": { "type": "object", "additionalProperties": true }
        },
        "examples": {
            "SuccessEnvelope": { "value": envelope_example() },
            "ErrorEnvelope": { "value": error_example() },
            "TorrentListPage": { "value": { "success": true, "data": { "rows": [torrent_example()], "total": 1, "filtered": 1, "page": 1, "per_page": 200, "page_count": 1, "counts": {}, "groups": [] }, "error": null } }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    #[test]
    fn catalog_matches_registered_router_paths_and_methods() {
        let source = include_str!("routes.rs");
        let actual = parse_router_source(source);
        let documented = ROUTES
            .iter()
            .flat_map(|route| {
                route
                    .methods
                    .iter()
                    .map(move |method| format!("{method} {}", route.path))
            })
            .collect::<BTreeSet<_>>();
        assert_eq!(
            actual, documented,
            "OpenAPI route catalog drifted from routes.rs"
        );
    }

    #[test]
    fn deliberate_catalog_drift_is_detected() {
        let source = include_str!("routes.rs");
        let actual = parse_router_source(source);
        let mut documented = ROUTES
            .iter()
            .flat_map(|route| {
                route
                    .methods
                    .iter()
                    .map(move |method| format!("{method} {}", route.path))
            })
            .collect::<BTreeSet<_>>();
        documented.remove("GET /api/v1/version");
        assert_ne!(actual, documented);
    }

    fn parse_router_source(source: &str) -> BTreeSet<String> {
        let mut out = BTreeSet::new();
        let root_start = source.find("fn app_router_with_body_limit").unwrap();
        let v1_start = source.find("fn api_v1_router").unwrap();
        for (segment, prefix) in [
            (&source[root_start..v1_start], ""),
            (&source[v1_start..], "/api/v1"),
        ] {
            let mut offset = 0;
            while let Some(found) = segment[offset..].find(".route(") {
                let open = offset + found + ".route(".len();
                let mut path_start = open;
                while path_start < segment.len()
                    && segment.as_bytes()[path_start].is_ascii_whitespace()
                {
                    path_start += 1;
                }
                if segment.as_bytes().get(path_start) != Some(&b'"') {
                    offset = open;
                    continue;
                }
                let path_end = path_start + 1 + segment[path_start + 1..].find('"').unwrap();
                let path = &segment[path_start + 1..path_end];
                let mut depth = 1usize;
                let mut cursor = open;
                let mut in_string = false;
                let mut escaped = false;
                while cursor < segment.len() {
                    let byte = segment.as_bytes()[cursor];
                    if in_string {
                        if escaped {
                            escaped = false;
                        } else if byte == b'\\' {
                            escaped = true;
                        } else if byte == b'"' {
                            in_string = false;
                        }
                    } else if byte == b'"' {
                        in_string = true;
                    } else if byte == b'(' {
                        depth += 1;
                    } else if byte == b')' {
                        depth -= 1;
                        if depth == 0 {
                            break;
                        }
                    }
                    cursor += 1;
                }
                let expression = &segment[open..cursor];
                let mut methods = BTreeSet::new();
                for method in ["get", "post", "put", "patch", "delete"] {
                    let needle = format!("{method}(");
                    if expression.contains(&needle)
                        || (method == "delete" && expression.contains("routing::delete("))
                    {
                        methods.insert(method.to_ascii_uppercase());
                    }
                }
                let normalized = format!("{prefix}{path}");
                // The public /health route is intentionally duplicated with the
                // native /api/v1/health route; keep both in the catalog.
                for method in methods {
                    out.insert(format!("{method} {normalized}"));
                }
                offset = cursor.saturating_add(1);
            }
        }
        out
    }
}
