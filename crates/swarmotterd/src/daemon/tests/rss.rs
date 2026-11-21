// SPDX-License-Identifier: Apache-2.0

use super::*;
use std::collections::HashMap;
use std::sync::Arc;

use swarmotter_core::config::{Config, StartBehavior};
use swarmotter_core::hash::TorrentKey;
use swarmotter_core::models::network::{NetworkContainmentMode, NetworkContainmentStatus};
use swarmotter_core::rss::{RssConfig, RssFilterRule, RssImportMode, RssRuleSet, RssSubscription};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::Mutex as AsyncMutex;

#[derive(Clone)]
struct FixtureResponse {
    status: u16,
    body: Vec<u8>,
    content_type: String,
    etag: Option<String>,
    retry_after: Option<u64>,
}

impl FixtureResponse {
    fn xml(body: impl Into<Vec<u8>>, etag: &str) -> Self {
        Self {
            status: 200,
            body: body.into(),
            content_type: "application/rss+xml".into(),
            etag: Some(etag.into()),
            retry_after: None,
        }
    }

    fn torrent(body: Vec<u8>) -> Self {
        Self {
            status: 200,
            body,
            content_type: "application/x-bittorrent".into(),
            etag: None,
            retry_after: None,
        }
    }

    fn status(status: u16) -> Self {
        Self {
            status,
            body: b"temporary failure".to_vec(),
            content_type: "text/plain".into(),
            etag: None,
            retry_after: None,
        }
    }
}

type FixtureMap = Arc<AsyncMutex<HashMap<String, FixtureResponse>>>;

async fn serve_fixture(
    listener: tokio::net::TcpListener,
    responses: FixtureMap,
) -> std::io::Result<()> {
    loop {
        let (mut stream, _) = listener.accept().await?;
        let responses = responses.clone();
        tokio::spawn(async move {
            let mut buffer = vec![0u8; 16 * 1024];
            let Ok(read) = stream.read(&mut buffer).await else {
                return;
            };
            let request = String::from_utf8_lossy(&buffer[..read]);
            let mut lines = request.lines();
            let path = lines
                .next()
                .and_then(|line| line.split_whitespace().nth(1))
                .unwrap_or("/")
                .to_string();
            let if_none_match = lines.find_map(|line| {
                line.split_once(':').and_then(|(name, value)| {
                    name.eq_ignore_ascii_case("if-none-match")
                        .then(|| value.trim().to_string())
                })
            });
            let response = responses.lock().await.get(&path).cloned();
            let Some(response) = response else {
                let _ = stream
                    .write_all(
                        b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                    )
                    .await;
                return;
            };
            if response
                .etag
                .as_deref()
                .is_some_and(|etag| if_none_match.as_deref() == Some(etag))
            {
                let body = format!(
                    "HTTP/1.1 304 Not Modified\r\nETag: {}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                    response.etag.as_deref().unwrap_or_default()
                );
                let _ = stream.write_all(body.as_bytes()).await;
                return;
            }
            let reason = match response.status {
                200 => "OK",
                500 => "Internal Server Error",
                503 => "Service Unavailable",
                _ => "Status",
            };
            let headers = format!(
                "HTTP/1.1 {} {}\r\nContent-Type: {}\r\nContent-Length: {}\r\n{}{}Connection: close\r\n\r\n",
                response.status,
                reason,
                response.content_type,
                response.body.len(),
                response
                    .etag
                    .as_deref()
                    .map(|etag| format!("ETag: {etag}\r\n"))
                    .unwrap_or_default(),
                response
                    .retry_after
                    .map(|seconds| format!("Retry-After: {seconds}\r\n"))
                    .unwrap_or_default()
            );
            let _ = stream.write_all(headers.as_bytes()).await;
            let _ = stream.write_all(&response.body).await;
        });
    }
}

async fn fixture_server() -> (
    String,
    FixtureMap,
    tokio::task::JoinHandle<std::io::Result<()>>,
) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let responses: FixtureMap = Arc::new(AsyncMutex::new(HashMap::new()));
    let server = tokio::spawn(serve_fixture(listener, responses.clone()));
    (format!("http://{address}"), responses, server)
}

fn rss_config(root: &Path, state_path: &Path, subscriptions: Vec<RssSubscription>) -> Config {
    let mut config = Config::default();
    config.network.mode = NetworkContainmentMode::Disabled;
    config.queue.auto_start = false;
    config.torrent.selfish = true;
    config.storage.download_dir = Some(root.join("downloads").display().to_string());
    config.rss = RssConfig {
        history_file: Some(
            state_path
                .with_extension("rss.sqlite")
                .display()
                .to_string(),
        ),
        subscriptions,
        retry_base_seconds: 1,
        retry_max_seconds: 2,
        max_retries: 3,
        ..Default::default()
    };
    config
}

fn subscription(id: &str, url: String, rules: RssRuleSet, mode: RssImportMode) -> RssSubscription {
    RssSubscription {
        id: id.into(),
        name: id.into(),
        url,
        enabled: true,
        poll_interval_seconds: 900,
        profile: None,
        start_behavior: StartBehavior::Paused,
        import_mode: mode,
        rules,
        labels: vec!["rss".into()],
    }
}

fn fixture_rules() -> RssRuleSet {
    RssRuleSet {
        include: vec![RssFilterRule {
            title: Some("*fixture*".into()),
            category: None,
        }],
        exclude: vec![RssFilterRule {
            title: Some("*excluded*".into()),
            category: None,
        }],
    }
}

fn feed(entries: &str) -> Vec<u8> {
    format!(
        "<?xml version=\"1.0\"?><rss version=\"2.0\"><channel><title>Fixture</title>{entries}</channel></rss>"
    )
    .into_bytes()
}

fn torrent_fixture(name: &str) -> (Vec<u8>, TorrentKey) {
    let bytes = swarmotter_core::meta::build_single_file_torrent(
        name,
        b"rss generated fixture payload".as_slice(),
        16,
        None,
        false,
    );
    let parsed = swarmotter_core::meta::parse_torrent(&bytes).unwrap();
    (bytes, TorrentKey::v1(parsed.info_hash))
}

fn start_runtime(config: Config, state_path: &Path) -> Arc<DaemonRuntime> {
    Arc::new(DaemonRuntime::with_paths_broker_and_state(
        config,
        disabled_health(),
        None,
        None,
        Some(state_path.to_path_buf()),
        EventBroker::default(),
    ))
}

fn xml_escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

#[tokio::test]
async fn rss_end_to_end_imports_match_explains_exclusion_and_skips_after_selfish_removal_restart() {
    let root = unique_dir("rss-e2e");
    let state_path = root.join("state.json");
    let (base, responses, server) = fixture_server().await;
    let (torrent_bytes, key) = torrent_fixture("rss-fixture.bin");
    let match_url = format!("{base}/downloads/match");
    let excluded_url = format!("{base}/downloads/excluded");
    let feed_a = feed(&format!(
        "<item><title>Fixture release</title><guid>a-1</guid><category>Tests</category>\
         <link>{match_url}</link>\
         <enclosure url=\"{match_url}\" type=\"application/x-bittorrent\"/></item>\
         <item><title>Excluded Fixture release</title><guid>a-2</guid>\
         <link>{excluded_url}</link>\
         <enclosure url=\"{excluded_url}\" type=\"application/x-bittorrent\"/></item>\
         <item><title>Fixture page only</title><guid>a-3</guid><link>{base}/details/3</link></item>\
         <item><title>Fixture malformed</title><guid>a-4</guid>\
         <link>{base}/downloads/malformed</link>\
         <enclosure url=\"{base}/downloads/malformed\" type=\"application/x-bittorrent\"/></item>"
    ));
    let feed_b_initial = feed(
        "<item><title>Other Release</title><guid>b-old</guid><link>https://example.test/old</link></item>",
    );
    responses.lock().await.insert(
        "/feed-a.xml".into(),
        FixtureResponse::xml(feed_a, "\"a-v1\""),
    );
    responses.lock().await.insert(
        "/feed-b.xml".into(),
        FixtureResponse::xml(feed_b_initial, "\"b-v1\""),
    );
    responses.lock().await.insert(
        "/downloads/match".into(),
        FixtureResponse::torrent(torrent_bytes.clone()),
    );
    responses.lock().await.insert(
        "/downloads/excluded".into(),
        FixtureResponse::torrent(torrent_bytes.clone()),
    );
    responses.lock().await.insert(
        "/downloads/malformed".into(),
        FixtureResponse {
            status: 200,
            body: b"this is not bencoded metainfo".to_vec(),
            content_type: "application/x-bittorrent".into(),
            etag: None,
            retry_after: None,
        },
    );
    let config = rss_config(
        &root,
        &state_path,
        vec![
            subscription(
                "feed-a",
                format!("{base}/feed-a.xml"),
                fixture_rules(),
                RssImportMode::CurrentMatches,
            ),
            subscription(
                "feed-b",
                format!("{base}/feed-b.xml"),
                fixture_rules(),
                RssImportMode::NewOnly,
            ),
        ],
    );
    let runtime = start_runtime(config.clone(), &state_path);
    runtime.restore_persisted_state().await.unwrap();

    // Preview must explain decisions without adding or consuming anything.
    let preview = runtime.preview_rss_subscription("feed-a").await.unwrap();
    assert_eq!(preview.decisions.len(), 4);
    assert!(preview
        .decisions
        .iter()
        .any(|decision| decision.action == swarmotter_core::rss::RssAction::Import));
    assert!(preview.decisions.iter().any(|decision| {
        decision.action == swarmotter_core::rss::RssAction::Filtered
            && decision.reason.contains("excluded by rule")
    }));
    assert!(preview
        .decisions
        .iter()
        .any(|decision| decision.action == swarmotter_core::rss::RssAction::Unsupported));
    assert!(runtime.registry.lock().await.torrents.is_empty());
    let store = runtime.rss_history_store().await.unwrap();
    assert!(store.feed_state("feed-a").unwrap().is_none());

    // Establish feed B's baseline before the torrent exists anywhere.
    let baseline_b = runtime
        .poll_rss_subscription("feed-b", false, false)
        .await
        .unwrap();
    assert!(baseline_b.success);
    assert!(baseline_b.decisions.iter().all(|decision| matches!(
        decision.action,
        swarmotter_core::rss::RssAction::Baseline
            | swarmotter_core::rss::RssAction::Filtered
            | swarmotter_core::rss::RssAction::Unsupported
    )));

    let poll = runtime
        .poll_rss_subscription("feed-a", false, false)
        .await
        .unwrap();
    assert!(poll.success, "{:?}", poll.error);
    assert_eq!(poll.imported, vec![key.to_locator()]);
    let registry = runtime.registry.lock().await;
    assert_eq!(registry.torrents.len(), 1);
    let imported = registry.get(&key).unwrap().clone();
    drop(registry);
    assert_eq!(imported.labels, vec!["rss".to_string()]);
    assert_eq!(imported.state, TorrentState::Paused);
    assert!(poll.decisions.iter().any(|decision| {
        decision.action == swarmotter_core::rss::RssAction::Filtered
            && decision.reason.contains("excluded")
    }));

    // A later poll that still offers the same paused torrent reports it as
    // already present and preserves its state.
    let same_feed_again = feed(&format!(
        "<item><title>Fixture release</title><guid>a-1</guid><category>Tests</category>\
         <link>{match_url}</link>\
         <enclosure url=\"{match_url}\" type=\"application/x-bittorrent\"/></item>"
    ));
    responses.lock().await.insert(
        "/feed-a.xml".into(),
        FixtureResponse::xml(same_feed_again, "\"a-v1b\""),
    );
    let already = runtime
        .poll_rss_subscription("feed-a", false, false)
        .await
        .unwrap();
    assert!(already.imported.is_empty());
    assert!(already
        .decisions
        .iter()
        .any(|decision| { decision.action == swarmotter_core::rss::RssAction::AlreadyPresent }));
    assert_eq!(
        runtime.registry.lock().await.get(&key).unwrap().state,
        TorrentState::Paused
    );

    // Conditional request: unchanged feed returns 304 and adds no rows.
    let listing_rows_before: i64 = {
        let connection = rusqlite::Connection::open(store.path()).unwrap();
        connection
            .query_row("SELECT COUNT(*) FROM listings", [], |row| row.get(0))
            .unwrap()
    };
    let conditional = runtime
        .poll_rss_subscription("feed-a", false, false)
        .await
        .unwrap();
    assert!(conditional.conditional_not_modified, "{conditional:?}");
    let listing_rows_after: i64 = {
        let connection = rusqlite::Connection::open(store.path()).unwrap();
        connection
            .query_row("SELECT COUNT(*) FROM listings", [], |row| row.get(0))
            .unwrap()
    };
    assert_eq!(listing_rows_before, listing_rows_after);

    // Simulate the engine's verified completion transition, then selfish
    // removal. The completion event is captured durably before the record is
    // deleted.
    {
        let mut registry = runtime.registry.lock().await;
        let torrent = registry.get_mut(&key).unwrap();
        let total = torrent.progress.total;
        for piece in 0..total {
            torrent.progress.have_piece(piece);
        }
        torrent.recompute_file_bytes_completed();
        torrent.state = TorrentState::Completed;
        torrent.date_completed = Some(now());
    }
    let completed = runtime.registry.lock().await.get(&key).cloned().unwrap();
    runtime.queue_rss_verified_completion(&completed).await;
    runtime.persist_state().await.unwrap();
    runtime.selfish_remove_completed(key).await;
    runtime.apply_rss_outbox_once().await.unwrap();
    assert!(runtime.registry.lock().await.get(&key).is_none());
    let completed_alias = store.completed_alias(&[key.to_locator()]).unwrap();
    assert!(
        completed_alias.is_some(),
        "verified completion was not recorded"
    );
    drop(runtime);
    drop(store);

    // Restart from the same state and history databases. Feed B now offers the
    // same torrent under a different title and GUID.
    let renamed_feed = feed(&format!(
        "<item><title>Other Release</title><guid>b-old</guid><link>https://example.test/old</link></item>\
         <item><title>Renamed Fixture release</title><guid>b-new</guid><link>{match_url}</link>\
         <enclosure url=\"{match_url}\" type=\"application/x-bittorrent\"/></item>"
    ));
    responses.lock().await.insert(
        "/feed-b.xml".into(),
        FixtureResponse::xml(renamed_feed, "\"b-v2\""),
    );
    let restarted = start_runtime(config, &state_path);
    restarted.restore_persisted_state().await.unwrap();
    restarted.apply_rss_outbox_once().await.unwrap();
    let poll_b = restarted
        .poll_rss_subscription("feed-b", false, false)
        .await
        .unwrap();
    assert!(poll_b.success, "{:?}", poll_b.error);
    assert!(poll_b.imported.is_empty(), "feed B added a second torrent");
    assert!(poll_b.decisions.iter().any(|decision| {
        decision.action == swarmotter_core::rss::RssAction::PreviouslyCompleted
            && decision.reason.contains("verified completion")
    }));
    assert!(restarted.registry.lock().await.torrents.is_empty());

    server.abort();
    std::fs::remove_dir_all(root).ok();
}

#[tokio::test]
async fn simultaneous_magnet_and_torrent_offers_produce_one_library_entry() {
    let root = unique_dir("rss-simultaneous");
    let state_path = root.join("state.json");
    let (base, responses, server) = fixture_server().await;
    let (torrent_bytes, key) = torrent_fixture("rss-simultaneous.bin");
    let parsed = swarmotter_core::meta::parse_torrent(&torrent_bytes).unwrap();
    let magnet = format!(
        "magnet:?xt=urn:btih:{}&dn=simultaneous",
        parsed.info_hash.to_hex()
    );
    let attachment = format!("{base}/simultaneous");
    responses.lock().await.insert(
        "/magnet.xml".into(),
        FixtureResponse::xml(
            feed(&format!(
                "<item><title>Fixture magnet</title><guid>m-1</guid><link>{}</link></item>",
                xml_escape(&magnet)
            )),
            "\"m\"",
        ),
    );
    responses.lock().await.insert(
        "/torrent.xml".into(),
        FixtureResponse::xml(
            feed(&format!(
                "<item><title>Fixture torrent</title><guid>t-1</guid><link>{attachment}</link>\
                 <enclosure url=\"{attachment}\" type=\"application/x-bittorrent\"/></item>"
            )),
            "\"t\"",
        ),
    );
    responses.lock().await.insert(
        "/simultaneous".into(),
        FixtureResponse::torrent(torrent_bytes),
    );
    let config = rss_config(
        &root,
        &state_path,
        vec![
            subscription(
                "magnet-feed",
                format!("{base}/magnet.xml"),
                RssRuleSet::default(),
                RssImportMode::CurrentMatches,
            ),
            subscription(
                "torrent-feed",
                format!("{base}/torrent.xml"),
                RssRuleSet::default(),
                RssImportMode::CurrentMatches,
            ),
        ],
    );
    let runtime = start_runtime(config, &state_path);
    runtime.restore_persisted_state().await.unwrap();
    let (a, b) = tokio::join!(
        runtime.poll_rss_subscription("magnet-feed", false, false),
        runtime.poll_rss_subscription("torrent-feed", false, false)
    );
    let a = a.unwrap();
    let b = b.unwrap();
    assert!(a.success && b.success, "{a:?} {b:?}");
    assert_eq!(runtime.registry.lock().await.torrents.len(), 1);
    assert!(runtime.registry.lock().await.get(&key).is_some());
    let imported_total = a.imported.len() + b.imported.len();
    assert_eq!(imported_total, 1, "both feeds imported a torrent");
    let decisions = a.decisions.iter().chain(b.decisions.iter());
    assert!(decisions
        .clone()
        .any(|decision| { decision.action == swarmotter_core::rss::RssAction::Import }));
    assert!(decisions
        .clone()
        .any(|decision| { decision.action == swarmotter_core::rss::RssAction::AlreadyPresent }));
    server.abort();
    std::fs::remove_dir_all(root).ok();
}

#[tokio::test]
async fn transient_attachment_failure_remains_eligible_after_leaving_feed() {
    let root = unique_dir("rss-retry");
    let state_path = root.join("state.json");
    let (base, responses, server) = fixture_server().await;
    let (torrent_bytes, key) = torrent_fixture("rss-retry.bin");
    let attachment = format!("{base}/retry-attachment");
    let first_feed = feed(&format!(
        "<item><title>Fixture retry</title><guid>r-1</guid><link>{attachment}</link>\
         <enclosure url=\"{attachment}\" type=\"application/x-bittorrent\"/></item>"
    ));
    responses.lock().await.insert(
        "/retry.xml".into(),
        FixtureResponse::xml(first_feed, "\"r1\""),
    );
    responses
        .lock()
        .await
        .insert("/retry-attachment".into(), FixtureResponse::status(500));
    let config = rss_config(
        &root,
        &state_path,
        vec![subscription(
            "retry-feed",
            format!("{base}/retry.xml"),
            RssRuleSet::default(),
            RssImportMode::CurrentMatches,
        )],
    );
    let runtime = start_runtime(config, &state_path);
    runtime.restore_persisted_state().await.unwrap();
    let first = runtime
        .poll_rss_subscription("retry-feed", false, false)
        .await
        .unwrap();
    assert!(first.success);
    assert!(first.decisions.iter().any(|decision| {
        decision.action == swarmotter_core::rss::RssAction::Failed
            && decision.reason.contains("transient")
    }));
    assert!(runtime.registry.lock().await.torrents.is_empty());
    // The listing leaves the feed; only the durable retry row remains.
    responses.lock().await.insert(
        "/retry.xml".into(),
        FixtureResponse::xml(
            feed("<item><title>Unrelated</title><guid>other</guid></item>"),
            "\"r2\"",
        ),
    );
    responses.lock().await.insert(
        "/retry-attachment".into(),
        FixtureResponse::torrent(torrent_bytes),
    );
    tokio::time::sleep(std::time::Duration::from_millis(1_200)).await;
    let second = runtime
        .poll_rss_subscription("retry-feed", false, false)
        .await
        .unwrap();
    assert!(second.success, "{:?}", second.error);
    assert!(second.imported.contains(&key.to_locator()), "{second:?}");
    assert_eq!(runtime.registry.lock().await.torrents.len(), 1);
    server.abort();
    std::fs::remove_dir_all(root).ok();
}

#[tokio::test]
async fn containment_loss_fails_closed_and_preserves_retry_eligibility() {
    let root = unique_dir("rss-containment");
    let state_path = root.join("state.json");
    let (base, responses, server) = fixture_server().await;
    responses.lock().await.insert(
        "/feed.xml".into(),
        FixtureResponse::xml(feed("<item><title>Fixture blocked</title><guid>b</guid><link>magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567</link></item>"), "\"x\""),
    );
    let mut config = rss_config(
        &root,
        &state_path,
        vec![subscription(
            "blocked",
            format!("{base}/feed.xml"),
            RssRuleSet::default(),
            RssImportMode::CurrentMatches,
        )],
    );
    config.network.mode = NetworkContainmentMode::Strict;
    config.network.fail_closed = true;
    config.network.required_interface = Some("swarmotter-missing-rss-test".into());
    let health = NetworkHealth::blocked(
        NetworkContainmentMode::Strict,
        NetworkContainmentStatus::InterfaceMissing,
        "required interface is missing",
    );
    let runtime = Arc::new(DaemonRuntime::with_paths_broker_and_state(
        config,
        health,
        None,
        None,
        Some(state_path.clone()),
        EventBroker::default(),
    ));
    runtime.restore_persisted_state().await.unwrap();
    let result = runtime
        .poll_rss_subscription("blocked", false, false)
        .await
        .unwrap();
    assert!(!result.success);
    assert!(result
        .error
        .as_deref()
        .is_some_and(|message| message.contains("blocked") || message.contains("network")));
    assert!(runtime.registry.lock().await.torrents.is_empty());
    let store = runtime.rss_history_store().await.unwrap();
    let state = store.feed_state("blocked").unwrap().unwrap();
    assert!(!state.baseline_established);
    assert!(state.last_error.is_some());
    assert!(state.next_attempt_at.is_some());
    server.abort();
    std::fs::remove_dir_all(root).ok();
}

#[tokio::test]
async fn outbox_replay_is_idempotent_across_crash_boundaries() {
    use crate::rss_store::{PendingEventKind, PendingLibraryEvent, RssStore};
    use crate::state_store::{
        acknowledge_outbox_events, load_outbox_events, save_with_original_metainfo_and_events,
        DaemonState,
    };

    let root = unique_dir("rss-outbox");
    let state_path = root.join("state.json");
    let history_path = root.join("history.rss.sqlite");
    let event = PendingLibraryEvent::completion(
        "0123456789abcdef0123456789abcdef01234567".into(),
        vec![
            "0123456789abcdef0123456789abcdef01234567".into(),
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into(),
        ],
        "fixture".into(),
        100,
    );
    let state = DaemonState::new(Vec::new(), Default::default());
    save_with_original_metainfo_and_events(&state_path, &state, None, std::slice::from_ref(&event))
        .unwrap();

    let persisted = load_outbox_events(&state_path, 10).unwrap();
    assert_eq!(persisted.len(), 1);
    assert_eq!(persisted[0].kind, PendingEventKind::Completed);
    let store = RssStore::new(&history_path);
    store.initialize().unwrap();

    // Crash after history write but before acknowledgement. Replay must
    // produce one logical completion and later acknowledgement removes it.
    store.apply_event(&persisted[0]).unwrap();
    let after_first_apply = load_outbox_events(&state_path, 10).unwrap();
    assert_eq!(after_first_apply.len(), 1);
    store.apply_event(&after_first_apply[0]).unwrap();
    assert!(store
        .completed_alias(&[
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into()
        ])
        .unwrap()
        .is_some());
    acknowledge_outbox_events(&state_path, &[after_first_apply[0].id.unwrap()]).unwrap();
    assert!(load_outbox_events(&state_path, 10).unwrap().is_empty());
    let completions: i64 = {
        let connection = rusqlite::Connection::open(&history_path).unwrap();
        connection
            .query_row("SELECT COUNT(*) FROM completions", [], |row| row.get(0))
            .unwrap()
    };
    assert_eq!(completions, 1);
    std::fs::remove_dir_all(root).ok();
}

#[tokio::test]
async fn indexed_duplicate_lookup_handles_large_seeded_history() {
    use crate::rss_store::RssStore;
    let root = unique_dir("rss-scale");
    let history_path = root.join("history.rss.sqlite");
    let store = RssStore::new(&history_path);
    store.initialize().unwrap();
    {
        let mut connection = rusqlite::Connection::open(&history_path).unwrap();
        let transaction = connection.transaction().unwrap();
        for index in 0..5_000u64 {
            let key = format!("{index:040x}");
            transaction
                .execute(
                    "INSERT INTO completions(canonical_key, first_completed_at, last_observed_at, source)
                     VALUES (?1, 1, 1, 'scale')",
                    rusqlite::params![&key],
                )
                .unwrap();
            transaction
                .execute(
                    "INSERT INTO completion_aliases(alias_key, canonical_key) VALUES (?1, ?1)",
                    rusqlite::params![&key],
                )
                .unwrap();
            transaction
                .execute(
                    "INSERT INTO listings(subscription_id, entry_key, title, entry_json, status, reason,
                        first_seen_at, last_seen_at, last_decision_at)
                     VALUES (?1, ?2, 'seeded', ?3, 'baseline', 'seeded', 1, 1, 1)",
                    rusqlite::params![
                        format!("feed-{}", index % 60),
                        format!("entry-{index}"),
                        b"{}".to_vec()
                    ],
                )
                .unwrap();
        }
        transaction.commit().unwrap();
    }
    assert!(store.duplicate_lookup_uses_index().unwrap());
    let found = store
        .completed_alias(&["00000000000000000000000000000000000004d2".into()])
        .unwrap();
    assert!(found.is_some());
    store.prune(1, 1).unwrap();
    let (completions, listings): (i64, i64) = {
        let connection = rusqlite::Connection::open(&history_path).unwrap();
        (
            connection
                .query_row("SELECT COUNT(*) FROM completions", [], |row| row.get(0))
                .unwrap(),
            connection
                .query_row("SELECT COUNT(*) FROM listings", [], |row| row.get(0))
                .unwrap(),
        )
    };
    assert_eq!(completions, 5_000, "pruning removed completion identities");
    assert!(
        listings <= 60,
        "pruning kept more listing rows than expected"
    );
    std::fs::remove_dir_all(root).ok();
}

#[tokio::test]
async fn unfinished_rss_import_stays_dismissed_until_explicit_retry() {
    let root = unique_dir("rss-dismissal");
    let state_path = root.join("state.json");
    let (base, responses, server) = fixture_server().await;
    let (torrent_bytes, key) = torrent_fixture("rss-dismissed.bin");
    let attachment = format!("{base}/dismissed");
    responses.lock().await.insert(
        "/dismissed.xml".into(),
        FixtureResponse::xml(
            feed(&format!(
                "<item><title>Fixture dismissed</title><guid>d-1</guid><link>{attachment}</link>\
                 <enclosure url=\"{attachment}\" type=\"application/x-bittorrent\"/></item>"
            )),
            "\"d1\"",
        ),
    );
    responses
        .lock()
        .await
        .insert("/dismissed".into(), FixtureResponse::torrent(torrent_bytes));
    let config = rss_config(
        &root,
        &state_path,
        vec![subscription(
            "dismiss-feed",
            format!("{base}/dismissed.xml"),
            RssRuleSet::default(),
            RssImportMode::CurrentMatches,
        )],
    );
    let runtime = start_runtime(config, &state_path);
    runtime.restore_persisted_state().await.unwrap();
    let first = runtime
        .poll_rss_subscription("dismiss-feed", false, false)
        .await
        .unwrap();
    assert_eq!(first.imported, vec![key.to_locator()]);
    assert!(runtime.registry.lock().await.get(&key).is_some());

    // Explicit removal before completion must dismiss this listing, not cause
    // the next unchanged poll to add it again.
    DaemonOps::remove_torrent(&*runtime, &key, false)
        .await
        .unwrap();
    runtime.apply_rss_outbox_once().await.unwrap();
    responses.lock().await.insert(
        "/dismissed.xml".into(),
        FixtureResponse::xml(
            feed(&format!(
                "<item><title>Fixture dismissed</title><guid>d-1</guid><link>{attachment}</link>\
                 <enclosure url=\"{attachment}\" type=\"application/x-bittorrent\"/></item>"
            )),
            "\"d2\"",
        ),
    );
    let after_removal = runtime
        .poll_rss_subscription("dismiss-feed", false, false)
        .await
        .unwrap();
    assert!(after_removal.imported.is_empty());
    assert!(
        after_removal
            .decisions
            .iter()
            .any(|decision| { decision.action == swarmotter_core::rss::RssAction::Dismissed }),
        "{after_removal:?}"
    );
    assert!(runtime.registry.lock().await.get(&key).is_none());

    // Explicit retry clears the dismissal and re-runs intake.
    let retry = runtime
        .retry_rss_listing("dismiss-feed", None)
        .await
        .unwrap();
    assert!(retry.imported.contains(&key.to_locator()), "{retry:?}");
    assert!(runtime.registry.lock().await.get(&key).is_some());
    server.abort();
    std::fs::remove_dir_all(root).ok();
}

#[tokio::test]
async fn feed_http_error_respects_retry_after_and_does_not_establish_baseline() {
    let root = unique_dir("rss-http-backoff");
    let state_path = root.join("state.json");
    let (base, responses, server) = fixture_server().await;
    let mut error_response = FixtureResponse::status(503);
    error_response.retry_after = Some(2);
    responses
        .lock()
        .await
        .insert("/backoff.xml".into(), error_response);
    let config = rss_config(
        &root,
        &state_path,
        vec![subscription(
            "backoff-feed",
            format!("{base}/backoff.xml"),
            RssRuleSet::default(),
            RssImportMode::CurrentMatches,
        )],
    );
    let runtime = start_runtime(config, &state_path);
    runtime.restore_persisted_state().await.unwrap();
    let started = now();
    let failed = runtime
        .poll_rss_subscription("backoff-feed", false, false)
        .await
        .unwrap();
    assert!(!failed.success);
    assert!(failed
        .error
        .as_deref()
        .is_some_and(|message| message.contains("HTTP 503")));
    let store = runtime.rss_history_store().await.unwrap();
    let state = store.feed_state("backoff-feed").unwrap().unwrap();
    assert!(!state.baseline_established);
    assert!(state.next_attempt_at.is_some_and(|at| at >= started + 2));

    // A manual poll remains available, and a later success establishes the
    // baseline. The failed response never marked a listing seen.
    responses.lock().await.insert(
        "/backoff.xml".into(),
        FixtureResponse::xml(
            feed("<item><title>Fixture recovered</title><guid>r</guid><link>https://example.test/r</link></item>"),
            "\"recovered\"",
        ),
    );
    let recovered = runtime
        .poll_rss_subscription("backoff-feed", false, false)
        .await
        .unwrap();
    assert!(recovered.success, "{recovered:?}");
    let state = store.feed_state("backoff-feed").unwrap().unwrap();
    assert!(state.baseline_established);
    assert!(state.last_error.is_none());
    server.abort();
    std::fs::remove_dir_all(root).ok();
}

#[tokio::test]
async fn subscription_controls_create_edit_pause_and_delete() {
    let root = unique_dir("rss-controls");
    let state_path = root.join("state.json");
    let config = rss_config(&root, &state_path, Vec::new());
    let runtime = start_runtime(config, &state_path);
    runtime.restore_persisted_state().await.unwrap();

    let created = subscription(
        "control-feed",
        "http://127.0.0.1:9/feed.xml".into(),
        RssRuleSet::default(),
        RssImportMode::NewOnly,
    );
    let status = DaemonOps::rss_upsert_subscription(&*runtime, created.clone(), None)
        .await
        .unwrap();
    assert_eq!(status.id, "control-feed");
    assert!(status.enabled);
    assert!(!status.baseline_established);
    assert_eq!(runtime.rss_status().await.len(), 1);

    let mut paused = created.clone();
    paused.enabled = false;
    paused.poll_interval_seconds = 3600;
    paused.rules.include = vec![RssFilterRule {
        title: Some("*FLAC*".into()),
        category: Some("Music".into()),
    }];
    let status = DaemonOps::rss_upsert_subscription(&*runtime, paused, Some("control-feed".into()))
        .await
        .unwrap();
    assert!(!status.enabled);
    assert_eq!(status.poll_interval_seconds, 3600);
    assert_eq!(status.rules.include.len(), 1);

    DaemonOps::rss_delete_subscription(&*runtime, "control-feed")
        .await
        .unwrap();
    assert!(runtime.rss_status().await.is_empty());
    assert!(
        DaemonOps::rss_delete_subscription(&*runtime, "control-feed")
            .await
            .is_err()
    );
    std::fs::remove_dir_all(root).ok();
}
