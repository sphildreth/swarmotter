// SPDX-License-Identifier: Apache-2.0

//! Bounded RSS 2.0 intake model, wildcard rules, and durable-history helpers.
//!
//! This module deliberately contains no network or filesystem I/O. The daemon
//! fetches feeds and torrent attachments through the contained HTTP client,
//! then uses these pure functions to explain decisions. Feed parsing is
//! bounded by entry count and input size so untrusted XML cannot allocate
//! without limit.
//!
//! Listing identity is the feed's GUID when present. When a feed omits GUIDs,
//! the documented fallback prefers the entry link, then the first enclosure
//! URL, then `title|pubDate`, and finally `title`. The fallback is stable for
//! an unchanged listing even when a feed reorders entries or omits dates.

use crate::config::StartBehavior;
use crate::error::{CoreError, Result};
use crate::hash::TorrentIdentity;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

/// Suffix appended to the configured state file for the dedicated RSS history
/// database. All subscriptions share this one database.
pub const RSS_HISTORY_SUFFIX: &str = ".rss.sqlite";

/// Default maximum feed document size.
pub const DEFAULT_MAX_FEED_BYTES: usize = 2 * 1024 * 1024;
/// Default maximum attachment/metainfo document size.
pub const DEFAULT_MAX_ATTACHMENT_BYTES: usize = 4 * 1024 * 1024;
/// Default maximum number of items parsed from one feed.
pub const DEFAULT_MAX_ENTRIES: usize = 2_000;
/// Hard upper bounds keep a misconfigured operator from disabling the limits.
pub const MAX_FEED_BYTES_LIMIT: usize = 16 * 1024 * 1024;
pub const MAX_ATTACHMENT_BYTES_LIMIT: usize = 16 * 1024 * 1024;
pub const MAX_ENTRIES_LIMIT: usize = 20_000;

/// Derive the RSS history database path from the active state-file path.
pub fn history_path_for_state(state_path: &Path) -> PathBuf {
    let mut value = state_path.as_os_str().to_os_string();
    value.push(RSS_HISTORY_SUFFIX);
    PathBuf::from(value)
}

/// How a newly created subscription treats listings found on its first poll.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RssImportMode {
    /// Establish a baseline on the first successful poll and import only
    /// entries first observed later.
    #[default]
    NewOnly,
    /// Explicitly import every current rule match on the first successful
    /// poll, then continue with new entries only.
    CurrentMatches,
}

/// Case-insensitive wildcard rule over title and category.
///
/// Every populated field must match. A missing category can never satisfy a
/// category condition. `*` matches any sequence and `?` matches one character.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RssFilterRule {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub category: Option<String>,
}

impl RssFilterRule {
    /// True when the rule has at least one populated field.
    pub fn is_populated(&self) -> bool {
        self.title
            .as_deref()
            .is_some_and(|value| !value.trim().is_empty())
            || self
                .category
                .as_deref()
                .is_some_and(|value| !value.trim().is_empty())
    }

    /// Evaluate this rule against an entry. The returned reason names the
    /// first failing field so preview output can explain exclusions.
    pub fn evaluate(&self, entry: &RssEntry) -> RuleEvaluation {
        let mut matched = true;
        let mut reasons = Vec::new();
        if let Some(pattern) = self
            .title
            .as_deref()
            .filter(|value| !value.trim().is_empty())
        {
            if entry.title.trim().is_empty() {
                matched = false;
                reasons.push(format!("title is missing (rule title {pattern:?})"));
            } else if !wildcard_match(pattern, &entry.title) {
                matched = false;
                reasons.push(format!(
                    "title {:?} does not match {pattern:?}",
                    truncate(&entry.title, 120)
                ));
            }
        }
        if let Some(pattern) = self
            .category
            .as_deref()
            .filter(|value| !value.trim().is_empty())
        {
            if entry.categories.is_empty() {
                matched = false;
                reasons.push(format!("category is missing (rule category {pattern:?})"));
            } else if !entry
                .categories
                .iter()
                .any(|category| wildcard_match(pattern, category))
            {
                matched = false;
                reasons.push(format!(
                    "categories {:?} do not match {pattern:?}",
                    entry
                        .categories
                        .iter()
                        .map(|value| truncate(value, 60))
                        .collect::<Vec<_>>()
                ));
            }
        }
        RuleEvaluation {
            matched,
            reason: if reasons.is_empty() {
                "all populated rule fields matched".to_string()
            } else {
                reasons.join("; ")
            },
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuleEvaluation {
    pub matched: bool,
    pub reason: String,
}

/// The complete inclusive/exclusive rule set for one subscription.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RssRuleSet {
    #[serde(default)]
    pub include: Vec<RssFilterRule>,
    #[serde(default)]
    pub exclude: Vec<RssFilterRule>,
}

impl RssRuleSet {
    /// Exclusions always win. An empty include list accepts every entry.
    pub fn evaluate(&self, entry: &RssEntry) -> RssRuleDecision {
        for (index, rule) in self.exclude.iter().enumerate() {
            if !rule.is_populated() {
                continue;
            }
            let evaluation = rule.evaluate(entry);
            if evaluation.matched {
                return RssRuleDecision {
                    included: false,
                    reason: format!(
                        "excluded by rule {} ({})",
                        index + 1,
                        rule_description(rule)
                    ),
                };
            }
        }
        if self.include.iter().all(|rule| !rule.is_populated()) {
            return RssRuleDecision {
                included: true,
                reason: "no include rules; all entries accepted".to_string(),
            };
        }
        let mut failures = Vec::new();
        for (index, rule) in self.include.iter().enumerate() {
            if !rule.is_populated() {
                continue;
            }
            let evaluation = rule.evaluate(entry);
            if evaluation.matched {
                return RssRuleDecision {
                    included: true,
                    reason: format!(
                        "included by rule {} ({})",
                        index + 1,
                        rule_description(rule)
                    ),
                };
            }
            failures.push(format!("rule {}: {}", index + 1, evaluation.reason));
        }
        RssRuleDecision {
            included: false,
            reason: format!("no include rule matched ({})", failures.join("; ")),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RssRuleDecision {
    pub included: bool,
    pub reason: String,
}

fn rule_description(rule: &RssFilterRule) -> String {
    let mut fields = Vec::new();
    if let Some(title) = rule
        .title
        .as_deref()
        .filter(|value| !value.trim().is_empty())
    {
        fields.push(format!("title {title:?}"));
    }
    if let Some(category) = rule
        .category
        .as_deref()
        .filter(|value| !value.trim().is_empty())
    {
        fields.push(format!("category {category:?}"));
    }
    fields.join(", ")
}

/// A subscription as configured by the operator. Live poll state belongs to
/// the shared RSS history database, not this configuration value.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RssSubscription {
    /// Stable operator-chosen identifier used by the API and history database.
    pub id: String,
    pub name: String,
    /// Feed URL. Credentials in URL user information are rejected; sensitive
    /// query values are redacted from status and logs.
    pub url: String,
    #[serde(default = "default_rss_enabled")]
    pub enabled: bool,
    #[serde(default = "default_rss_interval_seconds")]
    pub poll_interval_seconds: u64,
    #[serde(default)]
    pub profile: Option<String>,
    #[serde(default)]
    pub start_behavior: StartBehavior,
    #[serde(default)]
    pub import_mode: RssImportMode,
    #[serde(default)]
    pub rules: RssRuleSet,
    #[serde(default)]
    pub labels: Vec<String>,
}

fn default_rss_enabled() -> bool {
    true
}

fn default_rss_interval_seconds() -> u64 {
    900
}

impl RssSubscription {
    /// Validate operator input. Intervals are bounded to avoid a busy loop.
    pub fn validate(&self) -> Result<()> {
        if self.id.trim().is_empty() {
            return Err(CoreError::InvalidConfig(
                "rss subscription id must not be empty".into(),
            ));
        }
        if !self
            .id
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.'))
        {
            return Err(CoreError::InvalidConfig(format!(
                "rss subscription id {:?} may contain only ASCII letters, digits, '-', '_', and '.'",
                self.id
            )));
        }
        if self.name.trim().is_empty() {
            return Err(CoreError::InvalidConfig(format!(
                "rss subscription {:?} name must not be empty",
                self.id
            )));
        }
        validate_feed_url(&self.url)?;
        if !(30..=86_400).contains(&self.poll_interval_seconds) {
            return Err(CoreError::InvalidConfig(format!(
                "rss subscription {:?} poll_interval_seconds must be between 30 and 86400",
                self.id
            )));
        }
        validate_rule_set(&self.rules)?;
        Ok(())
    }
}

/// RSS runtime limits. All values are used by feed parsing and polling.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RssConfig {
    /// Optional explicit path for the shared RSS history database. When
    /// unset, the path is `<state file>.rss.sqlite`.
    #[serde(default)]
    pub history_file: Option<String>,
    #[serde(default)]
    pub subscriptions: Vec<RssSubscription>,
    #[serde(default = "default_max_feed_bytes")]
    pub max_feed_bytes: usize,
    #[serde(default = "default_max_attachment_bytes")]
    pub max_attachment_bytes: usize,
    #[serde(default = "default_max_entries")]
    pub max_entries: usize,
    #[serde(default = "default_request_timeout_seconds")]
    pub request_timeout_seconds: u64,
    #[serde(default = "default_max_concurrent_polls")]
    pub max_concurrent_polls: usize,
    #[serde(default = "default_max_retries")]
    pub max_retries: u32,
    #[serde(default = "default_retry_base_seconds")]
    pub retry_base_seconds: u64,
    #[serde(default = "default_retry_max_seconds")]
    pub retry_max_seconds: u64,
    #[serde(default = "default_decision_retention")]
    pub decision_retention: i64,
    #[serde(default = "default_listing_retention")]
    pub listing_retention: i64,
}

fn default_max_feed_bytes() -> usize {
    DEFAULT_MAX_FEED_BYTES
}
fn default_max_attachment_bytes() -> usize {
    DEFAULT_MAX_ATTACHMENT_BYTES
}
fn default_max_entries() -> usize {
    DEFAULT_MAX_ENTRIES
}
fn default_request_timeout_seconds() -> u64 {
    30
}
fn default_max_concurrent_polls() -> usize {
    2
}
fn default_max_retries() -> u32 {
    5
}
fn default_retry_base_seconds() -> u64 {
    60
}
fn default_retry_max_seconds() -> u64 {
    3_600
}
fn default_decision_retention() -> i64 {
    5_000
}
fn default_listing_retention() -> i64 {
    10_000
}

impl Default for RssConfig {
    fn default() -> Self {
        Self {
            history_file: None,
            subscriptions: Vec::new(),
            max_feed_bytes: default_max_feed_bytes(),
            max_attachment_bytes: default_max_attachment_bytes(),
            max_entries: default_max_entries(),
            request_timeout_seconds: default_request_timeout_seconds(),
            max_concurrent_polls: default_max_concurrent_polls(),
            max_retries: default_max_retries(),
            retry_base_seconds: default_retry_base_seconds(),
            retry_max_seconds: default_retry_max_seconds(),
            decision_retention: default_decision_retention(),
            listing_retention: default_listing_retention(),
        }
    }
}

impl RssConfig {
    /// Validate every configured subscription and clamp hard limits.
    pub fn validate(&self) -> Result<()> {
        if self.max_feed_bytes == 0 || self.max_feed_bytes > MAX_FEED_BYTES_LIMIT {
            return Err(CoreError::InvalidConfig(format!(
                "rss.max_feed_bytes must be between 1 and {MAX_FEED_BYTES_LIMIT}"
            )));
        }
        if self.max_attachment_bytes == 0 || self.max_attachment_bytes > MAX_ATTACHMENT_BYTES_LIMIT
        {
            return Err(CoreError::InvalidConfig(format!(
                "rss.max_attachment_bytes must be between 1 and {MAX_ATTACHMENT_BYTES_LIMIT}"
            )));
        }
        if self.max_entries == 0 || self.max_entries > MAX_ENTRIES_LIMIT {
            return Err(CoreError::InvalidConfig(format!(
                "rss.max_entries must be between 1 and {MAX_ENTRIES_LIMIT}"
            )));
        }
        if !(1..=16).contains(&self.max_concurrent_polls) {
            return Err(CoreError::InvalidConfig(
                "rss.max_concurrent_polls must be between 1 and 16".into(),
            ));
        }
        if !(5..=300).contains(&self.request_timeout_seconds) {
            return Err(CoreError::InvalidConfig(
                "rss.request_timeout_seconds must be between 5 and 300".into(),
            ));
        }
        if self.max_retries > 50 {
            return Err(CoreError::InvalidConfig(
                "rss.max_retries must not exceed 50".into(),
            ));
        }
        if self.retry_base_seconds == 0 || self.retry_max_seconds < self.retry_base_seconds {
            return Err(CoreError::InvalidConfig(
                "rss.retry_base_seconds must be positive and no larger than retry_max_seconds"
                    .into(),
            ));
        }
        if !(100..=1_000_000).contains(&self.decision_retention)
            || !(100..=1_000_000).contains(&self.listing_retention)
        {
            return Err(CoreError::InvalidConfig(
                "rss retention limits must be between 100 and 1000000 rows".into(),
            ));
        }
        let mut ids = std::collections::HashSet::new();
        let mut urls = std::collections::HashSet::new();
        for subscription in &self.subscriptions {
            subscription.validate()?;
            if !ids.insert(subscription.id.as_str()) {
                return Err(CoreError::InvalidConfig(format!(
                    "duplicate rss subscription id {:?}",
                    subscription.id
                )));
            }
            if !urls.insert(subscription.url.as_str()) {
                return Err(CoreError::InvalidConfig(format!(
                    "duplicate rss subscription url {:?}",
                    redact_feed_url(&subscription.url)
                )));
            }
        }
        Ok(())
    }

    /// Resolve the history database path from explicit configuration or the
    /// active state file. Returns `None` when neither is available.
    pub fn history_path(&self, state_path: Option<&Path>) -> Result<Option<PathBuf>> {
        if let Some(path) = self.history_file.as_deref() {
            if path.trim().is_empty() {
                return Err(CoreError::InvalidConfig(
                    "rss.history_file must not be blank".into(),
                ));
            }
            return Ok(Some(PathBuf::from(path)));
        }
        Ok(state_path.map(history_path_for_state))
    }

    pub fn subscription(&self, id: &str) -> Option<&RssSubscription> {
        self.subscriptions
            .iter()
            .find(|subscription| subscription.id == id)
    }
}

fn validate_feed_url(url: &str) -> Result<()> {
    let parsed = url::Url::parse(url)
        .map_err(|error| CoreError::InvalidConfig(format!("invalid rss url: {error}")))?;
    if !matches!(parsed.scheme(), "http" | "https") {
        return Err(CoreError::InvalidConfig(
            "rss url must use http or https".into(),
        ));
    }
    if parsed.host().is_none() {
        return Err(CoreError::InvalidConfig("rss url has no host".into()));
    }
    if !parsed.username().is_empty() || parsed.password().is_some() {
        return Err(CoreError::InvalidConfig(
            "rss url must not contain user information; use a token in the query instead".into(),
        ));
    }
    Ok(())
}

fn validate_rule_set(rules: &RssRuleSet) -> Result<()> {
    for (index, rule) in rules.include.iter().chain(rules.exclude.iter()).enumerate() {
        for (field, value) in [("title", &rule.title), ("category", &rule.category)] {
            if let Some(value) = value {
                if value.chars().count() > 256 {
                    return Err(CoreError::InvalidConfig(format!(
                        "rss filter rule {} {field} exceeds 256 characters",
                        index + 1
                    )));
                }
            }
        }
    }
    Ok(())
}

/// Redact credentials and sensitive query values for logs and status output.
pub fn redact_feed_url(raw: &str) -> String {
    let Ok(mut url) = url::Url::parse(raw) else {
        return "<invalid-url>".to_string();
    };
    let _ = url.set_username("");
    let _ = url.set_password(None);
    if url.query().is_some() {
        let pairs = url
            .query_pairs()
            .map(|(name, value)| {
                let sensitive = {
                    let lower = name.to_ascii_lowercase();
                    lower.contains("token")
                        || lower.contains("key")
                        || lower.contains("pass")
                        || lower.contains("auth")
                        || lower.contains("secret")
                        || lower.contains("signature")
                        || lower == "sig"
                };
                (
                    name.into_owned(),
                    if sensitive && !value.is_empty() {
                        "***".to_string()
                    } else {
                        value.into_owned()
                    },
                )
            })
            .collect::<Vec<_>>();
        url.query_pairs_mut().clear().extend_pairs(pairs);
    }
    url.to_string()
}

/// Redact credentials in every HTTP(S) URL found in an error or diagnostic
/// string. Trailing punctuation is preserved so messages remain readable.
pub fn redact_urls_in_text(text: &str) -> String {
    let mut output = String::with_capacity(text.len());
    let mut rest = text;
    while let Some(start) = rest.find("http://").or_else(|| rest.find("https://")) {
        output.push_str(&rest[..start]);
        let tail = &rest[start..];
        let end = tail.find(|c: char| c.is_whitespace()).unwrap_or(tail.len());
        let (url, _suffix) = tail.split_at(end);
        let mut trimmed = url;
        let mut punctuation = String::new();
        while let Some(last) = trimmed.chars().last() {
            if matches!(last, ')' | ']' | '}' | ',' | '.' | ';' | ':') {
                punctuation.insert(0, last);
                trimmed = &trimmed[..trimmed.len() - last.len_utf8()];
            } else {
                break;
            }
        }
        output.push_str(&redact_feed_url(trimmed));
        output.push_str(&punctuation);
        rest = &tail[end..];
    }
    output.push_str(rest);
    output
}

/// A single RSS listing ready for rule evaluation and intake.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RssEntry {
    pub title: String,
    pub link: Option<String>,
    pub guid: Option<String>,
    pub categories: Vec<String>,
    pub pub_date: Option<String>,
    pub enclosures: Vec<RssEnclosure>,
}

impl RssEntry {
    /// Stable listing identity within a feed. See the module documentation.
    pub fn identity(&self) -> String {
        if let Some(guid) = self
            .guid
            .as_deref()
            .map(str::trim)
            .filter(|v| !v.is_empty())
        {
            return format!("guid:{guid}");
        }
        if let Some(link) = self
            .link
            .as_deref()
            .map(str::trim)
            .filter(|v| !v.is_empty())
        {
            return format!("link:{link}");
        }
        if let Some(url) = self
            .enclosures
            .iter()
            .map(|enclosure| enclosure.url.trim())
            .find(|url| !url.is_empty())
        {
            return format!("enclosure:{url}");
        }
        let title = self.title.trim();
        if let Some(date) = self
            .pub_date
            .as_deref()
            .map(str::trim)
            .filter(|v| !v.is_empty())
        {
            return format!("title-date:{title}|{date}");
        }
        format!("title:{title}")
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RssEnclosure {
    pub url: String,
    pub mime_type: Option<String>,
    pub length: Option<u64>,
}

/// A parsed RSS 2.0 document.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RssFeed {
    pub title: String,
    pub entries: Vec<RssEntry>,
}

/// Parse RSS 2.0 with explicit size and entry-count bounds. A document with
/// no recognizable channel or item element is rejected rather than silently
/// treated as an empty successful feed.
pub fn parse_feed(bytes: &[u8], max_bytes: usize, max_entries: usize) -> Result<RssFeed> {
    if bytes.len() > max_bytes {
        return Err(CoreError::InvalidArgument(format!(
            "rss document size {} exceeds limit {max_bytes}",
            bytes.len()
        )));
    }
    let text = std::str::from_utf8(bytes).map_err(|error| {
        CoreError::InvalidArgument(format!("rss document is not UTF-8: {error}"))
    })?;
    parse_feed_str(text, max_entries)
}

/// Parse an already decoded UTF-8 RSS document.
pub fn parse_feed_str(text: &str, max_entries: usize) -> Result<RssFeed> {
    if max_entries == 0 {
        return Err(CoreError::InvalidArgument(
            "rss entry limit must be positive".into(),
        ));
    }
    let mut parser = FeedParser {
        input: text,
        pos: 0,
        max_entries,
        feed: RssFeed::default(),
        current: None,
        capture: None,
        capture_text: String::new(),
        saw_rss: false,
        saw_item: false,
    };
    parser.run()?;
    if !parser.saw_rss {
        return Err(CoreError::InvalidArgument(
            "rss document has no rss/channel element".into(),
        ));
    }
    Ok(parser.feed)
}

struct FeedParser<'a> {
    input: &'a str,
    pos: usize,
    max_entries: usize,
    feed: RssFeed,
    current: Option<RssEntry>,
    capture: Option<String>,
    capture_text: String,
    saw_rss: bool,
    saw_item: bool,
}

impl FeedParser<'_> {
    fn run(&mut self) -> Result<()> {
        while let Some(offset) = self.input[self.pos..].find('<') {
            let text_end = self.pos + offset;
            if text_end > self.pos && self.capture.is_some() {
                let text = decode_entities(&self.input[self.pos..text_end]);
                self.capture_text.push_str(&text);
            }
            self.pos = text_end;
            if self.input[self.pos..].starts_with("<!--") {
                self.skip_until("-->")?;
                continue;
            }
            if self.input[self.pos..].starts_with("<![CDATA[") {
                self.pos += 9;
                let Some(end) = self.input[self.pos..].find("]]>") else {
                    return Err(CoreError::InvalidArgument(
                        "unterminated CDATA section in rss document".into(),
                    ));
                };
                if self.capture.is_some() {
                    self.capture_text
                        .push_str(&self.input[self.pos..self.pos + end]);
                }
                self.pos += end + 3;
                continue;
            }
            if self.input[self.pos..].starts_with("<?") {
                self.skip_until("?>")?;
                continue;
            }
            if self.input[self.pos..].starts_with("<!") {
                // DOCTYPE. Reject an internal subset rather than attempting
                // external entity handling.
                let Some(end) = self.input[self.pos..].find('>') else {
                    return Err(CoreError::InvalidArgument(
                        "unterminated declaration in rss document".into(),
                    ));
                };
                if self.input[self.pos..self.pos + end].contains('[') {
                    return Err(CoreError::InvalidArgument(
                        "rss document internal DTD subset is not supported".into(),
                    ));
                }
                self.pos += end + 1;
                continue;
            }
            self.parse_tag()?;
        }
        if self.capture.is_some() {
            return Err(CoreError::InvalidArgument(
                "unterminated element in rss document".into(),
            ));
        }
        if self.current.is_some() {
            return Err(CoreError::InvalidArgument("unterminated rss item".into()));
        }
        if !self.saw_item {
            return Err(CoreError::InvalidArgument(
                "rss document has no item elements".into(),
            ));
        }
        Ok(())
    }

    fn skip_until(&mut self, marker: &str) -> Result<()> {
        let Some(end) = self.input[self.pos..].find(marker) else {
            return Err(CoreError::InvalidArgument(format!(
                "unterminated construct in rss document; expected {marker}"
            )));
        };
        self.pos += end + marker.len();
        Ok(())
    }

    fn parse_tag(&mut self) -> Result<()> {
        let start = self.pos;
        let Some(end_offset) = find_tag_end(&self.input[start + 1..]) else {
            return Err(CoreError::InvalidArgument(
                "unterminated tag in rss document".into(),
            ));
        };
        let end = start + 1 + end_offset;
        let raw = &self.input[start + 1..end];
        self.pos = end + 1;
        let raw = raw.trim();
        if raw.is_empty() {
            return Ok(());
        }
        let closing = raw.starts_with('/');
        let body = if closing { raw[1..].trim() } else { raw };
        let self_closing = !closing && body.ends_with('/');
        let body = body.trim_end_matches('/').trim();
        let name_end = body.find(|c: char| c.is_whitespace()).unwrap_or(body.len());
        let qualified = &body[..name_end];
        if qualified.is_empty() {
            return Ok(());
        }
        let local = local_name(qualified).to_ascii_lowercase();
        let attributes = if closing {
            Vec::new()
        } else {
            parse_attributes(&body[name_end..])
        };

        if closing {
            self.close_element(&local);
            return Ok(());
        }

        match local.as_str() {
            "rss" | "channel" => {
                self.saw_rss = true;
                return Ok(());
            }
            "item" => {
                if self.current.is_some() {
                    return Err(CoreError::InvalidArgument(
                        "nested rss item is not supported".into(),
                    ));
                }
                if self.feed.entries.len() >= self.max_entries {
                    return Err(CoreError::InvalidArgument(format!(
                        "rss document exceeds the configured limit of {} entries",
                        self.max_entries
                    )));
                }
                self.saw_item = true;
                self.current = Some(RssEntry::default());
                if self_closing {
                    self.close_element("item");
                }
                return Ok(());
            }
            "enclosure" if self.current.is_some() => {
                if let Some(url) = attribute(&attributes, "url").filter(|v| !v.is_empty()) {
                    let length = attribute(&attributes, "length")
                        .and_then(|value| value.trim().parse::<u64>().ok());
                    if let Some(entry) = self.current.as_mut() {
                        entry.enclosures.push(RssEnclosure {
                            url,
                            mime_type: attribute(&attributes, "type")
                                .map(|v| v.trim().to_string())
                                .filter(|v| !v.is_empty()),
                            length,
                        });
                    }
                }
                return Ok(());
            }
            _ => {}
        }

        if (self.current.is_some() || local == "title")
            && matches!(
                local.as_str(),
                "title" | "link" | "guid" | "category" | "pubdate" | "date" | "description"
            )
            && self.capture.is_none()
        {
            if self_closing {
                self.assign_capture(&local, "");
            } else {
                self.capture = Some(local);
                self.capture_text.clear();
            }
        } else if self.capture.is_some() && local == "channel" {
            self.saw_rss = true;
        }
        Ok(())
    }

    fn close_element(&mut self, local: &str) {
        if local == "item" {
            if let Some(entry) = self.current.take() {
                self.feed.entries.push(entry);
            }
            return;
        }
        if self.capture.as_deref() == Some(local) {
            let text = std::mem::take(&mut self.capture_text);
            self.capture = None;
            self.assign_capture(local, text.trim());
        } else if local == "channel" && self.capture.is_some() && self.current.is_none() {
            // A channel title closing while an item capture is open would be
            // malformed; retain the captured value rather than discarding it.
        }
    }

    fn assign_capture(&mut self, local: &str, text: &str) {
        let title_target = if self.current.is_none() {
            Some(&mut self.feed.title)
        } else {
            None
        };
        if local == "title" {
            if let Some(target) = title_target {
                *target = text.to_string();
                return;
            }
        }
        let Some(entry) = self.current.as_mut() else {
            return;
        };
        match local {
            "title" => entry.title = text.to_string(),
            "link" => {
                if !text.is_empty() {
                    entry.link = Some(text.to_string());
                }
            }
            "guid" => {
                if !text.is_empty() {
                    entry.guid = Some(text.to_string());
                }
            }
            "category" => {
                if !text.is_empty() {
                    entry.categories.push(text.to_string());
                }
            }
            "pubdate" | "date" if !text.is_empty() => {
                entry.pub_date = Some(text.to_string());
            }
            _ => {}
        }
    }
}

fn find_tag_end(input: &str) -> Option<usize> {
    let mut quote = None;
    for (index, byte) in input.char_indices() {
        match byte {
            '"' | '\'' if quote.is_none() => quote = Some(byte),
            c if Some(c) == quote => quote = None,
            '>' if quote.is_none() => return Some(index),
            _ => {}
        }
    }
    None
}

fn local_name(qualified: &str) -> &str {
    qualified
        .rsplit_once(':')
        .map(|(_, local)| local)
        .unwrap_or(qualified)
}

fn parse_attributes(input: &str) -> Vec<(String, String)> {
    let mut attributes = Vec::new();
    let mut rest = input.trim();
    while !rest.is_empty() {
        rest = rest.trim_start();
        if rest.is_empty() {
            break;
        }
        let name_end = rest
            .find(|c: char| c.is_whitespace() || c == '=')
            .unwrap_or(rest.len());
        if name_end == 0 {
            break;
        }
        let name = rest[..name_end].to_ascii_lowercase();
        rest = rest[name_end..].trim_start();
        if let Some(after) = rest.strip_prefix('=') {
            rest = after.trim_start();
            let (value, after) =
                if let Some(quote) = rest.chars().next().filter(|c| *c == '"' || *c == '\'') {
                    rest = &rest[quote.len_utf8()..];
                    match rest.find(quote) {
                        Some(end) => (&rest[..end], &rest[end + quote.len_utf8()..]),
                        None => (rest, ""),
                    }
                } else {
                    let end = rest.find(char::is_whitespace).unwrap_or(rest.len());
                    (&rest[..end], &rest[end..])
                };
            attributes.push((name, decode_entities(value)));
            rest = after;
        }
    }
    attributes
}

fn attribute(attributes: &[(String, String)], name: &str) -> Option<String> {
    attributes
        .iter()
        .find(|(key, _)| key == name)
        .map(|(_, value)| value.clone())
}

fn decode_entities(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    let mut rest = input;
    while let Some(index) = rest.find('&') {
        out.push_str(&rest[..index]);
        rest = &rest[index..];
        let Some(end) = rest.find(';') else {
            out.push_str(rest);
            return out;
        };
        let entity = &rest[1..end];
        let decoded = match entity {
            "amp" => Some('&'),
            "lt" => Some('<'),
            "gt" => Some('>'),
            "quot" => Some('"'),
            "apos" => Some('\''),
            value if value.starts_with("#x") || value.starts_with("#X") => {
                u32::from_str_radix(&value[2..], 16)
                    .ok()
                    .and_then(char::from_u32)
            }
            value if value.starts_with('#') => {
                value[1..].parse::<u32>().ok().and_then(char::from_u32)
            }
            _ => None,
        };
        if let Some(character) = decoded {
            out.push(character);
            rest = &rest[end + 1..];
        } else {
            out.push('&');
            rest = &rest[1..];
        }
    }
    out.push_str(rest);
    out
}

/// One actionable intake candidate discovered on a listing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RssOfferKind {
    Magnet,
    Attachment,
    PageOnly,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RssOffer {
    pub kind: RssOfferKind,
    pub url: String,
    pub mime_type: Option<String>,
}

/// Classify an entry's magnet and torrent attachment offers. Page-only links
/// are reported separately so the daemon can explain that no torrent can be
/// obtained without fetching an arbitrary HTML page.
pub fn entry_offers(entry: &RssEntry) -> (Vec<RssOffer>, Vec<RssOffer>) {
    let mut offers = Vec::new();
    let mut page_links = Vec::new();
    if let Some(link) = entry
        .link
        .as_deref()
        .map(str::trim)
        .filter(|v| !v.is_empty())
    {
        if link.starts_with("magnet:?") {
            offers.push(RssOffer {
                kind: RssOfferKind::Magnet,
                url: link.to_string(),
                mime_type: None,
            });
        } else if looks_like_torrent_attachment(link, None) {
            offers.push(RssOffer {
                kind: RssOfferKind::Attachment,
                url: link.to_string(),
                mime_type: None,
            });
        } else {
            page_links.push(RssOffer {
                kind: RssOfferKind::PageOnly,
                url: link.to_string(),
                mime_type: None,
            });
        }
    }
    for enclosure in &entry.enclosures {
        let url = enclosure.url.trim();
        if url.is_empty() {
            continue;
        }
        if url.starts_with("magnet:?") {
            offers.push(RssOffer {
                kind: RssOfferKind::Magnet,
                url: url.to_string(),
                mime_type: enclosure.mime_type.clone(),
            });
        } else if looks_like_torrent_attachment(url, enclosure.mime_type.as_deref()) {
            offers.push(RssOffer {
                kind: RssOfferKind::Attachment,
                url: url.to_string(),
                mime_type: enclosure.mime_type.clone(),
            });
        }
    }
    (offers, page_links)
}

fn looks_like_torrent_attachment(url: &str, mime_type: Option<&str>) -> bool {
    if let Some(mime) = mime_type {
        let mime = mime.to_ascii_lowercase();
        if mime.contains("bittorrent") || mime.contains("x-torrent") {
            return true;
        }
    }
    let Ok(parsed) = url::Url::parse(url) else {
        return false;
    };
    if !matches!(parsed.scheme(), "http" | "https") {
        return false;
    }
    let path = parsed.path().to_ascii_lowercase();
    if path.ends_with(".torrent") {
        return true;
    }
    // Some distributors serve metainfo from suffixless download endpoints.
    // These markers are deliberately narrow; a plain page URL is never
    // fetched as metainfo.
    let lowered = parsed.as_str().to_ascii_lowercase();
    ["torrent", "/download", "action=download", "gettorrent"]
        .iter()
        .any(|marker| lowered.contains(marker))
}

/// Case-insensitive glob match supporting `*` and `?`.
pub fn wildcard_match(pattern: &str, value: &str) -> bool {
    let pattern: Vec<char> = pattern.to_lowercase().chars().collect();
    let value: Vec<char> = value.to_lowercase().chars().collect();
    let (mut p, mut v) = (0usize, 0usize);
    let mut star: Option<(usize, usize)> = None;
    while v < value.len() {
        if p < pattern.len() && (pattern[p] == '?' || pattern[p] == value[v]) {
            p += 1;
            v += 1;
        } else if p < pattern.len() && pattern[p] == '*' {
            star = Some((p, v));
            p += 1;
        } else if let Some((star_p, star_v)) = star {
            p = star_p + 1;
            v = star_v + 1;
            star = Some((star_p, star_v + 1));
        } else {
            return false;
        }
    }
    while p < pattern.len() && pattern[p] == '*' {
        p += 1;
    }
    p == pattern.len()
}

fn truncate(value: &str, limit: usize) -> String {
    if value.chars().count() <= limit {
        return value.to_string();
    }
    let mut out: String = value.chars().take(limit.saturating_sub(1)).collect();
    out.push('…');
    out
}

/// A library change captured in the main SQLite transaction for idempotent
/// application to the shared RSS history database.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RssLibraryEventKind {
    Completed,
    Removed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RssLibraryEvent {
    pub kind: RssLibraryEventKind,
    /// Canonical durable locator (40-char v1/hybrid primary or 64-char v2).
    pub canonical_key: String,
    /// Every full identity alias known for the torrent.
    pub aliases: Vec<String>,
    pub name: String,
    /// Whether verified completion existed when the event was captured.
    pub was_completed: bool,
    pub occurred_at: u64,
}

impl RssLibraryEvent {
    pub fn completion(
        canonical_key: String,
        aliases: Vec<String>,
        name: String,
        occurred_at: u64,
    ) -> Self {
        Self {
            kind: RssLibraryEventKind::Completed,
            canonical_key,
            aliases,
            name,
            was_completed: true,
            occurred_at,
        }
    }

    pub fn removal(
        canonical_key: String,
        aliases: Vec<String>,
        name: String,
        was_completed: bool,
        occurred_at: u64,
    ) -> Self {
        Self {
            kind: RssLibraryEventKind::Removed,
            canonical_key,
            aliases,
            name,
            was_completed,
            occurred_at,
        }
    }

    /// Every identity a duplicate check must consider.
    pub fn identities(&self) -> Vec<&str> {
        let mut values = vec![self.canonical_key.as_str()];
        for alias in &self.aliases {
            if !values.contains(&alias.as_str()) {
                values.push(alias);
            }
        }
        values
    }
}

/// Decision state exposed by preview, polling, and paginated history.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RssAction {
    /// Entry matched and would be/was imported by a fresh intake.
    Import,
    /// The torrent is already in the live library with its existing state.
    AlreadyPresent,
    /// A verified completion exists for the same torrent identity.
    PreviouslyCompleted,
    /// A rule excluded the entry.
    Filtered,
    /// The operator removed an unfinished RSS import; explicit retry needed.
    Dismissed,
    /// No torrent identity could be obtained (page-only or malformed).
    Unsupported,
    /// A transient failure remains eligible for retry.
    Failed,
    /// Automatic intake deferred because history could not be checked.
    Deferred,
    /// Seen during the baseline poll and deliberately not imported.
    Baseline,
}

impl RssAction {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Import => "import",
            Self::AlreadyPresent => "already_present",
            Self::PreviouslyCompleted => "previously_completed",
            Self::Filtered => "filtered",
            Self::Dismissed => "dismissed",
            Self::Unsupported => "unsupported",
            Self::Failed => "failed",
            Self::Deferred => "deferred",
            Self::Baseline => "baseline",
        }
    }

    pub fn is_terminal_success(self) -> bool {
        matches!(
            self,
            Self::Import | Self::AlreadyPresent | Self::PreviouslyCompleted
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RssDecision {
    pub entry_key: String,
    pub title: String,
    pub link: Option<String>,
    pub categories: Vec<String>,
    pub pub_date: Option<String>,
    pub action: RssAction,
    pub reason: String,
    pub torrent_keys: Vec<String>,
    /// Paginated decision-history row id when this came from storage.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub decision_id: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub decided_at: Option<u64>,
}

/// Counts exposed per subscription.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RssCounts {
    pub listings: u64,
    pub imported: u64,
    pub completed: u64,
    pub filtered: u64,
    pub dismissed: u64,
    pub unsupported: u64,
    pub failed: u64,
    pub pending_retry: u64,
    pub baseline: u64,
    pub already_present: u64,
    pub previously_completed: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RssFeedState {
    pub subscription_id: String,
    pub etag: Option<String>,
    pub last_modified: Option<String>,
    pub last_poll_at: Option<u64>,
    pub last_success_at: Option<u64>,
    pub next_attempt_at: Option<u64>,
    pub last_error: Option<String>,
    pub consecutive_failures: u32,
    pub baseline_established: bool,
}

impl RssFeedState {
    pub fn new(subscription_id: impl Into<String>) -> Self {
        Self {
            subscription_id: subscription_id.into(),
            etag: None,
            last_modified: None,
            last_poll_at: None,
            last_success_at: None,
            next_attempt_at: None,
            last_error: None,
            consecutive_failures: 0,
            baseline_established: false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RssSubscriptionStatus {
    pub id: String,
    pub name: String,
    pub url: String,
    pub enabled: bool,
    pub poll_interval_seconds: u64,
    pub profile: Option<String>,
    pub start_behavior: StartBehavior,
    pub import_mode: RssImportMode,
    pub rules: RssRuleSet,
    pub labels: Vec<String>,
    pub last_poll_at: Option<u64>,
    pub last_success_at: Option<u64>,
    pub next_attempt_at: Option<u64>,
    pub latest_error: Option<String>,
    pub consecutive_failures: u32,
    pub baseline_established: bool,
    pub counts: RssCounts,
    pub poll_in_progress: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RssPreview {
    pub subscription_id: String,
    pub feed_title: String,
    pub fetched_at: u64,
    pub conditional_not_modified: bool,
    pub decisions: Vec<RssDecision>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RssPollResult {
    pub subscription_id: String,
    pub success: bool,
    pub baseline_established: bool,
    pub conditional_not_modified: bool,
    pub decisions: Vec<RssDecision>,
    pub imported: Vec<String>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RssDecisionPage {
    pub decisions: Vec<RssDecision>,
    pub next_before: Option<i64>,
}

/// Backoff in seconds after `failures` consecutive transient failures.
pub fn retry_backoff_seconds(
    failures: u32,
    base_seconds: u64,
    max_seconds: u64,
    max_retries: u32,
) -> Option<u64> {
    if failures > max_retries {
        return None;
    }
    let exponent = failures.saturating_sub(1).min(31);
    Some(
        base_seconds
            .saturating_mul(1u64 << exponent)
            .min(max_seconds),
    )
}

/// Convert a torrent identity into all full durable aliases.
pub fn torrent_identity_aliases(identity: &TorrentIdentity) -> Vec<String> {
    identity
        .keys()
        .into_iter()
        .map(|key| key.to_locator())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE: &str = r#"<?xml version="1.0"?>
    <rss version="2.0"><channel>
      <title>Fixture feed</title>
      <item>
        <title>Studio Album FLAC</title>
        <link>https://example.test/release/1</link>
        <guid isPermaLink="false">release-1</guid>
        <category>Music/Lossless</category>
        <pubDate>Mon, 01 Jan 2024 00:00:00 GMT</pubDate>
        <enclosure url="https://example.test/release/1.torrent" type="application/x-bittorrent" length="1234"/>
      </item>
      <item>
        <title>Live Album FLAC</title>
        <link>https://example.test/release/2</link>
        <guid isPermaLink="false">release-2</guid>
        <category>Music/Live</category>
      </item>
      <item>
        <title>Magnet Release</title>
        <guid isPermaLink="false">release-3</guid>
        <link>magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567&amp;dn=x</link>
      </item>
      <item>
        <title>Page only</title>
        <link>https://example.test/details/4</link>
        <guid isPermaLink="false">release-4</guid>
      </item>
    </channel></rss>"#;

    #[test]
    fn parses_bounded_rss_with_enclosures_and_magnets() {
        let feed = parse_feed(SAMPLE.as_bytes(), 1024 * 1024, 100).unwrap();
        assert_eq!(feed.title, "Fixture feed");
        assert_eq!(feed.entries.len(), 4);
        assert_eq!(feed.entries[0].identity(), "guid:release-1");
        assert_eq!(feed.entries[0].categories, vec!["Music/Lossless"]);
        assert_eq!(
            feed.entries[0].enclosures[0].mime_type.as_deref(),
            Some("application/x-bittorrent")
        );
        assert!(feed.entries[2]
            .link
            .as_deref()
            .unwrap()
            .starts_with("magnet:?xt=urn:btih:"));
        let (offers, pages) = entry_offers(&feed.entries[0]);
        assert_eq!(offers.len(), 1);
        assert_eq!(offers[0].kind, RssOfferKind::Attachment);
        // The human-facing detail page is retained as a separate page link;
        // it does not prevent the torrent attachment from being imported.
        assert_eq!(pages.len(), 1);
        let (offers, pages) = entry_offers(&feed.entries[2]);
        assert_eq!(offers[0].kind, RssOfferKind::Magnet);
        assert!(pages.is_empty());
        let (offers, pages) = entry_offers(&feed.entries[3]);
        assert!(offers.is_empty());
        assert_eq!(pages[0].kind, RssOfferKind::PageOnly);
    }

    #[test]
    fn missing_guid_uses_documented_fallbacks() {
        let mut entry = RssEntry {
            title: "A".into(),
            ..Default::default()
        };
        assert_eq!(entry.identity(), "title:A");
        entry.pub_date = Some("Mon".into());
        assert_eq!(entry.identity(), "title-date:A|Mon");
        entry.enclosures.push(RssEnclosure {
            url: "https://example.test/x.torrent".into(),
            ..Default::default()
        });
        assert_eq!(entry.identity(), "enclosure:https://example.test/x.torrent");
        entry.link = Some("https://example.test/details".into());
        assert_eq!(entry.identity(), "link:https://example.test/details");
        entry.guid = Some(" fixed ".into());
        assert_eq!(entry.identity(), "guid:fixed");
    }

    #[test]
    fn reordered_entries_keep_identity_even_without_dates() {
        let first = parse_feed_str(SAMPLE, 100).unwrap();
        let identities: Vec<_> = first.entries.iter().map(RssEntry::identity).collect();
        assert_eq!(
            identities,
            vec![
                "guid:release-1",
                "guid:release-2",
                "guid:release-3",
                "guid:release-4"
            ]
        );
        // A feed that reorders entries without dates still produces the same
        // per-entry identities; baseline/duplicate checks never depend on
        // document order.
        let mut reordered = first.clone();
        reordered.entries.reverse();
        let reordered_identities: Vec<_> =
            reordered.entries.iter().map(RssEntry::identity).collect();
        assert_eq!(
            reordered_identities,
            vec![
                "guid:release-4",
                "guid:release-3",
                "guid:release-2",
                "guid:release-1"
            ]
        );
    }

    #[test]
    fn wildcard_rules_are_case_insensitive_and_missing_category_fails() {
        let entry = RssEntry {
            title: "Studio Album FLAC".into(),
            categories: vec!["Music/Lossless".into()],
            ..Default::default()
        };
        let rules = RssRuleSet {
            include: vec![RssFilterRule {
                title: Some("*flac*".into()),
                category: None,
            }],
            exclude: vec![RssFilterRule {
                title: Some("*live*".into()),
                category: None,
            }],
        };
        assert!(rules.evaluate(&entry).included);
        let live = RssEntry {
            title: "LIVE Album FLAC".into(),
            ..entry.clone()
        };
        let decision = rules.evaluate(&live);
        assert!(!decision.included);
        assert!(decision.reason.contains("excluded by rule 1"));
        let category_rule = RssRuleSet {
            include: vec![RssFilterRule {
                title: None,
                category: Some("*lossless*".into()),
            }],
            exclude: Vec::new(),
        };
        assert!(category_rule.evaluate(&entry).included);
        let missing = RssEntry {
            title: "No category".into(),
            ..Default::default()
        };
        assert!(!category_rule.evaluate(&missing).included);
    }

    #[test]
    fn empty_include_accepts_all_and_exclusions_win() {
        let entry = RssEntry {
            title: "Anything".into(),
            ..Default::default()
        };
        let rules = RssRuleSet {
            include: Vec::new(),
            exclude: vec![RssFilterRule {
                title: Some("*any*".into()),
                category: None,
            }],
        };
        assert!(!rules.evaluate(&entry).included);
        let rules = RssRuleSet::default();
        assert!(rules.evaluate(&entry).included);
    }

    #[test]
    fn wildcard_edge_cases() {
        assert!(wildcard_match("*", ""));
        assert!(wildcard_match("a*b?c", "axxbzc"));
        assert!(!wildcard_match("a*b?c", "axxbc"));
        assert!(wildcard_match("FLAC", "flac"));
        assert!(!wildcard_match("flac", "flac "));
    }

    #[test]
    fn feed_limits_and_malformed_documents_fail() {
        assert!(parse_feed(SAMPLE.as_bytes(), 10, 100).is_err());
        assert!(parse_feed_str(SAMPLE, 1).is_err());
        assert!(parse_feed_str("<html>no feed</html>", 100).is_err());
        assert!(parse_feed_str("<rss><channel></channel></rss>", 100).is_err());
        assert!(parse_feed_str("<rss><channel><item><title>x</title>", 100).is_err());
        assert!(parse_feed_str("<rss><channel><item/><item/><item/></channel></rss>", 2).is_err());
    }

    #[test]
    fn cdata_and_entities_are_decoded() {
        let doc = "<rss><channel><title>Feed &amp; More</title><item><title><![CDATA[A < B]]></title><guid>g1</guid></item></channel></rss>";
        let feed = parse_feed_str(doc, 10).unwrap();
        assert_eq!(feed.title, "Feed & More");
        assert_eq!(feed.entries[0].title, "A < B");
    }

    #[test]
    fn redacts_url_credentials_and_sensitive_query_values() {
        let redacted = redact_feed_url(
            "https://user:pass@example.test/feed?token=secret&category=music&api_key=abc",
        );
        assert!(!redacted.contains("user"));
        assert!(!redacted.contains("secret"));
        assert!(!redacted.contains("abc"));
        assert!(redacted.contains("category=music"));
        assert!(redacted.contains("token=***"));
    }

    #[test]
    fn error_text_url_redaction_keeps_messages_readable() {
        let text = "feed https://example.test/f.xml?token=secret failed: HTTP 500; see (https://example.test/x?api_key=abc).";
        let redacted = redact_urls_in_text(text);
        assert!(!redacted.contains("secret"));
        assert!(!redacted.contains("abc"));
        assert!(redacted.contains("token=***"));
        assert!(redacted.contains("HTTP 500"));
    }

    #[test]
    fn retry_backoff_is_bounded_and_stops_after_max_retries() {
        assert_eq!(retry_backoff_seconds(1, 60, 3600, 5), Some(60));
        assert_eq!(retry_backoff_seconds(3, 60, 3600, 5), Some(240));
        assert_eq!(retry_backoff_seconds(6, 60, 3600, 5), None);
        assert_eq!(retry_backoff_seconds(10, 60, 600, 50), Some(600));
    }

    #[test]
    fn hybrid_identity_aliases_include_full_v1_and_v2_locators() {
        use crate::hash::{InfoHash, TorrentIdentity, V2InfoHash};
        let identity = TorrentIdentity::hybrid(
            InfoHash::from_bytes([0x11; 20]),
            V2InfoHash::from_bytes([0x22; 32]),
        );
        let aliases = torrent_identity_aliases(&identity);
        assert_eq!(aliases.len(), 2);
        assert_eq!(aliases[0].len(), 40);
        assert_eq!(aliases[1].len(), 64);
        assert_eq!(aliases[0], "11".repeat(20));
        assert_eq!(aliases[1], "22".repeat(32));
    }

    #[test]
    fn history_path_is_adjacent_to_state_file() {
        assert_eq!(
            history_path_for_state(Path::new("/data/library")),
            PathBuf::from("/data/library.rss.sqlite")
        );
    }
}
