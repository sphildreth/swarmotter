// SPDX-License-Identifier: Apache-2.0

//! Peer-admission filtering for abuse mitigation and operational safety.
//!
//! This policy is intentionally separate from [`crate::net::NetworkBinder`].
//! Callers still use contained sockets; the policy only decides whether a
//! remote IP address or peer-id may be admitted at a contained peer boundary.

use crate::error::{CoreError, Result};
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::fs;
use std::net::IpAddr;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::Arc;

/// Imports are local-only and bounded so they cannot unexpectedly consume an
/// unbounded amount of daemon memory during startup or configuration updates.
pub const MAX_BLOCKLIST_FILE_BYTES: u64 = 32 * 1024 * 1024;
pub const MAX_PEER_FILTER_RULES: usize = 250_000;
const MAX_BLOCKLIST_LINE_BYTES: usize = 4 * 1024;

/// Persisted global peer-admission configuration.
///
/// It is explicitly disabled and empty by default. When enabled, every rule
/// source applies globally to tracker, DHT, PEX, direct, metadata, and inbound
/// peer admission. It never changes network-containment routing.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PeerFilterConfig {
    #[serde(default)]
    pub enabled: bool,
    /// CIDR, inclusive IP range, or single-IP rules.
    #[serde(default)]
    pub rules: Vec<String>,
    /// Local eMule/PeerGuardian-style source files. URLs are deliberately not
    /// supported: obtaining a source is an explicit operator action.
    #[serde(default)]
    pub blocklist_paths: Vec<String>,
    /// Explicit global IP bans added by an operator.
    #[serde(default)]
    pub manual_bans: Vec<ManualPeerBan>,
    /// Printable peer-id prefixes to reject after a BitTorrent handshake.
    #[serde(default)]
    pub blocked_client_ids: Vec<String>,
}

impl PeerFilterConfig {
    /// Compile all bounded local sources during config validation. This keeps
    /// a requested policy from becoming silently partial at startup or on a
    /// live settings replacement.
    pub fn validate(&self) -> Result<()> {
        PeerFilter::from_config(self).map(|_| ())
    }
}

/// An operator-created global peer ban.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManualPeerBan {
    pub ip: String,
    #[serde(default)]
    pub reason: Option<String>,
}

impl ManualPeerBan {
    fn parse(&self) -> Result<IpAddr> {
        let ip = self.ip.trim().parse::<IpAddr>().map_err(|error| {
            CoreError::InvalidConfig(format!("peer_filter.manual_bans.ip '{}': {error}", self.ip))
        })?;
        if self
            .reason
            .as_deref()
            .is_some_and(|reason| reason.trim().is_empty() || reason.len() > 240)
        {
            return Err(CoreError::InvalidConfig(
                "peer_filter.manual_bans.reason must be 1 to 240 characters when set".into(),
            ));
        }
        Ok(ip)
    }
}

/// An IP selector accepted in configuration or a local import.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PeerIpRule {
    Single(IpAddr),
    Cidr { network: IpAddr, prefix: u8 },
    Range { start: IpAddr, end: IpAddr },
}

impl PeerIpRule {
    /// Parse a single IP, CIDR, or inclusive `start-end` IP range.
    pub fn parse(value: &str) -> Result<Self> {
        let value = value.trim();
        if value.is_empty() {
            return Err(CoreError::InvalidConfig(
                "peer filter rule must not be empty".into(),
            ));
        }
        if let Some((start, end)) = value.split_once('-') {
            if end.contains('-') {
                return Err(CoreError::InvalidConfig(format!(
                    "peer filter range '{value}' contains more than one '-'"
                )));
            }
            let start = parse_rule_ip(start, value)?;
            let end = parse_rule_ip(end, value)?;
            if !same_family(start, end) {
                return Err(CoreError::InvalidConfig(format!(
                    "peer filter range '{value}' mixes IPv4 and IPv6"
                )));
            }
            if compare_ip(start, end) == Ordering::Greater {
                return Err(CoreError::InvalidConfig(format!(
                    "peer filter range '{value}' has an end before its start"
                )));
            }
            return Ok(Self::Range { start, end });
        }
        if let Some((network, prefix)) = value.split_once('/') {
            if prefix.contains('/') {
                return Err(CoreError::InvalidConfig(format!(
                    "peer filter CIDR '{value}' contains more than one '/'"
                )));
            }
            let network = parse_rule_ip(network, value)?;
            let prefix = prefix.trim().parse::<u8>().map_err(|error| {
                CoreError::InvalidConfig(format!("peer filter CIDR '{value}' prefix: {error}"))
            })?;
            let max = match network {
                IpAddr::V4(_) => 32,
                IpAddr::V6(_) => 128,
            };
            if prefix > max {
                return Err(CoreError::InvalidConfig(format!(
                    "peer filter CIDR '{value}' prefix must be at most {max}"
                )));
            }
            return Ok(Self::Cidr { network, prefix });
        }
        Ok(Self::Single(parse_rule_ip(value, value)?))
    }

    pub fn matches(&self, ip: IpAddr) -> bool {
        match self {
            Self::Single(expected) => *expected == ip,
            Self::Range { start, end } => {
                same_family(*start, ip)
                    && compare_ip(*start, ip) != Ordering::Greater
                    && compare_ip(ip, *end) != Ordering::Greater
            }
            Self::Cidr { network, prefix } => cidr_matches(*network, *prefix, ip),
        }
    }
}

fn parse_rule_ip(value: &str, rule: &str) -> Result<IpAddr> {
    value.trim().parse::<IpAddr>().map_err(|error| {
        CoreError::InvalidConfig(format!("peer filter rule '{rule}' has invalid IP: {error}"))
    })
}

fn same_family(left: IpAddr, right: IpAddr) -> bool {
    matches!(
        (left, right),
        (IpAddr::V4(_), IpAddr::V4(_)) | (IpAddr::V6(_), IpAddr::V6(_))
    )
}

fn compare_ip(left: IpAddr, right: IpAddr) -> Ordering {
    match (left, right) {
        (IpAddr::V4(left), IpAddr::V4(right)) => left.octets().cmp(&right.octets()),
        (IpAddr::V6(left), IpAddr::V6(right)) => left.octets().cmp(&right.octets()),
        (IpAddr::V4(_), IpAddr::V6(_)) => Ordering::Less,
        (IpAddr::V6(_), IpAddr::V4(_)) => Ordering::Greater,
    }
}

fn cidr_matches(network: IpAddr, prefix: u8, candidate: IpAddr) -> bool {
    match (network, candidate) {
        (IpAddr::V4(network), IpAddr::V4(candidate)) => {
            bytes_prefix_match(&network.octets(), &candidate.octets(), prefix)
        }
        (IpAddr::V6(network), IpAddr::V6(candidate)) => {
            bytes_prefix_match(&network.octets(), &candidate.octets(), prefix)
        }
        _ => false,
    }
}

fn bytes_prefix_match(network: &[u8], candidate: &[u8], prefix: u8) -> bool {
    let whole_bytes = usize::from(prefix / 8);
    let remaining_bits = prefix % 8;
    if network[..whole_bytes] != candidate[..whole_bytes] {
        return false;
    }
    if remaining_bits == 0 {
        return true;
    }
    let mask = u8::MAX << (8 - remaining_bits);
    network
        .get(whole_bytes)
        .zip(candidate.get(whole_bytes))
        .is_some_and(|(left, right)| (left & mask) == (right & mask))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RuleOrigin {
    Configured,
    Imported,
}

#[derive(Debug, Clone)]
struct CompiledRule {
    rule: PeerIpRule,
    display: String,
    origin: RuleOrigin,
}

#[derive(Debug, Clone)]
struct CompiledManualBan {
    configured: ManualPeerBan,
    ip: IpAddr,
}

/// Why an otherwise contained peer was accepted or rejected.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PeerAdmissionDecision {
    Allowed,
    Disabled,
    BlockedByManualBan { reason: Option<String> },
    BlockedByConfiguredRule { rule: String },
    BlockedByImportedRule { rule: String },
    BlockedByClientId { prefix: String },
    FailClosed { detail: String },
}

impl PeerAdmissionDecision {
    pub fn is_allowed(&self) -> bool {
        matches!(self, Self::Allowed | Self::Disabled)
    }

    pub fn audit_reason(&self) -> &'static str {
        match self {
            Self::Allowed => "allowed",
            Self::Disabled => "filter_disabled",
            Self::BlockedByManualBan { .. } => "manual_ban",
            Self::BlockedByConfiguredRule { .. } => "configured_rule",
            Self::BlockedByImportedRule { .. } => "imported_rule",
            Self::BlockedByClientId { .. } => "client_id",
            Self::FailClosed { .. } => "filter_load_failure",
        }
    }

    pub fn rejection_message(&self) -> Option<String> {
        match self {
            Self::Allowed | Self::Disabled => None,
            Self::BlockedByManualBan { reason } => Some(match reason {
                Some(reason) => format!("peer rejected by manual ban: {reason}"),
                None => "peer rejected by manual ban".into(),
            }),
            Self::BlockedByConfiguredRule { rule } => {
                Some(format!("peer rejected by configured rule {rule}"))
            }
            Self::BlockedByImportedRule { rule } => {
                Some(format!("peer rejected by imported blocklist rule {rule}"))
            }
            Self::BlockedByClientId { prefix } => {
                Some(format!("peer rejected by client-id prefix {prefix}"))
            }
            Self::FailClosed { detail } => Some(format!(
                "peer filter is fail-closed after load failure: {detail}"
            )),
        }
    }
}

#[derive(Default)]
struct PeerFilterCounters {
    ip_checks: AtomicU64,
    client_id_checks: AtomicU64,
    manual_bans: AtomicU64,
    configured_rules: AtomicU64,
    imported_rules: AtomicU64,
    client_ids: AtomicU64,
    fail_closed: AtomicU64,
}

/// Cumulative admission counters exposed to operators.
#[derive(Debug, Clone, Default, Serialize)]
pub struct PeerFilterRejectionCounts {
    pub ip_checks: u64,
    pub client_id_checks: u64,
    pub manual_bans: u64,
    pub configured_rules: u64,
    pub imported_rules: u64,
    pub client_ids: u64,
    pub fail_closed: u64,
}

/// One locally imported blocklist source and its parse outcome.
#[derive(Debug, Clone, Serialize)]
pub struct BlocklistSourceStatus {
    pub path: String,
    pub rules_loaded: usize,
    pub skipped_lines: usize,
}

/// API-safe effective admission policy status.
#[derive(Debug, Clone, Serialize)]
pub struct PeerFilterStatus {
    pub enabled: bool,
    /// Trimmed configured IP/CIDR/range rules, excluding imported rows.
    /// This lets operators audit the active direct-rule policy without
    /// reconstructing it from a separate configuration snapshot.
    pub rules: Vec<String>,
    /// Configured local import paths. Per-source parse outcomes are in
    /// [`Self::sources`].
    pub blocklist_paths: Vec<String>,
    pub configured_rule_count: usize,
    pub imported_rule_count: usize,
    pub manual_bans: Vec<ManualPeerBan>,
    pub blocked_client_ids: Vec<String>,
    pub sources: Vec<BlocklistSourceStatus>,
    pub rejections: PeerFilterRejectionCounts,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fail_closed_detail: Option<String>,
}

/// Immutable compiled policy. Clones retain one shared counter set, so all
/// engines and inbound sessions for a config generation share audit status.
#[derive(Clone)]
pub struct PeerFilter {
    enabled: bool,
    rules: Arc<Vec<CompiledRule>>,
    manual_bans: Arc<Vec<CompiledManualBan>>,
    blocked_client_ids: Arc<Vec<String>>,
    sources: Arc<Vec<BlocklistSourceStatus>>,
    counters: Arc<PeerFilterCounters>,
    fail_closed_detail: Option<Arc<str>>,
}

impl Default for PeerFilter {
    fn default() -> Self {
        Self {
            enabled: false,
            rules: Arc::new(Vec::new()),
            manual_bans: Arc::new(Vec::new()),
            blocked_client_ids: Arc::new(Vec::new()),
            sources: Arc::new(Vec::new()),
            counters: Arc::new(PeerFilterCounters::default()),
            fail_closed_detail: None,
        }
    }
}

impl PeerFilter {
    pub fn from_config(config: &PeerFilterConfig) -> Result<Self> {
        if config
            .rules
            .len()
            .saturating_add(config.manual_bans.len())
            .saturating_add(config.blocked_client_ids.len())
            > MAX_PEER_FILTER_RULES
        {
            return Err(CoreError::InvalidConfig(format!(
                "peer_filter contains more than {MAX_PEER_FILTER_RULES} configured entries"
            )));
        }
        let mut rules = Vec::with_capacity(config.rules.len());
        for configured in &config.rules {
            rules.push(CompiledRule {
                rule: PeerIpRule::parse(configured)?,
                display: configured.trim().to_string(),
                origin: RuleOrigin::Configured,
            });
        }
        let mut sources = Vec::with_capacity(config.blocklist_paths.len());
        for configured_path in &config.blocklist_paths {
            let path = configured_path.trim();
            if path.is_empty() {
                return Err(CoreError::InvalidConfig(
                    "peer_filter.blocklist_paths entries must not be empty".into(),
                ));
            }
            let imported = load_blocklist(Path::new(path))?;
            if rules.len().saturating_add(imported.rules.len()) > MAX_PEER_FILTER_RULES {
                return Err(CoreError::InvalidConfig(format!(
                    "peer_filter imports more than {MAX_PEER_FILTER_RULES} rules"
                )));
            }
            rules.extend(
                imported
                    .rules
                    .into_iter()
                    .map(|(rule, display)| CompiledRule {
                        rule,
                        display,
                        origin: RuleOrigin::Imported,
                    }),
            );
            sources.push(BlocklistSourceStatus {
                path: path.to_string(),
                rules_loaded: imported.rule_count,
                skipped_lines: imported.skipped_lines,
            });
        }
        let mut manual_bans = Vec::with_capacity(config.manual_bans.len());
        for ban in &config.manual_bans {
            let ip = ban.parse()?;
            manual_bans.push(CompiledManualBan {
                configured: ManualPeerBan {
                    ip: ip.to_string(),
                    reason: ban.reason.as_ref().map(|reason| reason.trim().to_string()),
                },
                ip,
            });
        }
        let mut blocked_client_ids = Vec::with_capacity(config.blocked_client_ids.len());
        for prefix in &config.blocked_client_ids {
            let prefix = prefix.trim();
            if prefix.is_empty()
                || prefix.len() > 20
                || !prefix.bytes().all(|byte| byte.is_ascii_graphic())
            {
                return Err(CoreError::InvalidConfig(
                    "peer_filter.blocked_client_ids entries must be 1 to 20 printable ASCII characters"
                        .into(),
                ));
            }
            blocked_client_ids.push(prefix.to_string());
        }
        Ok(Self {
            enabled: config.enabled,
            rules: Arc::new(rules),
            manual_bans: Arc::new(manual_bans),
            blocked_client_ids: Arc::new(blocked_client_ids),
            sources: Arc::new(sources),
            counters: Arc::new(PeerFilterCounters::default()),
            fail_closed_detail: None,
        })
    }

    /// Safe fallback for a source that disappears after a successful config
    /// validation. It denies peers rather than degrading to allow-all.
    pub fn fail_closed(detail: impl Into<String>) -> Self {
        Self {
            enabled: true,
            fail_closed_detail: Some(Arc::from(detail.into())),
            ..Self::default()
        }
    }

    pub fn admit_ip(&self, ip: IpAddr) -> PeerAdmissionDecision {
        self.counters
            .ip_checks
            .fetch_add(1, AtomicOrdering::Relaxed);
        if let Some(detail) = &self.fail_closed_detail {
            self.counters
                .fail_closed
                .fetch_add(1, AtomicOrdering::Relaxed);
            return PeerAdmissionDecision::FailClosed {
                detail: detail.to_string(),
            };
        }
        if !self.enabled {
            return PeerAdmissionDecision::Disabled;
        }
        if let Some(ban) = self.manual_bans.iter().find(|ban| ban.ip == ip) {
            self.counters
                .manual_bans
                .fetch_add(1, AtomicOrdering::Relaxed);
            return PeerAdmissionDecision::BlockedByManualBan {
                reason: ban.configured.reason.clone(),
            };
        }
        if let Some(rule) = self.rules.iter().find(|rule| rule.rule.matches(ip)) {
            return match rule.origin {
                RuleOrigin::Configured => {
                    self.counters
                        .configured_rules
                        .fetch_add(1, AtomicOrdering::Relaxed);
                    PeerAdmissionDecision::BlockedByConfiguredRule {
                        rule: rule.display.clone(),
                    }
                }
                RuleOrigin::Imported => {
                    self.counters
                        .imported_rules
                        .fetch_add(1, AtomicOrdering::Relaxed);
                    PeerAdmissionDecision::BlockedByImportedRule {
                        rule: rule.display.clone(),
                    }
                }
            };
        }
        PeerAdmissionDecision::Allowed
    }

    pub fn admit_client_id(&self, peer_id: &[u8; 20]) -> PeerAdmissionDecision {
        self.counters
            .client_id_checks
            .fetch_add(1, AtomicOrdering::Relaxed);
        if let Some(detail) = &self.fail_closed_detail {
            self.counters
                .fail_closed
                .fetch_add(1, AtomicOrdering::Relaxed);
            return PeerAdmissionDecision::FailClosed {
                detail: detail.to_string(),
            };
        }
        if !self.enabled {
            return PeerAdmissionDecision::Disabled;
        }
        if let Some(prefix) = self
            .blocked_client_ids
            .iter()
            .find(|prefix| peer_id.starts_with(prefix.as_bytes()))
        {
            self.counters
                .client_ids
                .fetch_add(1, AtomicOrdering::Relaxed);
            return PeerAdmissionDecision::BlockedByClientId {
                prefix: prefix.clone(),
            };
        }
        PeerAdmissionDecision::Allowed
    }

    pub fn status(&self) -> PeerFilterStatus {
        let configured_rule_count = self
            .rules
            .iter()
            .filter(|rule| rule.origin == RuleOrigin::Configured)
            .count();
        PeerFilterStatus {
            enabled: self.enabled,
            rules: self
                .rules
                .iter()
                .filter(|rule| rule.origin == RuleOrigin::Configured)
                .map(|rule| rule.display.clone())
                .collect(),
            blocklist_paths: self
                .sources
                .iter()
                .map(|source| source.path.clone())
                .collect(),
            configured_rule_count,
            imported_rule_count: self.rules.len().saturating_sub(configured_rule_count),
            manual_bans: self
                .manual_bans
                .iter()
                .map(|ban| ban.configured.clone())
                .collect(),
            blocked_client_ids: self.blocked_client_ids.as_ref().clone(),
            sources: self.sources.as_ref().clone(),
            rejections: PeerFilterRejectionCounts {
                ip_checks: self.counters.ip_checks.load(AtomicOrdering::Relaxed),
                client_id_checks: self.counters.client_id_checks.load(AtomicOrdering::Relaxed),
                manual_bans: self.counters.manual_bans.load(AtomicOrdering::Relaxed),
                configured_rules: self.counters.configured_rules.load(AtomicOrdering::Relaxed),
                imported_rules: self.counters.imported_rules.load(AtomicOrdering::Relaxed),
                client_ids: self.counters.client_ids.load(AtomicOrdering::Relaxed),
                fail_closed: self.counters.fail_closed.load(AtomicOrdering::Relaxed),
            },
            fail_closed_detail: self
                .fail_closed_detail
                .as_ref()
                .map(|detail| detail.to_string()),
        }
    }
}

struct ImportedBlocklist {
    rules: Vec<(PeerIpRule, String)>,
    rule_count: usize,
    skipped_lines: usize,
}

fn load_blocklist(path: &Path) -> Result<ImportedBlocklist> {
    let metadata = fs::metadata(path).map_err(|error| {
        CoreError::InvalidConfig(format!(
            "peer_filter.blocklist_paths '{}': {error}",
            path.display()
        ))
    })?;
    if !metadata.is_file() {
        return Err(CoreError::InvalidConfig(format!(
            "peer_filter.blocklist_paths '{}' is not a regular file",
            path.display()
        )));
    }
    if metadata.len() > MAX_BLOCKLIST_FILE_BYTES {
        return Err(CoreError::InvalidConfig(format!(
            "peer_filter.blocklist_paths '{}' exceeds {MAX_BLOCKLIST_FILE_BYTES} bytes",
            path.display()
        )));
    }
    let bytes = fs::read(path).map_err(|error| {
        CoreError::InvalidConfig(format!(
            "peer_filter.blocklist_paths '{}': {error}",
            path.display()
        ))
    })?;
    let text = std::str::from_utf8(&bytes).map_err(|error| {
        CoreError::InvalidConfig(format!(
            "peer_filter.blocklist_paths '{}' must be UTF-8 text: {error}",
            path.display()
        ))
    })?;
    let mut rules = Vec::new();
    let mut skipped_lines = 0usize;
    for (line_number, line) in text.lines().enumerate() {
        if line.len() > MAX_BLOCKLIST_LINE_BYTES {
            return Err(CoreError::InvalidConfig(format!(
                "peer_filter.blocklist_paths '{}' line {} exceeds {MAX_BLOCKLIST_LINE_BYTES} bytes",
                path.display(),
                line_number + 1
            )));
        }
        match parse_blocklist_line(line) {
            None => {}
            Some(Ok((rule, display))) => {
                if rules.len() >= MAX_PEER_FILTER_RULES {
                    return Err(CoreError::InvalidConfig(format!(
                        "peer_filter.blocklist_paths '{}' exceeds {MAX_PEER_FILTER_RULES} rules",
                        path.display()
                    )));
                }
                rules.push((rule, display));
            }
            Some(Err(())) => skipped_lines = skipped_lines.saturating_add(1),
        }
    }
    let rule_count = rules.len();
    Ok(ImportedBlocklist {
        rules,
        rule_count,
        skipped_lines,
    })
}

/// Accept raw rules plus common local eMule/PeerGuardian forms:
/// `label:first-last` and `first - last , level , label`.
fn parse_blocklist_line(line: &str) -> Option<std::result::Result<(PeerIpRule, String), ()>> {
    let line = line.trim();
    if line.is_empty() || line.starts_with('#') || line.starts_with(';') || line.starts_with("//") {
        return None;
    }
    if let Ok(rule) = PeerIpRule::parse(line) {
        return Some(Ok((rule, line.to_string())));
    }
    let comma_candidate = line.split(',').next().unwrap_or_default().trim();
    if let Ok(rule) = PeerIpRule::parse(comma_candidate) {
        return Some(Ok((rule, comma_candidate.to_string())));
    }
    if let Some((_, candidate)) = line.rsplit_once(':') {
        let candidate = candidate.trim();
        if let Ok(rule) = PeerIpRule::parse(candidate) {
            return Some(Ok((rule, candidate.to_string())));
        }
    }
    Some(Err(()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cidr_range_and_single_rules_match_both_families() {
        let cidr = PeerIpRule::parse("192.0.2.0/24").unwrap();
        assert!(cidr.matches("192.0.2.199".parse().unwrap()));
        assert!(!cidr.matches("192.0.3.1".parse().unwrap()));
        let range = PeerIpRule::parse("2001:db8::10 - 2001:db8::20").unwrap();
        assert!(range.matches("2001:db8::15".parse().unwrap()));
        assert!(!range.matches("2001:db8::21".parse().unwrap()));
        assert!(PeerIpRule::parse("203.0.113.7")
            .unwrap()
            .matches("203.0.113.7".parse().unwrap()));
    }

    #[test]
    fn invalid_rules_are_rejected_deterministically() {
        for rule in [
            "",
            "192.0.2.1/33",
            "192.0.2.10-192.0.2.1",
            "192.0.2.1-2001:db8::1",
            "not an IP",
        ] {
            assert!(PeerIpRule::parse(rule).is_err(), "{rule}");
        }
    }

    #[test]
    fn imported_formats_are_parsed_without_accepting_noise() {
        let raw = parse_blocklist_line("198.51.100.0/24").unwrap().unwrap();
        assert!(raw.0.matches("198.51.100.9".parse().unwrap()));
        let emule = parse_blocklist_line("abusive source:203.0.113.2-203.0.113.9")
            .unwrap()
            .unwrap();
        assert!(emule.0.matches("203.0.113.4".parse().unwrap()));
        let pg = parse_blocklist_line("203.0.113.10 - 203.0.113.20 , 000 , test")
            .unwrap()
            .unwrap();
        assert!(pg.0.matches("203.0.113.15".parse().unwrap()));
        assert!(parse_blocklist_line("# comment").is_none());
        assert!(parse_blocklist_line("not a list row").unwrap().is_err());
    }

    #[test]
    fn manual_bans_rules_and_client_prefixes_are_auditable() {
        let filter = PeerFilter::from_config(&PeerFilterConfig {
            enabled: true,
            rules: vec!["198.51.100.0/24".into()],
            blocklist_paths: Vec::new(),
            manual_bans: vec![ManualPeerBan {
                ip: "203.0.113.7".into(),
                reason: Some("repeated malformed requests".into()),
            }],
            blocked_client_ids: vec!["-XL".into()],
        })
        .unwrap();
        assert!(matches!(
            filter.admit_ip("203.0.113.7".parse().unwrap()),
            PeerAdmissionDecision::BlockedByManualBan { .. }
        ));
        assert!(matches!(
            filter.admit_ip("198.51.100.9".parse().unwrap()),
            PeerAdmissionDecision::BlockedByConfiguredRule { .. }
        ));
        let mut peer_id = [0u8; 20];
        peer_id[..7].copy_from_slice(b"-XL0001");
        assert!(matches!(
            filter.admit_client_id(&peer_id),
            PeerAdmissionDecision::BlockedByClientId { .. }
        ));
        let status = filter.status();
        assert_eq!(status.rules, vec!["198.51.100.0/24"]);
        assert_eq!(status.rejections.manual_bans, 1);
        assert_eq!(status.rejections.configured_rules, 1);
        assert_eq!(status.rejections.client_ids, 1);
    }

    #[test]
    fn disabled_filter_preserves_default_allow_behavior() {
        let filter = PeerFilter::default();
        assert!(filter.admit_ip("203.0.113.7".parse().unwrap()).is_allowed());
        assert!(filter.admit_client_id(&[0; 20]).is_allowed());
        assert!(!filter.status().enabled);
    }
}
