// SPDX-License-Identifier: Apache-2.0

//! Peer models.

use serde::{Deserialize, Serialize};
use std::net::IpAddr;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PeerDirection {
    Inbound,
    Outbound,
}

/// Peer flags describing connection state.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PeerFlags {
    pub choking: bool,
    pub interested: bool,
    pub peer_choking: bool,
    pub interested_in_us: bool,
    pub optimistic_unchoke: bool,
    pub snubbed: bool,
}

/// A connected peer.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Peer {
    pub address: String,
    pub ip: IpAddr,
    pub port: u16,
    pub direction: PeerDirection,
    pub client: Option<String>,
    pub progress: f64,
    pub rate_down: u64,
    pub rate_up: u64,
    pub flags: PeerFlags,
    pub banned: bool,
}

/// Per-peer health signals used by the engine to feed the health calculator.
/// The fields describe whether a peer is currently useful: does it carry any
/// piece we still need, is it unchoked or recently useful, and is it blocked?
#[derive(Debug, Clone, Default)]
pub struct EnginePeerHealth {
    pub piece_bitfield: Option<crate::storage::resume::PieceBitfield>,
    pub has_missing_pieces: bool,
    pub unchoked: bool,
    pub blocked: bool,
    pub last_valid_block: Option<std::time::Instant>,
    pub useful_recently: bool,
    pub discovered_from_pex: bool,
    pub last_seen: Option<std::time::Instant>,
}
