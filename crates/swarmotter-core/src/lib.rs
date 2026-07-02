// SPDX-License-Identifier: Apache-2.0

//! Core types and BitTorrent engine logic for SwarmOtter.
//!
//! This crate holds domain models, parsing, validation, and the central
//! network-containment-aware logic. Engine components must not create network
//! sockets directly; all torrent traffic goes through the containment layer
//! (see `swarmotter-core::net` and `design/vpn-network-containment.md`).

pub mod bandwidth;
pub mod bencode;
pub mod config;
pub mod error;
pub mod hash;
pub mod magnet;
pub mod meta;
pub mod models;
pub mod net;
pub mod queue;
pub mod ratio;
pub mod storage;
pub mod torrent;
pub mod watch;

pub use error::{CoreError, Result};
pub use hash::InfoHash;
pub use magnet::Magnet;
pub use meta::TorrentMeta;
pub use models::*;
