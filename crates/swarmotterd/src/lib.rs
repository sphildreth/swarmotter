// SPDX-License-Identifier: Apache-2.0

//! Library facade for `swarmotterd` so integration tests can access the
//! runtime modules. The binary entry point lives in `main.rs`.

pub mod daemon;
pub mod dht;
pub mod engine;
pub mod metadata;
pub mod netbinder;
pub mod runtime;
pub mod seeder;
