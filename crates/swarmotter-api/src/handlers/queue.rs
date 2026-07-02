// SPDX-License-Identifier: Apache-2.0

//! Queue handlers.

use crate::error::{err_response, ok_empty_response};
use crate::routes::parse_hash;
use crate::state::SharedState;
use axum::{
    extract::{Path, State},
    response::Response,
};

macro_rules! queue_action {
    ($name:ident, $method:ident) => {
        pub async fn $name(State(state): State<SharedState>, Path(hash): Path<String>) -> Response {
            match parse_hash(&hash) {
                Ok(h) => match state.daemon.$method(&h).await {
                    Ok(()) => ok_empty_response(),
                    Err(e) => err_response(e),
                },
                Err(e) => err_response(e),
            }
        }
    };
}

queue_action!(move_up, queue_move_up);
queue_action!(move_down, queue_move_down);
queue_action!(move_top, queue_move_to_top);
queue_action!(move_bottom, queue_move_to_bottom);
