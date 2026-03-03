// Copyright (c) Starcoin, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Bridge event handlers
//!
//! This module contains handlers for processing blockchain events:
//! - `stc_event_handler`: Direct event handler for Starcoin (new simplified architecture)

use move_core_types::ident_str;
use move_core_types::identifier::IdentStr;

pub mod stc_event_handler;

pub use stc_event_handler::{run_stc_event_handler, StcEventHandler, StcEventHandlerConfig};

// Event type identifiers for Starcoin bridge contract
pub const LIMITER: &IdentStr = ident_str!("Limiter");
pub const BRIDGE: &IdentStr = ident_str!("Bridge");
pub const COMMITTEE: &IdentStr = ident_str!("Committee");
pub const TREASURY: &IdentStr = ident_str!("Treasury");

pub const TOKEN_DEPOSITED_EVENT: &IdentStr = ident_str!("TokenDepositedEvent");
pub const TOKEN_TRANSFER_APPROVED: &IdentStr = ident_str!("TokenTransferApproved");
pub const TOKEN_TRANSFER_CLAIMED: &IdentStr = ident_str!("TokenTransferClaimed");
pub const UPDATE_ROUTE_LIMIT_EVENT: &IdentStr = ident_str!("UpdateRouteLimitEvent");

/// Macro to create a StructTag for event matching
#[macro_export]
macro_rules! struct_tag {
    ($address:ident, $module:ident, $name:ident) => {{
        StructTag {
            address: $address,
            module: $module.into(),
            name: $name.into(),
            type_params: vec![],
        }
    }};
}
