// Copyright (c) Starcoin, Inc.
// SPDX-License-Identifier: Apache-2.0

//! API handlers for querying cross-chain transfers
//!
//! Queries combine data from:
//! - Database (finalized events)
//! - Memory store (pending/unfinalized events from TransferTracker)

use crate::api::types::*;
use crate::api::ApiState;
use crate::network::NetworkType;
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Json, Response},
    routing::get,
    Router,
};
use diesel::dsl::count_star;
use diesel::prelude::*;
use diesel_async::RunQueryDsl;
use starcoin_bridge::pending_events::{
    ChainId as MemChainId, TransferRecord, TransferStatus as MemTransferStatus,
};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, error, info};

/// Format amount as "100.123456 USDT" string
/// USDT has 6 decimal places
fn format_usdt_amount(amount: i64) -> String {
    let whole = amount / 1_000_000;
    let frac = (amount % 1_000_000).abs();
    if frac == 0 {
        format!("{} USDT", whole)
    } else {
        // Remove trailing zeros
        let frac_str = format!("{:06}", frac).trim_end_matches('0').to_string();
        format!("{}.{} USDT", whole, frac_str)
    }
}

/// Format amount from u64 for USDT (6 decimals, no adjustment needed).
///
/// For USDT, the bridge's `starcoinDecimal` equals the ERC20 decimal (both 6),
/// so `convertERC20ToStarcoinDecimal` is a no-op and the event amount is already
/// in 6-decimal precision — the same precision `format_usdt_amount` expects.
///
/// NOTE: If non-USDT tokens with different starcoin decimals (e.g. ETH/BTC with
/// 8 decimals) are added in the future, this function must be updated to account
/// for the per-token decimal difference.
fn format_usdt_amount_u64(amount: u64) -> String {
    format_usdt_amount(amount as i64)
}

/// Convert memory ChainId to API chain_id (i32) using network-aware bridge chain ID mapping
fn mem_chain_id_to_i32(chain_id: MemChainId, network: NetworkType) -> i32 {
    network.chain_id_to_bridge_i32(chain_id)
}

/// Convert memory ChainId to DataSource
fn mem_chain_id_to_data_source(chain_id: MemChainId) -> DataSource {
    match chain_id {
        MemChainId::Starcoin => DataSource::Starcoin,
        MemChainId::Eth => DataSource::Eth,
    }
}

/// Build CrossChainProcedure from memory TransferRecord
fn build_procedure_from_memory(
    record: &TransferRecord,
    network: NetworkType,
) -> Option<CrossChainProcedure> {
    // Must have deposit info to build procedure
    let deposit = record.deposit.as_ref()?;

    let source_chain_id = mem_chain_id_to_i32(record.key.source_chain, network);
    let destination_chain_id = mem_chain_id_to_i32(deposit.destination_chain, network);

    let current_status = match record.status {
        MemTransferStatus::Deposited => TransferStatus::Deposited,
        MemTransferStatus::Approved => TransferStatus::Approved,
        MemTransferStatus::Claimed => TransferStatus::Claimed,
    };

    let approval_info = record.approval.as_ref().map(|a| ApprovalInfo {
        txn_hash: a.tx_hash.clone(),
        block_height: a.block_number as i64,
        timestamp_ms: a.timestamp_ms as i64,
        data_source: mem_chain_id_to_data_source(a.recorded_chain),
        is_finalized: a.is_finalized,
    });

    let claim_info = record.claim.as_ref().map(|c| ClaimInfo {
        txn_hash: c.tx_hash.clone(),
        block_height: c.block_number as i64,
        timestamp_ms: c.timestamp_ms as i64,
        claimer_address: c.claimer_address.clone(),
        gas_usage: 0, // Gas usage not tracked in memory
        data_source: mem_chain_id_to_data_source(deposit.destination_chain),
        is_finalized: c.is_finalized,
    });

    Some(CrossChainProcedure {
        source_chain_id,
        source_chain: chain_id_to_name(source_chain_id),
        destination_chain_id,
        destination_chain: chain_id_to_name(destination_chain_id),
        nonce: record.key.nonce as i64,
        current_status,
        is_complete: record.status == MemTransferStatus::Claimed,

        deposit: DepositInfo {
            txn_hash: deposit.tx_hash.clone(),
            block_height: deposit.block_number as i64,
            timestamp_ms: deposit.timestamp_ms as i64,
            sender_address: deposit.sender_address.clone(),
            recipient_address: deposit.recipient_address.clone(),
            token_id: deposit.token_id as i32,
            amount: format_usdt_amount_u64(deposit.amount),
            is_finalized: deposit.is_finalized,
        },

        approval: approval_info,
        claim: claim_info,
    })
}

/// Merge pending records from memory with DB results
///
/// Priority:
/// - If a transfer exists in both, prefer memory record (has latest state)
/// - Memory records marked as unfinalized
/// - DB records may be finalized
fn merge_with_memory_records(
    db_procedures: Vec<CrossChainProcedure>,
    memory_records: Vec<TransferRecord>,
    network: NetworkType,
) -> Vec<CrossChainProcedure> {
    let mut result_map: HashMap<(i32, i64), CrossChainProcedure> = HashMap::new();

    // First, add DB procedures
    for proc in db_procedures {
        let key = (proc.source_chain_id, proc.nonce);
        result_map.insert(key, proc);
    }

    // Then, merge/override with memory records
    for record in memory_records {
        if let Some(proc) = build_procedure_from_memory(&record, network) {
            let key = (proc.source_chain_id, proc.nonce);

            if let Some(existing) = result_map.get_mut(&key) {
                // Merge: update with memory data (may have newer status)
                debug!(
                    "[API] Merging memory record for chain={} nonce={}: mem_status={:?}, db_status={:?}",
                    key.0, key.1, proc.current_status, existing.current_status
                );

                // Update status if memory has more recent data
                if should_update_status(&existing.current_status, &proc.current_status) {
                    existing.current_status = proc.current_status.clone();
                    existing.is_complete = proc.is_complete;
                }

                // Update approval if not in DB but in memory
                if existing.approval.is_none() && proc.approval.is_some() {
                    existing.approval = proc.approval.clone();
                }

                // Update claim if not in DB but in memory
                if existing.claim.is_none() && proc.claim.is_some() {
                    existing.claim = proc.claim.clone();
                }

                // Mark as unfinalized if memory version is unfinalized
                if !proc.deposit.is_finalized {
                    existing.deposit.is_finalized = false;
                }
            } else {
                // New record from memory only
                debug!(
                    "[API] Adding memory-only record for chain={} nonce={}, status={:?}",
                    key.0, key.1, proc.current_status
                );
                result_map.insert(key, proc);
            }
        }
    }

    // Convert back to vec and sort by timestamp desc
    let mut result: Vec<_> = result_map.into_values().collect();
    result.sort_by(|a, b| {
        b.deposit
            .timestamp_ms
            .cmp(&a.deposit.timestamp_ms)
            .then_with(|| b.source_chain_id.cmp(&a.source_chain_id))
            .then_with(|| b.nonce.cmp(&a.nonce))
    });
    result
}

/// Check if we should update status based on progression
fn should_update_status(current: &TransferStatus, new: &TransferStatus) -> bool {
    let current_rank = match current {
        TransferStatus::Deposited => 0,
        TransferStatus::Approved => 1,
        TransferStatus::Claimed => 2,
    };
    let new_rank = match new {
        TransferStatus::Deposited => 0,
        TransferStatus::Approved => 1,
        TransferStatus::Claimed => 2,
    };
    new_rank > current_rank
}

/// Create the API router with all endpoints
pub fn create_api_router(state: Arc<ApiState>) -> Router {
    Router::new()
        .route("/transfers", get(list_transfers))
        .route(
            "/transfers/by-deposit-txn/:txn_hash",
            get(get_transfer_by_deposit_txn),
        )
        .route("/quota", get(get_quota))
        .route("/estimate_fees", get(estimate_fees))
        .route("/watermark", get(get_watermark))
        .route("/status", get(get_bridge_status))
        .route("/health", get(health_check))
        .with_state(state)
}

/// Health check endpoint
async fn health_check() -> impl IntoResponse {
    Json(serde_json::json!({
        "status": "ok",
        "service": "bridge-indexer-api"
    }))
}

/// Get bridge paused status for both chains
/// Returns whether ETH and Starcoin bridges are paused
async fn get_bridge_status(
    State(state): State<Arc<ApiState>>,
) -> Result<Json<BridgeStatusResponse>, ApiErrorResponse> {
    let mut eth_paused = false;
    let mut stc_paused = false;
    let mut errors = Vec::new();

    // Query ETH bridge paused state
    if !state.eth_rpc_url.is_empty() {
        match query_eth_paused(&state.eth_rpc_url).await {
            Ok(paused) => eth_paused = paused,
            Err(e) => {
                error!("Failed to query ETH paused state: {}", e);
                errors.push(format!("ETH: {}", e));
            }
        }
    } else {
        errors.push("ETH: RPC URL not configured".to_string());
    }

    // Query Starcoin bridge frozen state
    if !state.starcoin_rpc_url.is_empty() {
        match query_stc_frozen(&state.starcoin_rpc_url).await {
            Ok(frozen) => stc_paused = frozen,
            Err(e) => {
                error!("Failed to query Starcoin frozen state: {}", e);
                errors.push(format!("Starcoin: {}", e));
            }
        }
    } else {
        errors.push("Starcoin: RPC URL not configured".to_string());
    }

    Ok(Json(BridgeStatusResponse {
        eth_paused,
        stc_paused,
        errors,
    }))
}

/// Query ETH bridge paused state via eth_call
async fn query_eth_paused(rpc_url: &str) -> Result<bool, String> {
    use serde_json::json;

    // Get bridge address from config (stored in quota cache)
    let eth_bridge_addr = crate::api::get_global_quota_cache()
        .map(|c| c.eth_bridge_address())
        .unwrap_or_default();

    if eth_bridge_addr.is_empty() {
        return Err("ETH bridge address not configured".to_string());
    }

    // paused() selector = 0x5c975abb
    let call_data = "0x5c975abb";

    let client = reqwest::Client::new();
    let response = client
        .post(rpc_url)
        .json(&json!({
            "jsonrpc": "2.0",
            "method": "eth_call",
            "params": [{
                "to": eth_bridge_addr,
                "data": call_data
            }, "latest"],
            "id": 1
        }))
        .send()
        .await
        .map_err(|e| format!("HTTP request failed: {}", e))?;

    let result: serde_json::Value = response
        .json()
        .await
        .map_err(|e| format!("Failed to parse response: {}", e))?;

    if let Some(error) = result.get("error") {
        return Err(format!("RPC error: {}", error));
    }

    let hex_result = result["result"]
        .as_str()
        .ok_or("Missing result field")?;

    // Result is 32 bytes, check if last byte is non-zero
    let paused = hex_result.ends_with("1");
    Ok(paused)
}

/// Query Starcoin bridge frozen state via state.get_resource
async fn query_stc_frozen(rpc_url: &str) -> Result<bool, String> {
    use serde_json::json;

    // Get bridge address from config
    let stc_bridge_addr = crate::api::get_global_quota_cache()
        .map(|c| c.stc_bridge_address())
        .unwrap_or_default();

    if stc_bridge_addr.is_empty() {
        return Err("Starcoin bridge address not configured".to_string());
    }

    // Query Bridge resource to get is_frozen field
    let resource_type = format!("{}::Bridge::Bridge", stc_bridge_addr);

    let client = reqwest::Client::new();
    let response = client
        .post(rpc_url)
        .json(&json!({
            "jsonrpc": "2.0",
            "method": "state.get_resource",
            "params": [stc_bridge_addr, resource_type, {"decode": true}],
            "id": 1
        }))
        .send()
        .await
        .map_err(|e| format!("HTTP request failed: {}", e))?;

    let result: serde_json::Value = response
        .json()
        .await
        .map_err(|e| format!("Failed to parse response: {}", e))?;

    if let Some(error) = result.get("error") {
        return Err(format!("RPC error: {}", error));
    }

    // Extract is_frozen from decoded Bridge resource
    let is_frozen = result["result"]["json"]["is_frozen"]
        .as_bool()
        .unwrap_or(false);

    Ok(is_frozen)
}

/// Get current indexer watermarks for both chains
/// Returns the last finalized block processed by each indexer
async fn get_watermark(
    State(state): State<Arc<ApiState>>,
) -> Result<Json<WatermarkResponse>, ApiErrorResponse> {
    use crate::indexer_progress::{
        IndexerProgressStore, ETH_INDEXER_TASK_NAME, STC_INDEXER_TASK_NAME,
    };

    let progress_store = IndexerProgressStore::new(state.db.clone());

    let eth_watermark = progress_store
        .get_watermark(ETH_INDEXER_TASK_NAME)
        .await
        .ok()
        .flatten();

    let stc_watermark = progress_store
        .get_watermark(STC_INDEXER_TASK_NAME)
        .await
        .ok()
        .flatten();

    Ok(Json(WatermarkResponse {
        eth_watermark,
        stc_watermark,
    }))
}

/// Estimate fees endpoint - returns last gas consumption for deposit/approval/claim
/// Syncer updates cache asynchronously. Always returns cached value.
async fn estimate_fees(
    State(state): State<Arc<ApiState>>,
) -> Result<Json<FeeEstimateResponse>, ApiErrorResponse> {
    Ok(Json(state.fee_cache.get().await))
}

/// Get current bridge quota (remaining limit)
/// Returns the remaining transfer quota for each chain in USD (8 decimal precision)
///
/// Uses singleflight pattern:
/// - Returns cached values immediately when fresh
/// - Coalesces concurrent requests when stale
/// - Refreshes when transfer events occur
async fn get_quota(State(state): State<Arc<ApiState>>) -> impl IntoResponse {
    let quota = state.quota_cache.get().await;
    Json(quota)
}

/// List transfers with filtering and pagination
/// Returns complete cross-chain procedure info for each transfer
/// Address filter matches both sender_address and recipient_address
async fn list_transfers(
    State(state): State<Arc<ApiState>>,
    Query(params): Query<TransferListQuery>,
) -> Result<Json<TransferListResponse>, ApiErrorResponse> {
    use starcoin_bridge_schema::schema::{token_transfer, token_transfer_data};

    info!("Listing transfers with params: {:?}", params);

    // Get database connection
    let mut conn =
        state.db.connect().await.map_err(|e| {
            ApiErrorResponse::internal(&format!("Database connection error: {}", e))
        })?;

    // Parse filters once
    let addr_bytes = if let Some(ref addr) = params.address {
        Some(
            parse_hex_address(addr)
                .map_err(|e| ApiErrorResponse::bad_request(&format!("Invalid address: {}", e)))?,
        )
    } else {
        None
    };

    let sender_bytes = if let Some(ref addr) = params.sender {
        Some(parse_hex_address(addr).map_err(|e| {
            ApiErrorResponse::bad_request(&format!("Invalid sender address: {}", e))
        })?)
    } else {
        None
    };

    let receiver_bytes = if let Some(ref addr) = params.receiver {
        Some(parse_hex_address(addr).map_err(|e| {
            ApiErrorResponse::bad_request(&format!("Invalid receiver address: {}", e))
        })?)
    } else {
        None
    };

    let page = params.page.max(1);
    let page_size = params.page_size.clamp(1, 100);

    // Query deposit data (token_transfer_data) with filters
    // This is the source of truth for unique transfers
    let mut deposit_query = token_transfer_data::table.into_boxed();

    if let Some(ref addr) = addr_bytes {
        deposit_query = deposit_query.filter(
            token_transfer_data::sender_address
                .eq(addr)
                .or(token_transfer_data::recipient_address.eq(addr)),
        );
    }
    if let Some(ref sender) = sender_bytes {
        deposit_query = deposit_query.filter(token_transfer_data::sender_address.eq(sender));
    }
    if let Some(ref receiver) = receiver_bytes {
        deposit_query = deposit_query.filter(token_transfer_data::recipient_address.eq(receiver));
    }
    if let Some(chain_id) = params.chain_id {
        deposit_query = deposit_query.filter(token_transfer_data::chain_id.eq(chain_id));
    }
    if let Some(true) = params.finalized_only {
        deposit_query = deposit_query.filter(token_transfer_data::is_finalized.eq(true));
    }

    // Get total count first
    let total_count: i64 = {
        let mut count_query = token_transfer_data::table.into_boxed();
        if let Some(ref addr) = addr_bytes {
            count_query = count_query.filter(
                token_transfer_data::sender_address
                    .eq(addr)
                    .or(token_transfer_data::recipient_address.eq(addr)),
            );
        }
        if let Some(ref sender) = sender_bytes {
            count_query = count_query.filter(token_transfer_data::sender_address.eq(sender));
        }
        if let Some(ref receiver) = receiver_bytes {
            count_query = count_query.filter(token_transfer_data::recipient_address.eq(receiver));
        }
        if let Some(chain_id) = params.chain_id {
            count_query = count_query.filter(token_transfer_data::chain_id.eq(chain_id));
        }
        if let Some(true) = params.finalized_only {
            count_query = count_query.filter(token_transfer_data::is_finalized.eq(true));
        }
        count_query
            .select(count_star())
            .first(&mut conn)
            .await
            .map_err(|e| ApiErrorResponse::internal(&format!("Failed to count transfers: {}", e)))?
    };

    if total_count == 0 {
        return Ok(Json(TransferListResponse {
            transfers: vec![],
            pagination: Pagination {
                page,
                page_size,
                total_count: 0,
                total_pages: 0,
            },
            claim_delay_seconds: state.claim_delay_seconds,
        }));
    }

    // Get paginated deposit data
    let offset = ((page - 1) * page_size) as i64;
    let deposits: Vec<starcoin_bridge_schema::models::TokenTransferData> = deposit_query
        .order((
            token_transfer_data::timestamp_ms.desc(),
            token_transfer_data::chain_id.asc(),
            token_transfer_data::nonce.desc(),
        ))
        .limit(page_size as i64)
        .offset(offset)
        .load(&mut conn)
        .await
        .map_err(|e| ApiErrorResponse::internal(&format!("Failed to query deposits: {}", e)))?;

    // Collect all (chain_id, nonce) pairs for batch query
    let pairs: Vec<(i32, i64)> = deposits.iter().map(|d| (d.chain_id, d.nonce)).collect();

    // Batch query all status updates for these transfers
    let all_status_updates: Vec<starcoin_bridge_schema::models::TokenTransfer> =
        if !pairs.is_empty() {
            // Get all chain_ids and nonces
            let chain_ids: Vec<i32> = pairs.iter().map(|(c, _)| *c).collect();
            let nonces: Vec<i64> = pairs.iter().map(|(_, n)| *n).collect();

            token_transfer::table
                .filter(
                    token_transfer::chain_id
                        .eq_any(chain_ids)
                        .and(token_transfer::nonce.eq_any(nonces)),
                )
                .order((
                    token_transfer::chain_id.asc(),
                    token_transfer::nonce.asc(),
                    token_transfer::timestamp_ms.asc(),
                ))
                .load(&mut conn)
                .await
                .map_err(|e| {
                    ApiErrorResponse::internal(&format!("Failed to query status updates: {}", e))
                })?
        } else {
            vec![]
        };

    // Group status updates by (chain_id, nonce)
    let mut status_map: std::collections::HashMap<
        (i32, i64),
        Vec<starcoin_bridge_schema::models::TokenTransfer>,
    > = std::collections::HashMap::new();
    for update in all_status_updates {
        // Also verify this update actually belongs to one of our pairs
        if pairs.contains(&(update.chain_id, update.nonce)) {
            status_map
                .entry((update.chain_id, update.nonce))
                .or_default()
                .push(update);
        }
    }

    // Build CrossChainProcedure for each deposit using the helper function
    let db_procedures: Vec<CrossChainProcedure> = deposits
        .iter()
        .map(|deposit| {
            let key = (deposit.chain_id, deposit.nonce);
            let status_updates = status_map.get(&key).map(|v| v.as_slice()).unwrap_or(&[]);
            build_cross_chain_procedure(deposit, status_updates)
        })
        .collect();

    // Merge with memory records from TransferTracker (if available)
    let procedures = if let Some(ref tracker) = state.transfer_tracker {
        let memory_records = tracker.get_all_pending().await;
        debug!(
            "[API] list_transfers: merging {} DB records with {} memory records",
            db_procedures.len(),
            memory_records.len()
        );

        // Filter memory records by query params
        let filtered_memory: Vec<TransferRecord> = memory_records
            .into_iter()
            .filter(|r| {
                // Chain ID filter
                if let Some(chain_id) = params.chain_id {
                    if mem_chain_id_to_i32(r.key.source_chain, state.network) != chain_id {
                        return false;
                    }
                }

                // Address filters (need deposit info)
                if let Some(ref deposit) = r.deposit {
                    if let Some(ref addr_bytes) = addr_bytes {
                        let addr_hex = hex::encode(addr_bytes);
                        let sender_match = deposit
                            .sender_address
                            .to_lowercase()
                            .contains(&addr_hex.to_lowercase());
                        let recipient_match = deposit
                            .recipient_address
                            .to_lowercase()
                            .contains(&addr_hex.to_lowercase());
                        if !sender_match && !recipient_match {
                            return false;
                        }
                    }
                    if let Some(ref sender_bytes) = sender_bytes {
                        let sender_hex = hex::encode(sender_bytes);
                        if !deposit
                            .sender_address
                            .to_lowercase()
                            .contains(&sender_hex.to_lowercase())
                        {
                            return false;
                        }
                    }
                    if let Some(ref receiver_bytes) = receiver_bytes {
                        let receiver_hex = hex::encode(receiver_bytes);
                        if !deposit
                            .recipient_address
                            .to_lowercase()
                            .contains(&receiver_hex.to_lowercase())
                        {
                            return false;
                        }
                    }
                } else if addr_bytes.is_some() || sender_bytes.is_some() || receiver_bytes.is_some()
                {
                    // Need deposit info for address filtering but don't have it
                    return false;
                }

                // Finalized-only filter: skip unfinalized memory records
                if let Some(true) = params.finalized_only {
                    if r.deposit.as_ref().map(|d| !d.is_finalized).unwrap_or(true) {
                        return false;
                    }
                }

                true
            })
            .collect();

        merge_with_memory_records(db_procedures, filtered_memory, state.network)
    } else {
        db_procedures
    };

    // Apply status filter in memory if specified
    let procedures = if let Some(ref status_str) = params.status {
        let target_status = match status_str.to_lowercase().as_str() {
            "deposited" => TransferStatus::Deposited,
            "approved" => TransferStatus::Approved,
            "claimed" => TransferStatus::Claimed,
            _ => {
                return Err(ApiErrorResponse::bad_request(&format!(
                    "Invalid status: {}. Must be one of: deposited, approved, claimed",
                    status_str
                )))
            }
        };
        procedures
            .into_iter()
            .filter(|p| p.current_status == target_status)
            .collect()
    } else {
        procedures
    };

    let total_pages = ((total_count as f64) / (page_size as f64)).ceil() as u32;

    Ok(Json(TransferListResponse {
        transfers: procedures,
        pagination: Pagination {
            page,
            page_size,
            total_count,
            total_pages,
        },
        claim_delay_seconds: state.claim_delay_seconds,
    }))
}

/// Get transfer details by deposit transaction hash
/// This finds the transfer associated with a deposit transaction and returns
/// the full cross-chain procedure data including all status updates.
///
/// Searches both:
/// - Database (finalized events)
/// - Memory store (pending/unfinalized events from TransferTracker)
async fn get_transfer_by_deposit_txn(
    State(state): State<Arc<ApiState>>,
    Path(txn_hash): Path<String>,
) -> Result<Json<TransferByDepositTxnResponse>, ApiErrorResponse> {
    use starcoin_bridge_schema::schema::{token_transfer, token_transfer_data};

    info!("Getting transfer by deposit txn_hash={}", txn_hash);

    // Parse txn_hash
    let txn_hash_bytes = parse_hex_address(&txn_hash)
        .map_err(|e| ApiErrorResponse::bad_request(&format!("Invalid txn_hash: {}", e)))?;

    // Get database connection
    let mut conn =
        state.db.connect().await.map_err(|e| {
            ApiErrorResponse::internal(&format!("Database connection error: {}", e))
        })?;

    // First, find the deposit event (token_transfer_data) by txn_hash in DB
    let deposit_data: Option<starcoin_bridge_schema::models::TokenTransferData> =
        token_transfer_data::table
            .filter(token_transfer_data::txn_hash.eq(&txn_hash_bytes))
            .first(&mut conn)
            .await
            .optional()
            .map_err(|e| {
                ApiErrorResponse::internal(&format!("Failed to query deposit data: {}", e))
            })?;

    // Also search in memory store by txn_hash
    let memory_record = if let Some(ref tracker) = state.transfer_tracker {
        let all_pending = tracker.get_all_pending().await;
        all_pending.into_iter().find(|r| {
            r.deposit
                .as_ref()
                .map(|d| {
                    d.tx_hash.to_lowercase() == txn_hash.to_lowercase()
                        || d.tx_hash.to_lowercase().ends_with(&txn_hash.to_lowercase())
                        || txn_hash.to_lowercase().ends_with(&d.tx_hash.to_lowercase())
                })
                .unwrap_or(false)
        })
    } else {
        None
    };

    // If found in DB
    if let Some(deposit_data) = deposit_data {
        let chain_id = deposit_data.chain_id;
        let nonce = deposit_data.nonce;

        // Query all status updates for this transfer (chain_id, nonce)
        let status_updates: Vec<starcoin_bridge_schema::models::TokenTransfer> =
            token_transfer::table
                .filter(token_transfer::chain_id.eq(chain_id))
                .filter(token_transfer::nonce.eq(nonce))
                .order(token_transfer::timestamp_ms.asc())
                .load(&mut conn)
                .await
                .map_err(|e| {
                    ApiErrorResponse::internal(&format!("Failed to query status updates: {}", e))
                })?;

        // Build procedure from DB
        let mut procedure = build_cross_chain_procedure(&deposit_data, &status_updates);

        // Merge with memory record if available (may have newer status)
        if let Some(mem_record) = memory_record {
            debug!(
                "[API] get_transfer_by_deposit_txn: found in both DB and memory, merging. DB status={:?}, mem status={:?}",
                procedure.current_status, mem_record.status
            );

            // Update with memory data if it has newer status
            let mem_status = match mem_record.status {
                MemTransferStatus::Deposited => TransferStatus::Deposited,
                MemTransferStatus::Approved => TransferStatus::Approved,
                MemTransferStatus::Claimed => TransferStatus::Claimed,
            };

            if should_update_status(&procedure.current_status, &mem_status) {
                procedure.current_status = mem_status;
                procedure.is_complete = mem_record.status == MemTransferStatus::Claimed;
            }

            // Update approval if not in DB but in memory
            if procedure.approval.is_none() {
                if let Some(ref approval) = mem_record.approval {
                    procedure.approval = Some(ApprovalInfo {
                        txn_hash: approval.tx_hash.clone(),
                        block_height: approval.block_number as i64,
                        timestamp_ms: approval.timestamp_ms as i64,
                        data_source: mem_chain_id_to_data_source(approval.recorded_chain),
                        is_finalized: approval.is_finalized,
                    });
                }
            }

            // Update claim if not in DB but in memory
            if procedure.claim.is_none() {
                if let Some(ref claim) = mem_record.claim {
                    if let Some(ref deposit) = mem_record.deposit {
                        procedure.claim = Some(ClaimInfo {
                            txn_hash: claim.tx_hash.clone(),
                            block_height: claim.block_number as i64,
                            timestamp_ms: claim.timestamp_ms as i64,
                            claimer_address: claim.claimer_address.clone(),
                            gas_usage: 0,
                            data_source: mem_chain_id_to_data_source(deposit.destination_chain),
                            is_finalized: claim.is_finalized,
                        });
                    }
                }
            }
        }

        return Ok(Json(TransferByDepositTxnResponse {
            procedure,
            claim_delay_seconds: state.claim_delay_seconds,
        }));
    }

    // Not in DB, try memory only
    if let Some(mem_record) = memory_record {
        debug!(
            "[API] get_transfer_by_deposit_txn: found in memory only, chain={:?}, nonce={}",
            mem_record.key.source_chain, mem_record.key.nonce
        );

        if let Some(procedure) = build_procedure_from_memory(&mem_record, state.network) {
            return Ok(Json(TransferByDepositTxnResponse {
                procedure,
                claim_delay_seconds: state.claim_delay_seconds,
            }));
        }
    }

    // Not found anywhere
    Err(ApiErrorResponse::not_found(&format!(
        "No deposit found for txn_hash: {}",
        txn_hash
    )))
}

/// Parse hex address (with or without 0x prefix)
fn parse_hex_address(addr: &str) -> Result<Vec<u8>, String> {
    let addr_clean = addr.strip_prefix("0x").unwrap_or(addr);
    hex::decode(addr_clean).map_err(|e| format!("Invalid hex: {}", e))
}

/// Convert chain ID to human-readable name
fn chain_id_to_name(chain_id: i32) -> String {
    match chain_id {
        0..=2 => "STARCOIN".to_string(),
        10..=12 => "ETH".to_string(),
        _ => format!("CHAIN_{}", chain_id),
    }
}

/// Build CrossChainProcedure from deposit data and status updates
/// This is the core logic for assembling complete transfer procedure info
pub(crate) fn build_cross_chain_procedure(
    deposit: &starcoin_bridge_schema::models::TokenTransferData,
    status_updates: &[starcoin_bridge_schema::models::TokenTransfer],
) -> CrossChainProcedure {
    // Find approval and claim events
    let approval_event = status_updates
        .iter()
        .find(|t| t.status == starcoin_bridge_schema::models::TokenTransferStatus::Approved);
    let claim_event = status_updates
        .iter()
        .find(|t| t.status == starcoin_bridge_schema::models::TokenTransferStatus::Claimed);

    // Determine current status based on which events exist
    let current_status = if claim_event.is_some() {
        TransferStatus::Claimed
    } else if approval_event.is_some() {
        TransferStatus::Approved
    } else {
        TransferStatus::Deposited
    };

    CrossChainProcedure {
        source_chain_id: deposit.chain_id,
        source_chain: chain_id_to_name(deposit.chain_id),
        destination_chain_id: deposit.destination_chain,
        destination_chain: chain_id_to_name(deposit.destination_chain),
        nonce: deposit.nonce,
        current_status,
        is_complete: claim_event.is_some(),

        deposit: DepositInfo {
            txn_hash: hex::encode(&deposit.txn_hash),
            block_height: deposit.block_height,
            timestamp_ms: deposit.timestamp_ms,
            sender_address: hex::encode(&deposit.sender_address),
            recipient_address: hex::encode(&deposit.recipient_address),
            token_id: deposit.token_id,
            amount: format_usdt_amount(deposit.amount),
            is_finalized: deposit.is_finalized.unwrap_or(false),
        },

        approval: approval_event.map(|e| ApprovalInfo {
            txn_hash: hex::encode(&e.txn_hash),
            block_height: e.block_height,
            timestamp_ms: e.timestamp_ms,
            data_source: e.data_source.into(),
            is_finalized: e.is_finalized.unwrap_or(false),
        }),

        claim: claim_event.map(|e| ClaimInfo {
            txn_hash: hex::encode(&e.txn_hash),
            block_height: e.block_height,
            timestamp_ms: e.timestamp_ms,
            claimer_address: hex::encode(&e.txn_sender),
            gas_usage: e.gas_usage,
            data_source: e.data_source.into(),
            is_finalized: e.is_finalized.unwrap_or(false),
        }),
    }
}

/// API error response wrapper
pub(crate) struct ApiErrorResponse {
    status: StatusCode,
    body: Json<ApiError>,
}

impl ApiErrorResponse {
    fn new(status: StatusCode, error: &str, message: &str) -> Self {
        Self {
            status,
            body: Json(ApiError {
                error: error.to_string(),
                message: message.to_string(),
            }),
        }
    }

    fn bad_request(message: &str) -> Self {
        Self::new(StatusCode::BAD_REQUEST, "bad_request", message)
    }

    fn not_found(message: &str) -> Self {
        Self::new(StatusCode::NOT_FOUND, "not_found", message)
    }

    fn internal(message: &str) -> Self {
        error!("Internal error: {}", message);
        Self::new(StatusCode::INTERNAL_SERVER_ERROR, "internal_error", message)
    }
}

impl IntoResponse for ApiErrorResponse {
    fn into_response(self) -> Response {
        (self.status, self.body).into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_hex_address_with_0x_prefix() {
        let result = parse_hex_address("0x1234abcd").unwrap();
        assert_eq!(result, vec![0x12, 0x34, 0xab, 0xcd]);
    }

    #[test]
    fn test_parse_hex_address_without_prefix() {
        let result = parse_hex_address("1234abcd").unwrap();
        assert_eq!(result, vec![0x12, 0x34, 0xab, 0xcd]);
    }

    #[test]
    fn test_parse_hex_address_full_address() {
        let result = parse_hex_address("0x09865b8c5fbab038c3476df6b5540cc2").unwrap();
        assert_eq!(result.len(), 16); // 32 hex chars = 16 bytes
    }

    #[test]
    fn test_parse_hex_address_invalid() {
        let result = parse_hex_address("0xGGGG");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Invalid hex"));
    }

    #[test]
    fn test_parse_hex_address_odd_length() {
        // Odd length hex strings should fail
        let result = parse_hex_address("0x123");
        assert!(result.is_err());
    }

    #[test]
    fn test_api_error_response_bad_request() {
        let err = ApiErrorResponse::bad_request("Test error message");
        assert_eq!(err.status, StatusCode::BAD_REQUEST);
        assert_eq!(err.body.0.error, "bad_request");
        assert_eq!(err.body.0.message, "Test error message");
    }

    #[test]
    fn test_api_error_response_not_found() {
        let err = ApiErrorResponse::not_found("Resource not found");
        assert_eq!(err.status, StatusCode::NOT_FOUND);
        assert_eq!(err.body.0.error, "not_found");
        assert_eq!(err.body.0.message, "Resource not found");
    }

    #[test]
    fn test_api_error_response_internal() {
        let err = ApiErrorResponse::internal("Internal server error");
        assert_eq!(err.status, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(err.body.0.error, "internal_error");
        assert_eq!(err.body.0.message, "Internal server error");
    }

    // ========================================================================
    // Tests for build_cross_chain_procedure - validates status completeness
    // ========================================================================

    fn create_mock_deposit(
        chain_id: i32,
        nonce: i64,
    ) -> starcoin_bridge_schema::models::TokenTransferData {
        starcoin_bridge_schema::models::TokenTransferData {
            chain_id,
            nonce,
            block_height: 100,
            timestamp_ms: 1000000,
            txn_hash: vec![0x01, 0x02, 0x03, 0x04],
            sender_address: vec![0x11, 0x22, 0x33, 0x44],
            destination_chain: 12,
            recipient_address: vec![0x55, 0x66, 0x77, 0x88],
            token_id: 1,
            amount: 1000000,
            is_finalized: Some(true),
            monitor_verified: false,
        }
    }

    fn create_mock_status_update(
        chain_id: i32,
        nonce: i64,
        status: starcoin_bridge_schema::models::TokenTransferStatus,
        timestamp_ms: i64,
    ) -> starcoin_bridge_schema::models::TokenTransfer {
        starcoin_bridge_schema::models::TokenTransfer {
            chain_id,
            nonce,
            status,
            block_height: 100 + timestamp_ms / 1000,
            timestamp_ms,
            txn_hash: vec![0xaa, 0xbb, 0xcc, (timestamp_ms % 256) as u8],
            txn_sender: vec![0xdd, 0xee, 0xff, 0x00],
            gas_usage: 50000,
            data_source: starcoin_bridge_schema::models::BridgeDataSource::STARCOIN,
            is_finalized: Some(true),
        }
    }

    #[test]
    fn test_build_procedure_deposited_only() {
        // Test: only deposit event, no approval or claim
        let deposit = create_mock_deposit(254, 0);
        let status_updates = vec![];

        let procedure = build_cross_chain_procedure(&deposit, &status_updates);

        assert_eq!(procedure.source_chain_id, 254);
        assert_eq!(procedure.destination_chain_id, 12);
        assert_eq!(procedure.nonce, 0);
        assert_eq!(procedure.current_status, TransferStatus::Deposited);
        assert!(!procedure.is_complete);
        assert!(procedure.approval.is_none());
        assert!(procedure.claim.is_none());

        // Verify deposit info
        assert_eq!(procedure.deposit.txn_hash, "01020304");
        assert_eq!(procedure.deposit.sender_address, "11223344");
        assert_eq!(procedure.deposit.recipient_address, "55667788");
        assert_eq!(procedure.deposit.amount, "1 USDT");
        assert!(procedure.deposit.is_finalized);
    }

    #[test]
    fn test_build_procedure_approved() {
        // Test: deposit + approval, no claim yet
        let deposit = create_mock_deposit(254, 1);
        let status_updates = vec![create_mock_status_update(
            254,
            1,
            starcoin_bridge_schema::models::TokenTransferStatus::Approved,
            2000000,
        )];

        let procedure = build_cross_chain_procedure(&deposit, &status_updates);

        assert_eq!(procedure.current_status, TransferStatus::Approved);
        assert!(!procedure.is_complete);
        assert!(procedure.approval.is_some());
        assert!(procedure.claim.is_none());

        let approval = procedure.approval.unwrap();
        assert_eq!(approval.timestamp_ms, 2000000);
        assert!(approval.is_finalized);
    }

    #[test]
    fn test_build_procedure_claimed() {
        // Test: complete flow - deposit + approval + claim
        let deposit = create_mock_deposit(254, 2);
        let status_updates = vec![
            create_mock_status_update(
                254,
                2,
                starcoin_bridge_schema::models::TokenTransferStatus::Approved,
                2000000,
            ),
            create_mock_status_update(
                254,
                2,
                starcoin_bridge_schema::models::TokenTransferStatus::Claimed,
                3000000,
            ),
        ];

        let procedure = build_cross_chain_procedure(&deposit, &status_updates);

        assert_eq!(procedure.current_status, TransferStatus::Claimed);
        assert!(procedure.is_complete);
        assert!(procedure.approval.is_some());
        assert!(procedure.claim.is_some());

        let approval = procedure.approval.unwrap();
        assert_eq!(approval.timestamp_ms, 2000000);

        let claim = procedure.claim.unwrap();
        assert_eq!(claim.timestamp_ms, 3000000);
        assert_eq!(claim.gas_usage, 50000);
    }

    #[test]
    fn test_build_procedure_all_statuses_present() {
        // Test: verify all three statuses can be present in one procedure
        // This validates the frontend requirement for deposited, approved, claimed
        let deposit = create_mock_deposit(254, 3);
        let status_updates = vec![
            create_mock_status_update(
                254,
                3,
                starcoin_bridge_schema::models::TokenTransferStatus::Deposited,
                1000000,
            ),
            create_mock_status_update(
                254,
                3,
                starcoin_bridge_schema::models::TokenTransferStatus::Approved,
                2000000,
            ),
            create_mock_status_update(
                254,
                3,
                starcoin_bridge_schema::models::TokenTransferStatus::Claimed,
                3000000,
            ),
        ];

        let procedure = build_cross_chain_procedure(&deposit, &status_updates);

        // Current status should be claimed (most advanced)
        assert_eq!(procedure.current_status, TransferStatus::Claimed);
        assert!(procedure.is_complete);

        // Deposit info is always present from token_transfer_data
        assert!(!procedure.deposit.txn_hash.is_empty());

        // Approval info should be present
        assert!(procedure.approval.is_some());
        let approval = procedure.approval.as_ref().unwrap();
        assert_eq!(approval.timestamp_ms, 2000000);

        // Claim info should be present
        assert!(procedure.claim.is_some());
        let claim = procedure.claim.as_ref().unwrap();
        assert_eq!(claim.timestamp_ms, 3000000);
    }

    #[test]
    fn test_build_procedure_status_order_irrelevant() {
        // Test: status updates can be in any order, logic should still work
        let deposit = create_mock_deposit(254, 4);
        // Intentionally put claim before approval in the array
        let status_updates = vec![
            create_mock_status_update(
                254,
                4,
                starcoin_bridge_schema::models::TokenTransferStatus::Claimed,
                3000000,
            ),
            create_mock_status_update(
                254,
                4,
                starcoin_bridge_schema::models::TokenTransferStatus::Approved,
                2000000,
            ),
        ];

        let procedure = build_cross_chain_procedure(&deposit, &status_updates);

        // Should still detect claimed as current status
        assert_eq!(procedure.current_status, TransferStatus::Claimed);
        assert!(procedure.is_complete);
        assert!(procedure.approval.is_some());
        assert!(procedure.claim.is_some());
    }

    #[test]
    fn test_build_procedure_json_serialization() {
        // Test: verify the response can be properly serialized to JSON
        // This ensures frontend can parse the response correctly
        let deposit = create_mock_deposit(254, 5);
        let status_updates = vec![create_mock_status_update(
            254,
            5,
            starcoin_bridge_schema::models::TokenTransferStatus::Approved,
            2000000,
        )];

        let procedure = build_cross_chain_procedure(&deposit, &status_updates);
        let json = serde_json::to_string(&procedure).unwrap();

        // Verify status is serialized as snake_case
        assert!(json.contains(r#""current_status":"approved""#));

        // Verify deposit info is present
        assert!(json.contains(r#""deposit":"#));
        assert!(json.contains(r#""sender_address""#));
        assert!(json.contains(r#""recipient_address""#));

        // Verify approval is present
        assert!(json.contains(r#""approval":"#));

        // Verify claim is null (not present)
        assert!(json.contains(r#""claim":null"#));
    }

    #[test]
    fn test_transfer_list_response_uses_cross_chain_procedure() {
        // Test: verify TransferListResponse uses CrossChainProcedure format
        let deposit = create_mock_deposit(254, 6);
        let status_updates = vec![create_mock_status_update(
            254,
            6,
            starcoin_bridge_schema::models::TokenTransferStatus::Approved,
            2000000,
        )];

        let procedure = build_cross_chain_procedure(&deposit, &status_updates);

        let response = TransferListResponse {
            transfers: vec![procedure],
            pagination: Pagination {
                page: 1,
                page_size: 20,
                total_count: 1,
                total_pages: 1,
            },
            claim_delay_seconds: 300,
        };

        let json = serde_json::to_string(&response).unwrap();

        // Verify the response contains the full procedure format
        assert!(json.contains(r#""source_chain_id""#));
        assert!(json.contains(r#""destination_chain_id""#));
        assert!(json.contains(r#""current_status""#));
        assert!(json.contains(r#""deposit""#));
        assert!(json.contains(r#""approval""#));
        assert!(json.contains(r#""claim""#));
        assert!(json.contains(r#""claim_delay_seconds":300"#));
    }

    // ========================================================================
    // Tests for memory merge functionality
    // ========================================================================

    use starcoin_bridge::pending_events::{
        ChainId as MemChainId, DepositInfo as MemDeposit, TransferKey, TransferRecord,
        TransferStatus as MemStatus,
    };

    fn create_mem_deposit_info(
        tx_hash: &str,
        block_number: u64,
        amount: u64,
        sender: &str,
        recipient: &str,
    ) -> MemDeposit {
        MemDeposit {
            tx_hash: tx_hash.to_string(),
            block_number,
            timestamp_ms: block_number * 1000,
            destination_chain: MemChainId::Eth,
            token_id: 3, // USDT
            amount,
            sender_address: sender.to_string(),
            recipient_address: recipient.to_string(),
            is_finalized: false,
        }
    }

    fn create_mem_transfer_record(
        chain_id: MemChainId,
        nonce: u64,
        status: MemStatus,
        deposit: Option<MemDeposit>,
    ) -> TransferRecord {
        TransferRecord {
            key: TransferKey::new(chain_id, nonce),
            status,
            deposit,
            approval: None,
            claim: None,
        }
    }

    #[test]
    fn test_build_procedure_from_memory_basic() {
        let deposit = create_mem_deposit_info(
            "0xabc123",
            100,
            1_000_000, // 1 USDT in 6 decimals
            "0x1111",
            "0x2222",
        );
        let record = create_mem_transfer_record(
            MemChainId::Starcoin,
            1,
            MemStatus::Deposited,
            Some(deposit),
        );

        let procedure = build_procedure_from_memory(&record, NetworkType::Local);
        assert!(procedure.is_some());

        let proc = procedure.unwrap();
        assert_eq!(proc.source_chain_id, 2); // STARCOIN
        assert_eq!(proc.destination_chain_id, 12); // ETH
        assert_eq!(proc.nonce, 1);
        assert_eq!(proc.current_status, TransferStatus::Deposited);
        assert!(!proc.is_complete);
        assert!(!proc.deposit.is_finalized);
        assert!(proc.approval.is_none());
        assert!(proc.claim.is_none());
    }

    #[test]
    fn test_build_procedure_from_memory_no_deposit() {
        // Record without deposit info should return None
        let record = create_mem_transfer_record(MemChainId::Eth, 1, MemStatus::Deposited, None);

        let procedure = build_procedure_from_memory(&record, NetworkType::Local);
        assert!(procedure.is_none());
    }

    #[test]
    fn test_merge_empty_db_with_memory() {
        let db_procedures: Vec<CrossChainProcedure> = vec![];

        let deposit =
            create_mem_deposit_info("0xmem1", 100, 100_000_000, "0xsender", "0xrecipient");
        let mem_records = vec![create_mem_transfer_record(
            MemChainId::Starcoin,
            1,
            MemStatus::Deposited,
            Some(deposit),
        )];

        let merged = merge_with_memory_records(db_procedures, mem_records, NetworkType::Local);

        assert_eq!(merged.len(), 1);
        assert_eq!(merged[0].nonce, 1);
        assert_eq!(merged[0].deposit.txn_hash, "0xmem1");
    }

    #[test]
    fn test_merge_db_only() {
        let deposit = create_mock_deposit(2, 1);
        let db_procedures = vec![build_cross_chain_procedure(&deposit, &[])];
        let mem_records: Vec<TransferRecord> = vec![];

        let merged = merge_with_memory_records(db_procedures, mem_records, NetworkType::Local);

        assert_eq!(merged.len(), 1);
        assert_eq!(merged[0].nonce, 1);
    }

    #[test]
    fn test_merge_updates_status_from_memory() {
        // DB has deposit only
        let db_deposit = create_mock_deposit(2, 1);
        let db_procedures = vec![build_cross_chain_procedure(&db_deposit, &[])];

        // Memory has approved status
        let mem_deposit =
            create_mem_deposit_info("01020304", 100, 100_000_000, "11223344", "55667788");
        let mut mem_record = create_mem_transfer_record(
            MemChainId::Starcoin,
            1,
            MemStatus::Approved,
            Some(mem_deposit),
        );
        mem_record.approval = Some(starcoin_bridge::pending_events::ApprovalInfo {
            tx_hash: "0xapproval".to_string(),
            block_number: 200,
            timestamp_ms: 200000,
            recorded_chain: MemChainId::Eth,
            is_finalized: false,
        });
        let mem_records = vec![mem_record];

        let merged = merge_with_memory_records(db_procedures, mem_records, NetworkType::Local);

        assert_eq!(merged.len(), 1);
        assert_eq!(merged[0].current_status, TransferStatus::Approved);
        assert!(merged[0].approval.is_some());
    }

    #[test]
    fn test_merge_keeps_higher_status() {
        // DB has claimed status
        let db_deposit = create_mock_deposit(2, 1);
        let db_status_updates = vec![
            create_mock_status_update(
                2,
                1,
                starcoin_bridge_schema::models::TokenTransferStatus::Approved,
                2000000,
            ),
            create_mock_status_update(
                2,
                1,
                starcoin_bridge_schema::models::TokenTransferStatus::Claimed,
                3000000,
            ),
        ];
        let db_procedures = vec![build_cross_chain_procedure(&db_deposit, &db_status_updates)];

        // Memory has only deposited (older state)
        let mem_deposit =
            create_mem_deposit_info("01020304", 100, 100_000_000, "11223344", "55667788");
        let mem_records = vec![create_mem_transfer_record(
            MemChainId::Starcoin,
            1,
            MemStatus::Deposited,
            Some(mem_deposit),
        )];

        let merged = merge_with_memory_records(db_procedures, mem_records, NetworkType::Local);

        assert_eq!(merged.len(), 1);
        // Should keep claimed status from DB (higher)
        assert_eq!(merged[0].current_status, TransferStatus::Claimed);
        assert!(merged[0].is_complete);
    }

    #[test]
    fn test_should_update_status() {
        // Deposited -> Approved: should update
        assert!(should_update_status(
            &TransferStatus::Deposited,
            &TransferStatus::Approved
        ));

        // Approved -> Claimed: should update
        assert!(should_update_status(
            &TransferStatus::Approved,
            &TransferStatus::Claimed
        ));

        // Deposited -> Claimed: should update
        assert!(should_update_status(
            &TransferStatus::Deposited,
            &TransferStatus::Claimed
        ));

        // Same status: should not update
        assert!(!should_update_status(
            &TransferStatus::Approved,
            &TransferStatus::Approved
        ));

        // Higher to lower: should not update
        assert!(!should_update_status(
            &TransferStatus::Claimed,
            &TransferStatus::Approved
        ));
    }

    #[test]
    fn test_mem_chain_id_conversion() {
        assert_eq!(
            mem_chain_id_to_i32(MemChainId::Starcoin, NetworkType::Local),
            2
        );
        assert_eq!(mem_chain_id_to_i32(MemChainId::Eth, NetworkType::Local), 12);
    }

    #[test]
    fn test_mem_chain_id_conversion_all_networks() {
        // Testnet
        assert_eq!(
            mem_chain_id_to_i32(MemChainId::Starcoin, NetworkType::Testnet),
            1
        );
        assert_eq!(
            mem_chain_id_to_i32(MemChainId::Eth, NetworkType::Testnet),
            11
        );
        // Mainnet
        assert_eq!(
            mem_chain_id_to_i32(MemChainId::Starcoin, NetworkType::Mainnet),
            0
        );
        assert_eq!(
            mem_chain_id_to_i32(MemChainId::Eth, NetworkType::Mainnet),
            10
        );
    }

    #[test]
    fn test_build_procedure_from_memory_eth_source() {
        // Verify ETH-sourced transfers get correct chain IDs (this was the original bug)
        let deposit = MemDeposit {
            tx_hash: "0xeth_deposit".to_string(),
            block_number: 500,
            timestamp_ms: 500000,
            destination_chain: MemChainId::Starcoin,
            token_id: 3,
            amount: 100_000_000,
            sender_address: "0xEthSender".to_string(),
            recipient_address: "0xStcRecipient".to_string(),
            is_finalized: false,
        };
        let record =
            create_mem_transfer_record(MemChainId::Eth, 42, MemStatus::Deposited, Some(deposit));

        let procedure = build_procedure_from_memory(&record, NetworkType::Local);
        assert!(procedure.is_some());

        let proc = procedure.unwrap();
        // This is the critical assertion: source=ETH should NOT be in Starcoin range
        assert_eq!(proc.source_chain_id, 12); // EthCustom, NOT 1 (which was the bug)
        assert_eq!(proc.destination_chain_id, 2); // StarcoinCustom
        assert_eq!(proc.nonce, 42);
    }

    #[test]
    fn test_build_procedure_chain_ids_testnet() {
        let deposit =
            create_mem_deposit_info("0xtest", 100, 100_000_000, "0xsender", "0xrecipient");
        let record = create_mem_transfer_record(
            MemChainId::Starcoin,
            1,
            MemStatus::Deposited,
            Some(deposit),
        );

        let procedure = build_procedure_from_memory(&record, NetworkType::Testnet);
        let proc = procedure.unwrap();
        assert_eq!(proc.source_chain_id, 1); // StarcoinTestnet
        assert_eq!(proc.destination_chain_id, 11); // EthSepolia
    }

    #[test]
    fn test_format_usdt_amount_u64() {
        // For USDT, starcoinDecimal == erc20Decimal == 6, so the bridge event
        // amount is already in 6-decimal precision (no adjustment).
        // 1 USDT in 6 decimals = 1_000_000
        assert_eq!(format_usdt_amount_u64(1_000_000), "1 USDT");

        // 1.5 USDT in 6 decimals = 1_500_000
        assert_eq!(format_usdt_amount_u64(1_500_000), "1.5 USDT");

        // 0.123456 USDT in 6 decimals = 123_456
        assert_eq!(format_usdt_amount_u64(123_456), "0.123456 USDT");

        // 100 USDT in 6 decimals = 100_000_000
        assert_eq!(format_usdt_amount_u64(100_000_000), "100 USDT");
    }
}
