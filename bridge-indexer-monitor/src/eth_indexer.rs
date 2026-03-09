// Copyright (c) Starcoin, Inc.
// SPDX-License-Identifier: Apache-2.0

//! ETH Bridge Indexer module
//!
//! This module provides functionality to index Ethereum bridge events
//! and store them in the database using the unified EthChainSyncer.
//!
//! ## Architecture (using EthChainSyncer)
//!
//! ```text
//!                      ┌─────────────────────────────┐
//!                      │  EthChainSyncer             │
//!                      │  - Event sync               │
//!                      │  - Reorg detection          │
//!                      │  - Finality tracking        │
//!                      └─────────────┬───────────────┘
//!                                    │
//!                                    ▼ SyncerEvent
//!                      ┌─────────────────────────────┐
//!                      │     Event Processor         │
//!                      │  is_finalized=true: DB      │
//!                      │  is_finalized=false: Memory │
//!                      └─────────────┬───────────────┘
//!                          ┌─────────┴─────────┐
//!                          │                   │
//!                          ▼                   ▼
//!                   ┌────────────┐      ┌─────────────────┐
//!                   │  Monitor   │      │  TransferTracker │
//!                   │  (alerts)  │      │  (pending events)│
//!                   └────────────┘      └─────────────────┘
//! ```
//!
//! ## Features
//!
//! - Built-in reorg detection via BlockWindow
//! - Event-based output (SyncerEvent)
//! - Automatic DB cleanup on reorg
//! - Graceful shutdown with CancellationToken
//! - Memory-based storage for unfinalized events

use std::collections::HashSet;
use std::sync::Arc;

use crate::api::{mark_quota_stale, update_fee_cache_async};
use crate::caught_up::ChainCaughtUpTracker;
use crate::indexer_progress::{IndexerProgressStore, ETH_INDEXER_TASK_NAME};
use crate::monitor::reorg_handler::{EthBridgeReorgHandler, EthReorgHandlerConfig};
use crate::network::NetworkType;
use crate::telegram::{create_notify_events_from_record, BridgeNotifyEvent, NotifyChain, SharedTelegramNotifier};
use anyhow::{anyhow, Context, Result};
use diesel_async::RunQueryDsl;
use ethers::types::Address as EthAddress;
use starcoin_bridge::abi::{
    EthBridgeCommitteeEvents, EthBridgeEvent, EthBridgeLimiterEvents, EthStarcoinBridgeEvents,
    EthToStarcoinTokenBridgeV1,
};
use starcoin_bridge::chain_syncer::{EthChainSyncerBuilder, ReorgHandler, SyncerEvent};
use starcoin_bridge::eth_client::EthClient;
use starcoin_bridge::metrics::BridgeMetrics;
use starcoin_bridge::pending_events::{
    ApprovalEvent, ChainId, ClaimEvent, DepositEvent, PendingEvent, PendingEventType,
    TransferRecord, TransferTracker,
};
use starcoin_bridge::types::EthLog;
use starcoin_bridge_pg_db::Db;
use starcoin_bridge_schema::models::{
    BridgeDataSource, TokenTransfer, TokenTransferData, TokenTransferStatus,
};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

/// Result containing handles and shared components
pub struct EthIndexerResult {
    /// Task handles for the indexer (syncer tasks + DB writer + finalizer)
    pub handles: Vec<JoinHandle<()>>,
    /// Cancellation token for graceful shutdown
    pub cancel: CancellationToken,
    /// Reorg handler for DB cleanup and notifications
    pub reorg_handler: Arc<EthBridgeReorgHandler>,
    /// Transfer tracker for pending (unfinalized) events - shared with API
    pub transfer_tracker: Arc<TransferTracker>,
    /// Caught-up tracker to signal when syncer is up to date
    pub caught_up_tracker: Arc<ChainCaughtUpTracker>,
}

/// Configuration for the ETH indexer
#[derive(Clone)]
pub struct EthIndexerConfig {
    pub eth_rpc_url: String,
    pub eth_bridge_address: String,
    pub eth_start_block: u64,
    /// Network type for correct bridge chain ID mapping
    pub network: NetworkType,
    /// Number of blocks considered finalized (default: 64 for ETH mainnet)
    pub finality_blocks: Option<u64>,
    /// Enable reorg detection and DB sync (default: true)
    pub enable_reorg_detection: Option<bool>,
    /// Shared transfer tracker (if None, creates new one)
    pub transfer_tracker: Option<Arc<TransferTracker>>,
    /// Telegram notifier for sending alerts
    pub telegram: Option<SharedTelegramNotifier>,
    /// Caught-up tracker for signaling when syncer is up to date
    pub caught_up_tracker: Option<Arc<ChainCaughtUpTracker>>,
    /// Security monitor for mismatch detection
    pub security_monitor: Option<crate::security_monitor::SharedSecurityMonitor>,
}

/// Start the unified ETH indexer with shared event broadcasting
///
/// This creates:
/// 1. A 'latest' syncer that broadcasts events to all subscribers (Monitor + Indexer)
/// 2. A 'finalized' syncer (on mainnet/testnet) to update is_finalized flag
///
/// ## Watermark Handling (Automatic)
/// Uses IndexerProgressStore to track progress:
/// - Reads watermark from progress_store table
/// - Falls back to max finalized event block from token_transfer
/// - Starts from max(config_start_block, watermark+1, max_finalized_event+1)
///
/// ## Finality-aware event processing
/// - is_finalized=true: Write directly to PostgreSQL
/// - is_finalized=false: Store in TransferTracker (memory) only
/// - BlockFinalized: Drain TransferTracker to DB
pub async fn start_unified_eth_indexer(
    config: EthIndexerConfig,
    db: Db,
    bridge_metrics: Arc<BridgeMetrics>,
) -> Result<EthIndexerResult> {
    info!("[ETH] Starting unified ETH indexer with EthChainSyncer...");
    info!("  ETH RPC URL: {}", config.eth_rpc_url);
    info!("  Bridge Address: {}", config.eth_bridge_address);
    info!("  Start Block: {}", config.eth_start_block);
    info!(
        "  Reorg detection: {}",
        config.enable_reorg_detection.unwrap_or(true)
    );

    let bridge_address: EthAddress = config
        .eth_bridge_address
        .parse()
        .context("Failed to parse eth_bridge_address")?;

    // Create cancellation token for graceful shutdown
    let cancel = CancellationToken::new();

    // Create or use provided caught-up tracker
    let caught_up_tracker = config
        .caught_up_tracker
        .clone()
        .unwrap_or_else(|| Arc::new(ChainCaughtUpTracker::new("ETH")));

    // Create or use provided transfer tracker
    let transfer_tracker = config
        .transfer_tracker
        .clone()
        .unwrap_or_else(|| Arc::new(TransferTracker::new()));

    // Get telegram notifier
    let telegram = config.telegram.clone();

    // Create progress store and auto-detect start block
    let progress_store = IndexerProgressStore::new(db.clone());
    let start_block = progress_store
        .get_eth_start_block(config.eth_start_block)
        .await;
    info!("[ETH] Auto-detected start block: {}", start_block);

    // Create EthClient - always use finalized mode for proper finality tracking
    let eth_client = Arc::new(
        EthClient::new_with_options(
            &config.eth_rpc_url,
            HashSet::from([bridge_address]),
            bridge_metrics.clone(),
            None,
            Some(true), // Always use finalized block tag
        )
        .await
        .map_err(|e| anyhow!("Failed to create ETH client: {:?}", e))?,
    );

    // Build syncer configuration - always use finality mode
    let finality_blocks = config.finality_blocks.unwrap_or(64);
    let enable_reorg = config.enable_reorg_detection.unwrap_or(true);

    let syncer_builder = EthChainSyncerBuilder::new("ETH", &config.eth_rpc_url)
        .with_contract(&format!("{:?}", bridge_address), start_block)
        .with_metrics(bridge_metrics.clone())
        .with_reorg_detection(enable_reorg)
        .with_finality_blocks(finality_blocks);

    let mut syncer_builder = syncer_builder;

    // Get additional contract addresses
    let provider = eth_client.provider();
    if let Ok(contract_addresses) =
        starcoin_bridge::utils::get_eth_contract_addresses(bridge_address, &provider).await
    {
        info!("Found ETH contract addresses:");
        info!("  Committee: {:?}", contract_addresses.0);
        info!("  Limiter: {:?}", contract_addresses.1);
        info!("  Config: {:?}", contract_addresses.3);

        syncer_builder = syncer_builder
            .with_contract(&format!("{:?}", contract_addresses.0), start_block)
            .with_contract(&format!("{:?}", contract_addresses.1), start_block)
            .with_contract(&format!("{:?}", contract_addresses.3), start_block);
    }

    // Build the syncer
    let syncer = syncer_builder.build(eth_client.clone())?;

    // Always run with reorg detection for proper finality tracking
    let (mut handles, event_rx) = syncer.run_with_reorg_detection(cancel.clone()).await?;

    // Create reorg handler
    let reorg_handler = Arc::new(EthBridgeReorgHandler::with_db(
        EthReorgHandlerConfig {
            chain_name: "ETH".to_string(),
            data_source: BridgeDataSource::ETH,
            enable_db_sync: enable_reorg,
            enable_notifications: false, // Telegram configured separately via Monitor
        },
        db.clone(),
    ));

    // Spawn event processing task
    let db_clone = db.clone();
    let reorg_handler_clone = reorg_handler.clone();
    let cancel_clone = cancel.clone();
    let eth_rpc_url = config.eth_rpc_url.clone();
    let transfer_tracker_clone = transfer_tracker.clone();
    let caught_up_tracker_clone = caught_up_tracker.clone();
    let telegram_clone = telegram.clone();

    let security_monitor = config.security_monitor.clone();
    let network = config.network;
    let processor_handle = tokio::spawn(async move {
        process_syncer_events(
            event_rx,
            db_clone,
            reorg_handler_clone,
            cancel_clone,
            eth_rpc_url,
            transfer_tracker_clone,
            caught_up_tracker_clone,
            telegram_clone,
            security_monitor,
            network,
        )
        .await;
    });
    handles.push(processor_handle);

    info!("[ETH] ETH indexer started successfully");

    Ok(EthIndexerResult {
        handles,
        cancel,
        reorg_handler,
        transfer_tracker,
        caught_up_tracker,
    })
}

/// Process events from EthChainSyncer
///
/// Finality-aware processing:
/// - is_finalized=true: write to DB immediately
/// - is_finalized=false: store in TransferTracker (memory) only
/// - BlockFinalized: drain TransferTracker and write to DB
/// - CaughtUp: signal that syncer is up to date
async fn process_syncer_events(
    mut event_rx: tokio::sync::mpsc::Receiver<SyncerEvent>,
    db: Db,
    reorg_handler: Arc<EthBridgeReorgHandler>,
    cancel: CancellationToken,
    eth_rpc_url: String,
    transfer_tracker: Arc<TransferTracker>,
    caught_up_tracker: Arc<ChainCaughtUpTracker>,
    telegram: Option<SharedTelegramNotifier>,
    security_monitor: Option<crate::security_monitor::SharedSecurityMonitor>,
    network: NetworkType,
) {
    info!("[ETH] Starting event processor");

    // Create progress store for watermark updates
    let progress_store = IndexerProgressStore::new(db.clone());

    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                info!("[ETH] Event processor cancelled");
                break;
            }
            event = event_rx.recv() => {
                let Some(event) = event else {
                    info!("[ETH] Event channel closed");
                    break;
                };

                match event {
                    SyncerEvent::NewLogs { contract, start_block, end_block, logs, is_finalized, .. } => {
                        debug!(
                            "[ETH] Received {} logs from {} (blocks {}-{}, finalized={})",
                            logs.len(), contract, start_block, end_block, is_finalized
                        );

                        // Convert ChainLogs to EthLogs for processing and broadcasting
                        let eth_logs: Vec<EthLog> = logs
                            .iter()
                            .filter_map(chain_log_to_eth_log)
                            .collect();

                        if is_finalized {
                            // Finalized events: write directly to DB
                            for eth_log in &eth_logs {
                                if let Err(e) = process_eth_log_to_db(eth_log, &db, &eth_rpc_url).await {
                                    error!("[ETH] Failed to process log to DB: {:?}", e);
                                }
                            }

                            // Update watermark
                            if let Err(e) = progress_store.update_watermark(ETH_INDEXER_TASK_NAME, end_block).await {
                                warn!("[ETH] Failed to update watermark: {:?}", e);
                            }
                        } else {
                            // Unfinalized events: store in memory only
                            info!(
                                "[ETH] Storing {} unfinalized logs in memory (blocks {}-{})",
                                eth_logs.len(),
                                start_block,
                                end_block
                            );

                            for eth_log in &eth_logs {
                                if let Err(e) = process_eth_log_to_memory(eth_log, &transfer_tracker, &security_monitor).await {
                                    error!("[ETH] Failed to store log in memory: {:?}", e);
                                }
                            }
                        }

                        // Send telegram notification for finalized events
                        if is_finalized {
                            if let Some(ref tg) = telegram {
                                for eth_log in &eth_logs {
                                    if let Some(notify_event) = create_telegram_event_from_eth_log(eth_log) {
                                        let _ = tg.notify_finalized(NotifyChain::Eth, &notify_event).await;
                                    }
                                }
                            }
                        }
                    }
                    SyncerEvent::Reorg(reorg_info) => {
                        warn!(
                            "[ETH] Reorg detected at fork_point={}, depth={}, orphaned_logs={}",
                            reorg_info.fork_point,
                            reorg_info.depth,
                            reorg_info.orphaned_logs.len()
                        );

                        // Handle reorg in TransferTracker (memory cleanup)
                        transfer_tracker.handle_reorg(reorg_info.fork_point).await;

                        // Handle reorg using the ReorgHandler (DB cleanup)
                        if let Err(e) = reorg_handler.handle_reorg(&reorg_info).await {
                            error!("[ETH] Failed to handle reorg: {}", e);
                        }

                        // Send telegram notification for reorg
                        if let Some(ref tg) = telegram {
                            let _ = tg.notify_reorg(
                                NotifyChain::Eth,
                                reorg_info.fork_point,
                                reorg_info.depth as u64,
                                reorg_info.orphaned_logs.len(),
                            ).await;
                        }
                    }
                    SyncerEvent::BlockFinalized { block_id } => {
                        debug!("[ETH] Block finalized: {}", block_id.number);

                        // Finalize and persist using "DB-first" pattern (memory deleted only after DB success)
                        finalize_and_persist_records_for_eth(
                            block_id.number,
                            &transfer_tracker,
                            &db,
                            &telegram,
                            network,
                        ).await;

                        if let Err(e) = reorg_handler.on_block_finalized(&block_id).await {
                            error!("[ETH] Failed to mark block finalized: {}", e);
                        }

                        // Update watermark when block is finalized
                        if let Err(e) = progress_store.update_watermark(ETH_INDEXER_TASK_NAME, block_id.number).await {
                            warn!("[ETH] Failed to update watermark on block finalized: {:?}", e);
                        }
                    }
                    SyncerEvent::FinalizedHeightUpdated { height, .. } => {
                        debug!("[ETH] Finalized height updated: {}", height);

                        // Finalize and persist using "DB-first" pattern (memory deleted only after DB success)
                        finalize_and_persist_records_for_eth(
                            height,
                            &transfer_tracker,
                            &db,
                            &telegram,
                            network,
                        ).await;

                        // Update watermark when finalized height is updated
                        if let Err(e) = progress_store.update_watermark(ETH_INDEXER_TASK_NAME, height).await {
                            warn!("[ETH] Failed to update watermark on finalized height: {:?}", e);
                        }
                    }
                    SyncerEvent::SyncError { error, recoverable, .. } => {
                        if recoverable {
                            warn!("[ETH] Sync error (recoverable): {}", error);
                        } else {
                            error!("[ETH] Sync error (non-recoverable): {}", error);
                        }
                    }
                    SyncerEvent::Started { from_block, .. } => {
                        info!("[ETH] Syncer started from block {}", from_block);
                    }
                    SyncerEvent::Stopped { reason, .. } => {
                        info!("[ETH] Syncer stopped: {}", reason);
                    }
                    SyncerEvent::CaughtUp { height, .. } => {
                        info!("[ETH] Syncer caught up to chain head at block {}", height);
                        caught_up_tracker.set_caught_up();
                    }
                    _ => {}
                }
            }
        }
    }
}

/// Create telegram event from ETH log (for notifications)
fn create_telegram_event_from_eth_log(log: &EthLog) -> Option<BridgeNotifyEvent> {
    let event = EthBridgeEvent::try_from_eth_log(log)?;

    match event {
        EthBridgeEvent::EthStarcoinBridgeEvents(bridge_event) => match bridge_event {
            EthStarcoinBridgeEvents::TokensDepositedFilter(deposit) => {
                let bridge_event = EthToStarcoinTokenBridgeV1::try_from(&deposit).ok()?;
                Some(BridgeNotifyEvent::Deposit {
                    source_chain_id: bridge_event.eth_chain_id as u8,
                    destination_chain_id: bridge_event.starcoin_bridge_chain_id as u8,
                    nonce: bridge_event.nonce,
                    token_id: bridge_event.token_id,
                    amount: bridge_event.starcoin_bridge_adjusted_amount,
                    sender_address: format!("{:?}", bridge_event.eth_address),
                    recipient_address: hex::encode(bridge_event.starcoin_bridge_address),
                    tx_hash: format!("{:?}", log.tx_hash),
                    block_number: log.block_number,
                })
            }
            EthStarcoinBridgeEvents::TokensClaimedFilter(claim) => {
                Some(BridgeNotifyEvent::Claim {
                    source_chain_id: claim.source_chain_id,
                    destination_chain_id: 12, // ETH Custom
                    nonce: claim.nonce,
                    token_id: claim.token_id,
                    amount: claim.erc_20_adjusted_amount.as_u64(),
                    recipient_address: format!("{:?}", claim.recipient_address),
                    tx_hash: format!("{:?}", log.tx_hash),
                    block_number: log.block_number,
                })
            }
            EthStarcoinBridgeEvents::PausedFilter(_) => Some(BridgeNotifyEvent::Emergency {
                paused: true,
                nonce: 0,
                tx_hash: format!("{:?}", log.tx_hash),
                block_number: log.block_number,
            }),
            EthStarcoinBridgeEvents::UnpausedFilter(_) => Some(BridgeNotifyEvent::Emergency {
                paused: false,
                nonce: 0,
                tx_hash: format!("{:?}", log.tx_hash),
                block_number: log.block_number,
            }),
            _ => None,
        },
        EthBridgeEvent::EthBridgeLimiterEvents(limiter_event) => match limiter_event {
            EthBridgeLimiterEvents::LimitUpdatedFilter(limit) => {
                Some(BridgeNotifyEvent::LimitUpdated {
                    source_chain_id: limit.source_chain_id,
                    new_limit: limit.new_limit,
                    nonce: 0,
                    tx_hash: format!("{:?}", log.tx_hash),
                    block_number: log.block_number,
                })
            }
            EthBridgeLimiterEvents::LimitUpdatedV2Filter(limit) => {
                Some(BridgeNotifyEvent::LimitUpdated {
                    source_chain_id: limit.source_chain_id,
                    new_limit: limit.new_limit,
                    nonce: limit.nonce,
                    tx_hash: format!("{:?}", log.tx_hash),
                    block_number: log.block_number,
                })
            }
            _ => None,
        },
        EthBridgeEvent::EthBridgeCommitteeEvents(committee_event) => match committee_event {
            EthBridgeCommitteeEvents::BlocklistUpdatedFilter(blocklist) => {
                Some(BridgeNotifyEvent::BlocklistUpdated {
                    members: blocklist
                        .updated_members
                        .iter()
                        .map(|m| format!("{:?}", m))
                        .collect(),
                    is_blocklisted: blocklist.is_blocklisted,
                    nonce: 0,
                    tx_hash: format!("{:?}", log.tx_hash),
                    block_number: log.block_number,
                })
            }
            EthBridgeCommitteeEvents::BlocklistUpdatedV2Filter(blocklist) => {
                Some(BridgeNotifyEvent::BlocklistUpdated {
                    members: blocklist
                        .updated_members
                        .iter()
                        .map(|m| format!("{:?}", m))
                        .collect(),
                    is_blocklisted: blocklist.is_blocklisted,
                    nonce: blocklist.nonce,
                    tx_hash: format!("{:?}", log.tx_hash),
                    block_number: log.block_number,
                })
            }
            _ => None,
        },
        _ => None,
    }
}

/// Convert ChainLog to EthLog
/// Note: This is a simplified conversion - full implementation would parse raw log data
fn chain_log_to_eth_log(chain_log: &starcoin_bridge::chain_syncer::ChainLog) -> Option<EthLog> {
    use ethers::types::{Log, H256, U256, U64};

    // Parse tx_hash
    let tx_hash = chain_log.tx_hash.parse::<H256>().ok()?;

    // Parse topics
    let topics: Vec<H256> = chain_log
        .topics
        .iter()
        .filter_map(|t| t.parse::<H256>().ok())
        .collect();

    // Create ethers Log
    let log = Log {
        address: chain_log.emitter.parse().ok()?,
        topics,
        data: chain_log.data.clone().into(),
        block_hash: None,
        block_number: Some(U64::from(chain_log.block_id.number)),
        transaction_hash: Some(tx_hash),
        transaction_index: None,
        log_index: Some(U256::from(chain_log.log_index)),
        transaction_log_index: None,
        log_type: None,
        removed: Some(false),
    };

    Some(EthLog {
        block_number: chain_log.block_id.number,
        tx_hash,
        log_index_in_tx: chain_log.log_index as u16,
        log,
    })
}

// ============================================================================
// Event Processing Functions
// ============================================================================

/// Process ETH log and store in memory (for unfinalized events)
async fn process_eth_log_to_memory(
    log: &EthLog,
    tracker: &TransferTracker,
    security_monitor: &Option<crate::security_monitor::SharedSecurityMonitor>,
) -> Result<()> {
    let event = match EthBridgeEvent::try_from_eth_log(log) {
        Some(e) => e,
        None => {
            debug!("Could not parse ETH log as bridge event: {:?}", log.tx_hash);
            return Ok(());
        }
    };

    let timestamp_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;

    match event {
        EthBridgeEvent::EthStarcoinBridgeEvents(bridge_event) => match bridge_event {
            EthStarcoinBridgeEvents::TokensDepositedFilter(deposit) => {
                info!(
                    "[ETH] Processing PENDING deposit: nonce={}, block={}",
                    deposit.nonce, log.block_number
                );

                let bridge_event = EthToStarcoinTokenBridgeV1::try_from(&deposit)
                    .map_err(|e| anyhow!("Failed to convert deposit event: {:?}", e))?;

                let deposit_event = DepositEvent {
                    source_chain: ChainId::Eth,
                    destination_chain: ChainId::Starcoin,
                    nonce: bridge_event.nonce,
                    token_id: bridge_event.token_id,
                    amount: bridge_event.starcoin_bridge_adjusted_amount,
                    sender_address: hex::encode(bridge_event.eth_address.as_bytes()),
                    recipient_address: hex::encode(bridge_event.starcoin_bridge_address),
                    block_number: log.block_number,
                };

                let pending_event = PendingEvent {
                    event: PendingEventType::Deposit(deposit_event.clone()),
                    tx_hash: format!("{:?}", log.tx_hash),
                    block_number: log.block_number,
                    timestamp_ms,
                    observed_chain: ChainId::Eth,
                };

                tracker.on_deposit(&pending_event, &deposit_event).await;
            }
            EthStarcoinBridgeEvents::TokensClaimedFilter(claim) => {
                info!(
                    "[ETH] Processing PENDING claim: nonce={}, block={}",
                    claim.nonce, log.block_number
                );

                let claim_event = ClaimEvent {
                    source_chain: ChainId::from(claim.source_chain_id),
                    destination_chain: ChainId::Eth,
                    nonce: claim.nonce,
                    token_id: claim.token_id,
                    amount: claim.erc_20_adjusted_amount.as_u64(),
                    sender_address: String::new(),
                    recipient_address: hex::encode(claim.recipient_address.as_bytes()),
                    claimer_address: hex::encode(claim.recipient_address.as_bytes()),
                    block_number: log.block_number,
                };

                let pending_event = PendingEvent {
                    event: PendingEventType::Claim(claim_event.clone()),
                    tx_hash: format!("{:?}", log.tx_hash),
                    block_number: log.block_number,
                    timestamp_ms,
                    observed_chain: ChainId::Eth,
                };

                // Note: on_claim returns alert but we only check approvals in simplified model
                let _ = tracker.on_claim(&pending_event, &claim_event).await;
            }
            EthStarcoinBridgeEvents::TransferApprovedFilter(approval) => {
                info!(
                    "[ETH] Processing PENDING approval: nonce={}, block={}",
                    approval.nonce, log.block_number
                );

                let approval_event = ApprovalEvent {
                    source_chain: ChainId::from(approval.source_chain_id),
                    nonce: approval.nonce,
                    recorded_chain: ChainId::Eth,
                    block_number: log.block_number,
                };

                let pending_event = PendingEvent {
                    event: PendingEventType::Approval(approval_event.clone()),
                    tx_hash: format!("{:?}", log.tx_hash),
                    block_number: log.block_number,
                    timestamp_ms,
                    observed_chain: ChainId::Eth,
                };

                // Check return value for immediate mismatch detection
                if let Some(alert) = tracker.on_approval(&pending_event, &approval_event).await {
                    // Handle synchronous mismatch - trigger emergency pause
                    if let Some(ref monitor) = security_monitor {
                        monitor.handle_mismatch_alert(alert).await;
                    }
                }
            }
            _ => {
                debug!("Ignoring non-transfer ETH bridge event");
            }
        },
        _ => {
            debug!("Received non-bridge ETH event (committee/limiter/config)");
        }
    }

    Ok(())
}

/// Process ETH log and write directly to DB (for finalized events)
async fn process_eth_log_to_db(log: &EthLog, db: &Db, eth_rpc_url: &str) -> Result<()> {
    let event = match EthBridgeEvent::try_from_eth_log(log) {
        Some(e) => e,
        None => {
            debug!("Could not parse ETH log as bridge event: {:?}", log.tx_hash);
            return Ok(());
        }
    };

    let mut conn = db
        .connect()
        .await
        .context("Failed to get database connection")?;

    let timestamp_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;

    match event {
        EthBridgeEvent::EthStarcoinBridgeEvents(bridge_event) => {
            process_bridge_event_finalized(bridge_event, log, timestamp_ms, &mut conn, eth_rpc_url)
                .await?;
        }
        EthBridgeEvent::EthBridgeLimiterEvents(limiter_event) => match limiter_event {
            EthBridgeLimiterEvents::LimitUpdatedFilter(limit) => {
                info!(
                    "[ETH] Processing FINALIZED LimitUpdated event at block {}: \
                    source_chain={}, new_limit={}",
                    log.block_number, limit.source_chain_id, limit.new_limit
                );
                // Mark quota cache stale so next /quota query re-fetches from chain
                mark_quota_stale();
            }
            EthBridgeLimiterEvents::LimitUpdatedV2Filter(limit) => {
                info!(
                    "[ETH] Processing FINALIZED LimitUpdatedV2 event at block {}: \
                    nonce={}, source_chain={}, new_limit={}",
                    log.block_number, limit.nonce, limit.source_chain_id, limit.new_limit
                );
                // Mark quota cache stale so next /quota query re-fetches from chain
                mark_quota_stale();
            }
            _ => {
                debug!("Received non-limit ETH limiter event");
            }
        },
        _ => {
            debug!("Received non-bridge ETH event (committee/config)");
        }
    }

    Ok(())
}

/// Finalize and persist records using the "DB-first" pattern for ETH
///
/// Safety guarantee: data is only removed from memory AFTER successful DB write.
/// If DB write fails, we send a critical Telegram alert and panic.
async fn finalize_and_persist_records_for_eth(
    finalized_block: u64,
    transfer_tracker: &Arc<TransferTracker>,
    db: &Db,
    telegram: &Option<SharedTelegramNotifier>,
    network: NetworkType,
) {
    // Step 1: Get finalized records WITHOUT removing from memory
    let (keys, records) = transfer_tracker
        .get_finalized_records(finalized_block)
        .await;

    if records.is_empty() {
        return;
    }

    info!(
        "[ETH] Persisting {} finalized records at block {}",
        records.len(),
        finalized_block
    );

    // Step 2: Write to DB
    let db_result = write_finalized_records_to_db(&records, db, network).await;

    // Step 3: Handle result
    match db_result {
        Ok(()) => {
            // Send telegram notifications for newly finalized records
            if let Some(ref tg) = telegram {
                for record in &records {
                    for event in create_notify_events_from_record(record, network) {
                        let _ = tg.notify_finalized(NotifyChain::Eth, &event).await;
                    }
                }
            }

            // Step 4: Mark deposits as finalized BEFORE archiving
            // This is required for archive_finalized_deposits to work correctly
            transfer_tracker.mark_finalized(&keys, "deposit").await;

            // Step 5: Archive finalized deposits before removing from memory
            // This ensures approvals/claims can still find matching deposits
            transfer_tracker.archive_finalized_deposits(&keys).await;

            // Step 6: Only remove from memory AFTER successful DB write
            transfer_tracker.remove_keys(&keys).await;
            debug!(
                "[ETH] Successfully finalized {} records at block {}",
                records.len(),
                finalized_block
            );
        }
        Err(e) => {
            // DB write failed - this is critical!
            error!(
                "[ETH] CRITICAL: DB write failed for {} finalized records at block {}: {:?}",
                records.len(),
                finalized_block,
                e
            );

            // Send critical alert to Telegram before panic
            if let Some(tg) = telegram {
                let alert_msg = format!(
                    "🚨🚨🚨 <b>CRITICAL: PostgreSQL Write Failed (ETH)</b> 🚨🚨🚨\n\n\
                    <b>ETH Indexer will PANIC to prevent data loss!</b>\n\n\
                    <b>Block:</b> {}\n\
                    <b>Records:</b> {}\n\
                    <b>Error:</b> {}\n\n\
                    ⚠️ <b>Action Required:</b>\n\
                    1. Check PostgreSQL connection\n\
                    2. Ensure database is accessible\n\
                    3. Restart indexer after fixing DB\n\n\
                    Data is safe in memory - restart will recover from last block.",
                    finalized_block,
                    records.len(),
                    e
                );
                let tg_clone = tg.clone();
                let _ = tokio::task::block_in_place(|| {
                    tokio::runtime::Handle::current()
                        .block_on(async { tg_clone.send_message(&alert_msg).await })
                });
            }

            // Panic to prevent data inconsistency
            panic!(
                "[ETH] FATAL: DB write failed - refusing to continue. \
                Records are safe in memory. Error: {:?}",
                e
            );
        }
    }
}

/// Write finalized records from TransferTracker to DB
async fn write_finalized_records_to_db(
    records: &[TransferRecord],
    db: &Db,
    network: NetworkType,
) -> Result<()> {
    use starcoin_bridge_schema::schema::{token_transfer, token_transfer_data};

    let mut conn = db
        .connect()
        .await
        .context("Failed to get database connection")?;

    let mut token_transfers = Vec::new();
    let mut token_transfer_datas = Vec::new();

    for record in records {
        let source_chain_id = network.chain_id_to_bridge_i32(record.key.source_chain);

        // Process deposit
        if let Some(deposit) = &record.deposit {
            // Mark quota cache stale
            mark_quota_stale();
            let dest_chain_id = network.chain_id_to_bridge_i32(deposit.destination_chain);

            token_transfers.push(TokenTransfer {
                chain_id: source_chain_id,
                nonce: record.key.nonce as i64,
                block_height: deposit.block_number as i64,
                timestamp_ms: deposit.timestamp_ms as i64,
                status: TokenTransferStatus::Deposited,
                data_source: BridgeDataSource::ETH,
                is_finalized: Some(true),
                txn_hash: hex::decode(deposit.tx_hash.trim_start_matches("0x")).unwrap_or_default(),
                txn_sender: hex::decode(deposit.sender_address.trim_start_matches("0x"))
                    .unwrap_or_else(|e| {
                        error!("[ETH] Failed to decode txn_sender '{}': {:?}", deposit.sender_address, e);
                        vec![]
                    }),
                gas_usage: 0,
            });

            token_transfer_datas.push(TokenTransferData {
                chain_id: source_chain_id,
                nonce: record.key.nonce as i64,
                block_height: deposit.block_number as i64,
                timestamp_ms: deposit.timestamp_ms as i64,
                sender_address: hex::decode(deposit.sender_address.trim_start_matches("0x"))
                    .unwrap_or_else(|e| {
                        error!("[ETH] Failed to decode sender_address '{}': {:?}", deposit.sender_address, e);
                        vec![]
                    }),
                destination_chain: dest_chain_id,
                recipient_address: hex::decode(deposit.recipient_address.trim_start_matches("0x"))
                    .unwrap_or_default(),
                token_id: deposit.token_id as i32,
                amount: deposit.amount as i64,
                is_finalized: Some(true),
                txn_hash: hex::decode(deposit.tx_hash.trim_start_matches("0x")).unwrap_or_default(),
                monitor_verified: false,
            });
        }

        // Process approval
        if let Some(approval) = &record.approval {
            token_transfers.push(TokenTransfer {
                chain_id: source_chain_id,
                nonce: record.key.nonce as i64,
                block_height: approval.block_number as i64,
                timestamp_ms: approval.timestamp_ms as i64,
                status: TokenTransferStatus::Approved,
                data_source: BridgeDataSource::ETH,
                is_finalized: Some(true),
                txn_hash: hex::decode(approval.tx_hash.trim_start_matches("0x"))
                    .unwrap_or_default(),
                txn_sender: vec![],
                gas_usage: 0,
            });
        }

        // Process claim
        if let Some(claim) = &record.claim {
            // Mark quota cache stale
            mark_quota_stale();

            token_transfers.push(TokenTransfer {
                chain_id: source_chain_id,
                nonce: record.key.nonce as i64,
                block_height: claim.block_number as i64,
                timestamp_ms: claim.timestamp_ms as i64,
                status: TokenTransferStatus::Claimed,
                data_source: BridgeDataSource::ETH,
                is_finalized: Some(true),
                txn_hash: hex::decode(claim.tx_hash.trim_start_matches("0x")).unwrap_or_default(),
                txn_sender: hex::decode(claim.claimer_address.trim_start_matches("0x"))
                    .unwrap_or_else(|e| {
                        error!("[ETH] Failed to decode claimer_address '{}': {:?}", claim.claimer_address, e);
                        vec![]
                    }),
                gas_usage: 0,
            });
        }
    }

    // Insert to DB
    if !token_transfers.is_empty() {
        let count = diesel::insert_into(token_transfer::table)
            .values(&token_transfers)
            .on_conflict_do_nothing()
            .execute(&mut conn)
            .await
            .context("Failed to insert token transfers")?;
        info!("[ETH] Inserted {} finalized token_transfer records", count);
    }

    if !token_transfer_datas.is_empty() {
        let count = diesel::insert_into(token_transfer_data::table)
            .values(&token_transfer_datas)
            .on_conflict_do_nothing()
            .execute(&mut conn)
            .await
            .context("Failed to insert token transfer data")?;
        info!(
            "[ETH] Inserted {} finalized token_transfer_data records",
            count
        );
    }

    Ok(())
}

/// Process bridge event and write directly to DB (finalized)
async fn process_bridge_event_finalized(
    bridge_event: EthStarcoinBridgeEvents,
    log: &EthLog,
    timestamp_ms: i64,
    conn: &mut starcoin_bridge_pg_db::Connection<'_>,
    eth_rpc_url: &str,
) -> Result<()> {
    let tx_hash_hex = format!("{:?}", log.tx_hash);

    match bridge_event {
        EthStarcoinBridgeEvents::TokensDepositedFilter(deposit) => {
            info!(
                "[ETH] Processing FINALIZED deposit: nonce={}, block={}",
                deposit.nonce, log.block_number
            );

            // Mark quota cache stale
            mark_quota_stale();

            let bridge_event = EthToStarcoinTokenBridgeV1::try_from(&deposit)
                .map_err(|e| anyhow!("Failed to convert deposit event: {:?}", e))?;

            let transfer = TokenTransfer {
                chain_id: bridge_event.eth_chain_id as i32,
                nonce: bridge_event.nonce as i64,
                block_height: log.block_number as i64,
                timestamp_ms,
                txn_hash: log.tx_hash.as_bytes().to_vec(),
                txn_sender: bridge_event.eth_address.as_bytes().to_vec(),
                status: TokenTransferStatus::Deposited,
                gas_usage: 0,
                data_source: BridgeDataSource::ETH,
                is_finalized: Some(true),
            };

            let transfer_data = TokenTransferData {
                chain_id: bridge_event.eth_chain_id as i32,
                nonce: bridge_event.nonce as i64,
                block_height: log.block_number as i64,
                timestamp_ms,
                txn_hash: log.tx_hash.as_bytes().to_vec(),
                sender_address: bridge_event.eth_address.as_bytes().to_vec(),
                destination_chain: bridge_event.starcoin_bridge_chain_id as i32,
                recipient_address: bridge_event.starcoin_bridge_address.to_vec(),
                token_id: bridge_event.token_id as i32,
                amount: bridge_event.starcoin_bridge_adjusted_amount as i64,
                is_finalized: Some(true),
                monitor_verified: false,
            };

            use starcoin_bridge_schema::schema::{token_transfer, token_transfer_data};

            diesel::insert_into(token_transfer::table)
                .values(&transfer)
                .on_conflict_do_nothing()
                .execute(conn)
                .await
                .context("Failed to insert token transfer")?;

            diesel::insert_into(token_transfer_data::table)
                .values(&transfer_data)
                .on_conflict_do_nothing()
                .execute(conn)
                .await
                .context("Failed to insert token transfer data")?;

            info!(
                "[ETH] Inserted FINALIZED deposit: chain_id={}, nonce={}, amount={}",
                bridge_event.eth_chain_id,
                bridge_event.nonce,
                bridge_event.starcoin_bridge_adjusted_amount
            );

            // Async update fee cache
            update_fee_cache_async(
                tx_hash_hex,
                BridgeDataSource::ETH,
                "Deposited".to_string(),
                eth_rpc_url.to_string(),
                String::new(),
            );
        }
        EthStarcoinBridgeEvents::TokensClaimedFilter(claim) => {
            info!(
                "[ETH] Processing FINALIZED claim: nonce={}, block={}",
                claim.nonce, log.block_number
            );

            // Mark quota cache stale
            mark_quota_stale();

            let transfer = TokenTransfer {
                chain_id: claim.source_chain_id as i32,
                nonce: claim.nonce as i64,
                block_height: log.block_number as i64,
                timestamp_ms,
                txn_hash: log.tx_hash.as_bytes().to_vec(),
                txn_sender: claim.recipient_address.as_bytes().to_vec(),
                status: TokenTransferStatus::Claimed,
                gas_usage: 0,
                data_source: BridgeDataSource::ETH,
                is_finalized: Some(true),
            };

            use starcoin_bridge_schema::schema::token_transfer;

            diesel::insert_into(token_transfer::table)
                .values(&transfer)
                .on_conflict_do_nothing()
                .execute(conn)
                .await
                .context("Failed to insert token transfer claim")?;

            info!(
                "[ETH] Inserted FINALIZED claim: chain_id={}, nonce={}",
                claim.source_chain_id, claim.nonce
            );

            // Async update fee cache
            update_fee_cache_async(
                tx_hash_hex,
                BridgeDataSource::ETH,
                "Claimed".to_string(),
                eth_rpc_url.to_string(),
                String::new(),
            );
        }
        EthStarcoinBridgeEvents::TransferApprovedFilter(approval) => {
            info!(
                "[ETH] Processing FINALIZED approval: nonce={}, block={}",
                approval.nonce, log.block_number
            );

            let transfer = TokenTransfer {
                chain_id: approval.source_chain_id as i32,
                nonce: approval.nonce as i64,
                block_height: log.block_number as i64,
                timestamp_ms,
                txn_hash: log.tx_hash.as_bytes().to_vec(),
                txn_sender: approval.recipient_address.as_bytes().to_vec(),
                status: TokenTransferStatus::Approved,
                gas_usage: 0,
                data_source: BridgeDataSource::ETH,
                is_finalized: Some(true),
            };

            use starcoin_bridge_schema::schema::token_transfer;

            diesel::insert_into(token_transfer::table)
                .values(&transfer)
                .on_conflict_do_nothing()
                .execute(conn)
                .await
                .context("Failed to insert token transfer approval")?;

            info!(
                "[ETH] Inserted FINALIZED approval: chain_id={}, nonce={}",
                approval.source_chain_id, approval.nonce
            );

            // Async update fee cache
            update_fee_cache_async(
                tx_hash_hex,
                BridgeDataSource::ETH,
                "Approved".to_string(),
                eth_rpc_url.to_string(),
                String::new(),
            );
        }
        _ => {
            debug!("Ignoring ETH bridge event: {:?}", bridge_event);
        }
    }

    Ok(())
}
#[cfg(test)]
mod tests {
    /// Helper to compute watermark from query results (extracted for testability)
    ///
    /// This mirrors the logic in get_latest_syncer_start_block without requiring DB
    fn compute_watermark_from_results(
        min_unfinalized: Option<i64>,
        max_finalized: Option<i64>,
        config_start_block: u64,
    ) -> u64 {
        // Priority 1: If there are unfinalized events, start from the earliest
        if let Some(min_unfinalized_block) = min_unfinalized {
            let watermark = min_unfinalized_block as u64;
            return std::cmp::max(config_start_block, watermark);
        }

        // Priority 2: If all events are finalized, start from max + 1
        if let Some(max_finalized_block) = max_finalized {
            let watermark = (max_finalized_block as u64).saturating_add(1);
            return std::cmp::max(config_start_block, watermark);
        }

        // Priority 3: No events, use config
        config_start_block
    }

    #[test]
    fn test_watermark_no_events() {
        // No events in DB -> use config_start_block
        let result = compute_watermark_from_results(None, None, 100);
        assert_eq!(result, 100);
    }

    #[test]
    fn test_watermark_only_finalized_events() {
        // All events finalized, max at block 500 -> start from 501
        let result = compute_watermark_from_results(None, Some(500), 100);
        assert_eq!(result, 501);
    }

    #[test]
    fn test_watermark_only_finalized_events_config_higher() {
        // All events finalized at block 50, but config is 100 -> use config
        let result = compute_watermark_from_results(None, Some(50), 100);
        assert_eq!(result, 100);
    }

    #[test]
    fn test_watermark_has_unfinalized_events() {
        // Has unfinalized events starting at block 450 -> start from 450
        let result = compute_watermark_from_results(Some(450), Some(400), 100);
        assert_eq!(result, 450);
    }

    #[test]
    fn test_watermark_unfinalized_lower_than_config() {
        // Unfinalized at block 50, config is 100 -> use config (never go below)
        let result = compute_watermark_from_results(Some(50), Some(40), 100);
        assert_eq!(result, 100);
    }

    #[test]
    fn test_watermark_unfinalized_takes_priority() {
        // Even if max finalized is higher, unfinalized takes priority
        // (this shouldn't happen in practice, but tests the logic)
        let result = compute_watermark_from_results(Some(300), Some(500), 100);
        assert_eq!(result, 300);
    }

    #[test]
    fn test_watermark_saturating_add() {
        // Test overflow protection with saturating_add
        // i64::MAX = 9223372036854775807, as u64 + 1 = 9223372036854775808
        let result = compute_watermark_from_results(None, Some(i64::MAX), 0);
        assert_eq!(result, (i64::MAX as u64).saturating_add(1));
    }
}
