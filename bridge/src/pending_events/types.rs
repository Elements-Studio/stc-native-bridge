// Copyright (c) Starcoin, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Type definitions for pending events management

use serde::{Deserialize, Serialize};
use std::fmt;

/// Chain identifier
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ChainId {
    Starcoin = 0,
    Eth = 1,
}

impl fmt::Display for ChainId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ChainId::Starcoin => write!(f, "STC"),
            ChainId::Eth => write!(f, "ETH"),
        }
    }
}

impl From<u8> for ChainId {
    fn from(v: u8) -> Self {
        // Map BridgeChainId values to ChainId:
        // - StarcoinMainnet (0), StarcoinTestnet (1), StarcoinCustom (2) -> Starcoin
        // - EthMainnet (10), EthSepolia (11), EthCustom (12) -> Eth
        match v {
            0..=2 => ChainId::Starcoin,
            _ => ChainId::Eth,
        }
    }
}

/// Unique identifier for a cross-chain transfer
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TransferKey {
    /// Source chain where deposit occurred
    pub source_chain: ChainId,
    /// Nonce on source chain (unique per chain)
    pub nonce: u64,
}

impl TransferKey {
    pub fn new(source_chain: ChainId, nonce: u64) -> Self {
        Self {
            source_chain,
            nonce,
        }
    }

    pub fn stc(nonce: u64) -> Self {
        Self::new(ChainId::Starcoin, nonce)
    }

    pub fn eth(nonce: u64) -> Self {
        Self::new(ChainId::Eth, nonce)
    }
}

impl fmt::Display for TransferKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.source_chain, self.nonce)
    }
}

/// Event types that can be pending
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PendingEventType {
    /// Token deposit on source chain
    Deposit(DepositEvent),
    /// Bridge committee approval
    Approval(ApprovalEvent),
    /// Token claim on destination chain
    Claim(ClaimEvent),
}

impl PendingEventType {
    /// Get the transfer key for this event
    pub fn transfer_key(&self) -> TransferKey {
        match self {
            PendingEventType::Deposit(e) => TransferKey::new(e.source_chain, e.nonce),
            PendingEventType::Approval(e) => TransferKey::new(e.source_chain, e.nonce),
            PendingEventType::Claim(e) => TransferKey::new(e.source_chain, e.nonce),
        }
    }

    /// Get the block number where this event occurred
    pub fn block_number(&self) -> u64 {
        match self {
            PendingEventType::Deposit(e) => e.block_number,
            PendingEventType::Approval(e) => e.block_number,
            PendingEventType::Claim(e) => e.block_number,
        }
    }

    /// Get the chain where this event occurred
    pub fn event_chain(&self) -> ChainId {
        match self {
            PendingEventType::Deposit(e) => e.source_chain,
            PendingEventType::Approval(e) => e.recorded_chain,
            PendingEventType::Claim(e) => e.destination_chain,
        }
    }

    /// Get event type name for logging
    pub fn type_name(&self) -> &'static str {
        match self {
            PendingEventType::Deposit(_) => "deposit",
            PendingEventType::Approval(_) => "approval",
            PendingEventType::Claim(_) => "claim",
        }
    }

    /// Get event type for logging (alias for type_name)
    pub fn event_type(&self) -> &'static str {
        self.type_name()
    }
}

/// A pending event with metadata
#[derive(Debug, Clone)]
pub struct PendingEvent {
    /// The event data
    pub event: PendingEventType,
    /// Transaction hash
    pub tx_hash: String,
    /// Block number where event occurred
    pub block_number: u64,
    /// Timestamp (milliseconds)
    pub timestamp_ms: u64,
    /// Chain where this event was observed
    pub observed_chain: ChainId,
}

impl PendingEvent {
    pub fn transfer_key(&self) -> TransferKey {
        self.event.transfer_key()
    }
}

/// Deposit event data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DepositEvent {
    /// Source chain ID
    pub source_chain: ChainId,
    /// Destination chain ID
    pub destination_chain: ChainId,
    /// Nonce (unique per source chain)
    pub nonce: u64,
    /// Token ID
    pub token_id: u8,
    /// Amount (bridge-adjusted, same precision as token's starcoin decimals; 6 for USDT)
    pub amount: u64,
    /// Sender address (hex)
    pub sender_address: String,
    /// Recipient address on destination chain (hex)
    pub recipient_address: String,
    /// Block number
    pub block_number: u64,
}

/// Approval event data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApprovalEvent {
    /// Source chain of the transfer
    pub source_chain: ChainId,
    /// Nonce
    pub nonce: u64,
    /// Chain where approval was recorded
    pub recorded_chain: ChainId,
    /// Block number
    pub block_number: u64,
}

/// Claim event data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClaimEvent {
    /// Source chain of the original deposit
    pub source_chain: ChainId,
    /// Destination chain where claim occurred
    pub destination_chain: ChainId,
    /// Nonce
    pub nonce: u64,
    /// Token ID
    pub token_id: u8,
    /// Amount claimed (may differ in decimals from deposit)
    pub amount: u64,
    /// Sender address from original deposit
    pub sender_address: String,
    /// Recipient who received tokens
    pub recipient_address: String,
    /// Address that executed the claim
    pub claimer_address: String,
    /// Block number
    pub block_number: u64,
}

/// Status of a cross-chain transfer
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TransferStatus {
    /// Deposit observed, waiting for approval
    Deposited,
    /// Approved by bridge committee
    Approved,
    /// Claimed on destination chain
    Claimed,
}

impl fmt::Display for TransferStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TransferStatus::Deposited => write!(f, "Deposited"),
            TransferStatus::Approved => write!(f, "Approved"),
            TransferStatus::Claimed => write!(f, "Claimed"),
        }
    }
}

/// Complete transfer record linking deposit/approval/claim
#[derive(Debug, Clone)]
pub struct TransferRecord {
    /// Transfer identifier
    pub key: TransferKey,
    /// Current status
    pub status: TransferStatus,
    /// Deposit information
    pub deposit: Option<DepositInfo>,
    /// Approval information
    pub approval: Option<ApprovalInfo>,
    /// Claim information
    pub claim: Option<ClaimInfo>,
}

/// Deposit information in transfer record
#[derive(Debug, Clone)]
pub struct DepositInfo {
    pub tx_hash: String,
    pub block_number: u64,
    pub timestamp_ms: u64,
    pub destination_chain: ChainId,
    pub token_id: u8,
    pub amount: u64,
    pub sender_address: String,
    pub recipient_address: String,
    /// Whether deposit is finalized on source chain
    pub is_finalized: bool,
}

/// Approval information in transfer record
#[derive(Debug, Clone)]
pub struct ApprovalInfo {
    pub tx_hash: String,
    pub block_number: u64,
    pub timestamp_ms: u64,
    pub recorded_chain: ChainId,
    /// Whether approval is finalized
    pub is_finalized: bool,
}

/// Claim information in transfer record
#[derive(Debug, Clone)]
pub struct ClaimInfo {
    pub tx_hash: String,
    pub block_number: u64,
    pub timestamp_ms: u64,
    pub claimer_address: String,
    /// Whether claim is finalized
    pub is_finalized: bool,
}

/// Mismatch detected between deposit and claim (potential key compromise)
#[derive(Debug, Clone)]
pub struct TransferMismatch {
    pub key: TransferKey,
    pub reason: MismatchReason,
    pub deposit: Option<DepositInfo>,
    pub claim: ClaimInfo,
}

/// Reason for deposit/claim mismatch
#[derive(Debug, Clone)]
pub enum MismatchReason {
    /// Claim without matching deposit
    NoMatchingDeposit,
    /// Amount doesn't match
    AmountMismatch {
        deposit_amount: u64,
        claim_amount: u64,
    },
    /// Token ID doesn't match
    TokenMismatch { deposit_token: u8, claim_token: u8 },
    /// Recipient doesn't match
    RecipientMismatch { expected: String, actual: String },
}

impl fmt::Display for MismatchReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MismatchReason::NoMatchingDeposit => {
                write!(
                    f,
                    "CRITICAL: Claim without matching deposit (key compromise?)"
                )
            }
            MismatchReason::AmountMismatch {
                deposit_amount,
                claim_amount,
            } => {
                write!(
                    f,
                    "Amount mismatch: deposited {} but claimed {}",
                    deposit_amount, claim_amount
                )
            }
            MismatchReason::TokenMismatch {
                deposit_token,
                claim_token,
            } => {
                write!(
                    f,
                    "Token mismatch: deposited token {} but claimed token {}",
                    deposit_token, claim_token
                )
            }
            MismatchReason::RecipientMismatch { expected, actual } => {
                write!(
                    f,
                    "Recipient mismatch: expected {} but got {}",
                    expected, actual
                )
            }
        }
    }
}

/// Result of draining finalized events
#[derive(Debug, Default)]
pub struct DrainResult {
    /// Events that are now finalized (ready to write to DB)
    pub finalized_events: Vec<PendingEvent>,
    /// Number of events drained
    pub count: usize,
    /// New finalized block number
    pub new_finalized_block: u64,
}

/// Result of handling a reorg
#[derive(Debug)]
pub struct ReorgResult {
    /// Number of events removed
    pub removed_count: usize,
    /// Block numbers affected
    pub affected_blocks: Vec<u64>,
}

/// Immediate alert when a security-critical mismatch is detected
///
/// This is returned synchronously from TransferTracker::on_approval/on_claim
/// when a potential attack is detected (e.g., approval without deposit).
/// The caller should act on this immediately to trigger emergency pause.
#[derive(Debug, Clone)]
pub enum MismatchAlert {
    /// Approval received without a matching deposit
    /// This is a CRITICAL security issue - possible key compromise
    ApprovalWithoutDeposit {
        source_chain: ChainId,
        nonce: u64,
        tx_hash: String,
        block_number: u64,
    },
    /// Claim received without a matching deposit
    /// This is a CRITICAL security issue - possible key compromise
    ClaimWithoutDeposit {
        source_chain: ChainId,
        nonce: u64,
        tx_hash: String,
        block_number: u64,
    },
}

impl fmt::Display for MismatchAlert {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MismatchAlert::ApprovalWithoutDeposit {
                source_chain,
                nonce,
                tx_hash,
                ..
            } => {
                write!(
                    f,
                    "CRITICAL: Approval without deposit (source={}, nonce={}, tx={})",
                    source_chain, nonce, tx_hash
                )
            }
            MismatchAlert::ClaimWithoutDeposit {
                source_chain,
                nonce,
                tx_hash,
                ..
            } => {
                write!(
                    f,
                    "CRITICAL: Claim without deposit (source={}, nonce={}, tx={})",
                    source_chain, nonce, tx_hash
                )
            }
        }
    }
}
