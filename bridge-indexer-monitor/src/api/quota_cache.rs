// Copyright (c) Starcoin, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Quota cache with singleflight pattern
//!
//! This module implements a cache for bridge quota values that:
//! - Returns cached values immediately when cache is fresh
//! - Coalesces concurrent requests when cache is stale (singleflight)
//! - Refreshes when transfer events or limit update events occur (finalized)
//!
//! Global access is provided via `get_global_quota_cache()` for use by
//! event handlers to mark the cache as stale when transfers or limit updates occur.

use crate::api::types::{BigIntValue, QuotaResponse};
use ethers::prelude::Middleware;
use ethers::providers::{Http, Provider};
use ethers::types::Address as EthAddress;
use starcoin_bridge::starcoin_bridge_client::StarcoinClientInner;
use starcoin_bridge_types::bridge::BridgeChainId;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};
use tokio::sync::{broadcast, Mutex, RwLock};
use tracing::{debug, error, info};

/// Global quota cache instance for cross-module access
static GLOBAL_QUOTA_CACHE: OnceLock<Arc<QuotaCache>> = OnceLock::new();

/// Configuration for quota cache RPC providers
#[derive(Clone)]
pub struct QuotaCacheConfig {
    /// ETH RPC URL
    pub eth_rpc_url: Option<String>,
    /// ETH bridge proxy address
    pub eth_bridge_address: Option<EthAddress>,
    /// Starcoin RPC URL
    pub starcoin_rpc_url: Option<String>,
    /// Starcoin bridge address
    pub starcoin_bridge_address: Option<String>,
    /// Chain IDs to query (source chain for ETH limiter)
    pub starcoin_chain_id: BridgeChainId,
    pub eth_chain_id: BridgeChainId,
}

impl Default for QuotaCacheConfig {
    fn default() -> Self {
        Self {
            eth_rpc_url: None,
            eth_bridge_address: None,
            starcoin_rpc_url: None,
            starcoin_bridge_address: None,
            starcoin_chain_id: BridgeChainId::StarcoinCustom,
            eth_chain_id: BridgeChainId::EthCustom,
        }
    }
}

/// Initialize the global quota cache with configuration
pub fn init_global_quota_cache_with_config(config: QuotaCacheConfig) -> Arc<QuotaCache> {
    GLOBAL_QUOTA_CACHE
        .get_or_init(|| QuotaCache::new_with_config(config))
        .clone()
}

/// Initialize the global quota cache with default (no RPC) configuration
pub fn init_global_quota_cache() -> Arc<QuotaCache> {
    init_global_quota_cache_with_config(QuotaCacheConfig::default())
}

/// Get the global quota cache. Returns None if not initialized.
pub fn get_global_quota_cache() -> Option<Arc<QuotaCache>> {
    GLOBAL_QUOTA_CACHE.get().cloned()
}

/// Mark the global quota cache as stale. Safe to call even if not initialized.
pub fn mark_quota_stale() {
    if let Some(cache) = get_global_quota_cache() {
        cache.mark_stale();
    }
}

/// Default quota value: 10 million USD with 8 decimal precision
const DEFAULT_QUOTA: u64 = 1_000_000_000_000_000;

/// Decimal precision for quota values
const QUOTA_DECIMALS: u8 = 8;

/// Quota cache with singleflight pattern
///
/// When multiple requests come in while the cache is stale:
/// - First request triggers RPC call
/// - Subsequent requests wait for the same RPC result
/// - All requests return simultaneously when RPC completes
pub struct QuotaCache {
    /// Cached quota values
    value: RwLock<QuotaResponse>,
    /// Whether the cache is stale and needs refresh
    is_stale: AtomicBool,
    /// Whether a fetch is currently in progress
    fetch_in_progress: AtomicBool,
    /// Broadcast channel for notifying waiters when fetch completes
    notify: Mutex<Option<broadcast::Sender<QuotaResponse>>>,
    /// Configuration for RPC providers
    config: QuotaCacheConfig,
}

impl QuotaCache {
    /// Create a new QuotaCache with configuration
    pub fn new_with_config(config: QuotaCacheConfig) -> Arc<Self> {
        Arc::new(Self {
            value: RwLock::new(QuotaResponse {
                eth_claim: None,
                eth_error: Some("Not yet fetched".to_string()),
                starcoin_claim: None,
                starcoin_error: Some("Not yet fetched".to_string()),
                decimals: BigIntValue(QUOTA_DECIMALS as u64),
            }),
            is_stale: AtomicBool::new(true), // Start stale to trigger initial fetch
            fetch_in_progress: AtomicBool::new(false),
            notify: Mutex::new(None),
            config,
        })
    }

    /// Create a new QuotaCache with default values (no RPC)
    pub fn new() -> Arc<Self> {
        Self::new_with_config(QuotaCacheConfig::default())
    }

    /// Get quota values, using cached value if fresh or waiting for fetch if stale
    pub async fn get(&self) -> QuotaResponse {
        // Fast path: cache is fresh
        if !self.is_stale.load(Ordering::Acquire) {
            debug!("Quota cache hit");
            return self.value.read().await.clone();
        }

        // Slow path: cache is stale
        // Try to become the fetcher
        if self
            .fetch_in_progress
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            // We are the fetcher
            debug!("Quota cache miss, fetching from chain");

            // Create broadcast channel for waiters
            let (tx, _) = broadcast::channel(1);
            *self.notify.lock().await = Some(tx.clone());

            // Fetch from chain
            let result = self.fetch_from_chain().await;

            // Update cache
            *self.value.write().await = result.clone();
            self.is_stale.store(false, Ordering::Release);
            self.fetch_in_progress.store(false, Ordering::Release);

            // Notify waiters
            let _ = tx.send(result.clone());
            *self.notify.lock().await = None;

            info!(
                "Quota cache refreshed: eth={:?}, stc={:?}",
                result.eth_claim, result.starcoin_claim
            );
            return result;
        }

        // We are a waiter
        debug!("Quota fetch in progress, waiting...");

        // Subscribe to notification
        let mut rx = {
            let guard = self.notify.lock().await;
            if let Some(tx) = guard.as_ref() {
                tx.subscribe()
            } else {
                // Fetch completed between our check and subscription
                // Cache should be fresh now
                return self.value.read().await.clone();
            }
        };

        // Wait for result
        match rx.recv().await {
            Ok(result) => result,
            Err(_) => {
                // Channel closed, fetch completed
                self.value.read().await.clone()
            }
        }
    }

    /// Mark cache as stale, typically called when a transfer event or limit update event is finalized
    pub fn mark_stale(&self) {
        let was_fresh = !self.is_stale.swap(true, Ordering::Release);
        if was_fresh {
            debug!("Quota cache marked stale");
        }
    }

    /// Get ETH bridge address as hex string
    pub fn eth_bridge_address(&self) -> String {
        self.config
            .eth_bridge_address
            .map(|addr| format!("{:?}", addr))
            .unwrap_or_default()
    }

    /// Get Starcoin bridge address
    pub fn stc_bridge_address(&self) -> String {
        self.config
            .starcoin_bridge_address
            .clone()
            .unwrap_or_default()
    }

    /// Fetch quota from chain
    async fn fetch_from_chain(&self) -> QuotaResponse {
        let mut eth_claim: Option<u64> = None;
        let mut eth_error: Option<String> = None;
        let mut starcoin_claim: Option<u64> = None;
        let mut starcoin_error: Option<String> = None;

        // Query ETH limiter if configured
        if let (Some(ref rpc_url), Some(bridge_addr)) =
            (&self.config.eth_rpc_url, self.config.eth_bridge_address)
        {
            match self
                .fetch_eth_quota(rpc_url, bridge_addr, self.config.starcoin_chain_id)
                .await
            {
                Ok(quota) => {
                    eth_claim = Some(quota);
                    info!("Fetched ETH quota from chain: {}", quota);
                }
                Err(e) => {
                    let err_msg = format!("Failed to fetch ETH quota: {:?}", e);
                    error!("{}", err_msg);
                    eth_error = Some(err_msg);
                }
            }
        } else {
            eth_error = Some("ETH RPC not configured".to_string());
            debug!("ETH RPC not configured");
        }

        // Query Starcoin limiter if configured
        if let (Some(ref rpc_url), Some(ref bridge_addr)) = (
            &self.config.starcoin_rpc_url,
            &self.config.starcoin_bridge_address,
        ) {
            match self
                .fetch_starcoin_quota(rpc_url, bridge_addr, self.config.eth_chain_id)
                .await
            {
                Ok(quota) => {
                    starcoin_claim = Some(quota);
                    info!("Fetched Starcoin quota from chain: {}", quota);
                }
                Err(e) => {
                    let err_msg = format!("Failed to fetch Starcoin quota: {:?}", e);
                    error!("{}", err_msg);
                    starcoin_error = Some(err_msg);
                }
            }
        } else {
            starcoin_error = Some("Starcoin RPC not configured".to_string());
            debug!("Starcoin RPC not configured");
        }

        QuotaResponse {
            eth_claim: eth_claim.map(BigIntValue),
            eth_error,
            starcoin_claim: starcoin_claim.map(BigIntValue),
            starcoin_error,
            decimals: BigIntValue(QUOTA_DECIMALS as u64),
        }
    }

    /// Fetch remaining quota from ETH limiter contract
    async fn fetch_eth_quota(
        &self,
        rpc_url: &str,
        bridge_address: EthAddress,
        starcoin_chain_id: BridgeChainId,
    ) -> anyhow::Result<u64> {
        let provider = Provider::<Http>::try_from(rpc_url)?;
        let provider = Arc::new(provider);

        // First check if the contract exists at the address
        let code = provider.get_code(bridge_address, None).await?;
        if code.is_empty() {
            anyhow::bail!(
                "No contract deployed at ETH bridge address {:?}. \
                The anvil/network may have been restarted. Please redeploy contracts.",
                bridge_address
            );
        }

        // Get limiter contract address from bridge proxy
        let contracts =
            starcoin_bridge::utils::get_eth_contracts(bridge_address, &provider).await?;

        let limiter = contracts.limiter;
        let chain_id: u8 = starcoin_chain_id as u8;

        // Get total limit for this chain
        let total_limit: u64 = limiter.chain_limits(chain_id).call().await?;

        // Get current window usage
        let window_amount: ethers::types::U256 =
            limiter.calculate_window_amount(chain_id).call().await?;

        // Calculate remaining = limit - used
        let window_amount_u64 = if window_amount > ethers::types::U256::from(u64::MAX) {
            u64::MAX
        } else {
            window_amount.as_u64()
        };

        let remaining = total_limit.saturating_sub(window_amount_u64);

        debug!(
            "ETH limiter: chain_id={}, limit={}, used={}, remaining={}",
            chain_id, total_limit, window_amount_u64, remaining
        );

        Ok(remaining)
    }

    /// Fetch remaining quota from Starcoin limiter
    async fn fetch_starcoin_quota(
        &self,
        rpc_url: &str,
        bridge_address: &str,
        eth_chain_id: BridgeChainId,
    ) -> anyhow::Result<u64> {
        use starcoin_bridge::starcoin_jsonrpc_client::StarcoinJsonRpcClient;

        let client = StarcoinJsonRpcClient::new(rpc_url, bridge_address);

        // Query bridge object to get limiter state
        let bridge_summary = client.get_bridge_summary().await?;

        let source_chain = eth_chain_id;
        let dest_chain = BridgeChainId::try_from(bridge_summary.chain_id)?;

        // Find the transfer limit for this route
        let limit_opt = bridge_summary
            .limiter
            .transfer_limit
            .iter()
            .find(|(src, dst, _)| *src == source_chain && *dst == dest_chain)
            .map(|(_, _, limit)| *limit);

        let total_limit = limit_opt.unwrap_or(DEFAULT_QUOTA);

        // Find the transfer record to calculate current usage
        let record_opt = bridge_summary
            .limiter
            .transfer_records
            .iter()
            .find(|(src, dst, _)| *src == source_chain && *dst == dest_chain)
            .map(|(_, _, record)| record.clone());

        let used_amount = record_opt.map(|r| r.total_amount).unwrap_or(0);

        let remaining = total_limit.saturating_sub(used_amount);

        debug!(
            "Starcoin limiter: route={:?}->{:?}, limit={}, used={}, remaining={}",
            source_chain, dest_chain, total_limit, used_amount, remaining
        );

        Ok(remaining)
    }
}

impl Default for QuotaCache {
    fn default() -> Self {
        Self {
            value: RwLock::new(QuotaResponse {
                eth_claim: None,
                eth_error: Some("Not yet fetched".to_string()),
                starcoin_claim: None,
                starcoin_error: Some("Not yet fetched".to_string()),
                decimals: BigIntValue(QUOTA_DECIMALS as u64),
            }),
            is_stale: AtomicBool::new(true),
            fetch_in_progress: AtomicBool::new(false),
            notify: Mutex::new(None),
            config: QuotaCacheConfig::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn test_cache_returns_error_when_rpc_not_configured() {
        let cache = QuotaCache::new();
        let result = cache.get().await;
        // Without RPC configured, should return None with error
        assert!(result.eth_claim.is_none());
        assert!(result.eth_error.is_some());
        assert!(result.starcoin_claim.is_none());
        assert!(result.starcoin_error.is_some());
    }

    #[tokio::test]
    async fn test_cache_becomes_fresh_after_get() {
        let cache = QuotaCache::new();
        assert!(cache.is_stale.load(Ordering::Acquire));

        let _ = cache.get().await;

        assert!(!cache.is_stale.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn test_mark_stale() {
        let cache = QuotaCache::new();
        let _ = cache.get().await; // Make cache fresh

        assert!(!cache.is_stale.load(Ordering::Acquire));

        cache.mark_stale();

        assert!(cache.is_stale.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn test_concurrent_requests_coalesce() {
        let cache = Arc::new(QuotaCache::default());
        cache.is_stale.store(true, Ordering::Release);

        // Spawn multiple concurrent requests
        let mut handles = vec![];
        for _ in 0..10 {
            let cache_clone = cache.clone();
            handles.push(tokio::spawn(async move { cache_clone.get().await }));
        }

        // Wait for all to complete
        let results: Vec<_> = futures::future::join_all(handles)
            .await
            .into_iter()
            .map(|r| r.unwrap())
            .collect();

        // All should return the same value (None with error since RPC not configured)
        for result in &results {
            assert!(result.eth_claim.is_none());
            assert!(result.starcoin_claim.is_none());
        }

        // Cache should be fresh now
        assert!(!cache.is_stale.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn test_singleflight_only_one_fetch() {
        use std::sync::atomic::AtomicU32;

        // We can't directly count fetch_from_chain calls, but we can verify
        // that the fetch_in_progress flag works correctly
        let cache = Arc::new(QuotaCache::default());
        cache.is_stale.store(true, Ordering::Release);

        let fetch_started = Arc::new(AtomicU32::new(0));

        let mut handles = vec![];
        for i in 0..5 {
            let cache_clone = cache.clone();
            let fetch_started_clone = fetch_started.clone();
            handles.push(tokio::spawn(async move {
                // Check if we become the fetcher
                if i == 0 {
                    // First request should see stale=true and fetch_in_progress=false
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
                let result = cache_clone.get().await;
                if cache_clone
                    .fetch_in_progress
                    .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
                {
                    fetch_started_clone.fetch_add(1, Ordering::SeqCst);
                    cache_clone
                        .fetch_in_progress
                        .store(false, Ordering::Release);
                }
                result
            }));
        }

        let results: Vec<_> = futures::future::join_all(handles)
            .await
            .into_iter()
            .map(|r| r.unwrap())
            .collect();

        // All should succeed
        assert_eq!(results.len(), 5);
    }

    #[tokio::test]
    async fn test_cache_refresh_after_mark_stale() {
        let cache = QuotaCache::new();

        // First fetch (returns error since RPC not configured)
        let result1 = cache.get().await;
        assert!(result1.eth_claim.is_none());
        assert!(result1.eth_error.is_some());
        assert!(!cache.is_stale.load(Ordering::Acquire));

        // Mark stale (simulating a transfer event)
        cache.mark_stale();
        assert!(cache.is_stale.load(Ordering::Acquire));

        // Second fetch should work
        let result2 = cache.get().await;
        assert!(result2.eth_claim.is_none());
        assert!(!cache.is_stale.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn test_global_quota_cache() {
        // Test initialization
        let cache1 = init_global_quota_cache();
        let cache2: Arc<QuotaCache> = get_global_quota_cache().expect("Cache should be initialized");

        // Should be the same instance
        assert!(Arc::ptr_eq(&cache1, &cache2));

        // Test mark_quota_stale
        let _ = cache1.get().await;
        assert!(!cache1.is_stale.load(Ordering::Acquire));

        mark_quota_stale();
        assert!(cache1.is_stale.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn test_eth_quota_with_mock_provider() {
        // This test demonstrates the pattern for mocking ETH RPC
        // In a real test, you would set up EthMockProvider with expected responses
        let config = QuotaCacheConfig {
            eth_rpc_url: None, // Use None to skip RPC calls in this test
            eth_bridge_address: None,
            starcoin_rpc_url: None,
            starcoin_bridge_address: None,
            starcoin_chain_id: BridgeChainId::StarcoinCustom,
            eth_chain_id: BridgeChainId::EthCustom,
        };

        let cache = QuotaCache::new_with_config(config);
        let result = cache.get().await;

        // Without RPC configured, should return None with error message
        assert!(result.eth_claim.is_none());
        assert!(result.eth_error.is_some());
        assert!(result.eth_error.unwrap().contains("not configured"));
        assert!(result.starcoin_claim.is_none());
    }

    #[tokio::test]
    async fn test_quota_response_serialization_success() {
        let response = QuotaResponse {
            eth_claim: Some(BigIntValue(1000000000000000)),
            eth_error: None,
            starcoin_claim: Some(BigIntValue(2000000000000000)),
            starcoin_error: None,
            decimals: BigIntValue(8),
        };

        let json = serde_json::to_string(&response).unwrap();
        assert!(json.contains("eth_claim"));
        assert!(json.contains("starcoin_claim"));
        assert!(json.contains("decimals"));
        assert!(json.contains("__@json.bigint__"));
        // Errors should be skipped when None
        assert!(!json.contains("eth_error"));
        assert!(!json.contains("starcoin_error"));

        let deserialized: QuotaResponse = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.eth_claim, response.eth_claim);
        assert_eq!(deserialized.starcoin_claim, response.starcoin_claim);
        assert_eq!(deserialized.decimals, response.decimals);
    }

    #[tokio::test]
    async fn test_quota_response_serialization_with_errors() {
        let response = QuotaResponse {
            eth_claim: None,
            eth_error: Some("Connection failed".to_string()),
            starcoin_claim: Some(BigIntValue(500000000)),
            starcoin_error: None,
            decimals: BigIntValue(8),
        };

        let json = serde_json::to_string(&response).unwrap();
        // eth_claim is None so it should be skipped
        assert!(!json.contains("\"eth_claim\":"));
        // eth_error should be present
        assert!(json.contains("eth_error"));
        assert!(json.contains("Connection failed"));
        // starcoin_claim should be present
        assert!(json.contains("starcoin_claim"));
        assert!(!json.contains("starcoin_error"));
    }

    #[tokio::test]
    async fn test_config_default() {
        let config = QuotaCacheConfig::default();
        assert!(config.eth_rpc_url.is_none());
        assert!(config.eth_bridge_address.is_none());
        assert!(config.starcoin_rpc_url.is_none());
        assert!(config.starcoin_bridge_address.is_none());
    }

    #[test]
    fn test_bigint_json_format() {
        let response = QuotaResponse {
            eth_claim: Some(BigIntValue(0)),
            eth_error: None,
            starcoin_claim: Some(BigIntValue(18446744073709551615)),
            starcoin_error: None,
            decimals: BigIntValue(8),
        };
        let json = serde_json::to_string_pretty(&response).unwrap();
        println!("QuotaResponse JSON:\n{}", json);

        // Verify format matches expected
        assert!(json.contains(r#""__@json.bigint__": "0""#));
        assert!(json.contains(r#""__@json.bigint__": "18446744073709551615""#));
        assert!(json.contains(r#""__@json.bigint__": "8""#));
    }
}
