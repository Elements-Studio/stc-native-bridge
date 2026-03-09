// Copyright (c) Starcoin, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Monitor configuration

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::time::Duration;

/// Chain ID to name mapping
pub fn get_chain_name(chain_id: u8) -> String {
    match chain_id {
        0 => "Starcoin Mainnet".to_string(),
        1 => "Starcoin Testnet".to_string(),
        2 => "Starcoin Custom".to_string(),
        10 => "Ethereum Mainnet".to_string(),
        11 => "Ethereum Sepolia".to_string(),
        12 => "Ethereum Local".to_string(),
        _ => format!("Chain#{}", chain_id),
    }
}

/// Token ID to name mapping
pub fn get_token_name(token_id: u8) -> String {
    match token_id {
        3 => "USDT".to_string(),
        _ => format!("Token#{}", token_id),
    }
}

/// Complete monitor configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MonitorConfig {
    pub chain_a: ChainConfig,
    pub chain_b: ChainConfig,
    pub telegram: TelegramConfig,
    #[serde(default)]
    pub paired_message: PairedMessageConfig,
    #[serde(default = "default_state_file")]
    pub state_file: PathBuf,
    #[serde(default)]
    pub validator_health: Option<super::validator_health::HealthCheckerConfig>,
    #[serde(default)]
    pub validators: Vec<super::validator_health::ValidatorConfig>,
    #[serde(default)]
    pub emergency_pause: Option<EmergencyPauseConfig>,
    /// Reorg detection for ETH chain (monitors recently sent notifications)
    #[serde(default)]
    pub reorg_detection: Option<ReorgDetectionConfig>,
    /// Claim delay in seconds - time to wait after approval before claim is allowed
    /// This is set during bridge deployment and never changes
    #[serde(default)]
    pub claim_delay_seconds: u64,
}

fn default_state_file() -> PathBuf {
    PathBuf::from("monitor-state.json")
}

impl MonitorConfig {
    /// Load configuration from YAML file with environment variable substitution
    pub fn from_file(path: &PathBuf) -> Result<Self> {
        let contents = std::fs::read_to_string(path)
            .context(format!("Failed to read monitor config file: {:?}", path))?;

        // Perform environment variable substitution
        let contents = substitute_env_vars(&contents)?;

        let config: MonitorConfig =
            serde_yaml::from_str(&contents).context("Failed to parse monitor config YAML")?;
        Ok(config)
    }
}

/// Substitute environment variables in the format ${VAR_NAME}
fn substitute_env_vars(content: &str) -> Result<String> {
    use regex::Regex;

    let re = Regex::new(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}").unwrap();
    let mut result = content.to_string();

    for cap in re.captures_iter(content) {
        let full_match = &cap[0];
        let var_name = &cap[1];

        if let Ok(var_value) = std::env::var(var_name) {
            result = result.replace(full_match, &var_value);
        } else {
            // Replace with empty string so is_configured() correctly returns false
            tracing::warn!(
                "Environment variable {} not found, replacing with empty string",
                var_name
            );
            result = result.replace(full_match, "");
        }
    }

    Ok(result)
}

/// Chain configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChainConfig {
    pub chain_id: u8,
    pub contract_address: String,
    #[serde(default)]
    pub rpc_url: String, // Backward compatibility
    #[serde(default)]
    pub rpc_urls: Vec<String>, // Multiple URLs for failover
    #[serde(default = "default_poll_interval")]
    pub poll_interval: u64, // seconds
    #[serde(default)]
    pub start_block: Option<u64>,
    #[serde(default)]
    pub limiter_address: Option<String>,
    #[serde(default)]
    pub committee_address: Option<String>,
}

fn default_poll_interval() -> u64 {
    5
}

impl ChainConfig {
    /// Check if this is an EVM chain
    pub fn is_evm(&self) -> bool {
        self.chain_id >= 10
    }

    /// Check if this is a local/dev chain
    pub fn is_local(&self) -> bool {
        matches!(self.chain_id, 2 | 12)
    }

    /// Get RPC URLs (with fallback to single rpc_url)
    pub fn get_rpc_urls(&self) -> Vec<String> {
        if !self.rpc_urls.is_empty() {
            self.rpc_urls.clone()
        } else if !self.rpc_url.is_empty() {
            vec![self.rpc_url.clone()]
        } else {
            vec![]
        }
    }

    /// Get chain name
    pub fn name(&self) -> String {
        get_chain_name(self.chain_id)
    }

    /// Get poll interval as Duration
    pub fn poll_duration(&self) -> Duration {
        Duration::from_secs(self.poll_interval)
    }
}

/// Telegram notification configuration
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TelegramConfig {
    #[serde(default)]
    pub bot_token: String,
    #[serde(default)]
    pub chat_id: String,
    #[serde(default)]
    pub emergency_mention_users: Vec<String>,
}

impl TelegramConfig {
    /// Load from environment variables
    pub fn from_env() -> Self {
        let bot_token = std::env::var("TELEGRAM_BOT_TOKEN").unwrap_or_default();
        let chat_id = std::env::var("TELEGRAM_CHAT_ID").unwrap_or_default();
        let emergency_mention_users = std::env::var("TELEGRAM_EMERGENCY_MENTION")
            .unwrap_or_default()
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();

        Self {
            bot_token,
            chat_id,
            emergency_mention_users,
        }
    }
}

/// Paired message detection configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PairedMessageConfig {
    #[serde(default = "default_paired_enabled")]
    pub enabled: bool,
    #[serde(default = "default_alert_threshold")]
    pub alert_threshold_seconds: u64,
    #[serde(default = "default_check_interval")]
    pub check_interval_seconds: u64,
}

fn default_paired_enabled() -> bool {
    true
}

fn default_alert_threshold() -> u64 {
    3600 // 1 hour
}

fn default_check_interval() -> u64 {
    300 // 5 minutes
}

impl Default for PairedMessageConfig {
    fn default() -> Self {
        Self {
            enabled: default_paired_enabled(),
            alert_threshold_seconds: default_alert_threshold(),
            check_interval_seconds: default_check_interval(),
        }
    }
}

impl PairedMessageConfig {
    pub fn alert_threshold(&self) -> Duration {
        Duration::from_secs(self.alert_threshold_seconds)
    }

    pub fn check_interval(&self) -> Duration {
        Duration::from_secs(self.check_interval_seconds)
    }
}

/// Emergency Pause Configuration
///
/// Automatically pause bridge when detecting unauthorized minting.
/// Scenario: If we see a mint on Starcoin without corresponding ETH deposit,
/// it indicates key compromise - immediately pause both chains.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EmergencyPauseConfig {
    /// Time window (seconds) to wait for matching deposit before triggering pause
    /// Default: 300 (5 minutes)
    #[serde(default = "default_detection_window")]
    pub detection_window_seconds: u64,

    /// Path to bridge-cli binary (supports ${ENV_VAR} substitution)
    /// Default: reads from STARCOIN_BRIDGE_CLI environment variable
    #[serde(default)]
    pub bridge_cli_path: Option<String>,

    /// Path to bridge-cli config file (contains RPC URLs and keys for transaction submission)
    #[serde(default)]
    pub bridge_cli_config_path: Option<PathBuf>,

    /// Pre-signed pause signatures for ETH chain (hex encoded, comma-separated)
    #[serde(default)]
    pub eth_signatures: Vec<String>,

    /// Pre-signed pause signatures for Starcoin chain (hex encoded, comma-separated)
    #[serde(default)]
    pub starcoin_signatures: Vec<String>,

    /// Current governance nonce for ETH emergency actions
    #[serde(default)]
    pub eth_nonce: u64,

    /// Current governance nonce for Starcoin emergency actions
    #[serde(default)]
    pub starcoin_nonce: u64,
}

fn default_detection_window() -> u64 {
    300 // 5 minutes
}

impl Default for EmergencyPauseConfig {
    fn default() -> Self {
        Self {
            detection_window_seconds: default_detection_window(),
            bridge_cli_path: None,
            bridge_cli_config_path: None,
            eth_signatures: vec![],
            starcoin_signatures: vec![],
            eth_nonce: 0,
            starcoin_nonce: 0,
        }
    }
}

impl EmergencyPauseConfig {
    pub fn detection_window(&self) -> Duration {
        Duration::from_secs(self.detection_window_seconds)
    }

    /// Get bridge-cli binary path, with fallback to environment variable
    pub fn get_bridge_cli_path(&self) -> Option<String> {
        if let Some(ref path) = self.bridge_cli_path {
            Some(path.clone())
        } else {
            std::env::var("STARCOIN_BRIDGE_CLI").ok()
        }
    }

    /// Check if emergency pause is executable (has required config)
    pub fn can_execute(&self) -> bool {
        self.get_bridge_cli_path().is_some()
            && self.bridge_cli_config_path.is_some()
            && !self.eth_signatures.is_empty()
            && !self.starcoin_signatures.is_empty()
    }
}

/// Reorg detection configuration
///
/// Tracks sent notifications and alerts if blockchain reorganization causes
/// previously notified events to become invalid.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReorgDetectionConfig {
    /// Enable reorg detection
    #[serde(default = "default_reorg_enabled")]
    pub enabled: bool,

    /// Duration (in seconds) to track events after notification
    /// After this time, events are assumed finalized and removed from tracking
    /// Default: 300 seconds (5 minutes)
    #[serde(default = "default_reorg_tracking_duration")]
    pub tracking_duration_seconds: u64,

    /// How often to check for reorgs (seconds)
    /// Default: 10 seconds
    #[serde(default = "default_reorg_check_interval")]
    pub check_interval_seconds: u64,
}

fn default_reorg_enabled() -> bool {
    false // Disabled by default, enable for testnet/mainnet
}

fn default_reorg_tracking_duration() -> u64 {
    300 // 5 minutes
}

fn default_reorg_check_interval() -> u64 {
    10 // 10 seconds
}

impl Default for ReorgDetectionConfig {
    fn default() -> Self {
        Self {
            enabled: default_reorg_enabled(),
            tracking_duration_seconds: default_reorg_tracking_duration(),
            check_interval_seconds: default_reorg_check_interval(),
        }
    }
}

impl ReorgDetectionConfig {
    pub fn tracking_duration(&self) -> Duration {
        Duration::from_secs(self.tracking_duration_seconds)
    }

    pub fn check_interval(&self) -> Duration {
        Duration::from_secs(self.check_interval_seconds)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_substitute_env_vars_basic() {
        std::env::set_var("TEST_VAR", "test_value");
        std::env::set_var("ANOTHER_VAR", "another_value");

        let input = "bot_token: ${TEST_VAR}\nchat_id: ${ANOTHER_VAR}";
        let result = substitute_env_vars(input).unwrap();

        assert_eq!(result, "bot_token: test_value\nchat_id: another_value");

        std::env::remove_var("TEST_VAR");
        std::env::remove_var("ANOTHER_VAR");
    }

    #[test]
    fn test_substitute_env_vars_missing() {
        std::env::remove_var("MISSING_VAR");

        let input = "value: ${MISSING_VAR}";
        let result = substitute_env_vars(input).unwrap();

        // Should keep placeholder when var is missing
        assert_eq!(result, "value: ${MISSING_VAR}");
    }

    #[test]
    fn test_substitute_env_vars_multiple_occurrences() {
        std::env::set_var("REPEATED_VAR", "repeated");

        let input = "first: ${REPEATED_VAR}\nsecond: ${REPEATED_VAR}";
        let result = substitute_env_vars(input).unwrap();

        assert_eq!(result, "first: repeated\nsecond: repeated");

        std::env::remove_var("REPEATED_VAR");
    }

    #[test]
    fn test_substitute_env_vars_yaml_format() {
        std::env::set_var("TELEGRAM_BOT_TOKEN", "123456:ABC-DEF");
        std::env::set_var("TELEGRAM_CHAT_ID", "-100123456789");
        std::env::set_var("TELEGRAM_EMERGENCY_MENTION", "@user1,@user2");

        let input = r#"
telegram:
  bot_token: ${TELEGRAM_BOT_TOKEN}
  chat_id: ${TELEGRAM_CHAT_ID}
  emergency_mention_users: ${TELEGRAM_EMERGENCY_MENTION}
"#;
        let result = substitute_env_vars(input).unwrap();

        assert!(result.contains("bot_token: 123456:ABC-DEF"));
        assert!(result.contains("chat_id: -100123456789"));
        assert!(result.contains("emergency_mention_users: @user1,@user2"));

        std::env::remove_var("TELEGRAM_BOT_TOKEN");
        std::env::remove_var("TELEGRAM_CHAT_ID");
        std::env::remove_var("TELEGRAM_EMERGENCY_MENTION");
    }

    #[test]
    fn test_substitute_env_vars_no_substitution() {
        let input = "plain: value\nno_vars: here";
        let result = substitute_env_vars(input).unwrap();

        assert_eq!(result, input);
    }

    #[test]
    fn test_substitute_env_vars_invalid_syntax() {
        // Incomplete placeholder should not be substituted
        let input = "incomplete: ${INCOMPLETE";
        let result = substitute_env_vars(input).unwrap();

        assert_eq!(result, input);
    }
}
