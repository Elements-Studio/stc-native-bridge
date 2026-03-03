# Starcoin Bridge

Bidirectional bridge connecting Starcoin and Ethereum for cross-chain USDT transfers.

## Overview

The bridge uses a validator committee with BFT consensus and signature aggregation.

### Transfer Flow

| Direction | Flow |
|-----------|------|
| **ETH → Starcoin** | User deposits → Validators sign → Auto-claim on Starcoin |
| **Starcoin → ETH** | User deposits → Validators approve → **User manually claims on ETH** |

## Architecture

```
    Ethereum                          Starcoin
    ┌──────────┐                     ┌──────────┐
    │  Bridge  │                     │  Bridge  │
    │ Contract │                     │  Module  │
    └────┬─────┘                     └────┬─────┘
         │           Events               │
         └──────────┐    ┌────────────────┘
                    ▼    ▼
              ┌─────────────────┐
              │  Bridge Server  │
              │  (Validators)   │
              └─────────────────┘
```

## Project Structure

```
stc-native-bridge/
├── bridge/                    # Core validator node (binary: starcoin-bridge)
├── bridge-cli/                # CLI tools (binary: starcoin-bridge-cli) ← Main binary
├── bridge-indexer-monitor/    # Event indexer & security monitor
├── contracts/
│   ├── evm/                   # Solidity (StarcoinBridge.sol, BridgeCommittee.sol, BridgeVault.sol)
│   └── move/                  # Move modules
├── starcoin-bridge-sdk/       # SDK library
├── starcoin-bridge-types/     # Shared types
├── starcoin-bridge-vm-types/  # VM types (bridge actions, committee)
└── fastcrypto/                # Cryptography primitives
```

### Main Binaries

| Binary | Crate | Description |
|--------|-------|-------------|
| `starcoin-bridge-cli` | `bridge-cli/` | **Primary CLI** - governance, transfers, bridge state queries |
| `starcoin-bridge` | `bridge/` | Validator node daemon |
| `starcoin-bridge-indexer-monitor` | `bridge-indexer-monitor/` | Event indexer & emergency pause monitor |

## Build

```bash
cargo build --release -p starcoin-bridge-cli
```

## Governance Commands

```bash
# Emergency pause
starcoin-bridge-cli governance \
  --config-path config.yaml \
  --eth-chain-id 12 \
  emergency-button --nonce 0 --action-type pause

# Offline signing (air-gapped)
starcoin-bridge-cli governance-sign \
  --key-path admin.key \
  --eth-chain-id 12 \
  emergency-button --nonce 0 --action-type pause

# Execute with signature
starcoin-bridge-cli governance-execute \
  --config-path config.yaml \
  --eth-chain-id 12 \
  --signatures <SIG_HEX> \
  emergency-button --nonce 0 --action-type pause
```

### Chain IDs

| ETH | ID | Starcoin | ID |
|-----|----|----------|----|
| Mainnet | 10 | Mainnet | 0 |
| Sepolia | 11 | Testnet | 1 |
| Local | 12 | Local | 2 |

## Security

- **Multi-Sig Committee**: BFT consensus with signature aggregation
- **Rate Limiting**: Per-chain transfer limits
- **Manual Claim**: Starcoin→ETH requires user action
- **Air-Gapped Governance**: Offline signing for admin operations

## License

Apache License 2.0

