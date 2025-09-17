/// Testing pending block functionality with mempool transactions
use std::str::FromStr;
use std::time::Duration;

use alloy_primitives::Address;
use alloy_rpc_types::BlockNumberOrTag;
use citrea_common::SequencerConfig;
use citrea_stf::genesis_config::GenesisPaths;
use tokio::time::sleep;

use super::evm::init_test_rollup;
use crate::common::helpers::{
    create_default_rollup_config, start_rollup, tempdir_with_children, wait_for_l2_block, NodeMode,
};
use crate::common::TEST_DATA_GENESIS_PATH;

/// Test that pending block returns valid block when queried
#[tokio::test(flavor = "multi_thread")]
async fn test_pending_block_basic() -> Result<(), anyhow::Error> {
    // citrea::initialize_logging(tracing::Level::INFO);

    let storage_dir = tempdir_with_children(&["DA", "sequencer"]);
    let da_db_dir = storage_dir.path().join("DA").to_path_buf();
    let sequencer_db_dir = storage_dir.path().join("sequencer").to_path_buf();

    let (seq_port_tx, seq_port_rx) = tokio::sync::oneshot::channel();

    let rollup_config = create_default_rollup_config(
        true,
        &sequencer_db_dir,
        &da_db_dir,
        NodeMode::SequencerNode,
        None,
    );

    let sequencer_config = SequencerConfig {
        max_l2_blocks_per_commitment: 1000,
        da_update_interval_ms: 500,
        block_production_interval_ms: 2,
        ..Default::default()
    };

    let seq_task = start_rollup(
        seq_port_tx,
        GenesisPaths::from_dir(TEST_DATA_GENESIS_PATH),
        None,
        None,
        rollup_config,
        Some(sequencer_config),
        None,
        false,
    )
    .await;

    let seq_port = seq_port_rx.await.unwrap();
    let seq_test_client = init_test_rollup(seq_port).await;

    let latest_block = seq_test_client
        .eth_get_block_by_number_with_detail(Some(BlockNumberOrTag::Latest))
        .await;

    let latest_block_number = latest_block.header.number;

    let pending_block = seq_test_client
        .eth_get_block_by_number_with_detail(Some(BlockNumberOrTag::Pending))
        .await;

    assert_eq!(pending_block.header.number, latest_block_number + 1);

    seq_task.graceful_shutdown();
    Ok(())
}

/// Test that pending block includes transactions from mempool
#[tokio::test(flavor = "multi_thread")]
async fn test_pending_block_with_transactions() -> Result<(), anyhow::Error> {
    // citrea::initialize_logging(tracing::Level::INFO);

    let storage_dir = tempdir_with_children(&["DA", "sequencer"]);
    let da_db_dir = storage_dir.path().join("DA").to_path_buf();
    let sequencer_db_dir = storage_dir.path().join("sequencer").to_path_buf();

    let (seq_port_tx, seq_port_rx) = tokio::sync::oneshot::channel();

    let rollup_config = create_default_rollup_config(
        true,
        &sequencer_db_dir,
        &da_db_dir,
        NodeMode::SequencerNode,
        None,
    );

    let sequencer_config = SequencerConfig {
        max_l2_blocks_per_commitment: 1000,
        da_update_interval_ms: 500,
        block_production_interval_ms: 2,
        ..Default::default()
    };

    let seq_task = start_rollup(
        seq_port_tx,
        GenesisPaths::from_dir(TEST_DATA_GENESIS_PATH),
        None,
        None,
        rollup_config,
        Some(sequencer_config),
        None,
        false,
    )
    .await;

    let seq_port = seq_port_rx.await.unwrap();
    let seq_test_client = init_test_rollup(seq_port).await;

    let to_address = Address::from_str("0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb7")?;
    let tx_pending = seq_test_client
        .send_eth(to_address, None, None, None, 1e18 as u128)
        .await?;
    let tx_hash = *tx_pending.tx_hash();

    sleep(Duration::from_millis(500)).await;

    let pending_block = seq_test_client
        .eth_get_block_by_number_with_detail(Some(BlockNumberOrTag::Pending))
        .await;

    if let alloy_rpc_types::BlockTransactions::Full(txs) = &pending_block.transactions {
        assert!(!txs.is_empty(), "Pending block should contain transactions");

        let found = txs.iter().any(|tx| *tx.inner.hash() == tx_hash);
        assert!(found, "Our transaction should be in the pending block");
    } else {
        panic!("Expected full transaction details in pending block");
    }

    seq_task.graceful_shutdown();
    Ok(())
}

/// Test that pending block resets after mining
#[tokio::test(flavor = "multi_thread")]
async fn test_pending_block_resets_after_mining() -> Result<(), anyhow::Error> {
    // citrea::initialize_logging(tracing::Level::INFO);

    let storage_dir = tempdir_with_children(&["DA", "sequencer"]);
    let da_db_dir = storage_dir.path().join("DA").to_path_buf();
    let sequencer_db_dir = storage_dir.path().join("sequencer").to_path_buf();

    let (seq_port_tx, seq_port_rx) = tokio::sync::oneshot::channel();

    let rollup_config = create_default_rollup_config(
        true,
        &sequencer_db_dir,
        &da_db_dir,
        NodeMode::SequencerNode,
        None,
    );

    let sequencer_config = SequencerConfig {
        max_l2_blocks_per_commitment: 1000,
        da_update_interval_ms: 500,
        block_production_interval_ms: 2,
        ..Default::default()
    };

    let seq_task = start_rollup(
        seq_port_tx,
        GenesisPaths::from_dir(TEST_DATA_GENESIS_PATH),
        None,
        None,
        rollup_config,
        Some(sequencer_config),
        None,
        false,
    )
    .await;

    let seq_port = seq_port_rx.await.unwrap();
    let seq_test_client = init_test_rollup(seq_port).await;

    let to_address = Address::from_str("0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb7")?;
    let _tx_hash = seq_test_client
        .send_eth(to_address, None, None, None, 1e18 as u128)
        .await?;

    sleep(Duration::from_millis(500)).await;

    let pending_before = seq_test_client
        .eth_get_block_by_number(Some(BlockNumberOrTag::Pending))
        .await;
    let pending_before_number = pending_before.header.number;

    assert!(!pending_before.transactions.is_empty());

    let current_block = seq_test_client
        .eth_get_block_by_number(Some(BlockNumberOrTag::Latest))
        .await;
    let current_block_number = current_block.header.number;

    seq_test_client.send_publish_batch_request().await;
    wait_for_l2_block(&seq_test_client, current_block_number + 1, None).await;

    let pending_after = seq_test_client
        .eth_get_block_by_number(Some(BlockNumberOrTag::Pending))
        .await;

    assert_eq!(
        pending_after.header.number,
        pending_before_number + 1,
        "Pending block should advance after mining"
    );
    assert!(pending_after.transactions.is_empty());

    seq_task.graceful_shutdown();
    Ok(())
}
