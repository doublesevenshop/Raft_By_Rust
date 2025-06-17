use super::logging::*;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tracing_subscriber::fmt::writer::MakeWriterExt;

use super::*;
use tokio::sync::Mutex as TokioMutex;

pub async fn start (
    server_id: u64,
    port: u32,
    initial_peers_info: Vec<proto::ServerInfo>,
    state_machine: Box<dyn state_machine::StateMachine>,
    snapshot_dir_str: String,
    metadata_dir_str: String,
) -> Result<Arc<TokioMutex<consensus::Consensus>>, Box<dyn std::error::Error + Send + Sync>> {

    info!("Starting Raft node {} on port {}", server_id, port);
    // 初始化共识模块
    let consensus_arc = consensus::Consensus::new(
        server_id,
        port,
        initial_peers_info, // 使用 ServerInfo 列表
        state_machine,
        snapshot_dir_str,  // 直接传递 String
        metadata_dir_str,  // 直接传递 String
    ).await; // 调用 await

    // 启动 rpc server
    let consensus_clone_for_rpc = Arc::clone(&consensus_arc);
    let addr = format!("[::1]:{}", port);
    tokio::spawn(async move {
        info!("Attempting to start RPC server on {} for Raft node {}", addr, server_id);
        if let Err(e) = rpc::start_server(&addr, consensus_clone_for_rpc).await { // 调用 await
            error!("Tonic rpc server for node {} failed to start or encountered an error: {}", server_id, e);
            // 在实际应用中，这里可能需要更健壮的错误处理，例如通知主程序或尝试重启
        } else {
            info!("RPC server for Raft node {} has shut down.", server_id);
        }
    });
    info!("RPC server task for node {} spawned.", server_id);

    info!("Raft node {} fully started and initialized.", server_id);
    Ok(consensus_arc)
}

pub async fn stop(
    consensus_arc: Arc<TokioMutex<consensus::Consensus>>,
    // rpc_server_handle: Option<tokio::task::JoinHandle<()>> // 如果 rpc::start_server 返回句柄
) -> Result<(), String> {
    info!("Attempting to stop Raft node...");
    let mut consensus_guard = consensus_arc.lock().await;


    // 调用Consensus内部的 shutdown 方法
    consensus_guard.shutdown().await;
    info!("Consensus module shutdown initiated for node {}.", consensus_guard.server_id);

    // TODO 这里的处理不够细腻，比较复杂，后续需要重新设计
    
    // 对于测试，简单地让spawned RPC在服务器任务在Consensus被drop后自然结束
    drop(consensus_guard);
    info!("Raft node stop sequence complete. RPC server might need separate handling for graceful shutdown.");
    Ok(())

}