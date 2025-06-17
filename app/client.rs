use serde_json::error;
use KEEP_RUNNING::raft::{proto, rpc};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex as TokioMutex;
use tracing::{error, info, warn};

const CLUSTER_ADDRS: [&str; 5] = [
    "[::1].9001",
    "[::1]:9002", 
    "[::1]:9003",
    "[::1]:9004",
    "[::1]:9005",
];

// 维护一个全局的 LeaderCache， 避免每个任务都去查找Leader
struct LeaderCache {
    leader_info : TokioMutex<Option<proto::ServerInfo>>,
    rpc_client: rpc::Client,
}

impl LeaderCache {
    fn new() -> Self {
        Self {
            leader_info: TokioMutex::new(None),
            rpc_client: rpc::Client {},
        }
    }
    async fn get_leader(&self) -> Option<proto::ServerInfo> {
        let mut leader_info_guard = self.leader_info.lock().await;

        if let Some(leader) = &*leader_info_guard {
            return Some(leader.clone());
        }

        // 如果没有缓存的 Leader 信息，则查询
        info!("No cached leader info, querying cluster...");
        for addr in CLUSTER_ADDRS.iter() {
            let request = proto::GetLeaderRequest {};
            if let Ok(resp) = self.rpc_client.get_leader(request, addr.to_string()).await {
                if let Some(leader) = resp.leader {
                    info!("Found leader: ID={}, Addr={}", leader.server_id, leader.server_addr);
                    *leader_info_guard = Some(leader.clone());
                    return Some(leader);
                }
            }
            warn!("Failed to get leader from {}. Trying next node.", addr);
        }
        None 
    }

    async fn update(&self, new_leader: Option<proto::ServerInfo>) {
        let mut leader_info_guard = self.leader_info.lock().await;
        *leader_info_guard = new_leader;
    }
}


async fn find_leader(rpc_client: &rpc::Client) -> Option<proto::ServerInfo> {
    for addr in CLUSTER_ADDRS.iter() {
        info!("Querying get-leader from {}", addr);
        let request = proto::GetLeaderRequest {};
        match rpc_client.get_leader(request, addr.to_string()).await {
            Ok(resp) => if let Some(leader) = resp.leader { return Some(leader) },
            Err(e) => warn!("Failed to get leader from {}: {}. Trying next node.", addr, e),
        }
    }
    None
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let _ = tracing_subscriber::fmt().with_max_level(tracing::Level::INFO).init();

    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        println!("Usage:\n");
        println!("  client get-leader");
        println!("  client get-config");
        println!("  client propose <DATA>");
        println!("  client bench <CONSURRENT_TASKS> <TOTAL_REQUESTS>");
        return Ok(());
    }

    let command = &args[1];
    let mut rpc_client = rpc::Client {};
    let leader_cache = Arc::new(LeaderCache::new());

    match command.as_str() {
        "get-leader" => {
            if let Some(leader) = leader_cache.get_leader().await {
                println!("Current Leader: ID={}, Addr={}", leader.server_id, leader.server_addr);
            } else {
                error!("Could not find the leader in the cluster.");
            }
        }
        "get-config" => {
            for addr in CLUSTER_ADDRS.iter() {
                let request = proto::GetConfigurationRequest {};
                match rpc_client.get_configuration(request, addr.to_string()).await {
                    Ok(resp) => {
                        println!("Current Cluster Configuration:");
                        for server in resp.servers {
                            println!("  - ID: {}, Addr: {}", server.server_id, server.server_addr);
                        }
                        return Ok(());
                    }
                    Err(e) => warn!("Failed to get config from {}: {}. Trying next node.", addr, e),
                }
            }
            error!("Could not get configuration from any node in the cluster.");
        }
        "set-config" => {
            if args.len() < 3 {
                error!("Usage: client set-config <id:addr> [id:addr] ...");
                return Ok(());
            }

            let mut new_servers = vec![];
            for arg in &args[2..] {
                let parts: Vec<&str> = arg.split(':').collect();
                if parts.len() < 2 {
                    error!("Invalid server format: {}. Expected 'id:address'", arg);
                    return Ok(());
                }
                let server_id = parts[0].parse::<u64>()?;
                let server_addr = parts[1..].join(":");
                new_servers.push(proto::ServerInfo { server_id, server_addr });
            }

            if let Some(leader) = find_leader(&rpc_client).await {
                info!("Found leader {}: {}. Sending SetConfiguration request.", leader.server_id, leader.server_addr);
                let request = proto::SetConfigurationRequest { new_servers };
                match rpc_client.set_configuration(request, leader.server_addr).await {
                    Ok(resp) if resp.success => println!("Successfully proposed new configuration!"),
                    _ => error!("Leader rejected or failed to process the configuration change."),
                }
            } else {
                error!("Could not find the leader to send the configuration change.");
            }
        }
        "propose" => {
            if args.len() < 3 {
                error!("Usage client propose <DATA>");
                return Ok(());
            }
            let data_to_propose = args[2].clone().into_bytes();

            // 循环直到成功
            // 循环直到成功
            for _ in 0..5 { // 最多重试5次
                if let Some(leader) = leader_cache.get_leader().await {
                    let req = proto::ProposeRequest { data: data_to_propose.clone() };
                    match leader_cache.rpc_client.propose(req, leader.server_addr).await {
                        Ok(resp) if resp.success => {
                            println!("Successfully proposed data!");
                            return Ok(());
                        }
                        Ok(resp) => { // Propose 失败，但收到了 Leader 提示
                            warn!("Propose failed, updating leader hint...");
                            leader_cache.update(resp.leader_addr.map(|addr| proto::ServerInfo {
                                server_id: resp.index.unwrap_or(0),
                                server_addr: addr,
                            })).await;
                        }
                        Err(e) => { // RPC 级别的错误
                            warn!("RPC to leader failed: {}. Invalidating leader cache.", e);
                            leader_cache.update(None).await;
                        }
                    }
                } else {
                    error!("Could not find leader to propose to.");
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        }
        "bench" => {
            if args.len() != 4 {
                error!("Usage: client bench <CONCURRENT_TASKS> <TOTAL_REQUESTS>");
                return Ok(());
            }
            let concurrent_tasks: usize = args[2].parse()?;
            let total_requests: usize = args[3].parse()?;
            
            info!("Starting benchmark with {} concurrent tasks, {} total requests.", concurrent_tasks, total_requests);

            let successful_requests = Arc::new(AtomicUsize::new(0));
            let total_latency = Arc::new(AtomicU64::new(0));
            let start_time = Instant::now();

            let mut handles = vec![];

            for i in 0..concurrent_tasks {
                let leader_cache_clone = Arc::clone(&leader_cache);
                let successful_requests_clone = Arc::clone(&successful_requests);
                let total_latency_clone = Arc::clone(&total_latency);
                let requests_per_task = total_requests / concurrent_tasks;

                let handle = tokio::spawn(async move {
                    for j in 0..requests_per_task {
                        let data = format!("task-{}-req-{}", i, j).into_bytes();
                        let req_start_time = Instant::now();
                        
                        // 循环直到成功
                        loop {
                            if let Some(leader) = leader_cache_clone.get_leader().await {
                                let req = proto::ProposeRequest { data: data.clone() };
                                match leader_cache_clone.rpc_client.propose(req, leader.server_addr).await {
                                    Ok(resp) if resp.success => {
                                        let latency = req_start_time.elapsed().as_micros() as u64;
                                        successful_requests_clone.fetch_add(1, Ordering::SeqCst);
                                        total_latency_clone.fetch_add(latency, Ordering::SeqCst);
                                        break; // 成功，跳出循环
                                    }
                                    Ok(resp) => {
                                        warn!("Task {}: Propose failed, updating leader hint...", i);
                                        leader_cache_clone.update(resp.leader_addr.map(|addr| proto::ServerInfo {
                                            server_id: resp.index.unwrap_or(0),
                                            server_addr: addr,
                                        })).await;
                                    }
                                    Err(e) => {
                                        warn!("Task {}: RPC to leader failed: {}. Invalidating leader cache.", i, e);
                                        leader_cache_clone.update(None).await;
                                    }
                                }
                            } else {
                                error!("Task {}: Could not find leader. Retrying...", i);
                                tokio::time::sleep(Duration::from_millis(500)).await;
                            }
                        }
                    }
                });
                handles.push(handle);
            }
            
            // 等待所有压测任务完成
            for handle in handles {
                handle.await?;
            }

            let total_duration = start_time.elapsed();
            let successful_count = successful_requests.load(Ordering::Relaxed);
            let avg_latency_us = if successful_count > 0 {
                total_latency.load(Ordering::Relaxed) / successful_count as u64
            } else { 0 };

            println!("\n--- Benchmark Results ---");
            println!("Total time: {:?}", total_duration);
            println!("Concurrent tasks: {}", concurrent_tasks);
            println!("Total requests: {}", total_requests);
            println!("Successful requests: {}", successful_count);
            println!("Requests per second (RPS): {:.2}", successful_count as f64 / total_duration.as_secs_f64());
            println!("Average latency: {} \u{00B5}s (microseconds)", avg_latency_us);
        }
        _ => error!("Unknown command: {}", command),
    }

    Ok(())
}