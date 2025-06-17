use std::fs;
use std::path::Path;
use core::panic;
use std::io::{Read, Write};
use std::sync::Arc;
use tokio::sync::Mutex as TokioMutex;
use tracing::{error, info};
use KEEP_RUNNING::raft::{self, config, snapshot};
use KEEP_RUNNING::raft::{consensus, proto, rpc, state_machine};
use tracing_subscriber::fmt::writer::MakeWriterExt;
use std::collections::HashMap;
use tokio::task::JoinHandle;
use tokio::time::{Duration, Instant};


#[derive(Debug, Default, Clone)]
struct MystateMachine {
    // 使用Mutex以便在快照时安全访问
    datas: Arc<std::sync::Mutex<Vec<Vec<u8>>>>,
}

impl MystateMachine {
    fn new() -> Self {
        Self {
            datas: Arc::new(std::sync::Mutex::new(Vec::new())),
        }
    }
}

impl state_machine::StateMachine for MystateMachine {
    fn apply(&mut self, data: &Vec<u8>) {
        let mut datas_guard = self.datas.lock().unwrap();

        // 如果是配置条目，打印出来看看
        if let Ok(config) = serde_json::from_slice::<config::Config>(data) {
            info!("Applied a configuration change to state machine. New config: {:?}", config);
        } else {
            datas_guard.push(data.clone());
            info!("Applied data to state machine. Total entires: {}", datas_guard.len());
        }
    }

    fn take_snapshot(&mut self, snapshot_filepath: &str) {
        let datas_guard = self.datas.lock().unwrap();

        let snapshot_json = serde_json::to_string(&*datas_guard)
            .expect("Failed to serialize entries to JSON for snapshot");

        if let Err(e) = std::fs::write(snapshot_filepath, snapshot_json.as_bytes()) {
            panic!("failed to write snapshot file {}, error: {}", snapshot_filepath, e);
        }
        info!("State machine snapshot taken to {}", snapshot_filepath);
    }

    fn restore_snapshot(&mut self, snapshot_filepath: &str) {
        if std::path::Path::new(snapshot_filepath).exists() {
            let snapshot_json = std::fs::read_to_string(snapshot_filepath)
                .expect("failed to read snapshot file");
            let datas_from_disk: Vec<Vec<u8>> = serde_json::from_str(&snapshot_json).unwrap();
            let mut datas_guard = self.datas.lock().unwrap();
            *datas_guard = datas_from_disk;
            info!("State machine restored from snapshot {}. Total entries: {}", snapshot_filepath, datas_guard.len());
        }
    }
}


#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {

    // 日志初始化
    let file_appender = tracing_appender::rolling::hourly("./logs", "server.log");
    let (non_blocking, _guard) = tracing_appender::non_blocking(file_appender);
    let writer = non_blocking.and(std::io::stdout);
    let _ = tracing_subscriber::fmt()
        .with_writer(writer)
        .with_max_level(tracing::Level::INFO)
        .try_init();
    info!("Global logger initialized.");
    
    let project_root = std::env::current_dir()?;
    info!("Project root directory: {}", project_root.display());


    // 定义集群配置
    let cluster_info: Vec<(u64, u32)> = vec![
        (1, 9001), 
        (2, 9002), 
        (3, 9003),
        (4, 9004), 
        (5, 9005),
    ];
    
    
    // 1. 将 `all_peers_info` 包装在 Arc 中，使其可以在多个任务间安全共享而无需克隆整个 Vec
    let all_peers_info = Arc::new(
        cluster_info.iter()
            .map(|(id, port)| proto::ServerInfo {
                server_id: *id,
                server_addr: format!("[::1]:{}", port),
            })
            .collect::<Vec<_>>()
    );

    // 使用 HashMap 来管理节点的 JoinHandle，方便我们杀掉和重启
    let mut node_handles: HashMap<u64, JoinHandle<Option<Arc<TokioMutex<consensus::Consensus>>>>> = HashMap::new();
    let project_root = std::env::current_dir()?;

    for (server_id, port) in &cluster_info {
        let handle = spawn_node(*server_id, *port, Arc::clone(&all_peers_info), project_root.clone()).await;
        node_handles.insert(*server_id, handle);
    }

    // ========== 新增：混沌测试线程 ==========
    // 使用一个命令行参数来决定是否开启 chaos 模式
    let args: Vec<String> = std::env::args().collect();
    if args.contains(&"--chaos".to_string()) {
        info!("Chaos mode enabled! Nodes will be randomly killed and restarted.");
        
        let chaos_all_peers = Arc::clone(&all_peers_info);
        let chaos_project_root = project_root.clone();

        tokio::spawn(async move {
            loop {
                // 每隔 15-30 秒搞一次事情
                let sleep_duration = Duration::from_secs(rand::random_range(15..30));
                tokio::time::sleep(sleep_duration).await;
                
                let target_id = rand::random_range(1..=cluster_info.len() as u64);

                info!("[CHAOS] Targeting node {} for termination.", target_id);
                if let Some(handle) = node_handles.get(&target_id) {
                    handle.abort(); // 模拟进程被 kill
                    info!("[CHAOS] Node {} terminated.", target_id);
                }
                
                // 等待几秒钟，模拟节点恢复时间
                tokio::time::sleep(Duration::from_secs(5)).await;

                info!("[CHAOS] Restarting node {}.", target_id);
                let port = cluster_info.iter().find(|(id, _)| *id == target_id).unwrap().1;
                let new_handle = spawn_node(target_id, port, Arc::clone(&chaos_all_peers), chaos_project_root.clone()).await;
                node_handles.insert(target_id, new_handle);
                info!("[CHAOS] Node {} restarted.", target_id);
            }
        });
    }
    
    info!("All Raft nodes have been launched and are running.");
    info!("To run chaos test: `cargo run --example server -- --chaos`");
    
    tokio::signal::ctrl_c().await?;
    info!("Ctrl-C received, shutting down.");
    Ok(())
}

// 将节点启动逻辑封装成一个函数，方便复用
async fn spawn_node(
    server_id: u64,
    port: u32,
    all_peers_info: Arc<Vec<proto::ServerInfo>>,
    project_root: std::path::PathBuf,
) -> JoinHandle<Option<Arc<TokioMutex<consensus::Consensus>>>> {
    tokio::spawn(async move {
        info!("Preparing to start Raft node {} on port {}", server_id, port);
        // ... (构建路径、创建状态机等逻辑和之前一样) ...
        let snapshot_dir = project_root.join(format!(".snapshot/server_{}", server_id));
        let metadata_dir = project_root.join(format!(".metadata/server_{}", server_id));
        let _ = tokio::fs::create_dir_all(&snapshot_dir).await;
        let _ = tokio::fs::create_dir_all(&metadata_dir).await;
        let state_machine = Box::new(MystateMachine::new());
        let peers_vec: Vec<proto::ServerInfo> = (*all_peers_info).clone();
        
        match raft::lib::start(
            server_id, port, peers_vec, state_machine,
            snapshot_dir.to_str().unwrap().to_string(),
            metadata_dir.to_str().unwrap().to_string()
        ).await {
            Ok(arc) => Some(arc),
            Err(e) => {
                error!("Raft node {} failed to start: {}", server_id, e);
                None
            }
        }
    })
}