use crate::raft::{config, log, metadata, peer, proto, rpc, snapshot, state_machine, timer, util};
use super::logging::*; 
use std::io::{Read, Seek, Write};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, Instant as StdInstant};
use tokio::sync::Mutex as TokioMutex;
use futures::future;

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum State {
    Follower,
    Candidate,
    Leader,
}

pub struct Consensus {
    // 身份配置
    pub server_id: u64,                                 // 当前服务器唯一ID
    pub server_addr: String,                            // IP地址，用于RPC通信
    pub metadata: Arc<metadata::MetadataManager>,       // 持久化元数据管理器
    pub state: State,                                   // 当前节点状态(Follower, Candidate, Leader)
    pub current_config: config::Config,                 // 当前集群活跃配置
    pub node_config_state: config::ConfigState,         // 当前节点在集群中的角色(newing, olding)
    
    // 日志与状态机相关
    pub log: log::Log,                                  // 日志模块
    pub commit_index: u64,                              // 已知的被提交的最高日志条目索引
    pub last_applied: u64,                              // 已应用到状态机的最高日志条目索引
    pub state_machine: Box<dyn state_machine::StateMachine>,// 用户定义的状态机

    // Leader的选举与维护
    pub leader_id: u64,                                 // 当前认定的Leader ID
    pub election_timer: Arc<TokioMutex<timer::Timer>>,  // 选举超时计时器
    pub heartbeat_timer: Arc<TokioMutex<timer::Timer>>, // 心跳超时计时器(Leader计时器)
    
    // 集群管理
    pub peer_manager: peer::PeerManager,            // 管理集群中的其他节点

    // 快照相关 
    pub snapshot: snapshot::Snapshot,                   // 快照模块实例
    pub snapshot_timer: Arc<TokioMutex<timer::Timer>>,  // 快照生成定时器
    
    // RPC通信
    rpc_client: rpc::Client,                            // 用于向其他节点发送RPC的客户端
}

impl Consensus {
    pub async fn new(
        server_id: u64,
        port: u32,
        initial_peers_info: Vec<proto::ServerInfo>,
        state_machine: Box<dyn state_machine::StateMachine>,
        snapshot_dir: String,
        metadata_dir: String,
    ) -> Arc<TokioMutex<Consensus>> {


        // 初始化元数据管理器 (MetadataManager::new 内部会 tokio::spawn)
        let initial_metadata_result = metadata::Metadata::load(&metadata_dir);
        let initial_metadata = initial_metadata_result.unwrap_or_else(|e| {
            warn!("Consensus::new: Failed to load metadata from {}: {}. Creating new.", metadata_dir, e);
            metadata::Metadata::new(metadata_dir.clone())
        });

        // Metadata内部会tokio::spawn一个后台任务来处理异步持久化
        let metadata_manager = metadata::MetadataManager::new(initial_metadata, Duration::from_millis(100));

        let server_addr = format!("[::1]:{}", port);


        // 加载日志
        let mut log_instance = log::Log::new(1, metadata_dir.clone());
        log_instance.reload();
        // 加载快照
        let mut snapshot_instance = snapshot::Snapshot::new(snapshot_dir);
        snapshot_instance.reload_metadata();


        // 确定初始配置
        /*
            优先从快照中获取配置，
            如果快照没有，则尝试从日志的最后一个配置条目获取配置条目，
            如果二者都没有，则基于传入的initial_peers_info创建一个新的稳定的配置
         */
        let initial_config = snapshot_instance.configuration.clone().unwrap_or_else(|| {
            log_instance.last_configuration().unwrap_or_else(|| {
                info!("Consensus::new: No configuration found in snapshot or log. Creating initial stable configuration.");
                let mut initial_cluster_servers = initial_peers_info.clone();
                if !initial_cluster_servers.iter().any(|s| s.server_id == server_id) {
                    initial_cluster_servers.push(proto::ServerInfo {
                        server_id,
                        server_addr: server_addr.clone(),
                    });
                }
                config::Config::new_stable(initial_cluster_servers)
            })
        });
        // 根据初始配置计算当前节点的node_config_state
        let node_config_state = initial_config.get_node_state(server_id);


        // 填充所有字段
        let mut consensus_struct = Consensus {
            server_id,
            server_addr,
            metadata: metadata_manager,
            state: State::Follower,
            election_timer: Arc::new(TokioMutex::new(timer::Timer::new("election_timer"))),
            heartbeat_timer: Arc::new(TokioMutex::new(timer::Timer::new("heartbeat_timer"))),
            snapshot_timer: Arc::new(TokioMutex::new(timer::Timer::new("snapshot_timer"))),
            commit_index: 0,
            last_applied: 0,
            leader_id: config::NONE_SERVER_ID,
            peer_manager: peer::PeerManager::new(),
            log: log_instance,
            snapshot: snapshot_instance,
            current_config: initial_config,
            node_config_state,
            rpc_client: rpc::Client {},
            state_machine,
        };


        // 应用快照
        if consensus_struct.snapshot.last_included_index > 0 {  // 说明有快照
            // 调用接口将快照数据恢复到状态机
            if let Some(snapshot_filepath) = consensus_struct.snapshot.latest_snapshot_filepath() { // Removed &mut from latest_snapshot_filepath if it doesn't need it. Assuming it's &self.
                info!("Consensus::new: Restoring state machine from snapshot: {}", snapshot_filepath);
                consensus_struct.state_machine.restore_snapshot(&snapshot_filepath);
                // 更新commit_index和last_applied为快照的last_included_index
                consensus_struct.commit_index = consensus_struct.snapshot.last_included_index;
                consensus_struct.last_applied = consensus_struct.snapshot.last_included_index;
                // 丢弃快照已经覆盖的日志条目
                consensus_struct.log.truncate_prefix(consensus_struct.snapshot.last_included_index);
            } else {    // 没有快照
                warn!("Consensus::new: Snapshot metadata indicates last_included_index > 0 but no snapshot file found.");
            }
        }

        // 初始化PeerManager，遍历current_config中所有的服务器，如果服务器不是当前节点，则创建一个Peer实例
        let mut peers_for_manager = Vec::new();
        for server_info in consensus_struct.current_config.all_servers_in_config() {
            if server_info.server_id != server_id {
                peers_for_manager.push(peer::Peer::new(server_info.server_id, server_info.server_addr.clone()));
            }
        }
        // 将这些peer添加到管理器，并且设置其初始next_index
        consensus_struct.peer_manager.add(
            peers_for_manager,
            consensus_struct.log.last_index(consensus_struct.snapshot.last_included_index),
        );
        // 更新Peer配置状态
        consensus_struct.update_peer_config_states();


        // 方便在多任务间共享和同步访问
        let consensus_arc = Arc::new(TokioMutex::new(consensus_struct));

        // 启动定时器
        let election_timer_arc_clone;
        let heartbeat_timer_arc_clone;
        let snapshot_timer_arc_clone;
        {
            let tmp_consensus_guard = consensus_arc.lock().await;

            election_timer_arc_clone = Arc::clone(&tmp_consensus_guard.election_timer);
            heartbeat_timer_arc_clone = Arc::clone(&tmp_consensus_guard.heartbeat_timer);
            snapshot_timer_arc_clone = Arc::clone(&tmp_consensus_guard.snapshot_timer);

            drop(tmp_consensus_guard);  // 释放锁
        }

        let election_consensus_weak = Arc::downgrade(&consensus_arc);
        let mut election_timer_guard = election_timer_arc_clone.lock().await;
        election_timer_guard.schedule(
            util::rand_election_timeout(),
            move || {
                if let Some(sc_arc_strong) = election_consensus_weak.upgrade() {
                    tokio::spawn(async move {
                        let mut consensus_guard = sc_arc_strong.lock().await;
                        consensus_guard.handle_election_timeout().await;
                    });
                } else {
                    warn!("Election timer fired but Consensus Arc was dropped.");
                }
            },
        );
        drop(election_timer_guard); // 显式释放 guard
        
        
        // 仅Leader使用，向Leader周期性发送心跳，通常是空的AppendEntries RPC
        let heartbeat_consensus_weak = Arc::downgrade(&consensus_arc);
        let mut heartbeat_timer_guard = heartbeat_timer_arc_clone.lock().await; // <--- 使用 .await
        heartbeat_timer_guard.schedule(
            config::HEARTBEAT_INTERVAL,
            move || {
                if let Some(sc_arc_strong) = heartbeat_consensus_weak.upgrade() {
                    tokio::spawn(async move {
                        let mut consensus_guard = sc_arc_strong.lock().await;
                        consensus_guard.handle_heartbeat_timeout().await;
                    });
                } else {
                     warn!("Heartbeat timer fired but Consensus Arc was dropped.");
                }
            },
        );
        drop(heartbeat_timer_guard); // 显式释放 guard

        let snapshot_consensus_weak = Arc::downgrade(&consensus_arc);
        let mut snapshot_timer_guard = snapshot_timer_arc_clone.lock().await; // <--- 使用 .await
        snapshot_timer_guard.schedule(
            config::SNAPSHOT_INTERVAL,
            move || {
                 if let Some(sc_arc_strong) = snapshot_consensus_weak.upgrade() {
                    tokio::spawn(async move {
                        let mut consensus_guard = sc_arc_strong.lock().await;
                        consensus_guard.handle_snapshot_timeout().await;
                    });
                } else {
                    warn!("Snapshot timer fired but Consensus Arc was dropped.");
                }
            },
        );
        drop(snapshot_timer_guard); // 显式释放 guard

        consensus_arc
    }

    fn update_peer_config_states(&mut self) {
        self.node_config_state = self.current_config.get_node_state(self.server_id);
        for peer_in_manager in self.peer_manager.peers_mut().iter_mut() {
            peer_in_manager.config_state = self.current_config.get_node_state(peer_in_manager.id);
        }
    }


    async fn append_entries_to_peers(&mut self, heartbeat: bool) {
        if self.state != State::Leader {
            error!("state is {:?}, can't append entries", self.state);
            return;
        }


        
        let peer_server_ids: Vec<u64> = self.peer_manager.peers().iter().map(|p| p.id).collect();
        debug!(
            "start to append entries (heartbeat: {}) to peers: {:?}",
            heartbeat, &peer_server_ids
        );

        if peer_server_ids.is_empty() {
            self.leader_advance_commit_index().await;
            return;
        }
        // Consider using futures::future::join_all for concurrent appends
        for peer_id in peer_server_ids {
             self.append_one_entry_to_peer(peer_id, heartbeat).await;
        }
        self.leader_advance_commit_index().await;
    }

    async fn append_one_entry_to_peer(&mut self, peer_id: u64, heartbeat: bool) {
        // Use a temporary variable to hold peer_addr to avoid borrowing issues
        let peer_addr_opt = self.peer_manager.peer(peer_id).map(|p| p.addr.clone());

        if peer_addr_opt.is_none() {
            warn!("Peer {} not found in peer_manager when appending entries", peer_id);
            return;
        }
        let peer_addr = peer_addr_opt.unwrap();


        // MODIFIED: Added .await
        let current_term = self.metadata.get().await.current_term;
        let leader_commit_idx = self.commit_index;
        let server_id = self.server_id;


        let (req_prev_log_index, req_prev_log_term, entries_to_send, needs_snapshot) = {
            // Scoped borrow for peer_manager
            let peer_opt = self.peer_manager.peer(peer_id);
            if peer_opt.is_none() {
                warn!("Peer {} disappeared before preparing AppendEntries", peer_id);
                return;
            }
            let peer_ref = peer_opt.unwrap();

            let needs_snapshot_decision = !heartbeat && peer_ref.next_index < self.log.start_index();

            if needs_snapshot_decision {
                (0,0, Vec::new(), true)
            } else {
                let entries = if heartbeat {
                    Vec::new()
                } else {
                    self.log.pack_entries(peer_ref.next_index)
                };

                let prev_idx = peer_ref.next_index - 1;
                let prev_term = self.log.prev_log_term(
                    prev_idx,
                    self.snapshot.last_included_index,
                    self.snapshot.last_included_term,
                );
                (prev_idx, prev_term, entries, false)
            }
        };


        if needs_snapshot {
            let next_idx_for_log = self.peer_manager.peer(peer_id).map_or(0, |p| p.next_index);
            info!("Peer {} requires snapshot, next_index: {}, log_start_index: {}", peer_id, next_idx_for_log, self.log.start_index());
            Box::pin(self.install_snapshot_to_peer(peer_id)).await;
            return;
        }

        let req = proto::AppendEntriesRequest {
            term: current_term,
            leader_id: server_id,
            prev_log_index: req_prev_log_index,
            prev_log_term: req_prev_log_term,
            entries: entries_to_send.clone(), // Clone here if entries_to_send is used later
            leader_commit: leader_commit_idx,
        };

        // `self.rpc_client` methods are `async`, so they need `.await`
        // `rpc_client` should ideally not take `&mut self` if it's just making calls.
        // Assuming `self.rpc_client.append_entries` takes `&self` or `&mut self.rpc_client` implicitly.
        match Box::pin(self.rpc_client.append_entries(req.clone(), peer_addr.clone())).await { // req.clone() if needed by logging/error
            Ok(resp) => {
                // MODIFIED: Added .await (though current_term is already fetched, ensure consistency if it could change)
                if resp.term > self.metadata.get().await.current_term {
                    Box::pin(self.step_down(resp.term)).await;
                    return;
                }
                if let Some(peer_to_update) = self.peer_manager.peer(peer_id) {
                    if resp.success {
                        peer_to_update.match_index = req.prev_log_index + entries_to_send.len() as u64;
                        peer_to_update.next_index = peer_to_update.match_index + 1;
                    } else {
                        if peer_to_update.next_index > 1 {
                            peer_to_update.next_index -= 1;
                        }
                    }
                } else {
                    warn!("Peer {} disappeared before processing AppendEntries response", peer_id);
                }
            }
            Err(e) => {
                error!("AppendEntries RPC to peer {} ({}) failed: {}", peer_id, peer_addr, e);
            }
        }
    }


    async fn install_snapshot_to_peer(&mut self, peer_id: u64) {
        let peer_addr = match self.peer_manager.peer(peer_id) {
            Some(p) => p.addr.clone(),
            None => {
                warn!("Peer {} not found for install_snapshot", peer_id);
                return;
            }
        };

        let current_term = self.metadata.get().await.current_term;
        let leader_id = self.server_id;
        let snap_last_idx = self.snapshot.last_included_index;
        let snap_last_term = self.snapshot.last_included_term;

        let metadata_filepath_opt = self.snapshot.latest_metadata_filepath();
        let snapshot_filepath_opt = self.snapshot.latest_snapshot_filepath();


        if metadata_filepath_opt.is_none() || snapshot_filepath_opt.is_none() {
            error!("Cannot install snapshot: snapshot files (metadata or data) not found.");
            return;
        }
        let metadata_filepath = metadata_filepath_opt.unwrap();
        let snapshot_filepath = snapshot_filepath_opt.unwrap();

        info!("Installing snapshot to peer {}: metadata {} (size {}), snapshot {} (size {})",
            peer_id, metadata_filepath, std::fs::metadata(&metadata_filepath).map(|m| m.len()).unwrap_or(0),
            snapshot_filepath, std::fs::metadata(&snapshot_filepath).map(|m| m.len()).unwrap_or(0));

        let mut current_global_offset = 0;
        // NOTE: File operations here are synchronous. For large files, consider spawn_blocking or tokio::fs.
        if let Ok(mut meta_file) = std::fs::File::open(&metadata_filepath) {
            let meta_size = meta_file.metadata().unwrap().len();
            let mut local_offset = 0;
            while local_offset < meta_size {
                let chunk_len = std::cmp::min(config::SNAPSHOT_TRUNK_SIZE as u64, meta_size - local_offset) as usize;
                let mut data = vec![0; chunk_len];
                meta_file.seek(std::io::SeekFrom::Start(local_offset)).unwrap();
                meta_file.read_exact(&mut data).unwrap();

                let req_install_snap = proto::InstallSnapshotRequest { // Renamed
                    term: current_term, leader_id,
                    last_included_index: snap_last_idx, last_included_term: snap_last_term,
                    offset: current_global_offset,
                    data,
                    snapshot_data_type: proto::SnapshotDataType::Metadata as i32,
                    done: false,
                };
                match Box::pin(self.rpc_client.install_snapshot(req_install_snap, peer_addr.clone())).await {
                    Ok(resp) => if resp.term > self.metadata.get().await.current_term { 
                        Box::pin(self.step_down(resp.term)).await; 
                        return; 
                    }, // MODIFIED .await
                    Err(e) => { error!("Error sending snapshot metadata to {}: {}", peer_id, e); return; }
                }
                current_global_offset += chunk_len as u64;
                local_offset += chunk_len as u64;
            }
        } else { error!("Could not open metadata file {}", metadata_filepath); return; }

        // Send Snapshot Data Chunks
        if let Ok(mut snap_file) = std::fs::File::open(&snapshot_filepath) {
            let snap_size = snap_file.metadata().unwrap().len();
            let mut local_offset = 0;
            while local_offset < snap_size {
                let chunk_len = std::cmp::min(config::SNAPSHOT_TRUNK_SIZE as u64, snap_size - local_offset) as usize;
                let mut data = vec![0; chunk_len];
                snap_file.seek(std::io::SeekFrom::Start(local_offset)).unwrap();
                snap_file.read_exact(&mut data).unwrap();

                let is_last_chunk_of_snapshot = (local_offset + chunk_len as u64) >= snap_size;
                let req_install_snap_data = proto::InstallSnapshotRequest { // Renamed
                    term: current_term, leader_id,
                    last_included_index: snap_last_idx, last_included_term: snap_last_term,
                    offset: current_global_offset,
                    data,
                    snapshot_data_type: proto::SnapshotDataType::Snapshot as i32,
                    done: is_last_chunk_of_snapshot,
                };

                match self.rpc_client.install_snapshot(req_install_snap_data, peer_addr.clone()).await {
                    Ok(resp) => {
                        // MODIFIED: Added .await
                        if resp.term > self.metadata.get().await.current_term { 
                            Box::pin(self.step_down(resp.term)).await; 
                            return; 
                        }
                        if is_last_chunk_of_snapshot {
                            if let Some(p) = self.peer_manager.peer(peer_id) {
                                p.next_index = snap_last_idx + 1;
                                p.match_index = snap_last_idx;
                                info!("Snapshot successfully installed on peer {}. next_index set to {}", peer_id, p.next_index);
                            }
                        }
                    },
                    Err(e) => { error!("Error sending snapshot data to {}: {}", peer_id, e); return; }
                }
                current_global_offset += chunk_len as u64;
                local_offset += chunk_len as u64;
            }
        } else { error!("Could not open snapshot data file {}", snapshot_filepath); return; }
    }



    async fn leader_advance_commit_index(&mut self) {
        if self.state != State::Leader {
            return;
        }
        let new_commit_index = self.peer_manager.quoram_match_index(
            &self.node_config_state,
            self.log.last_index(self.snapshot.last_included_index),
        );

        if new_commit_index > self.commit_index {
            // MODIFIED: Added .await
            let current_term_val = self.metadata.get().await.current_term;
            if let Some(entry_to_check) = self.log.entry(new_commit_index) {
                if entry_to_check.term != current_term_val {
                    debug!("Leader cannot advance commit_index to {} because its term {} is not current term {}",
                           new_commit_index, entry_to_check.term, current_term_val);
                    return;
                }
            } else {
                if new_commit_index <= self.snapshot.last_included_index {
                    // fine
                } else {
                    warn!("Leader wants to advance commit_index to {} but entry not found in log.", new_commit_index);
                    return;
                }
            }

            info!(
                "Leader advancing commit_index from {} to {}",
                self.commit_index, new_commit_index
            );

            for index_to_apply in (self.commit_index + 1)..=new_commit_index {
                if index_to_apply <= self.last_applied {
                    continue;
                }
                if let Some(entry) = self.log.entry(index_to_apply) {
                    let entry_data = entry.data.clone();
                    let entry_type_val = proto::EntryType::from_i32(entry.entry_type).unwrap_or(proto::EntryType::Data);

                    match entry_type_val {
                        proto::EntryType::Data => {
                            debug!("Leader applying data entry to state machine: index {}", entry.index);
                            self.state_machine.apply(&entry_data);
                        }
                        proto::EntryType::Configuration => {
                            info!("Leader applying configuration entry to state machine (committing): index {}", entry.index);
                            let committed_config = config::Config::from_data(&entry_data);
                            self.apply_configuration_to_internal_state(committed_config.clone(), true).await;

                            if committed_config.is_joint() {
                                info!("Committed C(old,new) config. Leader replicating C(new). Config: {:?}", committed_config);
                                self.append_and_replicate_final_config().await;
                            }
                        }
                        proto::EntryType::Noop => {
                            debug!("Leader applying NOOP entry: index {}", entry.index);
                        }
                    }
                    self.last_applied = index_to_apply;
                } else {
                    error!("Entry {} not found in log for leader application, though commit_index advanced.", index_to_apply);
                    break;
                }
            }
            self.commit_index = new_commit_index;
        }
    }

    async fn follower_advance_commit_index(&mut self, leader_commit_index: u64) {
        let new_commit_index = std::cmp::min(
            leader_commit_index,
            self.log.last_index(self.snapshot.last_included_index)
        );

        if new_commit_index > self.commit_index {
            info!(
                "Follower advancing commit_index from {} to {} (leader_commit: {})",
                self.commit_index, new_commit_index, leader_commit_index
            );

            for index_to_apply in (self.commit_index + 1)..=new_commit_index {
                if index_to_apply <= self.last_applied {
                    continue;
                }
                if let Some(entry) = self.log.entry(index_to_apply) {
                    let entry_data = entry.data.clone();
                    let entry_type_val = proto::EntryType::from_i32(entry.entry_type).unwrap_or(proto::EntryType::Data);

                    match entry_type_val {
                        proto::EntryType::Data => {
                            debug!("Follower applying data entry to state machine: index {}", entry.index);
                            self.state_machine.apply(&entry_data);
                        }
                        proto::EntryType::Configuration => {
                             info!("Follower applying configuration entry to state machine (committing): index {}", entry.index);
                            let committed_config = config::Config::from_data(&entry_data);
                            self.apply_configuration_to_internal_state(committed_config, true).await;
                        }
                        proto::EntryType::Noop => {
                             debug!("Follower applying NOOP entry: index {}", entry.index);
                        }
                    }
                    self.last_applied = index_to_apply;
                } else {
                    error!("Entry {} not found in log for follower application. Breaking. Leader commit: {}", index_to_apply, leader_commit_index);
                    break;
                }
            }
            self.commit_index = self.last_applied;
        }
    }

    async fn apply_configuration_to_internal_state(&mut self, config_to_apply: config::Config, committed: bool) { // Renamed `config` to avoid conflict
        info!(
            "Applying configuration (committed: {}): Old servers: {:?}, New servers: {:?}",
            committed, config_to_apply.old_servers, config_to_apply.new_servers
        );

        if committed {
            self.current_config = config_to_apply.clone();
            self.update_peer_config_states();

            info!("Committed new configuration. Node state: {:?}. All peer states updated.", self.node_config_state);

            if self.state == State::Leader && self.current_config.is_stable() && !self.node_config_state.newing {
                info!("Leader is not in the newly committed stable configuration. Stepping down.");
                // MODIFIED: Added .await to inner get() call
                // self.step_down(self.metadata.get().await.current_term).await;
                // OR prefer shutdown for a leader being removed.
                self.shutdown().await;
            }

        } else { // Appended but not committed
            let pending_node_state = config_to_apply.get_node_state(self.server_id);

            if config_to_apply.is_joint() {
                info!("Pending C(old,new) configuration appended. Node state in this pending config: {:?}", pending_node_state);
                let mut new_peers_to_add = Vec::new();
                for server_info in config_to_apply.new_servers.iter() {
                    if server_info.server_id != self.server_id && !self.peer_manager.contains(server_info.server_id) {
                        new_peers_to_add.push(peer::Peer::new(server_info.server_id, server_info.server_addr.clone()));
                    }
                }
                if !new_peers_to_add.is_empty() {
                    info!("Adding new peers for C(old,new): {:?}", new_peers_to_add.iter().map(|p|p.id).collect::<Vec<_>>());
                    self.peer_manager.add(new_peers_to_add, self.log.last_index(self.snapshot.last_included_index));
                }
            } else if config_to_apply.is_stable() {
                info!("Pending C(new) configuration appended. Node state in this pending config: {:?}", pending_node_state);
                let mut peers_to_remove_ids = Vec::new();
                for existing_peer in self.peer_manager.peers() {
                    if !config_to_apply.new_servers.iter().any(|s| s.server_id == existing_peer.id) {
                        peers_to_remove_ids.push(existing_peer.id);
                    }
                }
                if !peers_to_remove_ids.is_empty() {
                    info!("Removing peers for C(new) not present in new_servers: {:?}", peers_to_remove_ids);
                    self.peer_manager.remove(peers_to_remove_ids);
                }

                if self.state != State::Leader && !pending_node_state.newing {
                    info!("Node (Follower/Candidate) is not in pending C(new) config. Shutting down.");
                    self.shutdown().await;
                }
            }

            self.node_config_state = pending_node_state;
            for p_mut in self.peer_manager.peers_mut().iter_mut() {
                p_mut.config_state = config_to_apply.get_node_state(p_mut.id);
            }
        }
    }

    async fn append_and_replicate_config_change(&mut self, target_new_servers_opt: Option<Vec<proto::ServerInfo>>) -> bool {
        if self.state != State::Leader {
            error!("Only leader can append configuration changes.");
            return false;
        }

        let config_to_replicate = match target_new_servers_opt {
            Some(target_new_servers) => {
                if target_new_servers.is_empty() {
                    error!("Cannot start configuration change with empty target server list.");
                    return false;
                }
                if !self.current_config.is_stable() {
                    error!("Cannot start a new configuration change: current configuration is not stable (is {:?})", self.current_config);
                    return false;
                }
                info!("Starting transition from stable config {:?} to new servers: {:?}", self.current_config.new_servers, target_new_servers);
                self.current_config.start_transition(target_new_servers)
            }
            None => {
                if !self.current_config.is_joint() {
                    error!("Cannot finalize to C(new): current configuration {:?} is not C(old,new).", self.current_config);
                    return false;
                }
                info!("Finalizing transition from C(old,new) config: {:?}", self.current_config);
                self.current_config.finalize_transition()
            }
        };

        info!("Replicating new configuration: Old:{:?}, New:{:?}", config_to_replicate.old_servers, config_to_replicate.new_servers);
        match Box::pin(self.replicate(proto::EntryType::Configuration, config_to_replicate.to_data())).await {
            std::result::Result::Ok(_) => true,
            Err(e) => {
                error!("Failed to replicate configuration change: {}", e);
                false
            }
        }
    }

    async fn append_and_replicate_final_config(&mut self) {
        if self.state != State::Leader { return; }
        if !self.current_config.is_joint() {
            warn!("Tried to append final config, but current config is not C(old,new). Current: {:?}", self.current_config);
            return;
        }
        info!("Leader automatically appending C(new) as C(old,new) is committed.");
        self.append_and_replicate_config_change(None).await;
    }

    pub async fn shutdown(&mut self) {
        info!("Shutting down this node (server_id: {})", self.server_id);
        self.state = State::Follower;
        self.leader_id = config::NONE_SERVER_ID;

        // MODIFIED: Added .await for timer stop
        self.heartbeat_timer.lock().await.stop().await;
        self.election_timer.lock().await.stop().await;
        self.snapshot_timer.lock().await.stop().await;

        info!("Node {} timers stopped.", self.server_id);
        info!("Node {} shutdown sequence in Consensus complete. External server shutdown needed.", self.server_id);
    }

    

    pub async fn handle_snapshot_timeout(&mut self) {
        if self.log.committed_entries_len(self.commit_index) > config::SNAPSHOT_LOG_LENGTH_THRESHOLD {
            info!("Snapshot timeout: Log length exceeds threshold. Starting snapshot.");

            let last_included_idx = self.last_applied;
            if last_included_idx == 0 {
                info!("Skipping snapshot: last_applied is 0.");
                 // MODIFIED: Explicitly reset timer
                self.snapshot_timer.lock().await.reset(config::SNAPSHOT_INTERVAL);
                return;
            }
            let last_included_term = self.log.entry(last_included_idx).map_or_else(
                || {
                    if last_included_idx == self.snapshot.last_included_index {
                        self.snapshot.last_included_term
                    } else {
                        error!("Cannot determine term for last_applied index {} for snapshot.", last_included_idx);
                        0
                    }
                },
                |entry| entry.term
            );

            if last_included_term == 0 && last_included_idx > 0 {
                 error!("Failed to get term for snapshot at index {}. Aborting snapshot.", last_included_idx);
                  // MODIFIED: Explicitly reset timer
                 self.snapshot_timer.lock().await.reset(config::SNAPSHOT_INTERVAL);
                 return;
            }

            let config_for_snapshot = self.current_config.clone();
            // Snapshot::gen_snapshot_filepath likely takes &self
            let snapshot_filepath = self.snapshot.gen_snapshot_filepath(last_included_idx, last_included_term);

            info!("Taking snapshot for index {}, term {}. File: {}", last_included_idx, last_included_term, snapshot_filepath);

            // If state_machine.take_snapshot is very slow, use spawn_blocking
            // For now, assuming it's acceptable.
            // tokio::task::spawn_blocking({
            //    let state_machine_clone = self.state_machine.clone(); // If state_machine is Arc<Mutex<dyn ...>> or similar
            //    let snapshot_filepath_clone = snapshot_filepath.clone();
            //    move || state_machine_clone.take_snapshot(&snapshot_filepath_clone) // Pass as &str
            // }).await.unwrap();
            // Or if it's Box<dyn ...> and the trait method takes `&mut self`, you can't easily clone it.
            // Direct call if it's not too blocking:
            self.state_machine.take_snapshot(&snapshot_filepath); // Pass as &str. Typo `take_snapshow` fixed.


            if !std::path::Path::new(&snapshot_filepath).exists() {
                error!("State machine failed to create snapshot file: {}", snapshot_filepath);
                 // MODIFIED: Explicitly reset timer
                self.snapshot_timer.lock().await.reset(config::SNAPSHOT_INTERVAL);
                return;
            }
            info!("Successfully took snapshot data to {}", snapshot_filepath);

            self.snapshot.take_snapshot_metadata(
                last_included_idx,
                last_included_term,
                Some(config_for_snapshot),
            );

            self.log.truncate_prefix(last_included_idx);
            info!("Log truncated up to index {}. New log start_index: {}", last_included_idx, self.log.start_index());
        }
        // MODIFIED: Explicitly reset timer
        self.snapshot_timer.lock().await.reset(config::SNAPSHOT_INTERVAL);
    }


    pub async fn handle_propose_rpc(
        &mut self, 
        request: & proto::ProposeRequest,
    ) -> proto::ProposeResponse {
        if self.state != State::Leader {
            // 如果当前节点不是 Leader，返回失败并告知客户端 Leader 的信息
            let leader_info = if self.leader_id != config::NONE_SERVER_ID {
                self.peer_manager.peers().iter()
                    .find(|p| p.id == self.leader_id)
                    .map(|p| (p.id, p.addr.clone()))
                    .or_else(|| {
                        if self.leader_id == self.server_id {
                            Some((self.server_id, self.server_addr.clone()))
                        } else { None }
                    })
            } else { None };
    
            if let Some((id, addr)) = leader_info {
                return proto::ProposeResponse {
                    success: false,
                    index: Some(id),
                    leader_addr: Some(addr),
                };
            } else {
                 // 还不知道 Leader 是谁
                return proto::ProposeResponse {
                    success: false,
                    index: None,
                    leader_addr: None,
                };
            }
        }
    
        info!("Leader handling Propose request, data size: {}", request.data.len());
        
        // 调用已有的 replicate 方法
        match self.replicate(proto::EntryType::Data, request.data.clone()).await {
            Ok(_) => proto::ProposeResponse {
                success: true,
                index: Some(self.server_id),
                leader_addr: Some(self.server_addr.clone()),
            },
            Err(e) => {
                error!("Failed to replicate data from client: {}", e);
                proto::ProposeResponse { success: false, index: Some(self.server_id), leader_addr: Some(self.server_addr.clone()) }
            }
        }

    }


    pub async fn handle_append_entries_rpc(
        &mut self,
        request: &proto::AppendEntriesRequest,
    ) -> proto::AppendEntriesResponse {
        let meta = self.metadata.get().await;
        let current_term = meta.current_term;

        let mut refuse_resp = proto::AppendEntriesResponse {
            term: current_term,
            success: false,
        };

        if request.term < current_term {
            info!("AE Refused: request term {} < current term {}", request.term, current_term);
            return refuse_resp;
        }

        if request.term > current_term {
            // 只有在收到更高任期时才会step_down
            Box::pin(self.step_down(request.term)).await;
            // 更新refuse_resp的term
            refuse_resp.term = self.metadata.get().await.current_term;
        } else if self.state == State::Leader && request.leader_id != self.server_id {
            // 自己是Leader，但收到同任期的另一个Leader的心跳，这是一种分区情况，需要退位
            info!("Leader received AR from another leader {} in same term {}. Stepping down.", request.leader_id, request.term);
            Box::pin(self.step_down(request.term)).await;
        }

        self.election_timer.lock().await.reset(util::rand_election_timeout());
        self.leader_id = request.leader_id;

        if request.prev_log_index > 0 {
            if request.prev_log_index < self.log.start_index() {
                if request.prev_log_index == self.snapshot.last_included_index {
                    if request.prev_log_term != self.snapshot.last_included_term {
                        warn!("AE Refused: prev_log_index {} is snapshot's last, but term mismatch (req_term: {}, snap_term: {})",
                              request.prev_log_index, request.prev_log_term, self.snapshot.last_included_term);
                        return refuse_resp;
                    }
                } else {
                     debug!("AE: prev_log_index {} is within current snapshot (ends at {}). Assuming term match for consistency check up to snapshot.",
                           request.prev_log_index, self.snapshot.last_included_index);
                }
            } else {
                match self.log.entry(request.prev_log_index) {
                    Some(local_prev_entry) => {
                        if local_prev_entry.term != request.prev_log_term {
                            warn!("AE Refused: Log mismatch at index {}. Local term: {}, Request's prev_log_term: {}",
                                  request.prev_log_index, local_prev_entry.term, request.prev_log_term);
                            warn!("Local log state: start_index={}, last_index={}", self.log.start_index(), self.log.last_index(self.snapshot.last_included_index));
                            return refuse_resp;
                        }
                    }
                    None => {
                        warn!("AE Refused: Log doesn't contain prev_log_index {}. Local last_index: {}",
                              request.prev_log_index, self.log.last_index(self.snapshot.last_included_index));
                        return refuse_resp;
                    }
                }
            }
        }

        if !request.entries.is_empty() {
            // Conflict check needs to compare against the first entry in the request.
            // If request.entries[0].index exists in log and terms differ, truncate.
            let first_new_entry_index_in_request = request.entries[0].index;
            if let Some(existing_entry_at_conflict) = self.log.entry(first_new_entry_index_in_request) {
                if existing_entry_at_conflict.term != request.entries[0].term {
                    info!("Conflict detected at index {}. Deleting suffix from log index {}.",
                          first_new_entry_index_in_request, first_new_entry_index_in_request -1); // Truncate *before* this index
                    self.log.truncate_suffix(first_new_entry_index_in_request - 1);
                }
            }
        }


        if !request.entries.is_empty() {
            let mut entries_to_add_to_log = Vec::new();
            for entry_from_req in request.entries.iter() {
                if entry_from_req.index > self.log.last_index(self.snapshot.last_included_index) ||
                   self.log.entry(entry_from_req.index).map_or(true, |e| e.term != entry_from_req.term) {
                    entries_to_add_to_log.push(entry_from_req.clone());
                }
            }
            if !entries_to_add_to_log.is_empty() {
                self.log.append_entries(entries_to_add_to_log.clone());
                info!("Appended {} new entries from leader. New last_index: {}", entries_to_add_to_log.len(), self.log.last_index(self.snapshot.last_included_index));

                for entry_being_applied in entries_to_add_to_log.iter() {
                    if proto::EntryType::from_i32(entry_being_applied.entry_type) == Some(proto::EntryType::Configuration) {
                        let pending_config = config::Config::from_data(&entry_being_applied.data);
                        self.apply_configuration_to_internal_state(pending_config, false).await;
                    }
                }
            }
        }

        if request.leader_commit > self.commit_index {
            self.follower_advance_commit_index(request.leader_commit).await;
        }

        proto::AppendEntriesResponse {
            // MODIFIED: Added .await
            term: self.metadata.get().await.current_term,
            success: true,
        }
    }


    pub async fn handle_install_snapshot_rpc(
        &mut self,
        request: &proto::InstallSnapshotRequest,
    ) -> proto::InstallSnapshotResponse {
        let current_term_val = self.metadata.get().await.current_term;
        if request.term < current_term_val {
            info!("IS Refused: request term {} < current term {}", request.term, current_term_val);
            return proto::InstallSnapshotResponse { term: self.metadata.get().await.current_term };
        }

        if request.term > current_term_val {
            Box::pin(self.step_down(request.term)).await;
        } else if self.state == State::Leader && request.leader_id != self.server_id {
            info!("Leader received IS from another leader {} in same term {}. Stepping down. ", request.leader_id, request.term);
            Box::pin(self.step_down(request.term)).await;
        }
        self.election_timer.lock().await.reset(util::rand_election_timeout());
        self.leader_id = request.leader_id;

        let data_type = proto::SnapshotDataType::from_i32(request.snapshot_data_type).unwrap_or(proto::SnapshotDataType::Snapshot);

        // Snapshot file handling is complex and stateful across chunks.
        // This is a simplified version. Robust impl needs careful state management for chunks.
        // File I/O is sync; consider spawn_blocking for very large chunks/files.
        let tmp_filepath_str = match data_type { // Renamed
            proto::SnapshotDataType::Metadata => self.snapshot.gen_tmp_snapshot_metadata_filepath(
                request.last_included_index, request.last_included_term
            ),
            proto::SnapshotDataType::Snapshot => self.snapshot.gen_tmp_snapshot_filepath(
                request.last_included_index, request.last_included_term
            ),
        };
        // 在写入文件前，确保父目录存在
        if let Some(parent_dir) = std::path::Path::new(&tmp_filepath_str).parent() {
            if !parent_dir.exists() {
                if let Err(e) = std::fs::create_dir_all(parent_dir) {
                    error!("Failed to create parent directory for snapshot file {}: {}", parent_dir.display(), e);
                    // 返回一个错误响应，而不是 panic
                    return proto::InstallSnapshotResponse { term: self.metadata.get().await.current_term };
                }
            }
        }
        let mut file_handle = match std::fs::OpenOptions::new() // 使用 match 替代 .unwrap()
            .create(request.offset == 0)
            .write(true)
            .append(request.offset > 0) // 使用 append 模式更安全
            .open(&tmp_filepath_str)
        {
            std::result::Result::Ok(file) => file,
            Err(e) => {
                error!("Failed to open/create tmp snapshot file {}: {}", tmp_filepath_str, e);
                // 返回错误响应
                return proto::InstallSnapshotResponse { term: self.metadata.get().await.current_term };
            }
        };

        if request.offset > 0 && data_type == proto::SnapshotDataType::Snapshot {
        } else if request.offset > 0 {
             file_handle.seek(std::io::SeekFrom::Start(request.offset)).unwrap();
        }


        file_handle.write_all(&request.data).unwrap();


        if request.done {
            info!("InstallSnapshot: received final chunk for LII {}, LIT {}.", request.last_included_index, request.last_included_term);
            let final_meta_path_str = self.snapshot.gen_snapshot_metadata_filepath(request.last_included_index, request.last_included_term); // Renamed
            let final_snap_path_str = self.snapshot.gen_snapshot_filepath(request.last_included_index, request.last_included_term); // Renamed
            let tmp_meta_path_str = self.snapshot.gen_tmp_snapshot_metadata_filepath(request.last_included_index, request.last_included_term); // Renamed
            let tmp_snap_path_str = self.snapshot.gen_tmp_snapshot_filepath(request.last_included_index, request.last_included_term); // Renamed

            // These renames should be atomic if on the same filesystem.
            if let Err(e) = std::fs::rename(&tmp_meta_path_str, &final_meta_path_str) {
                error!("Failed to rename temp metadata snapshot {} to {}: {}", tmp_meta_path_str, final_meta_path_str, e);
            }
            if let Err(e) = std::fs::rename(&tmp_snap_path_str, &final_snap_path_str) {
                error!("Failed to rename temp data snapshot {} to {}: {}", tmp_snap_path_str, final_snap_path_str, e);
            }

            self.snapshot.reload_metadata(); // Assumes this reads the new final files

            if let Some(snap_file_to_restore) = self.snapshot.latest_snapshot_filepath() { // Assumes &self
                info!("Restoring state machine from received snapshot: {}", snap_file_to_restore);
                self.state_machine.restore_snapshot(&snap_file_to_restore); // Pass as &str
            }

            self.commit_index = self.snapshot.last_included_index;
            self.last_applied = self.snapshot.last_included_index;

            if let Some(conf) = &self.snapshot.configuration {
                self.current_config = conf.clone();
                self.update_peer_config_states();
            }

            self.log.truncate_prefix(self.snapshot.last_included_index);
            info!("Successfully processed installed snapshot. commit_idx={}, applied_idx={}", self.commit_index, self.last_applied);
        }
        // MODIFIED: Added .await
        proto::InstallSnapshotResponse { term: self.metadata.get().await.current_term }
    }

    // These are synchronous handlers, as they don't await anything internally.
    pub fn handle_get_leader_rpc(
        &mut self, // &mut self is okay if PeerManager methods need it, but &self might be enough
        _request: &proto::GetLeaderRequest,
    ) -> proto::GetLeaderResponse {
        if self.state == State::Leader {
            return proto::GetLeaderResponse {
                leader: Some(proto::ServerInfo {
                    server_id: self.server_id,
                    server_addr: self.server_addr.clone(),
                }),
                redirect_to: None,
            };
        }
        if self.leader_id != config::NONE_SERVER_ID {
            // Borrow immutably if possible
            if let Some(peer) = self.peer_manager.peers().iter().find(|p| p.id == self.leader_id) {
                 return proto::GetLeaderResponse {
                    leader: Some(proto::ServerInfo {
                        server_id: peer.id,
                        server_addr: peer.addr.clone(),
                    }),
                    redirect_to: None,
                };
            } else if self.leader_id == self.server_id {
                 return proto::GetLeaderResponse {
                    leader: Some(proto::ServerInfo {
                        server_id: self.server_id,
                        server_addr: self.server_addr.clone(),
                    }),
                    redirect_to: None,
                };
            }
        }
        proto::GetLeaderResponse { leader: None , redirect_to: None }
    }

    pub fn handle_get_configuration_rpc(
        &mut self, // &self should be enough here
        _request: &proto::GetConfigurationRequest,
    ) -> proto::GetConfigurationResponse {
        let servers = self.current_config.all_servers_in_config();
        proto::GetConfigurationResponse { servers }
    }

    pub async fn handle_set_configuration_rpc(
        &mut self,
        request: &proto::SetConfigurationRequest,
    ) -> proto::SetConfigurationResponse {
        if self.state != State::Leader {
            error!("SetConfiguration can only be handled by the leader.");
            return proto::SetConfigurationResponse { success: false };
        }

        if request.new_servers.is_empty() {
            error!("SetConfiguration failed: new_servers list is empty.");
            return proto::SetConfigurationResponse { success: false };
        }

        if self.current_config.is_joint() {
            error!("SetConfiguration failed: a joint consensus C(old,new) is already active and must be finalized first.");
            return proto::SetConfigurationResponse { success: false };
        }
        if let Some(last_log_cfg) = self.log.last_configuration() {
            if last_log_cfg.is_joint() {
                 error!("SetConfiguration failed: last configuration entry in log is C(old,new) and not yet committed/finalized.");
                 return proto::SetConfigurationResponse { success: false };
            }
        }

        info!("Leader handling SetConfiguration request. New target servers: {:?}", request.new_servers);
        let success_flag = self.append_and_replicate_config_change(Some(request.new_servers.clone())).await; // Renamed

        proto::SetConfigurationResponse { success: success_flag }
    }




    pub async fn handle_heartbeat_timeout(&mut self) {
        if self.state == State::Leader {
            debug!("Heartbeat timeout: Leader sending heartbeats/empty AppendEntries.");
            self.append_entries_to_peers(true).await;
        }
        // MODIFIED: Explicitly reset timer after handling, as original timer might not auto-reschedule on simple tick
        self.heartbeat_timer.lock().await.reset(config::HEARTBEAT_INTERVAL);
    }



















    // ———————————— 领导者选举流程 ——————————
    /*
        1. 选举超时             handle_election_timeout
        2. 发起投票请求         request_vote_rpc
        3. 处理投票结果         handle_request_vote_rpc
        4. 成为领导者           become_leader
        5. 状态回退             step_down
        
     */

    // 领导者选举流程——选举超时
    pub async fn handle_election_timeout(&mut self) {
        info!("Election timeout received. Current state: {:?}, term: {}", self.state, self.metadata.get().await.current_term);
        match self.state {
            // 如果当前是Leader，通常是一个警告，因为Leader 不应该选举超时
            State::Leader => {
                warn!("Leader received election timeout. This should ideally not happen.");
            }
            // 如果是Follower或者Candidate
            State::Candidate | State::Follower => {
                info!("Election timeout: Starting new election (or re-election).");
                // 状态转换为Candidate
                self.state = State::Candidate;

                // 增加当前任期
                let new_term = self.metadata.get().await.current_term + 1;
                
                // 更新元数据
                self.metadata.update_current_term(new_term).await;
                self.metadata.update_voted_for(self.server_id).await;
                self.metadata.sync().await;
                // 重置LeaderID
                self.leader_id = config::NONE_SERVER_ID;
                
                // 发送投票请求
                self.request_vote_rpc().await;
            }
        }

        // 重置选举计时器
        self.election_timer.lock().await.reset(util::rand_election_timeout());
    }

    // 发起投票请求
    async fn request_vote_rpc(&mut self) {
        info!("Start request vote process");

        // 重置所有vote_granted状态。
        self.peer_manager.reset_vote();

        // 获取当前的term、id、log_last_idx和log_last_term
        let candidate_term = self.metadata.get().await.current_term;
        let candidate_id = self.server_id;
        let log_last_idx = self.log.last_index(self.snapshot.last_included_index);
        let log_last_term = self.log.last_term(self.snapshot.last_included_term);


        // 遍历所有peer，为每个peer构建一个proto::RequestVoteRequest
        let peer_infos: Vec<(u64, String)> = self.peer_manager.peers().iter()
            .map(|p| (p.id, p.addr.clone()))
            .collect();

        let mut vote_futs = Vec::new();

        for (peer_id, peer_addr) in peer_infos {
            let req_vote = proto::RequestVoteRequest {
                term: candidate_term,
                candidate_id: candidate_id,
                last_log_index: log_last_idx,
                last_log_term: log_last_term,
            };
            // 并发发送RPC，为每个请求调用self.rpc_client.request_vote，使用join_all来并发等待所有投票结果
            let fut = self.rpc_client.request_vote(req_vote, peer_addr.clone());
            vote_futs.push(async move { (peer_id, peer_addr, fut.await) });
        }



        // -- 统计投票结果 --
        let mut granted_votes_for_new = 0;
        let mut total_nodes_in_new = 0;
        let mut granted_votes_for_old = 0;
        let mut total_nodes_in_old = 0;

        // 统计自己获得的票数
        if self.node_config_state.newing {
            granted_votes_for_new +=1;
            total_nodes_in_new +=1;
        }
        if self.node_config_state.olding {
            granted_votes_for_old +=1;
            total_nodes_in_old +=1;
        }
        // 如果new_quorum和old_quorum都满足，并且当前状态是Candidate，则成为Leader
        let results = future::join_all(vote_futs).await;

        for result_item in results {
            // result_item is (peer_id, peer_addr, Result<Response, Error>)
            let (peer_id, peer_addr, rpc_result) = result_item;
            match rpc_result {
                Ok(resp) => {
                    info!("RequestVote response from {}({}): {:?}", peer_id, peer_addr, resp);

                    // 如果收到的响应中自己的任期落后，则选举失败
                    if resp.term > self.metadata.get().await.current_term {
                        info!("Received higher term {} from peer {} during election. Stepping down.", resp.term, peer_id);
                        Box::pin(self.step_down(resp.term)).await;
                        return;
                    }
                    if resp.vote_granted {
                        if let Some(peer) = self.peer_manager.peer(peer_id) {
                            peer.vote_granted = true;
                            if peer.config_state.newing {
                                granted_votes_for_new +=1;
                            }
                            if peer.config_state.olding {
                                granted_votes_for_old +=1;
                            }
                        }
                    }
                }
                Err(e) => {
                    error!("RequestVote RPC to {}({}) failed: {}", peer_id, peer_addr, e);
                }
            }
            if self.state != State::Candidate {
                return;
            }
        }

        for peer in self.peer_manager.peers() {
            if peer.config_state.newing {
                total_nodes_in_new +=1;
            }
            if peer.config_state.olding {
                total_nodes_in_old +=1;
            }
        }

        let new_config_has_quorum = total_nodes_in_new == 0 || granted_votes_for_new * 2 > total_nodes_in_new;
        let old_config_has_quorum = total_nodes_in_old == 0 || granted_votes_for_old * 2 > total_nodes_in_old;


        if new_config_has_quorum && old_config_has_quorum {
             if self.state == State::Candidate {
                info!("Election won. Becoming Leader.");
                self.become_leader().await;
            }
        } else {
            info!("Election lost or not enough votes. Granted New: {}/{}, Granted Old: {}/{}. New Quorum: {}, Old Quorum: {}",
                granted_votes_for_new, total_nodes_in_new, granted_votes_for_old, total_nodes_in_old, new_config_has_quorum, old_config_has_quorum);
        }
    }




    // 节点处理投票请求
    pub async fn handle_request_vote_rpc(
        &mut self,
        request: &proto::RequestVoteRequest,
    ) -> proto::RequestVoteResponse {
        let meta_initial = self.metadata.get().await;
        let initial_current_term = meta_initial.current_term; // Store for clarity, though meta gets updated
        let mut grant_vote = false;

        // 如果请求的任期小于当前任期，则拒绝投票
        if request.term < initial_current_term {
            info!("RV Refused for {}: request term {} < current term {}", request.candidate_id, request.term, initial_current_term);
        } else {
            // 如果请求的任期大于或等于当前任期，则更新当前任期并可能回退状态
            if request.term > initial_current_term {
                info!("RV: request term {} > current term {}. Stepping down.", request.term, initial_current_term);
                Box::pin(self.step_down(request.term)).await;
            }

            // 投票条件，在任期检查通过或者更新之后
            let meta_after_stepdown = self.metadata.get().await;
            let updated_current_term_val = meta_after_stepdown.current_term;
            let voted_for_val = meta_after_stepdown.voted_for;



            let log_ok = request.last_log_term > self.log.last_term(self.snapshot.last_included_term) ||
                         (request.last_log_term == self.log.last_term(self.snapshot.last_included_term) &&
                          request.last_log_index >= self.log.last_index(self.snapshot.last_included_index));


            // 检查日志是否符合条件
            if !log_ok {
                info!("RV Refused for {}: Candidate's log is not up-to-date. Candidate: (idx={}, term={}), Self: (idx={}, term={})",
                    request.candidate_id,
                    request.last_log_index, request.last_log_term,
                    self.log.last_index(self.snapshot.last_included_index),
                    self.log.last_term(self.snapshot.last_included_term)
                );
            }
            // 检查是否可以投票给候选人
            if log_ok && (voted_for_val == config::NONE_SERVER_ID || voted_for_val == request.candidate_id) {
                 let candidate_in_current_config = self.current_config.all_ids_in_config().contains(&request.candidate_id);
                 if !candidate_in_current_config && !self.current_config.is_empty() {
                     info!("RV Refused for {}: Candidate not in current configuration.", request.candidate_id);
                 } else {
                    // 
                    info!("RV Granted for server {} in term {}", request.candidate_id, updated_current_term_val);
                    self.metadata.update_voted_for(request.candidate_id).await;
                    self.metadata.sync().await;
                    grant_vote = true;
                    self.state = State::Follower;
                    self.leader_id = config::NONE_SERVER_ID;
                    self.election_timer.lock().await.reset(util::rand_election_timeout());
                 }
            } else {
                 info!("RV Refused for {}: log_ok={}, voted_for={}, candidate_id={}",
                    request.candidate_id, log_ok, voted_for_val, request.candidate_id);
            }
        }

        proto::RequestVoteResponse {
            term: self.metadata.get().await.current_term,
            vote_granted: grant_vote,
        }
    }

    // 成为领导者
    async fn become_leader(&mut self) {
        if self.state != State::Candidate {
            error!(
                "Can't become leader: current state is {:?}, not Candidate.",
                self.state
            );
            return;
        }
        
        self.state = State::Leader;
        self.leader_id = self.server_id;
        info!("Became Leader for term {}", self.metadata.get().await.current_term);

        let last_log_idx = self.log.last_index(self.snapshot.last_included_index);
        for peer in self.peer_manager.peers_mut() {
            peer.next_index = last_log_idx + 1;
            peer.match_index = 0;
        }

        // 提交一个NOOP条目以确保领导者状态下的日志一致性
        if let Err(e) = self.replicate(
            proto::EntryType::Noop,
            config::NONE_DATA.as_bytes().to_vec(),
        ).await {
            error!("Failed to replicate NOOP entry after becoming leader: {:?}", e);
        }
        // 重置心跳计时器
        self.heartbeat_timer.lock().await.reset(config::HEARTBEAT_INTERVAL);
    }

    // 状态回退
    async fn step_down(&mut self, new_term: u64) {
        let meta = self.metadata.get().await;
        let current_term = meta.current_term;

        info!(
            "Stepping down to term {}. Current term: {}, Current state: {:?}",
            new_term, current_term, self.state
        );

        if new_term < current_term {
            error!(
                "Step down failed: new term {} is less than current term {}",
                new_term, current_term
            );
            return;
        }

        let old_state = self.state;
        self.state = State::Follower;

        if new_term > current_term {
            self.metadata.update_current_term(new_term).await;
            self.metadata.update_voted_for(config::NONE_SERVER_ID).await;
            self.leader_id = config::NONE_SERVER_ID;
        } else {
            if old_state == State::Leader || old_state == State::Candidate {
                 self.leader_id = config::NONE_SERVER_ID;
            }
        }

        self.metadata.sync().await;

        self.election_timer
            .lock()
            .await
            .reset(util::rand_election_timeout());
        // MODIFIED: Added .await
        info!("Stepped down. New state: {:?}, New term: {}, Leader ID: {}", self.state, self.metadata.get().await.current_term, self.leader_id);
    }



    // ———————————— 日志复制 ——————————

    /*
        replicate(), Leader接收客户端命令并开始复制流程
        append_entries_to_peers(), Leader向所有Follower发送AppendEntries RPC
        append_one_entry_to_peer(), Leader向单个Peer发送AppendEntries RPC
        handle_append_entries_rpc(), Follower处理AppendEntries RPC
        leader_advance_commit_index(), Leader更新提交索引
        follower_advance_commit_index(), Follower更新提交索引
        install_snapshot_rpc(), Follower日志落后太多时，替代AppendEntries RPC
     */

     pub async fn replicate(
        &mut self,
        entry_type: proto::EntryType,
        data: Vec<u8>,
    ) -> Result<(), Box<dyn std::error::Error+Send+Sync>> {
        if self.state != State::Leader {
            error!("replicate should be processed by leader");
            return Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::Other,
                "not leader",
            )));
        }
        info!("replicate data type: {:?}, size: {}", entry_type, data.len());

        // MODIFIED: Added .await
        let current_term = self.metadata.get().await.current_term;
        self.log.append_data(current_term, vec![(entry_type, data.clone())]);

        if entry_type == proto::EntryType::Configuration {
            let pending_config = config::Config::from_data(&data);
            self.apply_configuration_to_internal_state(pending_config, false).await;
        }

        self.append_entries_to_peers(false).await;

        Ok(())
    }





}

