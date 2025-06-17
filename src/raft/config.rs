use serde::{Deserialize, Serialize};
use tonic::server;
use core::panic;
use std::time::Duration;
use crate::raft::{peer, proto};
use std::io::Error;

// 选举超时间隔范围
pub const ELECTION_TIMEOUT_MAX_MILLIS: u64 = 15000;
pub const ELECTION_TIMEOUT_MIN_MILLIS: u64 = 10000;
pub const ELECTION_TIMEOUT_MIN: Duration = Duration::from_millis(ELECTION_TIMEOUT_MIN_MILLIS);

// 心跳间隔时间
pub const HEARTBEAT_INTERVAL: Duration = Duration::from_millis(3000);

// 快照间隔时间
pub const SNAPSHOT_INTERVAL: Duration = Duration::from_millis(30000);

// 快照阈值（日志条目长度）
pub const SNAPSHOT_LOG_LENGTH_THRESHOLD: usize = 5;

pub const NONE_SERVER_ID: u64 = 0;
pub const NONE_DATA: &'static str = "None";

// 发送snapshot时分块大小
pub const SNAPSHOT_TRUNK_SIZE: usize = 30;

#[derive(Debug, PartialEq, Clone, Default)]
pub struct ConfigState {
    pub newing: bool, // 正常情况都会处于new
    pub olding: bool, // 成员变更期间会处于old
}


impl ConfigState {
    pub fn new() -> ConfigState {
        ConfigState { newing: true, olding: false }
    }
}

#[derive(Debug, PartialEq, Clone, Deserialize, Serialize)]
pub struct Config {
    // C(old, new)联合共识期间，属于C_old配置的节点列表
    // 如果配置是稳定的，仅为C_new，这个列表为空
    pub old_servers: Vec<proto::ServerInfo>,
    // 属于C_new配置的节点列表，在稳定配置中，这是活跃节点的列表，在C(old, new)联合共识期间，这是目标新配置的节点列表
    pub new_servers: Vec<proto::ServerInfo>,
}

impl Config {
    pub fn new() -> Config {
        Config { 
            old_servers: Vec::new(), 
            new_servers: Vec::new(), 
        }
    }
    // 在稳定配置中，old为空，只有new
    pub fn new_stable(initial_servers: Vec<proto::ServerInfo>) -> Config {
        Config {
            old_servers: Vec::new(),
            new_servers: initial_servers,
        }
    }
    // 从字节切片反序列化
    pub fn from_data(data: &[u8]) -> Config {
        serde_json::from_slice(data).expect("Failed to convert vec<u8> to config")
    }
    // 将Config序列化为字节向量
    pub fn to_data(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("Failed to convert config to vec<u8>")
    }

    // 向当前配置的new_server添加一组节点，不会添加具有重复id的节点
    pub fn append_new_servers(&mut self, servers_to_add: &Vec<proto::ServerInfo>) {
        for server in servers_to_add.iter() {
            if !self.new_servers.iter().any(|s| s.server_id == server.server_id) {
                self.new_servers.push(server.clone());
            }
        }
    }
    // 从当前配置的new_server列表中移除指定ID的节点
    pub fn remove_new_servers(&mut self, removed_ids: &[u64]) {
        self.new_servers.retain(|s| !removed_ids.contains(&s.server_id));
    }
    // 将Peer对象转换为ServerInfo并且添加到old_server列表
    pub fn append_old_servers(&mut self, peers_to_add: &[crate::raft::peer::Peer]) {
        for peer in peers_to_add.iter() {
            if !self.old_servers.iter().any(|s| s.server_id == peer.id) {
                self.old_servers.push(proto::ServerInfo {
                    server_id: peer.id,
                    server_addr: peer.addr.clone(),
                });
            }
        }
    }
    // 直接配置
    pub fn ser_new_servers(&mut self, servers: Vec<proto::ServerInfo>) {
        self.new_servers = servers;
    }
    pub fn set_old_servers(&mut self, servers: Vec<proto::ServerInfo>) {
        self.old_servers = servers;
    }

    // 将当前配置从C(old, new)状态转换到C(new)状态，完成成员变更。如果当前状态不是C(old, new)，则会panic
    pub fn finalize_transition(&self) -> Config {
        if self.old_servers.is_empty() {
            panic!("Cannot transition to C(new): current configuration is not C(old, new) because of old_servers is empty.");
        }
        if self.new_servers.is_empty() {
            panic!("Cannot transition to C(new): new_servers list is empty in C(old, new) state. ");
        }
        Config {
            old_servers: Vec::new(),
            new_servers: self.new_servers.clone(),
        }
    }

    // 从C_new创建联合配置C(old, new)，，开始成员变更
    pub fn start_transition(&self, target_new_servers: Vec<proto::ServerInfo>) -> Self {
        if !self.old_servers.is_empty() {
            panic!("Cannot create C(old,new): current configuration is already a joint state (old_servers is not empty).");
        }
        if self.new_servers.is_empty() {
            panic!("Cannot create C(old,new): current new_servers is empty (no C_old to transition from).");
        }
        Config {
            old_servers: self.new_servers.clone(), // 当前new_server变成old
            new_servers: target_new_servers,
        }
    }

    // 根据Config对象的内容，确定node_id的ConfigState
    pub fn get_node_state(&self, node_id: u64) -> ConfigState {
        ConfigState {
            newing: self.new_servers.iter().any(|s|s.server_id == node_id),
            olding: self.old_servers.iter().any(|s| s.server_id == node_id),
        }
    }

    // 检查Config实例是否代表联合共识状态
    pub fn is_joint(&self) -> bool {
        !self.old_servers.is_empty() && !self.new_servers.is_empty()
    }

    // 检查Config实例是否代表新稳定配置
    pub fn is_stable(&self) -> bool {
        self.old_servers.is_empty() && !self.new_servers.is_empty()
    }

    // 检查Config是否完全为空
    pub fn is_empty(&self) -> bool {
        self.old_servers.is_empty() && self.new_servers.is_empty()
    }
    // 如果一个id同时存在于新旧当中，优先使用new中的信息
    pub fn all_servers_in_config(&self) -> Vec<proto::ServerInfo> {
        let mut server_map = std::collections::HashMap::new();

        for server in self.old_servers.iter() {
            server_map.insert(server.server_id, server.clone());
        }
        for server in self.new_servers.iter() {
            server_map.insert(server.server_id, server.clone());
        }

        server_map.values().cloned().collect()
    }

    // 返回此Config中存在的所有唯一节点 ID 的列表
    pub fn all_ids_in_config(&self) -> Vec<u64> {
        let mut ids = std::collections::HashSet::new();

        for server in self.old_servers.iter() {
            ids.insert(server.server_id);
        }
        for server in self.new_servers.iter() {
            ids.insert(server.server_id);
        }
        ids.into_iter().collect()
    }
}

#[cfg(test)]
mod tests {
    use crate::raft::config::{Config, ConfigState};
    use crate::raft::proto::ServerInfo;
    use crate::raft::peer::Peer;

    #[test]
    fn test_configuration() {
        // Test initial empty config
        let mut config = Config::new();
        assert!(config.is_empty());
        assert!(!config.is_joint());
        assert!(!config.is_stable());
        assert_eq!(config.all_ids_in_config().len(), 0);
        assert_eq!(config.all_servers_in_config().len(), 0);

        // Test new_stable
        let initial_servers = vec![
            ServerInfo { server_id: 1, server_addr: "[::1]:9001".to_string() },
            ServerInfo { server_id: 2, server_addr: "[::1]:9002".to_string() },
        ];
        let stable_config = Config::new_stable(initial_servers.clone());
        assert!(!stable_config.is_empty());
        assert!(!stable_config.is_joint());
        assert!(stable_config.is_stable());
        assert_eq!(stable_config.new_servers, initial_servers);
        assert_eq!(stable_config.old_servers.len(), 0);
        assert_eq!(stable_config.all_ids_in_config().len(), 2);
        assert_eq!(stable_config.all_servers_in_config().len(), 2);

        // Test append_new_servers
        let mut config_append = Config::new();
        config_append.append_new_servers(&vec![
            ServerInfo { server_id: 1, server_addr: "[::1]:9001".to_string() },
        ]);
        assert_eq!(config_append.new_servers.len(), 1);
        config_append.append_new_servers(&vec![
            ServerInfo { server_id: 1, server_addr: "[::1]:9001".to_string() }, // Duplicate
            ServerInfo { server_id: 3, server_addr: "[::1]:9003".to_string() },
        ]);
        assert_eq!(config_append.new_servers.len(), 2);
        assert!(config_append.new_servers.iter().any(|s| s.server_id == 1));
        assert!(config_append.new_servers.iter().any(|s| s.server_id == 3));

        // Test remove_new_servers
        config_append.remove_new_servers(&[1]);
        assert_eq!(config_append.new_servers.len(), 1);
        assert!(!config_append.new_servers.iter().any(|s| s.server_id == 1));
        assert!(config_append.new_servers.iter().any(|s| s.server_id == 3));

        // Test start_transition and finalize_transition
        let mut current_config = Config::new_stable(vec![
            ServerInfo { server_id: 1, server_addr: "[::1]:9001".to_string() },
            ServerInfo { server_id: 2, server_addr: "[::1]:9002".to_string() },
        ]);
        let target_new_servers = vec![
            ServerInfo { server_id: 2, server_addr: "[::1]:9002".to_string() },
            ServerInfo { server_id: 3, server_addr: "[::1]:9003".to_string() },
        ];
        let joint_config = current_config.start_transition(target_new_servers.clone());
        assert!(joint_config.is_joint());
        assert!(!joint_config.is_stable());
        assert_eq!(joint_config.old_servers, current_config.new_servers);
        assert_eq!(joint_config.new_servers, target_new_servers);

        let final_config = joint_config.finalize_transition();
        assert!(!final_config.is_joint());
        assert!(final_config.is_stable());
        assert_eq!(final_config.old_servers.len(), 0);
        assert_eq!(final_config.new_servers, target_new_servers);

        // Test get_node_state
        let mut test_config = Config::new();
        test_config.append_old_servers(&vec![
            // 改为直接使用 ServerInfo，因为 append_old_servers 内部会处理 Peer 到 ServerInfo 的转换
            // 或者，如果 append_old_servers 期望的是 ServerInfo，则直接创建 ServerInfo
            // 这里假设 append_old_servers 期望 Peer，但为了与 new_servers 的 append 行为一致，我们调整测试
            // 实际上，append_old_servers 的实现是将 Peer 转换为 ServerInfo，所以这里传递 Peer 是符合其定义的
            // 但为了测试的清晰性和直接性，如果目的是测试添加 ServerInfo 到 old_servers，应该有相应的方法或调整
            // 考虑到 append_old_servers 的现有签名是 &mut self, peers_to_add: &[crate::raft::peer::Peer]
            // 保持原有调用方式，但确保 Peer 结构体被正确使用
            crate::raft::peer::Peer { 
                id: 1, 
                addr: "[::1]:9001".to_string(), 
                next_index: 0, // 根据 Peer 定义添加默认值或实际值
                match_index: 0, // 根据 Peer 定义添加默认值或实际值
                vote_granted: false, // 根据 Peer 定义添加默认值或实际值
                config_state: ConfigState::new() // 根据 Peer 定义添加默认值或实际值
            },
        ]);
        test_config.append_new_servers(&vec![
            ServerInfo { server_id: 2, server_addr: "[::1]:9002".to_string() },
            ServerInfo { server_id: 3, server_addr: "[::1]:9003".to_string() },
        ]);

        assert_eq!(test_config.get_node_state(1), ConfigState { newing: false, olding: true });
        assert_eq!(test_config.get_node_state(2), ConfigState { newing: true, olding: false });
        assert_eq!(test_config.get_node_state(3), ConfigState { newing: true, olding: false });
        assert_eq!(test_config.get_node_state(4), ConfigState { newing: false, olding: false });

        // Test serialization/deserialization
        let ser_data = test_config.to_data();
        let mut de_config = Config::from_data(&ser_data);

        // Sort servers before comparison to ensure consistent order
        test_config.old_servers.sort_by_key(|s| s.server_id);
        test_config.new_servers.sort_by_key(|s| s.server_id);
        de_config.old_servers.sort_by_key(|s| s.server_id);
        de_config.new_servers.sort_by_key(|s| s.server_id);

        assert_eq!(test_config, de_config);

        // Test all_servers_in_config and all_ids_in_config with joint config
        let all_servers = joint_config.all_servers_in_config();
        let all_ids = joint_config.all_ids_in_config();
        assert_eq!(all_servers.len(), 3); // 1, 2, 3
        assert_eq!(all_ids.len(), 3); // 1, 2, 3
        assert!(all_ids.contains(&1));
        assert!(all_ids.contains(&2));
        assert!(all_ids.contains(&3));

        // Test all_servers_in_config and all_ids_in_config with stable config
        let all_servers_stable = stable_config.all_servers_in_config();
        let all_ids_stable = stable_config.all_ids_in_config();
        assert_eq!(all_servers_stable.len(), 2);
        assert_eq!(all_ids_stable.len(), 2);
        assert!(all_ids_stable.contains(&1));
        assert!(all_ids_stable.contains(&2));
    }
}