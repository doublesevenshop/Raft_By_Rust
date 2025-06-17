use tonic::server;
use crate::raft::config::{self, ConfigState};


#[derive(Debug, Default, Clone)]
pub struct Peer {
    /// 节点唯一标识符，用于在集群中区分不同服务器节点
    pub id: u64,
    /// 节点ip地址，用于与其他服务器节点进行通信和交互
    pub addr: String,
    /// 下一个要发送给该节点的日志条目索引，初始值为Leader最后的日志条目索引+1，确保日志条目的连续性和一致性
    pub next_index: u64,
    /// 该节点已经成功匹配日志的最高索引，用于跟踪日志同步的进度和状态，确保Leader能够了解各子节点的日志情况 
    pub match_index: u64,
    /// 该节点是否已经授予当前Leader投票权，在选举过程中，Leader需要获得大多数节点的投票才能当选
    pub vote_granted: bool,
    /// 管理集群成员的动态变换等情况
    pub config_state: config::ConfigState,

}

impl Peer {
    pub fn new(server_id: u64, server_addr: String) -> Self {
        Peer {
            id: server_id,
            addr: server_addr,
            next_index: 1,
            match_index: 0,
            vote_granted: false,
            config_state: config::ConfigState::new(),
        }
    } 
}



#[derive(Debug)]
pub struct PeerManager {
    peers: Vec<Peer>,
}
impl PeerManager {
    pub fn new() -> Self {
        PeerManager { peers: Vec::new() }
    }

    pub fn add(&mut self, mut new_peers: Vec<Peer>, last_log_index: u64) {
        // 当新节点加入时，领导者会调用该方法将新节点添加到PeerManager，并且纳入集群管理范围
        // 设置初始的next_index为当前领导人最后的日志条目索引+1，以便新节点能够从何时的位置开始接受和同步信息
        for peer in new_peers.iter_mut() {
            peer.next_index = last_log_index + 1;
        }
        self.peers.extend(new_peers);
    }

    pub fn remove(&mut self, server_ids: Vec<u64>) {
        for server_id in server_ids.iter() {
            if let Some(pos) = self
                .peers
                .iter()
                .position(|peer|peer.id == server_id.clone()) {
                    self.peers.remove(pos);
                }
        }
    }
    pub fn peers_mut(&mut self) -> &mut Vec<Peer> {
        &mut self.peers
    }
    pub fn peers(&self) -> &Vec<Peer> {
        &self.peers
    }
    pub fn len(&self) -> usize {
        self.peers.len()
    }
    pub fn server_ids(&self) -> Vec<u64> {
        self.peers.iter()
            .map(|peer|peer.id).collect()
    }
    pub fn peer(&mut self, server_id: u64) -> Option<&mut Peer> {
        self.peers
            .iter_mut()
            .find(|peer| peer.id == server_id)
    }
    pub fn contains(&self, server_id: u64) -> bool {
        self.peers
            .iter()
            .find(|peer| peer.id == server_id)
            .is_some()
    }

    pub fn reset_vote(&mut self) {
        self.peers_mut()
            .iter_mut()
            .for_each(|peer| peer.vote_granted = false);
    }

    pub fn quoram_match_index(
        &self,
        leader_config_state: &config::ConfigState,
        leader_last_index: u64,
    ) -> u64 {
        // 无论是新旧集群节点，都可以进行联合共识
        fn get_quorum_match_index<F>(
            peers: &Vec<Peer>, 
            leader_last_index: u64,
            is_peer_in_config: F,
            is_leader_in_this_config: bool,
        ) -> u64 
        where F: Fn(&Peer) -> bool,
        {
            let mut match_indexes: Vec<u64> = Vec::new();
            if is_leader_in_this_config {
                match_indexes.push(leader_last_index);
            }
            for peer in peers.iter() {
                if is_peer_in_config(peer) {
                    match_indexes.push(peer.match_index);
                }
            }
            // 测试用的
            // match_indexes.iter()
            //     .for_each(|x|print!("{} ", *x));
            // println!("");
            if match_indexes.is_empty() {
                return std::u64::MAX;
            }
            match_indexes.sort_unstable();
            *match_indexes.get((match_indexes.len() - 1) / 2).unwrap()
        }

        let new_quorum_match_index = get_quorum_match_index(
            &self.peers, 
            leader_last_index, 
            |peer| peer.config_state.newing,
            leader_config_state.newing
        );
        let old_quorum_match_index = get_quorum_match_index(
            &self.peers, 
            leader_last_index, 
            |peer| peer.config_state.olding,
            leader_config_state.olding
        );
        // 测试用的
        // println!("新的中间值{}, 旧的中间值{}", new_quorum_match_index, old_quorum_match_index);
        
        std::cmp::min(new_quorum_match_index, old_quorum_match_index)
    }

    pub fn quorum_vote_granted(
        &self,
        leader_config_state: &config::ConfigState,
    ) -> bool {
        let mut total_new_servers = 0;
        let mut granted_new_servers = 0;
        let mut total_old_servers = 0;
        let mut granted_old_servers = 0;

        if leader_config_state.newing {
            total_new_servers += 1;
            granted_new_servers += 1;
        }
        if leader_config_state.olding {
            total_old_servers += 1;
            granted_old_servers += 1;
        }

        for peer in self.peers().iter() {
            if peer.config_state.newing {
                total_new_servers += 1;
                if peer.vote_granted {
                    granted_new_servers += 1;
                }
            }
            if peer.config_state.olding {
                total_old_servers += 1;
                if peer.vote_granted {
                    granted_old_servers += 1;
                }
            }
        }

        // 再次进行联合共识
        let new_servers_quorum = 
            {total_new_servers == 0 || granted_new_servers > (total_new_servers) / 2};
        let old_servers_quorum = 
            {total_old_servers == 0 || granted_old_servers > (total_old_servers)  / 2};

        return new_servers_quorum && old_servers_quorum;
    }



}




#[cfg(test)]
mod tests {
    use super::*;

    fn make_test_peer(id: u64, match_index: u64, newing: bool, olding: bool) -> Peer {
        Peer {
            id,
            addr:format!("127.0.0.1:{}", 9000+id),
            next_index: 1,
            match_index,
            vote_granted:false,
            config_state: ConfigState {newing, olding},
        }
    }
    
    #[test]
    fn test_peers_basic_add() { // Renamed to avoid conflict if you have other test_peers
        let mut peer_manager = PeerManager::new();
        let peer1 = Peer {
            id: 1,
            addr: "127.0.0.1:9001".to_string(),
            next_index: 3, // Will be overwritten by add
            match_index: 2,
            vote_granted: false,
            config_state: ConfigState::new(), // Uses the mock/local ConfigState::new
        };
        let peer2 = Peer {
            id: 2,
            addr: "127.0.0.1:9002".to_string(),
            next_index: 2, // Will be overwritten by add
            match_index: 2,
            vote_granted: false,
            config_state: ConfigState::new(), // Uses the mock/local ConfigState::new
        };
        peer_manager.add(vec![peer1, peer2.clone()], 5); // last_log_index = 5
        // println!("{:?}", peer_manager); // For debugging
        assert_eq!(peer_manager.peers().len(), 2);
        assert_eq!(peer_manager.peers[0].next_index, 6); // 5 + 1
        assert_eq!(peer_manager.peers[1].next_index, 6); // 5 + 1
        assert_eq!(peer_manager.peers[0].id, 1);
        assert_eq!(peer_manager.peers[1].id, 2);
    }


    #[test]
    fn test_qmi_all_in_both_configs() {
        // Leader and 2 peers, all in new and old configs
        let leader_cs = ConfigState { newing: true, olding: true };
        let leader_last_idx = 100;
        let peer_manager = PeerManager {
            peers: vec![
                make_test_peer(1, 90, true, true), // P1
                make_test_peer(2, 80, true, true), // P2
            ],
        };

        // New config: Leader (100), P1 (90), P2 (80). Sorted: [80, 90, 100]. Median (idx (3-1)/2=1): 90
        // Old config: Leader (100), P1 (90), P2 (80). Sorted: [80, 90, 100]. Median (idx (3-1)/2=1): 90
        // min(90, 90) = 90
        assert_eq!(peer_manager.quoram_match_index(&leader_cs, leader_last_idx), 90);
    }

    #[test]
    fn test_qmi_leader_in_new_peers_split() {
        // Leader in new config only. One peer in new, one in old.
        let leader_cs = ConfigState { newing: true, olding: false };
        let leader_last_idx = 100;
        let peer_manager = PeerManager {
            peers: vec![
                make_test_peer(1, 90, true, false),  // P1 (new only)
                make_test_peer(2, 80, false, true),  // P2 (old only)
                make_test_peer(3, 70, false, true),  // P3 (old only)
            ],
        };

        // New config: Leader (100), P1 (90). Sorted: [90, 100]. Median (idx (2-1)/2=0): 90
        // Old config: P2 (80), P3 (70). Sorted: [70, 80]. Median (idx (2-1)/2=0): 70
        // min(90, 70) = 70
        assert_eq!(peer_manager.quoram_match_index(&leader_cs, leader_last_idx), 70);
    }

    #[test]
    fn test_qmi_no_quorum_in_old_config() {
        // Leader in new config only. Peers only in new config. Old config has no members.
        let leader_cs = ConfigState { newing: true, olding: false };
        let leader_last_idx = 100;
        let peer_manager = PeerManager {
            peers: vec![
                make_test_peer(1, 90, true, false), // P1 (new only)
                make_test_peer(2, 85, true, false), // P2 (new only)
            ],
        };

        // New config: Leader (100), P1 (90), P2 (85). Sorted: [85, 90, 100]. Median: 90
        // Old config: No members. Returns u64::MAX
        // min(90, u64::MAX) = 90
        assert_eq!(peer_manager.quoram_match_index(&leader_cs, leader_last_idx), 90);
    }

    #[test]
    fn test_qmi_no_quorum_in_new_config() {
        // Leader in old config only. Peers only in old config. New config has no members.
        let leader_cs = ConfigState { newing: false, olding: true };
        let leader_last_idx = 100;
        let peer_manager = PeerManager {
            peers: vec![
                make_test_peer(1, 90, false, true), // P1 (old only)
                make_test_peer(2, 85, false, true), // P2 (old only)
            ],
        };

        // New config: No members. Returns u64::MAX
        // Old config: Leader (100), P1 (90), P2 (85). Sorted: [85, 90, 100]. Median: 90
        // min(u64::MAX, 90) = 90
        assert_eq!(peer_manager.quoram_match_index(&leader_cs, leader_last_idx), 90);
    }
    #[test]
    fn test_qmi_no_quorum_in_either_config() {
        // Leader not in any config. No peers in any relevant config.
        let leader_cs = ConfigState { newing: false, olding: false };
        let leader_last_idx = 100;
        let peer_manager = PeerManager {
            peers: vec![
                make_test_peer(1, 90, false, false), // P1 (neither)
            ],
        };

        // New config: No members. Returns u64::MAX
        // Old config: No members. Returns u64::MAX
        // min(u64::MAX, u64::MAX) = u64::MAX
        assert_eq!(peer_manager.quoram_match_index(&leader_cs, leader_last_idx), std::u64::MAX);
    }

    // ......未完全覆盖测试，使用gemini2.5pro写的测试用例，以上是都已经通过了的

}