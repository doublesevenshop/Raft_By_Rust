use std::fmt::Debug;
use std::fs::File;
use std::io::{Read, Write};
use std::path::Path;
use serde::{Deserialize, Serialize};
use crate::raft::proto;

use super::logging::*;
use std::any::Any;


pub trait StateMachine: Debug + Send + 'static {
    
    // 应用日志条目
    fn apply(&mut self, data: &Vec<u8>);

    // 生成快照
    fn take_snapshot(&mut self, snapshot_filepath: &str);

    // 从快照回复
    fn restore_snapshot(&mut self, snapshot_filepath: &str);
}


#[derive(Debug, Serialize, Deserialize, Default)]
pub struct SimpleStateMachine {
    #[serde(default)]
    entries: Vec<Vec<u8>>,
}
impl SimpleStateMachine {
    pub fn new() -> Self {
        SimpleStateMachine {
            entries: Vec::new(),
        }
    }
    #[allow(dead_code)]
    pub fn get_entries(&self) -> Vec<String> {
        self.entries.iter()
            .map(|v|
                String::from_utf8_lossy(v).into_owned()
            )
            .collect::<Vec<String>>()


    }
}

impl StateMachine for SimpleStateMachine {
    fn apply(&mut self, data: &Vec<u8>) {
        self.entries.push(data.clone());
    }

    fn take_snapshot(&mut self, snapshot_filepath: &str) {

        let snapshot_json = match serde_json::to_string(&self.entries) {
            Ok(json) => json,
            Err(e) => {
                panic!("SimpleStateMachine: Failed to serialize entries to JSON for snapshot: {}", e);
            }
        };

        // 同步文件操作，在异步 Raft 的 handle_snapshot_timeout 中调用时，
        // 如果此操作耗时，应考虑使用 tokio::task::spawn_blocking。
        match File::create(&snapshot_filepath) {
            Ok(mut snapshot_file) => {
                if let Err(e) = snapshot_file.write_all(snapshot_json.as_bytes()) {
                    panic!("SimpleStateMachine: Failed to write snapshot file '{}': {}", snapshot_filepath, e);
                }
                // raft::logging::info!("SimpleStateMachine: Snapshot taken to {}", snapshot_filepath);
            }
            Err(e) => {
                panic!("SimpleStateMachine: Failed to create snapshot file '{}': {}", snapshot_filepath, e);
            }
        }
    }
    fn restore_snapshot(&mut self, snapshot_filepath: &str) {
        if Path::new(&snapshot_filepath).exists() {
            match File::open(&snapshot_filepath) {
                Ok(mut snapshot_file) => {
                    let mut snapshot_json = String::new();
                    if let Err(e) = snapshot_file.read_to_string(&mut snapshot_json) {
                        panic!("SimpleStateMachine: Failed to read snapshot file '{}': {}", snapshot_filepath, e);
                    }

                    match serde_json::from_str::<Vec<Vec<u8>>>(&snapshot_json) {
                        Ok(restored_entries) => {
                            self.entries = restored_entries;
                            info!("SimpleStateMachine: Snapshot restored from {}", snapshot_filepath);
                        }
                        Err(e) => {
                            panic!("SimpleStateMachine: Failed to deserialize snapshot JSON from '{}': {}", snapshot_filepath, e);
                        }
                    }
                }
                Err(e) => {
                    panic!("SimpleStateMachine: Failed to open snapshot file '{}': {}", snapshot_filepath, e);
                }
            }
        } else {
            // warn!("SimpleStateMachine: Snapshot file '{}' not found for restoring.", snapshot_filepath);
            println!("SimpleStateMachine: Snapshot file '{}' not found for restoring. State machine remains unchanged or empty.", snapshot_filepath);
            // self.entries = Vec::new(); // 或者保持当前状态，取决于期望行为
        }
    }


}