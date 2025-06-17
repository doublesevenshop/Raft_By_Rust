use crate::raft::config;
extern crate regex; // 这一行可以保留，但如果下面使用了 use regex::Regex; 则不是必需的
use lazy_static::lazy_static; // <--- 导入 lazy_static 宏
use super::logging::info;
use regex::Regex; // <--- 明确导入 Regex 类型
use serde::{Deserialize, Serialize};
use std::io::{Read, Write};

lazy_static! {
    // 这个正则表达式现在匹配 "raft-数字-数字" 后跟 ".snapshot" 或 ".snapshot.metadata"
    // 并确保这是字符串的结尾 (使用 $)。
    // 分组：
    // 1: index (数字)
    // 2: term (数字)
    // 3: 完整的扩展名 (例如 ".snapshot" 或 ".snapshot.metadata")
    static ref SNAPSHOT_FILENAME_RE: Regex = Regex::new(r"^raft-(\d+)-(\d+)(\.snapshot|\.snapshot\.metadata)$").unwrap();
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Snapshot {
    pub last_included_index: u64,
    pub last_included_term: u64,
    pub configuration: Option<config::Config>,
    pub snapshot_dir: String,
}

impl Snapshot {
    pub fn new(snapshot_dir: String) -> Self {
        Snapshot {
            last_included_index: 0,
            last_included_term: 0,
            configuration: None,
            snapshot_dir,
        }
    }

    pub fn take_snapshot_metadata(
        &mut self,
        last_included_index: u64,
        last_included_term: u64,
        configuration: Option<config::Config>,
    ) {
        info!("start to take snapshot metadata, last_included_index: {}, last_included_term: {}, configuration: {:?}", last_included_index, last_included_term, configuration.as_ref());
        self.last_included_index = last_included_index;
        self.last_included_term = last_included_term;
        self.configuration = configuration;

        let metadata_filepath =
            self.gen_snapshot_metadata_filepath(last_included_index, last_included_term);
        let mut metadata_file = match std::fs::File::create(metadata_filepath.clone()) {
            Ok(file) => file,
            Err(e) => {
                panic!("failed to create snapshot metadata file '{}', error: {}", metadata_filepath, e);
            }
        };

        let metadata_json = match serde_json::to_string(self) {
            Ok(json) => json,
            Err(e) => {
                panic!("failed to serialize snapshot metadata, error: {}", e);
            }
        };

        if let Err(e) = metadata_file.write_all(metadata_json.as_bytes()) {
            panic!("failed to write snapshot metadata file, error: {}", e);
        }
        info!(
            "success to take snapshot metadata, filepath: {}",
            metadata_filepath
        );
    }

    pub fn reload_metadata(&mut self) {
        if let Some(filepath) = self.latest_metadata_filepath() {
            info!("reloading from snapshot metadata file {}", &filepath);
            let mut metadata_file = match std::fs::File::open(&filepath) {
                Ok(file) => file,
                Err(e) => {
                    panic!("failed to open snapshot metadata file '{}': {}", filepath, e);
                }
            };
            let mut metadata_json = String::new();
            if let Err(e) = metadata_file.read_to_string(&mut metadata_json) {
                panic!("failed to read snapshot metadata from file '{}': {}", filepath, e);
            }

            match serde_json::from_str::<Snapshot>(metadata_json.as_str()) {
                Ok(snapshot) => {
                    self.last_included_index = snapshot.last_included_index;
                    self.last_included_term = snapshot.last_included_term;
                    self.configuration = snapshot.configuration;
                    info!(
                        "successfully reloaded snapshot metadata: LII={}, LIT={}, Config={:?}",
                        self.last_included_index, self.last_included_term, self.configuration.as_ref()
                    );
                }
                Err(e) => {
                    panic!("failed to deserialize snapshot metadata from file '{}': {}", filepath, e);
                }
            }
        } else {
            info!("no snapshot metadata found when reloading");
        }
    }

    // Helper function to parse filenames using the static regex
    fn parse_snapshot_filename(filename: &str, expected_extension: &str) -> Option<(u64, u64)> {
        // 使用预编译的静态正则表达式 SNAPSHOT_FILENAME_RE
        if let Some(caps) = SNAPSHOT_FILENAME_RE.captures(filename) {
            // 从捕获组中获取匹配到的扩展名
            // caps.get(0) 是整个匹配的字符串
            // caps.get(1) 是第一个捕获组 (index)
            // caps.get(2) 是第二个捕获组 (term)
            // caps.get(3) 是第三个捕获组 (实际的扩展名, .snapshot 或 .snapshot.metadata)
            let matched_extension = caps.get(3).map_or("", |m| m.as_str());

            // 确保捕获到的扩展名是我们期望的类型
            if matched_extension == expected_extension {
                let index_str = caps.get(1).map_or("", |m| m.as_str());
                let term_str = caps.get(2).map_or("", |m| m.as_str());
                if let (Ok(index), Ok(term)) = (index_str.parse::<u64>(), term_str.parse::<u64>()) {
                    return Some((index, term));
                }
            }
        }
        None
    }

    fn latest_file_with_pattern(&self, extension_suffix: &str) -> Option<String> {
        let dir_entries = match std::fs::read_dir(&self.snapshot_dir) {
            Ok(entries) => entries,
            Err(e) => {
                eprintln!("Error reading snapshot directory '{}': {}", self.snapshot_dir, e);
                return None;
            }
        };

        let mut latest_index_term: (u64, u64) = (0, 0);
        let mut found_file_path: Option<String> = None;

        for entry_result in dir_entries {
            let entry = match entry_result {
                Ok(e) => e,
                Err(e) => {
                    eprintln!("Error reading directory entry in '{}': {}", self.snapshot_dir, e);
                    continue;
                }
            };
            // 获取文件名 OsStr，然后转换为 &str
            let file_name_os_str = entry.file_name();
            if let Some(filename_str) = file_name_os_str.to_str() {
                // 调用 parse_snapshot_filename，传入文件名和期望的扩展名
                if let Some((index, term)) = Self::parse_snapshot_filename(filename_str, extension_suffix) {
                    if index > latest_index_term.0 || (index == latest_index_term.0 && term > latest_index_term.1) {
                        latest_index_term = (index, term);
                        // entry.path() 返回 PathBuf，可以转换为 String
                        found_file_path = Some(entry.path().to_string_lossy().into_owned());
                    }
                }
            }
        }
        // 如果找到了文件路径，直接返回它，否则返回 None
        // found_file_path 已经包含了完整的路径，所以不需要再 format
        found_file_path
    }


    pub fn latest_snapshot_filepath(&mut self) -> Option<String> {
        self.latest_file_with_pattern(".snapshot")
    }

    pub fn latest_metadata_filepath(&mut self) -> Option<String> {
        self.latest_file_with_pattern(".snapshot.metadata")
    }

    pub fn gen_snapshot_filepath(
        &self,
        last_included_index: u64,
        last_included_term: u64,
    ) -> String {
        format!(
            "{}/raft-{}-{}.snapshot",
            self.snapshot_dir, last_included_index, last_included_term
        )
    }
    pub fn gen_snapshot_metadata_filepath(
        &self,
        last_included_index: u64,
        last_included_term: u64,
    ) -> String {
        format!(
            "{}/raft-{}-{}.snapshot.metadata",
            self.snapshot_dir, last_included_index, last_included_term
        )
    }

    pub fn gen_tmp_snapshot_filepath(
        &self,
        last_included_index: u64,
        last_included_term: u64,
    ) -> String {
        format!(
            "{}/raft-{}-{}.snapshot.tmp",
            self.snapshot_dir, last_included_index, last_included_term
        )
    }
    pub fn gen_tmp_snapshot_metadata_filepath(
        &self,
        last_included_index: u64,
        last_included_term: u64,
    ) -> String {
        format!(
            "{}/raft-{}-{}.snapshot.metadata.tmp",
            self.snapshot_dir, last_included_index, last_included_term
        )
    }
}