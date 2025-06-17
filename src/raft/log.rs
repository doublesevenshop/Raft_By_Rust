use super::logging::*; 
use crate::raft::config;
use crate::raft::proto; 
use lazy_static::lazy_static;
use serde::{Deserialize, Serialize};
use std::io::{BufReader, BufWriter, Read, Write};
use std::sync::Mutex;
use std::fs::{File, OpenOptions}; 

lazy_static! {
    // VIRTUAL_LOG_ENTRY 用于表示快照之前的日志条目，其索引为0，任期为0
    static ref VIRTUAL_LOG_ENTRY: proto::LogEntry = proto::LogEntry {
        index: 0,
        term: 0,
        // 注意：prost 生成的枚举访问方式可能是 proto::EntryType::Noop as i32
        // 或者如果你的 proto 生成代码有 helper 方法，可能是 proto::EntryType::Noop.into()
        // 这里假设 proto::EntryType::Noop.into() 是正确的
        entry_type: proto::EntryType::Noop.into(),
        data: Vec::new(), // 空数据
    };
}

/// LogEntryData 是一个元组，包含日志条目的类型和具体数据
pub type LogEntryData = (proto::EntryType, Vec<u8>);

#[derive(Debug, Serialize, Deserialize)]
pub struct Log {
    entries: Vec<proto::LogEntry>, // 内存中的日志条目列表
    start_index: u64,              // entries 向量中第一条日志的索引（快照后的起始索引）
    metadata_dir: String,          // 日志文件存储目录

    // append_mutex 用于防止并发修改 entries 导致索引冲突
    // 注意：Mutex<String> 的 payload "String" 在这里没有实际意义，Mutex<()> 更合适。
    // 但为了保持与原代码一致，暂时保留 String。
    #[serde(skip)] // 持久化时跳过这个字段
    append_mutex: Mutex<String>,
}

impl Log {
    /// 创建一个新的 Log 实例
    /// start_index 通常是 1，或者在从快照恢复后是 last_included_index + 1
    pub fn new(start_index: u64, metadata_dir: String) -> Self {
        Log {
            entries: Vec::new(),
            start_index,
            metadata_dir,
            append_mutex: Mutex::new(String::new()), // 初始化互斥锁
        }
    }

    /// 追加新的日志数据
    /// term: 当前领导者的任期
    /// entry_data: 一个包含 (EntryType, data_bytes) 元组的向量
    pub fn append_data(&mut self, term: u64, entry_data_list: Vec<LogEntryData>) {
        // 获取互斥锁以保证追加操作的原子性
        // 如果你的 Raft 是单线程处理日志的，这个锁可能不是必需的
        let _lock = self.append_mutex.lock().unwrap_or_else(|poisoned| {
            error!("append_data: Mutex was poisoned, recovering.");
            poisoned.into_inner() // 尝试恢复
        });

        let mut current_last_index = self.last_index(0); // 获取当前日志的最后索引
        for (entry_type, data) in entry_data_list {
            current_last_index += 1;
            let log_entry = proto::LogEntry {
                index: current_last_index,
                term,
                entry_type: entry_type.into(), // 将 proto::EntryType 枚举转换为 i32
                data,
            };
            self.entries.push(log_entry);
        }
        self.dump(); // 追加后持久化日志
    }

    /// 追加已经构造好的日志条目 (通常用于 Follower 接收 Leader 的日志)
    pub fn append_entries(&mut self, entries_to_append: Vec<proto::LogEntry>) {
        if entries_to_append.is_empty() {
            return;
        }
        let _lock = self.append_mutex.lock().unwrap_or_else(|poisoned| {
            error!("append_entries: Mutex was poisoned, recovering.");
            poisoned.into_inner()
        });

        // 校验待追加日志的连续性 (可选，但推荐)
        // let expected_next_index = self.last_index(0) + 1;
        // if let Some(first_entry) = entries_to_append.first() {
        //     if first_entry.index != expected_next_index {
        //         error!(
        //             "append_entries: Log discontinuity. Expected index {}, got {}",
        //             expected_next_index, first_entry.index
        //         );
        //         // 根据 Raft 协议，这里可能需要更复杂的处理，比如让 Leader 重发
        //         return;
        //     }
        // }
        self.entries.extend(entries_to_append);
        self.dump(); // 追加后持久化日志
    }

    /// 返回所有内存中的日志条目的不可变引用
    pub fn entries(&self) -> &Vec<proto::LogEntry> {
        &self.entries
    }

    /// 返回日志的起始索引 (通常是快照的 last_included_index + 1)
    pub fn start_index(&self) -> u64 {
        self.start_index
    }

    /// 根据索引获取日志条目
    /// 如果索引小于 start_index (即在快照中)，则返回一个虚拟的日志条目
    /// 如果索引在内存日志的范围内，则返回对应的日志条目
    /// 否则返回 None
    pub fn entry(&self, index: u64) -> Option<&proto::LogEntry> {
        if index == 0 { // 通常 raft 日志索引从 1 开始，0 可以作为特殊值
            return Some(&VIRTUAL_LOG_ENTRY);
        }
        if index < self.start_index {
            // 这意味着请求的日志在快照中，并且这是一个有效的已提交日志
            // 返回 VIRTUAL_LOG_ENTRY 表示该条目存在但其内容未知（已快照）
            // 或者，如果知道快照的 last_included_term，可以构造一个更精确的虚拟条目
            // 但通常 VIRTUAL_LOG_ENTRY 就够用了，因为我们主要关心它的 term 和 index。
            // 这里的 VIRTUAL_LOG_ENTRY 的 index 是 0，需要注意其含义。
            // 也许应该返回一个 index 为请求的 index，term 为快照 term 的虚拟条目。
            // 目前的行为是：如果 index < start_index 且不为0，返回 VIRTUAL_LOG_ENTRY (index=0, term=0)
            // 这可能需要根据你的具体逻辑调整。
            // 如果你知道 `last_included_term`，可以这样：
            // return Some(&proto::LogEntry{index: index, term: last_included_term_from_snapshot, ...})
            // 但 VIRTUAL_LOG_ENTRY 已经预设为 index=0, term=0
            // Raft 论文中通常假设 index=0, term=0 是有效的“之前的”日志。
            return Some(&VIRTUAL_LOG_ENTRY);
        }
        // 计算在 `entries` Vec 中的实际索引
        let vec_index = (index - self.start_index) as usize;
        self.entries.get(vec_index)
    }

    /// 打包从 next_index 开始的所有日志条目 (用于发送给 Follower)
    pub fn pack_entries(&self, next_index: u64) -> Vec<proto::LogEntry> {
        if next_index < self.start_index {
            // 如果请求的 next_index 比内存日志的起始还早，
            // 这通常意味着 Follower 需要一个快照。
            // Leader 应该发送快照而不是尝试发送这些日志。
            // 返回空Vec表示没有可从内存打包的日志。
            warn!(
                "pack_entries: next_index {} is less than start_index {}. Follower might need a snapshot.",
                next_index, self.start_index
            );
            return Vec::new();
        }
        if next_index > self.last_index(0) + 1 {
            // 请求的索引超出了当前日志范围
            return Vec::new();
        }

        let skip_count = (next_index - self.start_index) as usize;
        self.entries.iter().skip(skip_count).cloned().collect()
    }

    /// 获取日志中的最后一个条目的索引
    /// last_included_index: 快照中的最后一个索引，如果日志为空且快照存在，则以此为准
    pub fn last_index(&self, last_included_index: u64) -> u64 {
        if self.entries.is_empty() {
            // 如果内存日志为空，则最后一个索引是 start_index - 1
            // 或者，如果提供了有效的 last_included_index (来自快照)，则使用它
            if last_included_index > 0 && last_included_index >= self.start_index -1 { // 确保 last_included_index 合理
                return last_included_index;
            } else {
                return self.start_index.saturating_sub(1); // 防止 start_index 为 0 或 1 时下溢
            }
        }
        // 否则，返回内存中最后一条日志的索引
        self.entries.last().map_or(self.start_index.saturating_sub(1), |entry| entry.index)
    }

    /// 获取日志中的最后一个条目的任期
    /// last_included_term: 快照中的最后一个任期，如果日志为空且快照存在，则以此为准
    pub fn last_term(&self, last_included_term: u64) -> u64 {
        if self.entries.is_empty() {
            // 如果内存日志为空
            if last_included_term > 0 && self.start_index > 0 { // 假设快照存在
                return last_included_term;
            } else {
                // 如果没有快照信息或 start_index 为 0，则认为任期为 0
                return 0;
            }
        }
        // 否则，返回内存中最后一条日志的任期
        self.entries.last().map_or(0, |entry| entry.term)
    }

    /// 获取指定索引的前一个日志条目的任期
    /// prev_log_index: 要查找其前一个条目任期的索引
    /// last_included_index: 快照的最后索引
    /// last_included_term: 快照的最后任期
    pub fn prev_log_term(
        &self,
        prev_log_index: u64,
        last_included_index: u64,
        last_included_term: u64,
    ) -> u64 {
        if prev_log_index == 0 { // 按照 Raft 论文，index 0 的 term 是 0
            return 0;
        }
        // 如果 prev_log_index 正好是快照的最后一个索引
        if prev_log_index == last_included_index {
            return last_included_term;
        }
        // 否则，从内存日志中查找
        // self.entry(prev_log_index).map_or(0, |entry| entry.term) // 如果 entry 不存在，则返回 0 (不安全)
        match self.entry(prev_log_index) {
            Some(entry) => {
                // 如果 entry 是 VIRTUAL_LOG_ENTRY 且其 index 不是 prev_log_index，
                // 那么这里的 term (0) 可能不准确。
                // 但如果 prev_log_index < start_index，并且不是 last_included_index，
                // 这种情况通常不应该发生，或者意味着状态不一致。
                if entry.index == prev_log_index || prev_log_index >= self.start_index {
                     entry.term
                } else {
                    // prev_log_index < start_index 但不是 last_included_index, 也不是 VIRTUAL_LOG_ENTRY 的 index 0
                    // 这是一种不一致的状态，或者 VIRTUAL_LOG_ENTRY 的设计需要调整
                    warn!("prev_log_term: Inconsistent state for prev_log_index {} which is before start_index {} but not last_included_index {}", prev_log_index, self.start_index, last_included_index);
                    0 // 或者 panic
                }

            }
            None => {
                error!("prev_log_term: Entry not found for index {}, which should not happen if prev_log_index is valid.", prev_log_index);
                0 // 或者 panic，因为这通常表示一个逻辑错误
            }
        }
    }

    /// 截断从 last_index_kept 之后的日志条目 (用于处理日志冲突)
    pub fn truncate_suffix(&mut self, last_index_kept: u64) {
        if self.entries.is_empty() || last_index_kept < self.start_index {
            // 如果要保留的索引在当前内存日志范围之前，或者日志为空，
            // 意味着所有内存日志都应该被清除。
            // 但 Raft 中，通常是 last_index_kept >= commit_index，且 commit_index >= start_index-1
            if last_index_kept < self.start_index.saturating_sub(1) { // 小于等于快照前的日志
                 warn!("truncate_suffix: last_index_kept {} is less than or equal to snapshot's last index. Clearing all in-memory entries.", last_index_kept);
                 self.entries.clear();
            } else if last_index_kept < self.start_index {
                // 如果 last_index_kept 恰好是快照的最后一条，则内存日志清空
                self.entries.clear();
            }
            // else (last_index_kept >= start_index), proceed to normal truncation below.
            // No, the condition is `last_index_kept < self.start_index`. If true, all current entries are after `last_index_kept`.
            // So, if `last_index_kept` is valid (e.g., `last_index_kept = prevLogIndex` from AppendEntries RPC),
            // and `prevLogIndex` is less than `self.start_index`, it means the leader's `prevLogIndex`
            // points to an entry in our snapshot. So, all our current `self.entries` are conflicting.
            // Example: self.entries = [idx=5, idx=6], start_index=5. Leader says prevLogIndex=3.
            // last_index_kept = 3. 3 < 5. So clear [idx=5, idx=6].
            else { // This case: last_index_kept < self.start_index.
                   // All entries in `self.entries` have index >= self.start_index.
                   // So, all entries in `self.entries` are after `last_index_kept`.
                   // They all need to be removed.
                self.entries.clear();
            }

        } else {
            // 计算在 Vec 中的截断点
            // 我们要保留到 last_index_kept (包含它)
            // 所以 Vec 的长度应该是 (last_index_kept - self.start_index + 1)
            let new_len = (last_index_kept - self.start_index + 1) as usize;
            if new_len < self.entries.len() { // 只有当新长度小于当前长度时才截断
                self.entries.truncate(new_len);
            } else if new_len > self.entries.len() {
                // 这表示 last_index_kept 指向了当前日志之外的未来条目
                // 这不应该通过 truncate_suffix 来处理，可能是逻辑错误
                error!(
                    "truncate_suffix: last_index_kept {} (new_len {}) is beyond current log entries (len {}). No truncation performed.",
                    last_index_kept, new_len, self.entries.len()
                );
                return; // 不做任何事或 panic
            }
            // 如果 new_len == self.entries.len()，则无需操作
        }
        self.dump(); // 截断后持久化
    }

    /// 截断由于快照而已过时的前缀日志条目
    pub fn truncate_prefix(&mut self, last_included_index_from_snapshot: u64) {
        // 如果快照的最后索引小于当前内存日志的起始索引，则无需操作
        if last_included_index_from_snapshot < self.start_index {
            info!(
                "truncate_prefix: Snapshot index {} is older than current start_index {}. No prefix truncation needed.",
                last_included_index_from_snapshot, self.start_index
            );
            return;
        }

        let current_last_log_index = self.last_index(0);

        if current_last_log_index <= last_included_index_from_snapshot {
            // 所有内存中的日志条目都已经被包含在快照中
            self.entries.clear();
        } else {
            // 计算需要从 entries Vec 中移除的元素数量
            // 我们要移除所有索引 <= last_included_index_from_snapshot 的条目
            // (last_included_index_from_snapshot - self.start_index + 1) 是要移除的数量
            let drain_count = (last_included_index_from_snapshot - self.start_index + 1) as usize;
            if drain_count > 0 && drain_count <= self.entries.len() {
                self.entries.drain(0..drain_count);
            } else if drain_count > self.entries.len() {
                // 要移除的比现有的还多，说明全部移除
                warn!("truncate_prefix: drain_count {} exceeds entries len {}. Clearing all entries.", drain_count, self.entries.len());
                self.entries.clear();
            }
            // 如果 drain_count == 0，则无需操作 (通常是因为 last_included_index < start_index)
        }
        // 更新 start_index
        self.start_index = last_included_index_from_snapshot + 1;
        self.dump(); // 截断后持久化
        info!("truncate_prefix: Log truncated. New start_index: {}. Entries count: {}", self.start_index, self.entries.len());
    }

    /// 获取已提交日志条目的数量 (在内存中)
    pub fn committed_entries_len(&self, commit_index: u64) -> usize {
        if commit_index < self.start_index {
            return 0;
        }
        // (commit_index - self.start_index + 1) 是相对于 start_index 的长度
        // 但要确保不超过实际内存中的日志数量
        let len_in_mem = (commit_index - self.start_index + 1) as usize;
        std::cmp::min(len_in_mem, self.entries.len())
    }

    /// 从后向前查找日志中最新的配置条目
    pub fn last_configuration(&self) -> Option<config::Config> { // 返回新的 config::Config
        for entry in self.entries.iter().rev() {
            // 假设你的 proto::EntryType::Configuration 的数值是固定的
            // 或者 entry.entry_type 直接就是 proto::EntryType 枚举类型 (取决于 prost 生成方式)
            // 这里我们用 as i32 来比较
            if entry.entry_type == proto::EntryType::Configuration as i32 {
                // 使用新的 config::Config::from_data
                return Some(config::Config::from_data(&entry.data));
            }
        }
        None // 如果内存日志中没有配置条目，则返回 None
    }

    /// 生成日志文件的完整路径
    pub fn gen_log_filepath(metadata_dir: &str) -> String { // &str 参数更通用
        format!("{}/raft.log", metadata_dir)
    }

    /// 从磁盘重新加载日志
    pub fn reload(&mut self) {
        let filepath = Log::gen_log_filepath(&self.metadata_dir);
        if std::path::Path::new(&filepath).exists() {
            info!("reloading raft log from {}", filepath);
            match File::open(&filepath) {
                Ok(file) => {
                    let reader = BufReader::new(file); // 使用 BufReader 提高读取效率
                    match serde_json::from_reader(reader) { // 从 reader 反序列化
                        Ok(log_from_disk) => {
                            let loaded_log: Log = log_from_disk;
                            self.entries = loaded_log.entries;
                            self.start_index = loaded_log.start_index;
                            info!(
                                "raft log reloaded successfully. Start_index: {}, Entries count: {}",
                                self.start_index,
                                self.entries.len()
                            );
                        }
                        Err(e) => {
                            error!("failed to deserialize raft log from {}: {}. Starting with an empty log.", filepath, e);
                            // 如果反序列化失败，可能文件损坏，可以选择清空或报错退出
                            self.entries.clear();
                            self.start_index = 1; // 或者从一个已知的安全点开始
                        }
                    }
                }
                Err(e) => {
                    error!("failed to open raft log file {} for reloading: {}. Starting with an empty log.", filepath, e);
                    self.entries.clear();
                    self.start_index = 1;
                }
            }
        } else {
            info!("no raft log file found at {}. Starting with an empty log.", filepath);
            // 文件不存在，通常是第一次启动，保持 new() 创建的空状态
        }
    }

    /// 将当前内存中的日志状态持久化到磁盘
    /// 性能提示：频繁地完整写入整个日志文件可能效率低下。
    /// 可以考虑追加写入（append-only file）或使用更专业的存储引擎。
    pub fn dump(&self) {
        let log_filepath = Log::gen_log_filepath(&self.metadata_dir);
        match OpenOptions::new().write(true).create(true).truncate(true).open(&log_filepath) {
            Ok(file) => {
                let writer = BufWriter::new(file); // 使用 BufWriter 提高写入效率
                match serde_json::to_writer_pretty(writer, self) { // 使用 to_writer_pretty 格式化JSON，便于调试
                    Ok(_) => {
                        // trace!("raft log dumped successfully to {}", log_filepath); // dump 通常很频繁，用 trace
                    }
                    Err(e) => {
                        // panic! 是一个粗暴的选择，生产环境应考虑更优雅的错误处理
                        error!("failed to serialize and write raft log to {}: {}", log_filepath, e);
                        // 根据需要，这里可以决定是否 panic
                        // panic!("failed to write raft log file, error: {}", e);
                    }
                }
            }
            Err(e) => {
                 error!("failed to create/open raft log file {} for dumping: {}", log_filepath, e);
                // panic!("failed to create raft log file, error: {}", e);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::Path;

    // 清理测试目录的辅助函数
    fn cleanup_test_dir(dir: &str) {
        if Path::new(dir).exists() {
            fs::remove_dir_all(dir).expect("Failed to remove test dir");
        }
        fs::create_dir_all(dir).expect("Failed to create test dir");
    }


    #[test]
    fn test_log_basic_operations() {
        let test_dir = "./test_log_basic";
        cleanup_test_dir(test_dir);
        let mut log = Log::new(1, test_dir.to_string());

        log.append_data(
            1, // term
            vec![(proto::EntryType::Data, "test1".as_bytes().to_vec())],
        );
        log.append_data(
            1, // term
            vec![(proto::EntryType::Data, "test2".as_bytes().to_vec())],
        );

        assert_eq!(log.entries().len(), 2);
        assert_eq!(log.start_index(), 1);
        assert_eq!(log.entry(1).unwrap().data, "test1".as_bytes());
        assert_eq!(log.entry(1).unwrap().index, 1);
        assert_eq!(log.entry(2).unwrap().data, "test2".as_bytes());
        assert_eq!(log.entry(2).unwrap().index, 2);
        assert_eq!(log.last_index(0), 2);
        assert_eq!(log.last_term(0), 1);

        // 测试 pack_entries
        let packed_all = log.pack_entries(1);
        assert_eq!(packed_all.len(), 2);
        assert_eq!(packed_all[0].data, "test1".as_bytes());
        assert_eq!(packed_all[1].data, "test2".as_bytes());

        let packed_from_2 = log.pack_entries(2);
        assert_eq!(packed_from_2.len(), 1);
        assert_eq!(packed_from_2[0].data, "test2".as_bytes());

        let packed_from_3 = log.pack_entries(3);
        assert_eq!(packed_from_3.len(), 0); // next_index = last_index + 1

        let packed_from_4 = log.pack_entries(4); // 超出范围
        assert_eq!(packed_from_4.len(), 0);

        // 测试 prev_log_term
        assert_eq!(log.prev_log_term(2, 0, 0), 1); // prev_log_index=2, 其 entry(index=2)的term是1
        assert_eq!(log.prev_log_term(1, 0, 0), 1); // prev_log_index=1, 其 entry(index=1)的term是1
        assert_eq!(log.prev_log_term(0, 0, 0), 0); // index 0, term 0 (by convention)

        fs::remove_dir_all(test_dir).ok();
    }

    #[test]
    fn test_log_append_entries() {
        let test_dir = "./test_log_append_entries";
        cleanup_test_dir(test_dir);
        let mut log = Log::new(1, test_dir.to_string());

        let entries_to_add = vec![
            proto::LogEntry { index: 1, term: 1, entry_type: proto::EntryType::Data.into(), data: b"entry1".to_vec() },
            proto::LogEntry { index: 2, term: 1, entry_type: proto::EntryType::Data.into(), data: b"entry2".to_vec() },
        ];
        log.append_entries(entries_to_add);
        assert_eq!(log.entries().len(), 2);
        assert_eq!(log.last_index(0), 2);
        assert_eq!(log.entry(2).unwrap().data, b"entry2".to_vec());

        let more_entries = vec![
            proto::LogEntry { index: 3, term: 2, entry_type: proto::EntryType::Data.into(), data: b"entry3".to_vec() },
        ];
        log.append_entries(more_entries);
        assert_eq!(log.entries().len(), 3);
        assert_eq!(log.last_index(0), 3);
        assert_eq!(log.entry(3).unwrap().term, 2);

        fs::remove_dir_all(test_dir).ok();
    }


    #[test]
    fn test_truncate_suffix() {
        let test_dir = "./test_truncate_suffix";
        cleanup_test_dir(test_dir);
        let mut log = Log::new(1, test_dir.to_string()); // start_index = 1
        log.append_data(1, vec![(proto::EntryType::Data, b"1".to_vec())]); // idx 1
        log.append_data(1, vec![(proto::EntryType::Data, b"2".to_vec())]); // idx 2
        log.append_data(1, vec![(proto::EntryType::Data, b"3".to_vec())]); // idx 3
        log.append_data(1, vec![(proto::EntryType::Data, b"4".to_vec())]); // idx 4
        log.append_data(1, vec![(proto::EntryType::Data, b"5".to_vec())]); // idx 5
        assert_eq!(log.entries().len(), 5);

        // 截断到索引 3 (保留 1, 2, 3)
        log.truncate_suffix(3);
        assert_eq!(log.entries().len(), 3);
        assert_eq!(log.last_index(0), 3);
        assert_eq!(log.entry(3).unwrap().data, b"3".to_vec());
        assert!(log.entry(4).is_none());

        // 截断到索引 1 (保留 1)
        log.truncate_suffix(1);
        assert_eq!(log.entries().len(), 1);
        assert_eq!(log.last_index(0), 1);
        assert_eq!(log.entry(1).unwrap().data, b"1".to_vec());
        assert!(log.entry(2).is_none());

        // 截断到索引 0 (如果 start_index 是 1, 意味着清空)
        // last_index_kept (0) < start_index (1)
        log.truncate_suffix(0);
        assert_eq!(log.entries().len(), 0);
        assert_eq!(log.last_index(0), 0); // start_index = 1, last_index = start_index - 1

        // 重新填充并测试另一种情况
        log.append_data(2, vec![(proto::EntryType::Data, b"n1".to_vec())]); // idx 1
        log.append_data(2, vec![(proto::EntryType::Data, b"n2".to_vec())]); // idx 2
        // 截断到索引 2 (不改变)
        log.truncate_suffix(2);
        assert_eq!(log.entries().len(), 2);
        assert_eq!(log.last_index(0), 2);

        // 截断到索引 3 (超出当前范围，不应改变)
        log.truncate_suffix(3);
        assert_eq!(log.entries().len(), 2);
        assert_eq!(log.last_index(0), 2);


        fs::remove_dir_all(test_dir).ok();
    }


    #[test]
    fn test_truncate_prefix() {
        let test_dir = "./test_truncate_prefix";
        cleanup_test_dir(test_dir);
        let mut log = Log::new(1, test_dir.to_string());
        for i in 1..=5 {
            log.append_data(1, vec![(proto::EntryType::Data, i.to_string().as_bytes().to_vec())]);
        }
        assert_eq!(log.entries().len(), 5); // [1,2,3,4,5]
        assert_eq!(log.start_index(), 1);

        // 快照到索引 2 (last_included_index_from_snapshot = 2)
        // 应该移除索引 1, 2。内存日志变为 [3,4,5]，start_index 变为 3
        log.truncate_prefix(2);
        assert_eq!(log.entries().len(), 3);
        assert_eq!(log.start_index(), 3);
        assert_eq!(log.entry(3).unwrap().data, b"3".to_vec());
        assert_eq!(log.entry(5).unwrap().data, b"5".to_vec());
        assert!(log.entry(2).is_some()); // entry(2) 应该返回 VIRTUAL_LOG_ENTRY
        assert_eq!(log.entry(2).unwrap().index, 0); // VIRTUAL_LOG_ENTRY 的 index 是 0
        assert_eq!(log.last_index(2), 5); // last_included_index for last_index should be from snapshot if entries empty


        // 继续追加
        for i in 6..=8 {
             log.append_data(1, vec![(proto::EntryType::Data, i.to_string().as_bytes().to_vec())]);
        }
        // 内存日志: index [3,4,5,6,7,8], data ["3".."8"]
        // start_index = 3
        assert_eq!(log.entries().len(), 6); // 3,4,5,6,7,8
        assert_eq!(log.last_index(2), 8);


        // 快照到索引 5 (last_included_index_from_snapshot = 5)
        // 应该移除索引 3, 4, 5。内存日志变为 [6,7,8]，start_index 变为 6
        log.truncate_prefix(5);
        assert_eq!(log.entries().len(), 3);
        assert_eq!(log.start_index(), 6);
        assert_eq!(log.entry(6).unwrap().data, b"6".to_vec());
        assert_eq!(log.last_index(5), 8);

        // 快照到索引 8 (所有内存日志都被包含)
        log.truncate_prefix(8);
        assert_eq!(log.entries().len(), 0);
        assert_eq!(log.start_index(), 9);
        assert_eq!(log.last_index(8), 8);

        // 快照到一个更早的索引，不应产生影响
        log.truncate_prefix(7);
        assert_eq!(log.entries().len(), 0);
        assert_eq!(log.start_index(), 9);

        fs::remove_dir_all(test_dir).ok();
    }

    #[test]
    fn test_log_persistence() {
        let test_dir = "./test_log_persistence";
        cleanup_test_dir(test_dir);
        {
            let mut log = Log::new(1, test_dir.to_string());
            log.append_data(1, vec![(proto::EntryType::Data, b"persist1".to_vec())]);
            log.append_data(2, vec![(proto::EntryType::Data, b"persist2".to_vec())]);
            // log.dump() is called internally by append_data
        } // log 被 drop，其数据应该已写入文件

        let mut reloaded_log = Log::new(1, test_dir.to_string()); // 初始状态
        assert_eq!(reloaded_log.entries().len(), 0);
        reloaded_log.reload(); // 从文件加载

        assert_eq!(reloaded_log.entries().len(), 2);
        assert_eq!(reloaded_log.start_index(), 1);
        assert_eq!(reloaded_log.entry(1).unwrap().data, b"persist1".to_vec());
        assert_eq!(reloaded_log.entry(1).unwrap().term, 1);
        assert_eq!(reloaded_log.entry(2).unwrap().data, b"persist2".to_vec());
        assert_eq!(reloaded_log.entry(2).unwrap().term, 2);
        assert_eq!(reloaded_log.last_index(0), 2);

        // 测试截断后再加载
        reloaded_log.truncate_prefix(1); // 快照到 idx 1, start_index=2, entries=[idx 2]
        // dump is called by truncate_prefix
        drop(reloaded_log);

        let mut final_log = Log::new(1, test_dir.to_string());
        final_log.reload();
        assert_eq!(final_log.entries().len(), 1);
        assert_eq!(final_log.start_index(), 2);
        assert_eq!(final_log.entry(2).unwrap().data, b"persist2".to_vec());

        fs::remove_dir_all(test_dir).ok();
    }

    #[test]
    fn test_last_configuration() {
        let test_dir = "./test_last_configuration";
        cleanup_test_dir(test_dir);
        let mut log = Log::new(1, test_dir.to_string());

        assert!(log.last_configuration().is_none()); // 空日志

        let cfg_data1 = config::Config::new_stable(vec![
            proto::ServerInfo { server_id: 1, server_addr: "addr1".to_string() }
        ]).to_data();
        log.append_data(1, vec![(proto::EntryType::Configuration, cfg_data1.clone())]); // idx 1

        let cfg_data2 = config::Config::new_stable(vec![
            proto::ServerInfo { server_id: 1, server_addr: "addr1".to_string() },
            proto::ServerInfo { server_id: 2, server_addr: "addr2".to_string() }
        ]).to_data();
        log.append_data(1, vec![(proto::EntryType::Data, b"some data".to_vec())]); // idx 2
        log.append_data(2, vec![(proto::EntryType::Configuration, cfg_data2.clone())]); // idx 3

        if let Some(last_cfg) = log.last_configuration() {
            assert_eq!(last_cfg.new_servers.len(), 2);
            assert_eq!(last_cfg.new_servers[1].server_id, 2);
        } else {
            panic!("Expected to find a configuration");
        }

        // 在配置条目后添加普通条目
        log.append_data(2, vec![(proto::EntryType::Data, b"more data".to_vec())]); // idx 4
         if let Some(last_cfg) = log.last_configuration() { // 仍然应该是 cfg_data2
            assert_eq!(last_cfg.new_servers.len(), 2);
        } else {
            panic!("Expected to find a configuration");
        }

        // 测试截断对 last_configuration 的影响
        log.truncate_suffix(2); // 保留到 idx 2 (包含 cfg_data1 和 "some data")
                                // 最新的配置应该是 cfg_data1
        if let Some(last_cfg) = log.last_configuration() {
            assert_eq!(last_cfg.new_servers.len(), 1);
            assert_eq!(last_cfg.new_servers[0].server_id, 1);
        } else {
            panic!("Expected to find a configuration after truncating to idx 2");
        }

        log.truncate_suffix(0); // 清空内存日志
        assert!(log.last_configuration().is_none());


        fs::remove_dir_all(test_dir).ok();
    }

    #[test]
    fn test_entry_method_with_snapshot_boundary() {
        let test_dir = "./test_entry_snapshot_boundary";
        cleanup_test_dir(test_dir);
        let mut log = Log::new(1, test_dir.to_string()); // start_index initially 1
        log.append_data(1, vec![(proto::EntryType::Data, b"1".to_vec())]);
        log.append_data(1, vec![(proto::EntryType::Data, b"2".to_vec())]);
        log.append_data(1, vec![(proto::EntryType::Data, b"3".to_vec())]);

        // 快照到索引1 (last_included_index = 1)
        log.truncate_prefix(1); // start_index becomes 2. Entries in memory: [idx=2, idx=3]

        assert_eq!(log.start_index(), 2);

        // 请求索引 0 (VIRTUAL_LOG_ENTRY)
        let entry0 = log.entry(0).unwrap();
        assert_eq!(entry0.index, 0);
        assert_eq!(entry0.term, 0);

        // 请求索引 1 (在快照中, < start_index)
        let entry1 = log.entry(1).unwrap();
        assert_eq!(entry1.index, 0); // VIRTUAL_LOG_ENTRY
        assert_eq!(entry1.term, 0); // VIRTUAL_LOG_ENTRY

        // 请求索引 2 (内存中第一条)
        let entry2 = log.entry(2).unwrap();
        assert_eq!(entry2.index, 2);
        assert_eq!(entry2.data, b"2".to_vec());

        // 请求索引 3 (内存中第二条)
        let entry3 = log.entry(3).unwrap();
        assert_eq!(entry3.index, 3);
        assert_eq!(entry3.data, b"3".to_vec());

        // 请求索引 4 (超出范围)
        assert!(log.entry(4).is_none());

        fs::remove_dir_all(test_dir).ok();
    }

     #[test]
    fn test_last_index_and_term_with_snapshot() {
        let test_dir = "./test_last_index_term_snapshot";
        cleanup_test_dir(test_dir);
        let mut log = Log::new(1, test_dir.to_string());

        // 初始状态
        assert_eq!(log.last_index(0), 0); // start_index=1, so 1-1=0
        assert_eq!(log.last_term(0), 0);

        log.append_data(1, vec![(proto::EntryType::Data, b"1".to_vec())]); // idx 1, term 1
        log.append_data(2, vec![(proto::EntryType::Data, b"2".to_vec())]); // idx 2, term 2
        assert_eq!(log.last_index(0), 2);
        assert_eq!(log.last_term(0), 2);

        // 模拟快照到索引 2, 任期 2
        let last_included_idx_snap = 2;
        let last_included_term_snap = 2;
        log.truncate_prefix(last_included_idx_snap); // start_index = 3, entries empty

        assert_eq!(log.entries.len(), 0);
        assert_eq!(log.start_index(), 3);

        // 此时内存日志为空，应使用快照信息
        assert_eq!(log.last_index(last_included_idx_snap), last_included_idx_snap);
        assert_eq!(log.last_term(last_included_term_snap), last_included_term_snap);

        // 再追加日志
        log.append_data(3, vec![(proto::EntryType::Data, b"3".to_vec())]); // idx 3, term 3
        assert_eq!(log.last_index(last_included_idx_snap), 3);
        assert_eq!(log.last_term(last_included_term_snap), 3);

        fs::remove_dir_all(test_dir).ok();
    }
}