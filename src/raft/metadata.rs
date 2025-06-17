use crate::raft::config;
use super::logging::info;
use serde::{Deserialize, Serialize};
use std::clone;
use std::io::{Read, Write};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use tokio::sync::{Mutex as TokioMutex, mpsc};

use tokio::time::{sleep, Duration, interval};
use anyhow::{Result};


#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Metadata {
    pub current_term: u64,
    pub voted_for: u64,
    pub metadata_dir: String,
}

#[derive(Debug)]
enum PersistCommand {
    UpdateTerm(u64),
    UpdateVotedFor(u64),
    Flush,
}

#[derive(Debug)]
pub struct MetadataManager {
    metadata_cache: TokioMutex<Metadata>, // 这是内存中的缓存
    tx: mpsc::Sender<PersistCommand>,     // tx直接存储Sender
}



impl Metadata {
    pub fn new(dir: String) -> Metadata {
        Metadata { 
            current_term: (0), 
            voted_for: (config::NONE_SERVER_ID), 
            metadata_dir: (dir) 
        }
    }

    pub fn gen_metadata_filepath(dir: &str) -> PathBuf {
        let mut path = PathBuf::from(dir);
        path.push("raft.metadata");
        path
    }

    // 从磁盘加载数据，这个方法是同步的，在启动时调用
    pub fn load(dir: &str) -> Result<Self> {
        let filepath = Self::gen_metadata_filepath(&dir);

        if !filepath.exists() {
            // 如果文件不存在，创建一个新的Metadata实例，确保new方法接收&str或者String::from(dir)
            info!("Metadata::load: File{} not found, creating new metadata. ", filepath.display());
            return Ok(Self::new(dir.to_string()));
        }
        info!("Metadata::load Loading metadata from {}.", filepath.display());


        let mut file = std::fs::File::open(filepath)?;
        let mut content = String::new();

        file.read_to_string(&mut content)?;

        let metadata: Metadata = serde_json::from_str(&content)?;
        Ok(metadata)
    }
}



impl MetadataManager {
    pub fn new(initial_metadata: Metadata, flush_interval: Duration) -> Arc<Self> {
        let (tx_cmd, mut rx_cmd) = mpsc::channel(100); // 持久化命令通道

        // 异步任务用于处理命令和定期/按需持久化
        // 这个任务需要访问 initial_metadata 的副本或者路径来写入
        let metadata_for_task = initial_metadata.clone(); // 克隆一份给异步任务使用和修改

        tokio::spawn(async move {
            let mut current_metadata_state = metadata_for_task; // 任务内部持有的状态
            let mut dirty = false;
            let mut periodic_flush_timer = interval(flush_interval);

            loop {
                tokio::select! {
                    Some(cmd) = rx_cmd.recv() => {
                        match cmd {
                            PersistCommand::UpdateTerm(term) => {
                                if current_metadata_state.current_term != term {
                                    current_metadata_state.current_term = term;
                                    dirty = true;
                                }
                            }
                            PersistCommand::UpdateVotedFor(id) => {
                                if current_metadata_state.voted_for != id {
                                    current_metadata_state.voted_for = id;
                                    dirty = true;
                                }
                            }
                            PersistCommand::Flush => {
                                if dirty { // 只有在脏的时候才写入
                                    if let Err(e) = Self::persist_to_disk(&current_metadata_state).await {
                                        log::error!("MetadataManager task: Failed to persist metadata on Flush command: {}", e);
                                    } else {
                                        dirty = false; // 持久化成功后清除脏标记
                                    }
                                }
                            }
                        }
                    }
                    _ = periodic_flush_timer.tick() => {
                        if dirty {
                            log::trace!("MetadataManager task: Periodic flush triggered for dirty metadata.");
                            if let Err(e) = Self::persist_to_disk(&current_metadata_state).await {
                                log::error!("MetadataManager task: Failed to persist metadata on periodic flush: {}", e);
                            } else {
                                dirty = false;
                            }
                        }
                    }
                    else => {
                        // 通道关闭，任务结束
                        log::info!("MetadataManager task: Command channel closed, shutting down persistence task.");
                        // 确保在退出前最后一次尝试持久化脏数据
                        if dirty {
                            log::info!("MetadataManager task: Flushing dirty metadata before exiting.");
                            if let Err(e) = Self::persist_to_disk(&current_metadata_state).await {
                                log::error!("MetadataManager task: Failed to persist metadata on exit: {}", e);
                            }
                        }
                        break;
                    }
                }
            }
        });

        let manager = Arc::new(MetadataManager {
            // get() 方法现在需要异步获取锁
            metadata_cache: TokioMutex::new(initial_metadata), // 主线程持有的缓存，用于快速 get()
            tx: tx_cmd, // 存储 Sender
        });
        manager
    }
    // 实际的磁盘写入操作变为静态异步方法
    async fn persist_to_disk(metadata_to_persist: &Metadata) -> Result<()> {
        let filepath = Metadata::gen_metadata_filepath(&metadata_to_persist.metadata_dir);
        log::trace!("MetadataManager: Persisting metadata to {}", filepath.display());
        let content = serde_json::to_string_pretty(metadata_to_persist)?; // 使用 pretty 方便调试
        tokio::fs::write(&filepath, content.as_bytes()).await?; // 使用 tokio::fs
        log::trace!("MetadataManager: Metadata persisted successfully to {}", filepath.display());
        Ok(())
    }

    pub async fn update_current_term(&self, current_term: u64) {
        // 1. 更新内存缓存 (用于快速 get)
        {
            let mut guard = self.metadata_cache.lock().await;
            if guard.current_term == current_term { // 如果值没有变化，则不发送命令
                return;
            }
            guard.current_term = current_term;
        }
        // 2. 发送持久化命令
        if let Err(e) = self.tx.send(PersistCommand::UpdateTerm(current_term)).await {
            log::error!("MetadataManager: Failed to send UpdateTerm command: {}", e);
        }
    }

    pub async fn update_voted_for(&self, voted_for: u64) {
        {
            let mut guard = self.metadata_cache.lock().await;
            if guard.voted_for == voted_for {
                return;
            }
            guard.voted_for = voted_for;
        }
        if let Err(e) = self.tx.send(PersistCommand::UpdateVotedFor(voted_for)).await {
             log::error!("MetadataManager: Failed to send UpdateVotedFor command: {}", e);
        }
    }

    // 强制将当前内存状态同步到磁盘（通过命令）
    pub async fn sync(&self) {
        if let Err(e) = self.tx.send(PersistCommand::Flush).await {
            log::error!("MetadataManager: Failed to send Flush command: {}", e);
        }
    }
    // get 方法现在是 async，因为它需要 lock TokioMutex
    pub async fn get(&self) -> Metadata {
        self.metadata_cache.lock().await.clone()
    }



    // // 刷新到磁盘
    // async fn flush(&self) {
    //     let meta = self.metadata.lock().unwrap().clone();

    //     let filepath = Metadata::gen_metadata_filepath(&meta.metadata_dir);

    //     let result = tokio::fs::write(
    //         &filepath, 
    //         serde_json::to_string(&meta).unwrap().as_bytes()
    //     ).await;

    //     if let Err(e) = result {
    //         log::error!("Failed to persist metadata: {}", e);
    //     } else {
    //         *self.dirty.lock().unwrap() = false;
    //     }
    // }
}
#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir; // 用于创建临时目录
    // use tokio::runtime::Runtime; // 单独测试时可能需要，但在 #[tokio::test] 中不需要显式创建
    use std::time::Instant as StdInstant; // 明确使用 std::time::Instant

    #[tokio::test]
    async fn test_metadata_manager_operations_and_persistence() {
        let dir = tempdir().unwrap();
        let metadata_dir_str = dir.path().to_str().unwrap().to_string();

        // 1. 测试初始化和首次加载 (应该创建新文件)
        let initial_meta = Metadata::load(&metadata_dir_str).expect("Initial load failed");
        assert_eq!(initial_meta.current_term, 0);
        assert_eq!(initial_meta.voted_for, config::NONE_SERVER_ID);

        let manager = MetadataManager::new(initial_meta, Duration::from_millis(50)); // 较短的刷新间隔

        // 2. 更新数据
        manager.update_current_term(10).await;
        manager.update_voted_for(101).await;

        // 检查内存缓存是否更新 (通过 get 方法)
        let cached_meta = manager.get().await;
        assert_eq!(cached_meta.current_term, 10);
        assert_eq!(cached_meta.voted_for, 101);

        // 3. 强制同步并等待一段时间让异步任务处理
        manager.sync().await;
        sleep(Duration::from_millis(150)).await; // 等待超过 flush_interval 和处理时间

        // 4. 从磁盘重新加载并验证
        let reloaded_meta = Metadata::load(&metadata_dir_str).expect("Reload after sync failed");
        assert_eq!(reloaded_meta.current_term, 10);
        assert_eq!(reloaded_meta.voted_for, 101);
        assert_eq!(reloaded_meta.metadata_dir, metadata_dir_str);

        // 5. 再次更新，不调用 sync，等待定期刷新
        manager.update_current_term(20).await;
        let cached_meta_2 = manager.get().await;
        assert_eq!(cached_meta_2.current_term, 20); // 缓存应立即更新

        sleep(Duration::from_millis(150)).await; // 等待定期刷新

        let reloaded_meta_2 = Metadata::load(&metadata_dir_str).expect("Reload after periodic flush failed");
        assert_eq!(reloaded_meta_2.current_term, 20); // 磁盘应已更新
        assert_eq!(reloaded_meta_2.voted_for, 101); // voted_for 未改变

        // 6. 测试无变化时不应触发写入 (通过日志或间接方式，这里较难直接断言无写入)
        // 我们可以在 persist_to_disk 中加日志，或者检查文件修改时间（不够精确）
        // 暂时跳过此精确测试，依赖于 `dirty` 标志的逻辑。
        // manager.update_current_term(20).await; // 值未变
        // sleep(Duration::from_millis(150)).await;
        // // 理论上此时不应该有新的磁盘写入。

        // 7. (可选) 测试 manager drop 时的最后一次刷新
        manager.update_current_term(30).await; // 使其变脏
        drop(manager); // 触发 manager 的 drop，进而关闭 tx channel
        sleep(Duration::from_millis(100)).await; // 给异步任务一点时间响应 channel 关闭并刷新

        let reloaded_meta_3 = Metadata::load(&metadata_dir_str).expect("Reload after manager drop failed");
        assert_eq!(reloaded_meta_3.current_term, 30);

        // 清理（tempdir 会在 drop 时自动清理）
    }


    #[tokio::test]
    async fn test_metadata_manager_performance_refactored() {
        let dir = tempdir().unwrap();
        let metadata_dir = dir.path().to_str().unwrap().to_string();
        let initial_metadata = Metadata::new(metadata_dir.clone());
        let manager = MetadataManager::new(initial_metadata, Duration::from_millis(10)); // 更快的刷新

        let num_ops = 10000; // 减少操作次数以便更快完成测试，但仍能体现性能

        let write_start = StdInstant::now();
        for i in 0..num_ops {
            manager.update_current_term(i).await;
            manager.update_voted_for(i).await;
        }
        // 调用 sync 确保所有挂起的命令都被发送到 actor，并且 actor 执行一次 flush
        manager.sync().await;
        // 需要等待 actor 处理完所有命令并执行了 sync 对应的 flush
        sleep(Duration::from_millis(100)).await; // 给予足够时间让 actor 处理队列和最终的 flush
        let write_duration = write_start.elapsed();


        let read_start = StdInstant::now();
        // Metadata::load 是同步的，这里没问题
        let reloaded = Metadata::load(&metadata_dir).unwrap();
        let read_duration = read_start.elapsed();

        println!(
            "异步 MetadataManager 写入 {} 次操作的耗时: {:?}, 平均每次操作（发送命令）耗时: {:?}",
            num_ops * 2, // 每个循环两次更新
            write_duration,
            write_duration / (num_ops as u32 * 2)
        );
        println!(
            "异步 MetadataManager 读取的耗时（同步 load）: {:?}",
            read_duration
        );

        assert_eq!(reloaded.current_term, num_ops - 1);
        assert_eq!(reloaded.voted_for, num_ops - 1);
    }
}