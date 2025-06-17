# Raft 元数据管理模块快速上手

本教程将带你快速了解如何使用 `Metadata` 和 `MetadataManager` 来管理 Raft 的核心元数据：`current_term`（当前任期）和 `voted_for`（当前任期投票给谁）。

## 0. 准备

确保你的项目中已引入必要的依赖 (tokio, serde, serde_json, anyhow) 并且 `Metadata` 和 `MetadataManager` 的代码已存在。

```rust
use crate::raft::config;
use super::logging::info;
use serde::{Deserialize, Serialize};
use std::io::{Read, Write};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;
use tokio::time::{sleep, Duration};
use anyhow::{Result, Ok};

```

## 1. 完整示例快速看

下面的示例展示了从创建、更新、同步到加载验证的完整流程：

```rust
mod benchmarks;
mod raft;
use raft::metadata::*;
use anyhow::Result;
use log as logging;
use tokio::time::{sleep, Duration};
// #[tokio::main]
// async fn main() {

//    benchmarks::time_bench::run_benchmarks().await;
// }

#[tokio::main]
async fn main() -> Result<()> {
   // 使用临时目录进行测试，测试结束后会自动清理
   let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
   let metadata_dir_str = temp_dir.path().to_str().unwrap().to_string();

   // 1. 初始加载元数据 (如果文件不存在，会创建新的)
   let initial_metadata = Metadata::load(metadata_dir_str.clone())?;

   // 2. 创建 MetadataManager
   // 每 1 秒自动刷盘一次 (如果数据有变动)
   let manager = MetadataManager::new(initial_metadata, Duration::from_secs(1));

   // 3. 更新元数据
   manager.update_current_term(5).await;
   manager.update_voted_for(101).await;
   println!("Updated term to 5, voted_for to 101.");

   // 短暂等待，让后台任务有机会处理（通常很快）
   sleep(Duration::from_millis(50)).await;
   let current_meta = manager.get();
   assert_eq!(current_meta.current_term, 5);
   assert_eq!(current_meta.voted_for, 101);
   println!("In-memory after update: {:?}", current_meta);

   // 4. 强制同步到磁盘
   manager.sync().await;
   // 等待后台IO完成
   sleep(Duration::from_millis(100)).await;
   println!("Data synced to disk.");

    // 5. 从磁盘重新加载并验证
   let reloaded_metadata = Metadata::load(metadata_dir_str.clone())?;
   println!("Reloaded from disk: {:?}", reloaded_metadata);
   assert_eq!(reloaded_metadata.current_term, 5);
   assert_eq!(reloaded_metadata.voted_for, 101);
   println!("----Finished----");
   Ok(())
}
```

在命令行中输入`cargo run`即可成功运行。


## 2. `Metadata`：元数据容器

`Metadata` 结构体直接存储元数据，并负责序列化到磁盘。

### 2.1 创建新元数据 (`Metadata::new`)

当节点首次启动或没有持久化的元数据文件时使用。

```rust
// 需要指定元数据存储的目录
let dir_path = "./my_raft_data_1".to_string();
// 确保目录存在
std::fs::create_dir_all(&dir_path).ok();

let new_meta = Metadata::new(dir_path.clone());

assert_eq!(new_meta.current_term, 0);
assert_eq!(new_meta.voted_for, raft::config::NONE_SERVER_ID); // 初始未投票
assert_eq!(new_meta.metadata_dir, dir_path);

// 清理示例目录 (可选)
// std::fs::remove_dir_all(&dir_path).ok();
```

### 2.2 从磁盘加载元数据 (`Metadata::load`)

当节点重启时，用于从磁盘加载之前保存的元数据。

```rust
let dir_path = "./my_raft_data_2".to_string();
std::fs::create_dir_all(&dir_path).ok();

// 情况1: 元数据文件不存在，会返回一个新创建的 Metadata 实例
let meta_from_empty_dir = Metadata::load(dir_path.clone())?;
assert_eq!(meta_from_empty_dir.current_term, 0);

// 情况2: 元数据文件存在 (手动创建一个示例文件)
let mut file_content = Metadata::new(dir_path.clone());
file_content.current_term = 3;
file_content.voted_for = 99;
let filepath = Metadata::gen_metadata_filepath(&dir_path);
std::fs::write(&filepath, serde_json::to_string(&file_content).unwrap().as_bytes())?;

let loaded_meta = Metadata::load(dir_path.clone())?;
assert_eq!(loaded_meta.current_term, 3);
assert_eq!(loaded_meta.voted_for, 99);

// 清理示例目录 (可选)
// std::fs::remove_dir_all(&dir_path).ok();
// # Ok::<(), anyhow::Error>(()) // 辅助类型推断
```

## 3. `MetadataManager`：异步管理与持久化

`MetadataManager` 包装了 `Metadata`，并提供异步接口来更新和持久化数据。

### 3.1 创建管理器 (`MetadataManager::new`)

需要一个初始的 `Metadata` 对象（通常通过 `Metadata::load` 获得）和自动刷盘的时间间隔。

```rust
// async fn example_manager_new() -> Result<()> { // 包装在异步函数中以使用 .await
    let dir_path = "./my_raft_data_3".to_string();
    std::fs::create_dir_all(&dir_path).ok();

    let initial_meta = Metadata::load(dir_path.clone())?;
    let flush_interval = Duration::from_secs(5); // 每5秒自动刷盘

    // new 方法返回 Arc<MetadataManager>
    let manager: Arc<MetadataManager> = MetadataManager::new(initial_meta, flush_interval);
    logging::info("Manager created.");
    
    // Manager 创建后会启动一个后台任务进行持久化
    // 清理示例目录 (可选)
    // std::fs::remove_dir_all(&dir_path).ok();
// # Ok(())
// # }
```

### 3.2 获取当前元数据 (`manager.get`)

同步获取当前内存中 `Metadata` 的一个克隆副本。

```rust
// (假设 manager 已按 3.1 创建)
// let manager = ... ;
// let current_metadata: Metadata = manager.get();
// logging::info(&format!("Current term: {}", current_metadata.current_term));
// logging::info(&format!("Current voted_for: {}", current_metadata.voted_for));
```
示例:
```rust
// async fn example_manager_get() -> Result<()> {
// let dir_path = "./my_raft_data_get".to_string();
// std::fs::create_dir_all(&dir_path).ok();
// let initial_meta = Metadata::load(dir_path.clone())?;
// let manager = MetadataManager::new(initial_meta, Duration::from_secs(5));

manager.update_current_term(7).await; // 先更新一下数据
sleep(Duration::from_millis(10)).await; // 给后台一点时间处理

let meta_snapshot = manager.get();
assert_eq!(meta_snapshot.current_term, 7);
// # std::fs::remove_dir_all(&dir_path).ok();
// # Ok(())
// # }
```

### 3.3 更新元数据 (异步)

`update_current_term` 和 `update_voted_for` 方法是异步的。它们将更新命令发送给后台任务，调用本身不会阻塞等待磁盘写入。

```rust
// (假设 manager 已按 3.1 创建)
// async fn example_manager_update() -> Result<()> {
// let manager = ... ;
// manager.update_current_term(10).await;
// manager.update_voted_for(202).await;

// // 此时命令已发送，后台任务会处理内存更新和后续的磁盘持久化
// // 如果需要立即确认内存中的值，可以短暂 sleep 后调用 get()
// sleep(Duration::from_millis(50)).await; // 示例等待
// logging::info(&format!("Updated in-memory term: {}", manager.get().current_term));
// # Ok(())
// # }
```

### 3.4 强制同步到磁盘 (`manager.sync`)

如果你需要确保当前的元数据更改立即写入磁盘（例如，在关键操作之后），可以使用 `sync`。

```rust
// (假设 manager 已按 3.1 创建，并且进行了一些更新)
// async fn example_manager_sync() -> Result<()> {
// let dir_path = "./my_raft_data_sync".to_string();
// std::fs::create_dir_all(&dir_path).ok();
// let initial_meta = Metadata::load(dir_path.clone())?;
// let manager = MetadataManager::new(initial_meta, Duration::from_secs(60)); // 长刷盘间隔

manager.update_current_term(15).await;
manager.update_voted_for(303).await;
logging::info("Data updated, preparing to sync.");

manager.sync().await; // 发送刷盘命令
// 等待后台IO完成
sleep(Duration::from_millis(100)).await;
logging::info("Data sync complete.");

// 可以通过 Metadata::load 验证磁盘上的文件是否已更新
// let reloaded = Metadata::load(manager.get().metadata_dir.clone())?;
// assert_eq!(reloaded.current_term, 15);
// # std::fs::remove_dir_all(&dir_path).ok();
// # Ok(())
// # }
```

## 4. 核心特性总结

*   **异步持久化**：更新操作（如 `update_current_term`）快速返回，实际的磁盘写入由后台任务异步完成，提高了主流程的响应速度。
*   **自动刷盘**：`MetadataManager` 会根据设定的 `flush_interval` 定期将变动的元数据刷写到磁盘。
*   **手动刷盘**：通过 `sync()` 方法，可以强制立即将当前元数据刷写到磁盘。
*   **易于集成**：API 简洁，易于集成到你的 Raft 节点实现中。

---

这个精简版的教程希望能帮助你更快地理解和使用该元数据管理模块。