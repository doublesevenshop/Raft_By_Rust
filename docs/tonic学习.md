这里写的不是很好，应该先介绍protobuf、再介绍gRPC、再介绍tonic，最终介绍如何实现一个自己的raft的，之后再修改文档吧，现在先简单记录一下。


# tonic学习
> 2025年5月16日11:13:05

> 参考链接：[Tonic基于gRPC的Rust实现](https://rustcc.cn/article?id=21934c4e-60eb-4796-80c2-70c4733032e1)
> 参考链接：[tonic官方库](https://github.com/hyperium/tonic)



我们要使用Rust实现raft协议，在需要gRPC进行网络通信的情况下，使用`Tonic`或许是一个不错的选择。

tonic是一个高性能的gRPC库，它基于`HTTP/2`协议，能够提供高效的双向通信，对于Raft协议中节点间的频繁通信非常重要。并且tonic可以根据Proto文件自动生成Rust代码，简化了网络请求中的处理逻辑。

并且通过`Protobuf`定义消息格式，因为tonic生成的代码具有严格的类型检查，因此会减少很多运行时错误。同时tonic支持异步编程模型，适合处理高并发的网络请求。

那么在Raft中，哪些地方会用到tonic呢？

- 日志复制
- 选举过程
- 心跳机制

哎？这不就是Raft协议中最主要的几个场景吗？换句话说，只要是涉及到节点间通信的，基本都需要tonic，因此如何使用高效的通信方式，就是我们需要首先考虑到的。

## Tonic概述
`Tonic`由三个主要组件组成：通用 `gRPC` 实现、高性能 `HTTP/2` 实现和由 `ProSt` 提供支持的 `CodeGen`。泛型实现可以通过一组泛型特征支持任何 `HTTP/2` 实现和任何编码。`HTTP/2` 实现基于 `hyper`，`hyper` 是一个快速的 `HTTP/1.1` 和 `HTTP/2` 客户端和服务器，构建在强大的 `tokio` 堆栈之上。`codegen` 包含用于从 `protobuf` 定义构建客户端和服务器的工具。


> 注意：这里的Tonic和prost在引入包的时候，需要二者版本保持一致，我就是因为tonic选择了"0.13"，prost选择了"0.12"，导致代码生成会有一些问题。


此时的cargo.toml可以先引入：
```rust
[package]
name = "KEEP_RUNNING"
version = "0.1.0"
edition = "2021"
authors = ["Ziyuan DU <xyw99726@gmail.com>"]

[dependencies]
async-trait = "0.1"
tokio = { version = "1", features = ["full"] }
serde = { version = "1.0", features = ["derive"]}
serde_json = "1.0.0"
tonic = "0.13.0"
prost = "0.13"  // 保持一致
tower = "0.4"
```

参考github的官方库，tonic具有诸多良好的特性，那么在这里我就不再赘述，我们可以在使用的过程中慢慢品味其中的美好。

我们可以先进行一些简单的实验。

## Hello Tonic

在使用tonic之前，需要先安装和protobuf相关的库——protoc。

我使用的是archlinux，直接输入命令
```bash
sudo pacman -S protoc
```

安装完成之后，可以通过指令`protoc --version`查看是否安装成功。

之后我们就可以在和src相同目录层级下创建一个proto文件夹。
```
|
|--src
|--proto
|--build.rs
|--Cargo.toml
```
除此之外我们还需要创建一个`build.rs`，为了保证我们能够根据proto生成对应的代码。

> 此处的内容可以参考先前提到的博客：[Tonic基于gRPC的Rust实现](https://rustcc.cn/article?id=21934c4e-60eb-4796-80c2-70c4733032e1)


在阅读这个博客的过程中，可能需要注意的几个点。

1. **注意编写顺序**
先写好proto文件，之后在`build.rs`中进行一个构建，最终在`client`和`server`中使用先前创建好的proto文件。



2. **`build.rs`的更新**
在博客中，用的版本比较老，新的example中针对`build.rs`进行了一个细化。


```rust
use std::{env, path::PathBuf};
fn main() {
    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap()); // 获取构建输出目录的路径
    
    tonic_build::configure()
        .file_descriptor_set_path(out_dir.join("helloworld_descriptor.bin"))    // 配置tonic构建工具，指定生成文件描述符集的保存路径
        .compile_protos(&["proto/helloworld.proto"], &["proto"])    // 指定要编译的proto文件和搜索路径，并开始编译
        .unwrap();
}
```

更多具体的内容，大家可以移步官方例子，总之我们可以初步使用tonic，便达到我们最初的目的
