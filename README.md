# godis

一个基于 Go 的简化 Redis/Cluster 原型，实现了 KV 存储、AOF 持久化和简单 Gossip 集群机制。

## 项目概述

`godis` 旨在演示一个轻量级的分布式键值存储系统的核心组件：

- 基于 `net/rpc` 的 RPC 服务
- 多节点 Gossip 集群拓扑
- 槽位分配与重定向机制
- AOF 持久化与重放恢复
- 线程安全的分片并发存储引擎

## 主要特性

- KV 操作：`Get`、`Set`、`Delete`
- 状态同步：Master 节点向从节点传播写操作
- 集群模式：每个节点维护自身槽位与节点信息
- 持久化：通过 AOF 日志记录写命令，支持服务重启后恢复
- 淘汰策略：支持 `LRU` / `FIFO` / `NONE`（可选）

## 目录结构

- `main.go` — 程序入口，负责参数解析、引擎初始化、RPC 服务启动和集群加入
- `api/` — RPC 参数与返回结构定义
- `cluster/` — Gossip 集群与槽位管理逻辑
- `server/` — KV 服务与 Gossip 服务 RPC 实现
- `store/` — 存储引擎、AOF 持久化和淘汰策略
- `utils/` — 辅助函数，例如键哈希槽计算

## 快速开始

1. 进入项目目录：

```bash
cd /Users/potato/project/godis
```

2. 构建并运行节点：

```bash
go run main.go -addr :1234 -id node1
```

3. 启动第二个节点并加入集群：

```bash
go run main.go -addr :1235 -id node2 -join :1234
```

## 运行参数

- `-addr`：本节点监听地址，默认 `:1234`
- `-id`：本节点唯一 ID，默认 `node1`
- `-join`：已有 Gossip 集群节点地址，用于加入集群
- `-max-entries`：存储最大元素数量，`0` 表示不限制
- `-policy`：淘汰策略，支持 `LRU`、`FIFO`、`NONE`
- `-aof`：AOF 持久化文件路径，默认 `appendonly.aof`

## RPC 接口

当前实现使用 Go 原生 `net/rpc`，暴露以下服务：

- `KVServer.Get` — 获取键值
- `KVServer.Set` — 设置键值
- `KVServer.Delete` — 删除键值
- `GossipServer.PingPong` — Gossip 心跳与元数据交换
- `GossipServer.RequestVote` — 投票请求
- `ClusterState.MigrateKeys` — 槽迁移辅助
- `KVServer.Propagate` — 从 Master 向从节点同步数据

## 持久化

`store/AOF` 模块负责把 `SET`、`DEL`、`EXPIRE` 命令追加到 AOF 文件，并在程序启动时重放文件中的命令进行恢复。

## 开发建议

- 可进一步完善集群槽迁移、故障检测和一致性协议
- 可增加客户端协议支持，例如 Redis RESP
- 可以补齐单元测试与性能基准测试

## 依赖

本仓库基于 Go Modules，使用 Go 1.25 进行开发。

```bash
go version
```

## 许可证

本项目目前未附带许可证，可根据需要补充 `LICENSE` 文件。
