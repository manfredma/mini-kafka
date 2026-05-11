# mini-kafka 设计文档

**日期：** 2026-05-11  
**技术栈：** Java 8 + Maven + JUnit4  
**目标：** 忠实还原 Kafka 核心架构，用于学习和作品集展示

---

## 一、模块结构

```
mini-kafka/
├── mini-kafka-common/       # 协议帧、API Key、错误码、Record 模型
├── mini-kafka-server/       # Broker、Controller、LogManager、GroupCoordinator
├── mini-kafka-clients/      # Producer、Consumer、NetworkClient
└── mini-kafka-examples/     # 演示用例
```

`mini-kafka-clients` 可独立打包，对齐真实 Kafka `kafka-clients.jar` 的发布形式。

---

## 二、核心组件

### mini-kafka-common

```
protocol/
  RequestHeader                  # apiKey + apiVersion + correlationId + clientId
  ResponseHeader                 # correlationId
  ApiKeys                        # 枚举：PRODUCE(0) FETCH(1) METADATA(3)
                                 #       JOIN_GROUP(11) HEARTBEAT(12) LEAVE_GROUP(13)
                                 #       SYNC_GROUP(14) CREATE_TOPICS(19)
                                 #       OFFSET_COMMIT(8) OFFSET_FETCH(9)
  Errors                         # 错误码枚举，对齐 Kafka ErrorCode
record/
  Record                         # 单条消息：offset + timestamp + key + value
  RecordBatch                    # magic=2, baseOffset, lastOffsetDelta, CRC32
  CompressionType                # NONE（第一期）
network/
  ByteBufferUtils                # 网络字节序读写工具
```

### mini-kafka-server

```
KafkaServer                      # 启动入口，组装所有组件
network/
  SocketServer                   # NIO Selector，管理 Acceptor + Processor
  Acceptor                       # 监听端口，accept 新连接，轮询分发给 Processor
  Processor                      # 每个线程管理一批 Channel，读完整请求帧
  RequestChannel                 # Processor → Handler 的阻塞队列（LinkedBlockingQueue）
  NetworkReceive                 # 4字节长度前缀读取逻辑
api/
  KafkaApis                      # 请求分发器
  handlers/
    ProduceHandler
    FetchHandler
    MetadataHandler
    CreateTopicsHandler
    OffsetCommitHandler
    OffsetFetchHandler
    GroupCoordinatorHandler      # JoinGroup / SyncGroup / Heartbeat / LeaveGroup
log/
  LogManager                     # 管理所有 TopicPartition 的 Log，启动时加载磁盘
  Log                            # 单个 Partition 的 Log，管理 Segment 列表，负责滚动
  LogSegment                     # 对应一对 .log + .index 文件
  OffsetIndex                    # 稀疏索引，(relativeOffset → filePosition)
  LogConfig                      # segmentBytes / segmentMs / indexIntervalBytes
controller/
  KafkaController                # 单节点 Controller，Topic 创建 / Leader 选举
  ControllerContext              # 元数据内存视图：topics / partitions / leaderMap
coordinator/
  GroupCoordinator               # JoinGroup / SyncGroup / Heartbeat 状态机
  GroupMetadata                  # ConsumerGroup 元数据 + 状态枚举
  MemberMetadata                 # Group 成员信息：memberId / clientId / sessionTimeout
  DelayedHeartbeat               # 心跳超时检测（定时任务）
```

### mini-kafka-clients

```
producer/
  KafkaProducer                  # 异步发送，send() 返回 Future<RecordMetadata>
  RecordAccumulator              # 消息按 TopicPartition 批量缓冲（Deque<ProducerBatch>）
  ProducerBatch                  # 单个批次，持有 ByteBuffer
  Sender                         # 后台 IO 线程，将 Batch 通过 NetworkClient 发送
  ProducerConfig                 # bootstrap.servers / batch.size / linger.ms / acks
  RecordMetadata                 # 发送结果：topic / partition / offset / timestamp
consumer/
  KafkaConsumer                  # poll(Duration) 模型，非线程安全
  ConsumerCoordinator            # 客户端侧 Rebalance：JoinGroup / SyncGroup / Heartbeat
  Fetcher                        # 构造 FetchRequest，解析 FetchResponse
  SubscriptionState              # 分区分配和 position / committed offset 追踪
  ConsumerConfig                 # group.id / auto.offset.reset / enable.auto.commit
  ConsumerRecords                # poll() 返回值，Iterable<ConsumerRecord>
  ConsumerRecord                 # 单条消费记录：topic / partition / offset / key / value
network/
  NetworkClient                  # 基于 NIO 的异步网络客户端
  InFlightRequests               # 在途请求管理（correlationId → ClientRequest）
  Metadata                       # 集群元数据缓存，定期刷新（MetadataRequest）
  ClientRequest                  # 封装请求 + 回调
  ClientResponse                 # 封装响应 + 对应 ClientRequest
```

---

## 三、Wire Protocol

### 帧格式（对齐 Kafka）

```
Request:
  [4 bytes] message_size          # 不含自身4字节
  [2 bytes] api_key
  [2 bytes] api_version
  [4 bytes] correlation_id
  [2 bytes] client_id_length
  [N bytes] client_id
  [N bytes] request_body          # 各 API 自定义

Response:
  [4 bytes] message_size
  [4 bytes] correlation_id
  [N bytes] response_body
```

### 支持的 API（第一期）

| ApiKey | 名称 | 说明 |
|--------|------|------|
| 0 | Produce | 写入消息 |
| 1 | Fetch | 拉取消息 |
| 3 | Metadata | 获取集群/Topic 元数据 |
| 8 | OffsetCommit | 提交 offset |
| 9 | OffsetFetch | 获取已提交 offset |
| 11 | JoinGroup | 加入 ConsumerGroup |
| 12 | Heartbeat | 心跳保活 |
| 13 | LeaveGroup | 离开 Group |
| 14 | SyncGroup | 同步分区分配结果 |
| 19 | CreateTopics | 创建 Topic |

---

## 四、数据流

### Producer 发送链路

```
KafkaProducer.send(ProducerRecord)
  → Serializer → Partitioner（选择 Partition）
  → RecordAccumulator.append()        # 满 batch.size 或 linger.ms 触发
  → Sender 线程 → NetworkClient.send(ProduceRequest)
  → Broker SocketServer Processor 读完整帧
  → RequestChannel → KafkaRequestHandler 线程池
  → KafkaApis → ProduceHandler
  → ReplicaManager.appendRecords()
  → Log.append() → LogSegment.append() → FileChannel.write()（顺序写）
  → ProduceResponse(baseOffset)
  → Future<RecordMetadata> 完成
```

### Consumer 拉取链路

```
KafkaConsumer.poll(Duration)
  → ConsumerCoordinator 确保已加入 Group（触发 JoinGroup/SyncGroup）
  → Fetcher.sendFetches() → FetchRequest
  → Broker FetchHandler → Log.read(offset, maxBytes)
  → LogSegment 通过 OffsetIndex 定位物理位置 → FileChannel.read()
  → FetchResponse(RecordBatch)
  → Fetcher 解析 → ConsumerRecords 返回应用层
  → SubscriptionState 更新 position
  → 下次 poll 前提交 offset（OffsetCommitRequest）
```

### Rebalance 流程

```
新 Consumer 加入 or 成员离开/心跳超时
  → GroupCoordinator 收到 JoinGroupRequest
  → 状态: Empty/Stable → PreparingRebalance
  → 等待所有成员 JoinGroup（或 sessionTimeout）
  → 选出 Leader Consumer（第一个加入的成员）
  → JoinGroupResponse：Leader 收到成员列表，Follower 收到空列表
  → Leader 执行 RangeAssignor 分配分区
  → 所有成员发 SyncGroupRequest（Leader 携带分配结果）
  → GroupCoordinator 下发分配 → 状态: Stable
  → 各 Consumer 按分配结果开始 Fetch
```

---

## 五、持久化设计

### 磁盘文件布局

```
${log.dirs}/
└── topic-name-0/                        # TopicPartition 目录
    ├── 00000000000000000000.log          # LogSegment 数据文件（baseOffset命名）
    ├── 00000000000000000000.index        # 稀疏 OffsetIndex
    ├── 00000000000000001024.log          # 滚动后新 Segment
    └── 00000000000000001024.index
```

### LogSegment 写入

- `.log` 文件：RecordBatch 顺序追加，`FileChannel` 直接写，不经过堆内存
- `.index` 文件：每累积 `indexIntervalBytes`（默认 4KB）写一条稀疏索引 `(relativeOffset → filePosition)`
- Segment 滚动：文件超过 `segmentBytes`（默认 1GB）或超过 `segmentMs`（默认 7天）

### 基于 Offset 的读取

```
1. 二分查找 Segment 列表，找到 baseOffset ≤ targetOffset 的 LogSegment
2. OffsetIndex 二分查找，找到最近的索引条目 (relativeOffset, filePosition)
3. 从 filePosition 顺序扫描 .log，直到找到 targetOffset 的 RecordBatch
4. 读取 maxBytes 数据返回
```

---

## 六、线程模型

```
KafkaServer.startup()
  ├── Acceptor 线程（1个）            # selector.accept() → 轮询分发给 Processor
  ├── Processor 线程（默认3个）        # 读完整请求帧 → RequestChannel
  ├── KafkaRequestHandler 线程池（8个）# RequestChannel 取请求 → KafkaApis
  ├── Sender 线程（客户端，1个）        # 批量发送 ProducerBatch
  ├── GroupCoordinator 定时线程        # 心跳超时检测（每 500ms）
  └── Controller 线程                  # 元数据变更处理
```

---

## 七、简化说明

与真实 Kafka 的主要差异（3处）：

| 简化点 | 真实 Kafka | mini-kafka |
|--------|-----------|------------|
| Controller 事件机制 | ControllerEventManager 异步事件队列 | 同步调用，单节点无选举 |
| GroupCoordinator 并发 | 支持多 Group 并发 Rebalance | 简化状态机，顺序处理 |
| 元数据持久化 | KRaft MetadataLog | 内存 ControllerContext，重启从磁盘 Log 重建 |

核心主干路径（Producer 写入 → Broker 持久化 → Consumer 拉取 → Rebalance）100% 对齐真实 Kafka。

---

## 八、测试策略

| 层级 | 测试内容 | 覆盖目标 |
|------|---------|---------|
| 单元测试 | RecordBatch 编解码、OffsetIndex 读写、协议帧解析、分区分配算法 | 80%+ |
| 集成测试 | Producer → Broker → Consumer 完整链路（内嵌 Broker） | 核心路径 |
| 场景测试 | Rebalance 流程、多 Partition 并发写读、Segment 滚动 | 关键场景 |

---

## 九、依赖清单

```xml
<!-- 全局：无 Netty、无 Spring，纯 java.nio -->
mini-kafka-common:  junit:junit:4.x（scope=test）
mini-kafka-server:  slf4j-api + logback-classic, mini-kafka-common
mini-kafka-clients: slf4j-api, mini-kafka-common
mini-kafka-examples: mini-kafka-server, mini-kafka-clients
```
