# mini-kafka

用 Java 8 实现的 Apache Kafka 核心架构——面向学习和作品集展示。代码结构、类名、包名尽量与 Kafka 源码保持一致。

> [English →](README.md)

---

## 这是什么

mini-kafka 实现了完整的 Producer → Broker → Consumer 主干链路，包括：

- **NIO 网络层** — Acceptor + Processor Reactor 模式（对标 `kafka.network.SocketServer`）
- **Log 持久化** — 每个 Partition 对应磁盘上的 `.log` + `.index` 文件对，稀疏 OffsetIndex，Segment 自动滚动
- **GroupCoordinator** — JoinGroup / SyncGroup / Heartbeat / LeaveGroup 完整状态机
- **KafkaController** — 单节点 Controller，负责 Topic 创建和 Partition Leader 管理
- **KafkaProducer** — `RecordAccumulator` 批量缓冲 + 后台 `Sender` 线程，返回 `Future<RecordMetadata>`
- **KafkaConsumer** — `poll()` 模型，内含 `ConsumerCoordinator`、`Fetcher`、`SubscriptionState`
- **Wire Protocol** — 4 字节长度前缀帧，`RequestHeader` / `ResponseHeader`，10 个 API

---

## 模块结构

```
mini-kafka/
├── mini-kafka-common/      # 协议帧、ApiKeys、Errors、RecordBatch（magic=2）
├── mini-kafka-server/      # Broker：NIO 网络、Log 存储、GroupCoordinator、Controller
├── mini-kafka-clients/     # Producer、Consumer、NetworkClient、Metadata 缓存
└── mini-kafka-examples/    # 可运行的 QuickStartExample
```

这与 Apache Kafka 将 `kafka-clients.jar` 独立发布的方式保持一致。

---

## 快速开始

**环境要求：** Java 8+，Maven 3.6+

```bash
# 构建并运行全部测试
mvn clean test -Dsort.skip=true

# 运行端到端演示（启动内嵌 Broker，发送 5 条消息，消费并打印）
mvn clean package -DskipTests -Dsort.skip=true
java -cp mini-kafka-examples/target/mini-kafka-examples-0.1.0-SNAPSHOT.jar \
     com.github.minikafka.examples.QuickStartExample
```

**预期输出：**

```
=== Producer: Sending 5 messages ===
  Sent: topic=quickstart-events, partition=0, offset=0, value=Event #1 from mini-kafka
  Sent: topic=quickstart-events, partition=0, offset=1, value=Event #2 from mini-kafka
  ...
=== Consumer: Polling messages ===
  Received 5 records
  Consumed: topic=quickstart-events, partition=0, offset=0, value=Event #1 from mini-kafka
  ...
  Committed offsets
=== Done ===
```

---

## Producer 用法

```java
Properties props = new Properties();
props.setProperty("bootstrap.servers", "localhost:9092");

try (KafkaProducer producer = new KafkaProducer(props)) {
    // 异步发送，返回 Future<RecordMetadata>
    Future<RecordMetadata> future = producer.send(
        new ProducerRecord("my-topic", "key".getBytes(), "value".getBytes())
    );

    // 同步发送，阻塞直到 Broker 确认
    RecordMetadata meta = producer.sendSync(
        new ProducerRecord("my-topic", 0, null, "hello".getBytes())
    );
    System.out.printf("写入 %s-%d@%d%n", meta.topic, meta.partition, meta.offset);
}
```

## Consumer 用法

```java
Properties props = new Properties();
props.setProperty("bootstrap.servers", "localhost:9092");
props.setProperty("group.id", "my-group");
props.setProperty("enable.auto.commit", "false");

try (KafkaConsumer consumer = new KafkaConsumer(props)) {
    consumer.subscribe(Collections.singletonList("my-topic"));

    ConsumerRecords records = consumer.poll(Duration.ofSeconds(3));
    for (ConsumerRecord record : records) {
        System.out.printf("offset=%d value=%s%n", record.offset, new String(record.value));
    }
    consumer.commitSync();
}
```

## 内嵌 Broker

```java
Properties props = new Properties();
props.setProperty("port", "9092");
props.setProperty("log.dirs", "/tmp/mini-kafka-data");

KafkaServer server = new KafkaServer(new KafkaConfig(props));
server.startup();
server.controller().createTopic("my-topic", 3); // 3 个分区
// ... 使用 Producer / Consumer ...
server.shutdown();
```

---

## 支持的 API

| API Key | 名称 | 说明 |
|---------|------|------|
| 0 | Produce | 向 Partition 写入消息 |
| 1 | Fetch | 从 Partition 拉取消息 |
| 3 | Metadata | 获取集群和 Topic 元数据 |
| 8 | OffsetCommit | 提交 Consumer Group 的 offset |
| 9 | OffsetFetch | 获取已提交的 offset |
| 11 | JoinGroup | 加入 Consumer Group |
| 12 | Heartbeat | 维持 Consumer Group 成员资格 |
| 13 | LeaveGroup | 离开 Consumer Group |
| 14 | SyncGroup | Rebalance 后同步分区分配结果 |
| 19 | CreateTopics | 创建 Topic |

---

## 与 Apache Kafka 的对齐关系

| 组件 | mini-kafka 类 | Kafka 对应类 |
|------|--------------|-------------|
| 网络层 | `SocketServer`, `Acceptor`, `Processor` | `kafka.network.SocketServer` |
| 请求分发 | `KafkaApis` | `kafka.server.KafkaApis` |
| Log 持久化 | `Log`, `LogSegment`, `OffsetIndex` | `kafka.log.Log`, `LogSegment`, `OffsetIndex` |
| Log 管理 | `LogManager` | `kafka.log.LogManager` |
| 集群元数据 | `KafkaController`, `ControllerContext` | `kafka.controller.KafkaController` |
| 消费组协调 | `GroupCoordinator`, `GroupMetadata` | `kafka.coordinator.group.GroupCoordinator` |
| Producer 内部 | `RecordAccumulator`, `Sender`, `ProducerBatch` | `org.apache.kafka.clients.producer.internals.*` |
| Consumer 内部 | `ConsumerCoordinator`, `Fetcher`, `SubscriptionState` | `org.apache.kafka.clients.consumer.internals.*` |
| 网络客户端 | `NetworkClient`, `InFlightRequests`, `Metadata` | `org.apache.kafka.clients.NetworkClient` |
| 消息格式 | `RecordBatch`（magic=2），`Record` | `DefaultRecordBatch` |

**核心链路对齐度约 85%**。已知简化点（设计阶段确认接受）：

- 单节点 Controller，无 ZooKeeper / KRaft，无 Leader 选举
- GroupCoordinator 顺序处理，无 delayed join（不等所有成员到齐再统一返回 JoinGroupResponse）
- Consumer 无独立心跳线程（心跳在 `poll()` 内部发送）
- 无消息压缩（仅支持 NONE）
- 无认证 / ACL

---

## 测试覆盖

3 个模块共 46 个测试，全部通过：

| 模块 | 测试数 | 覆盖内容 |
|------|-------|---------|
| mini-kafka-common | 12 | RecordBatch 编解码、协议帧、ByteBufferUtils |
| mini-kafka-server | 22 | OffsetIndex（含重启恢复）、LogSegment、Log（含并发 append）、GroupCoordinator、Broker 集成（原始 TCP 协议） |
| mini-kafka-clients | 12 | Metadata、RecordAccumulator、RangeAssignor（含 partitions < members 边界）、Producer+Consumer 端到端 |

```bash
mvn clean test -Dsort.skip=true
# [INFO] Tests run: 46, Failures: 0, Errors: 0
# [INFO] BUILD SUCCESS
```

---

## 技术栈

- **语言：** Java 8
- **构建：** Maven 3.6+
- **测试：** JUnit 4
- **日志：** SLF4J + Logback
- **无 Netty，无 Spring** — 纯 `java.nio`

---

## 关键文件一览

```
mini-kafka-common/src/main/java/com/github/minikafka/common/
├── protocol/   ApiKeys.java  Errors.java  RequestHeader.java  ResponseHeader.java
├── record/     Record.java  RecordBatch.java  CompressionType.java
└── network/    ByteBufferUtils.java

mini-kafka-server/src/main/java/com/github/minikafka/server/
├── KafkaServer.java  KafkaConfig.java
├── network/    SocketServer  Acceptor  Processor  RequestChannel  NetworkReceive
├── api/        KafkaApis  KafkaRequestHandler
├── log/        Log  LogSegment  LogManager  OffsetIndex  LogConfig
├── controller/ KafkaController  ControllerContext
└── coordinator/ GroupCoordinator  GroupMetadata  MemberMetadata

mini-kafka-clients/src/main/java/com/github/minikafka/clients/
├── network/    NetworkClient  Metadata  InFlightRequests  ClientRequest  ClientResponse
├── producer/   KafkaProducer  RecordAccumulator  Sender  ProducerBatch  ProducerConfig
└── consumer/   KafkaConsumer  ConsumerCoordinator  Fetcher  SubscriptionState  ConsumerConfig
```

---

## 设计文档

- [设计规格](docs/superpowers/specs/2026-05-11-mini-kafka-design.md)

---

## License

MIT
