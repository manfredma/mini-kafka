# mini-kafka

A Java 8 implementation of Apache Kafka's core architecture — built for learning and as a portfolio project. The codebase mirrors Kafka's source structure and terminology as closely as possible.

> [中文文档 →](README_CN.md)

---

## What This Is

mini-kafka implements the full Producer → Broker → Consumer pipeline, including:

- **NIO Network Layer** — Acceptor + Processor Reactor pattern (mirrors `kafka.network.SocketServer`)
- **Log Persistence** — `.log` + `.index` file pairs per partition, sparse offset index, segment rolling (mirrors `kafka.log`)
- **GroupCoordinator** — JoinGroup / SyncGroup / Heartbeat / LeaveGroup state machine
- **KafkaController** — single-node topic creation and partition leadership
- **KafkaProducer** — `RecordAccumulator` + background `Sender` thread, `Future<RecordMetadata>`
- **KafkaConsumer** — `poll()` model, `ConsumerCoordinator`, `Fetcher`, `SubscriptionState`
- **Wire Protocol** — 4-byte length-prefixed frames, `RequestHeader` / `ResponseHeader`, 10 API keys

---

## Module Structure

```
mini-kafka/
├── mini-kafka-common/      # Wire protocol, ApiKeys, Errors, RecordBatch (magic=2)
├── mini-kafka-server/      # Broker: NIO network, Log storage, GroupCoordinator, Controller
├── mini-kafka-clients/     # Producer, Consumer, NetworkClient, Metadata cache
└── mini-kafka-examples/    # Runnable QuickStartExample
```

This mirrors how Apache Kafka publishes `kafka-clients.jar` as a separate artifact from the broker.

---

## Quick Start

**Requirements:** Java 8+, Maven 3.6+

```bash
# Build and run all tests
mvn clean test -Dsort.skip=true

# Run the end-to-end demo (starts embedded broker, produces 5 messages, consumes them)
mvn clean package -DskipTests -Dsort.skip=true
java -cp mini-kafka-examples/target/mini-kafka-examples-0.1.0-SNAPSHOT.jar \
     com.github.minikafka.examples.QuickStartExample
```

**Expected output:**

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

## Producer API

```java
Properties props = new Properties();
props.setProperty("bootstrap.servers", "localhost:9092");

try (KafkaProducer producer = new KafkaProducer(props)) {
    // Async send — returns Future<RecordMetadata>
    Future<RecordMetadata> future = producer.send(
        new ProducerRecord("my-topic", "key".getBytes(), "value".getBytes())
    );

    // Synchronous send — blocks until broker confirms
    RecordMetadata meta = producer.sendSync(
        new ProducerRecord("my-topic", 0, null, "hello".getBytes())
    );
    System.out.printf("Written to %s-%d@%d%n", meta.topic, meta.partition, meta.offset);
}
```

## Consumer API

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

## Embedded Broker

```java
Properties props = new Properties();
props.setProperty("port", "9092");
props.setProperty("log.dirs", "/tmp/mini-kafka-data");

KafkaServer server = new KafkaServer(new KafkaConfig(props));
server.startup();
server.controller().createTopic("my-topic", 3); // 3 partitions
// ... use producer/consumer ...
server.shutdown();
```

---

## Supported APIs

| API Key | Name | Description |
|---------|------|-------------|
| 0 | Produce | Write messages to a partition |
| 1 | Fetch | Pull messages from a partition |
| 3 | Metadata | Get cluster and topic metadata |
| 8 | OffsetCommit | Commit consumer group offsets |
| 9 | OffsetFetch | Fetch committed offsets |
| 11 | JoinGroup | Join a consumer group |
| 12 | Heartbeat | Keep consumer group membership alive |
| 13 | LeaveGroup | Leave a consumer group |
| 14 | SyncGroup | Sync partition assignment after rebalance |
| 19 | CreateTopics | Create a new topic |

---

## Architecture Alignment with Apache Kafka

| Component | mini-kafka class | Kafka equivalent |
|-----------|-----------------|-----------------|
| Network layer | `SocketServer`, `Acceptor`, `Processor` | `kafka.network.SocketServer` |
| Request dispatch | `KafkaApis` | `kafka.server.KafkaApis` |
| Log persistence | `Log`, `LogSegment`, `OffsetIndex` | `kafka.log.Log`, `LogSegment`, `OffsetIndex` |
| Log management | `LogManager` | `kafka.log.LogManager` |
| Cluster metadata | `KafkaController`, `ControllerContext` | `kafka.controller.KafkaController` |
| Group coordination | `GroupCoordinator`, `GroupMetadata` | `kafka.coordinator.group.GroupCoordinator` |
| Producer internals | `RecordAccumulator`, `Sender`, `ProducerBatch` | `org.apache.kafka.clients.producer.internals.*` |
| Consumer internals | `ConsumerCoordinator`, `Fetcher`, `SubscriptionState` | `org.apache.kafka.clients.consumer.internals.*` |
| Network client | `NetworkClient`, `InFlightRequests`, `Metadata` | `org.apache.kafka.clients.NetworkClient` |
| Record format | `RecordBatch` (magic=2), `Record` | `DefaultRecordBatch` |

**Alignment: ~85%** on the core read/write/rebalance path. Known simplifications:

- Single-node Controller (no ZooKeeper / KRaft, no leader election)
- GroupCoordinator processes one group at a time (no delayed join wait)
- No independent heartbeat thread in Consumer (heartbeat sent inside `poll()`)
- No message compression (NONE only)
- No authentication / ACL

---

## Test Coverage

46 tests across 3 modules, all passing:

| Module | Tests | Coverage |
|--------|-------|---------|
| mini-kafka-common | 12 | RecordBatch encode/decode, protocol headers, ByteBufferUtils |
| mini-kafka-server | 22 | OffsetIndex (incl. restart recovery), LogSegment, Log (incl. concurrent append), GroupCoordinator, Broker integration (raw TCP) |
| mini-kafka-clients | 12 | Metadata, RecordAccumulator, RangeAssignor (incl. partitions < members), Producer+Consumer end-to-end |

```bash
mvn clean test -Dsort.skip=true
# [INFO] Tests run: 46, Failures: 0, Errors: 0
# [INFO] BUILD SUCCESS
```

---

## Tech Stack

- **Language:** Java 8
- **Build:** Maven 3.6+
- **Test:** JUnit 4
- **Logging:** SLF4J + Logback
- **No Netty, no Spring** — pure `java.nio`

---

## Project Layout (key files)

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

## License

MIT
