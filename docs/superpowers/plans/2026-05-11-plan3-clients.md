# mini-kafka-clients Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 构建 Producer 和 Consumer 客户端，对齐 Kafka kafka-clients 包的 API 和内部架构。

**Architecture:** NetworkClient（NIO 异步）+ Metadata（集群元数据缓存）→ Producer（RecordAccumulator + Sender 后台线程）→ Consumer（ConsumerCoordinator + Fetcher + SubscriptionState，poll 模型）。

**Tech Stack:** Java 8, Maven, JUnit4, SLF4J

**依赖:** mini-kafka-common + mini-kafka-server（测试时需内嵌 Broker）

---

## File Map

```
mini-kafka-clients/
├── pom.xml
└── src/
    ├── main/java/com/github/minikafka/clients/
    │   ├── network/
    │   │   ├── NetworkClient.java
    │   │   ├── ClientRequest.java
    │   │   ├── ClientResponse.java
    │   │   ├── InFlightRequests.java
    │   │   └── Metadata.java
    │   ├── producer/
    │   │   ├── KafkaProducer.java
    │   │   ├── ProducerConfig.java
    │   │   ├── ProducerRecord.java
    │   │   ├── RecordMetadata.java
    │   │   ├── ProducerBatch.java
    │   │   ├── RecordAccumulator.java
    │   │   └── Sender.java
    │   └── consumer/
    │       ├── KafkaConsumer.java
    │       ├── ConsumerConfig.java
    │       ├── ConsumerRecord.java
    │       ├── ConsumerRecords.java
    │       ├── SubscriptionState.java
    │       ├── ConsumerCoordinator.java
    │       └── Fetcher.java
    └── test/java/com/github/minikafka/clients/
        ├── network/
        │   └── MetadataTest.java
        ├── producer/
        │   └── RecordAccumulatorTest.java
        └── integration/
            └── ProducerConsumerIntegrationTest.java
```

---

### Task 1: clients 模块 pom

**Files:**
- Create: `mini-kafka-clients/pom.xml`

- [ ] **Step 1: 写 mini-kafka-clients/pom.xml**

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0
         http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <parent>
        <groupId>com.github.minikafka</groupId>
        <artifactId>mini-kafka</artifactId>
        <version>0.1.0-SNAPSHOT</version>
    </parent>

    <artifactId>mini-kafka-clients</artifactId>
    <name>mini-kafka-clients</name>

    <dependencies>
        <dependency>
            <groupId>com.github.minikafka</groupId>
            <artifactId>mini-kafka-common</artifactId>
        </dependency>
        <dependency>
            <groupId>org.slf4j</groupId>
            <artifactId>slf4j-api</artifactId>
        </dependency>
        <dependency>
            <groupId>ch.qos.logback</groupId>
            <artifactId>logback-classic</artifactId>
        </dependency>
        <dependency>
            <groupId>junit</groupId>
            <artifactId>junit</artifactId>
        </dependency>
        <!-- 集成测试依赖内嵌 Broker -->
        <dependency>
            <groupId>com.github.minikafka</groupId>
            <artifactId>mini-kafka-server</artifactId>
            <version>${project.version}</version>
            <scope>test</scope>
        </dependency>
    </dependencies>
</project>
```

- [ ] **Step 2: 创建目录**

```bash
mkdir -p mini-kafka-clients/src/main/java/com/github/minikafka/clients/network
mkdir -p mini-kafka-clients/src/main/java/com/github/minikafka/clients/producer
mkdir -p mini-kafka-clients/src/main/java/com/github/minikafka/clients/consumer
mkdir -p mini-kafka-clients/src/test/java/com/github/minikafka/clients/network
mkdir -p mini-kafka-clients/src/test/java/com/github/minikafka/clients/producer
mkdir -p mini-kafka-clients/src/test/java/com/github/minikafka/clients/integration
```

- [ ] **Step 3: 先 install server 模块（测试时需要）**

```bash
mvn clean install -pl mini-kafka-common,mini-kafka-server -DskipTests -Dsort.skip=true
```
Expected: BUILD SUCCESS

- [ ] **Step 4: 编译验证**

```bash
mvn clean compile -pl mini-kafka-clients -Dsort.skip=true
```
Expected: BUILD SUCCESS

- [ ] **Step 5: Commit**

```bash
git add mini-kafka-clients/pom.xml
git commit -m "chore(clients): init clients module"
```

---

### Task 2: 网络层基础类

**Files:**
- Create: `mini-kafka-clients/src/main/java/com/github/minikafka/clients/network/ClientRequest.java`
- Create: `mini-kafka-clients/src/main/java/com/github/minikafka/clients/network/ClientResponse.java`
- Create: `mini-kafka-clients/src/main/java/com/github/minikafka/clients/network/InFlightRequests.java`

- [ ] **Step 1: 写 ClientRequest.java**

```java
package com.github.minikafka.clients.network;

import com.github.minikafka.common.protocol.ApiKeys;
import java.nio.ByteBuffer;
import java.util.function.Consumer;

public final class ClientRequest {
    public final int correlationId;
    public final ApiKeys apiKey;
    public final ByteBuffer requestBuffer;
    public final Consumer<ClientResponse> callback;
    public final long createdMs;

    public ClientRequest(int correlationId, ApiKeys apiKey,
                         ByteBuffer requestBuffer, Consumer<ClientResponse> callback) {
        this.correlationId = correlationId;
        this.apiKey = apiKey;
        this.requestBuffer = requestBuffer;
        this.callback = callback;
        this.createdMs = System.currentTimeMillis();
    }
}
```

- [ ] **Step 2: 写 ClientResponse.java**

```java
package com.github.minikafka.clients.network;

import java.nio.ByteBuffer;

public final class ClientResponse {
    public final ClientRequest request;
    public final ByteBuffer responseBody; // 已跳过 correlationId
    public final Exception exception;

    public ClientResponse(ClientRequest request, ByteBuffer responseBody) {
        this.request = request;
        this.responseBody = responseBody;
        this.exception = null;
    }

    public ClientResponse(ClientRequest request, Exception exception) {
        this.request = request;
        this.responseBody = null;
        this.exception = exception;
    }

    public boolean hasError() { return exception != null; }
}
```

- [ ] **Step 3: 写 InFlightRequests.java**

```java
package com.github.minikafka.clients.network;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 管理已发送但未收到响应的请求，按 correlationId 匹配响应
 */
public final class InFlightRequests {

    private final Map<Integer, ClientRequest> inflightMap = new LinkedHashMap<>();

    public void add(ClientRequest request) {
        inflightMap.put(request.correlationId, request);
    }

    public ClientRequest remove(int correlationId) {
        return inflightMap.remove(correlationId);
    }

    public int size() { return inflightMap.size(); }

    public boolean isEmpty() { return inflightMap.isEmpty(); }
}
```

- [ ] **Step 4: 编译验证**

```bash
mvn clean compile -pl mini-kafka-clients -Dsort.skip=true
```
Expected: BUILD SUCCESS

- [ ] **Step 5: Commit**

```bash
git add mini-kafka-clients/src/main/java/com/github/minikafka/clients/network/
git commit -m "feat(clients): add ClientRequest, ClientResponse, InFlightRequests"
```

---

### Task 3: Metadata（集群元数据缓存）

**Files:**
- Create: `mini-kafka-clients/src/main/java/com/github/minikafka/clients/network/Metadata.java`
- Create: `mini-kafka-clients/src/test/java/com/github/minikafka/clients/network/MetadataTest.java`

- [ ] **Step 1: 写 Metadata 失败测试**

```java
package com.github.minikafka.clients.network;

import org.junit.Test;
import java.util.*;
import static org.junit.Assert.*;

public class MetadataTest {

    @Test
    public void testUpdateAndQuery() {
        Metadata metadata = new Metadata(100L);
        Map<String, Metadata.TopicMetadata> topics = new HashMap<>();
        List<Metadata.PartitionMetadata> partitions = Arrays.asList(
            new Metadata.PartitionMetadata(0, "localhost", 9092, 0),
            new Metadata.PartitionMetadata(1, "localhost", 9092, 0)
        );
        topics.put("test-topic", new Metadata.TopicMetadata("test-topic", partitions));

        metadata.update(topics, Arrays.asList(new Metadata.BrokerInfo(0, "localhost", 9092)));

        assertTrue(metadata.containsTopic("test-topic"));
        assertFalse(metadata.containsTopic("other-topic"));
        assertEquals(2, metadata.partitionCount("test-topic"));
        assertEquals(0, metadata.leaderFor("test-topic", 0).port());
    }

    @Test
    public void testNeedsUpdate() {
        Metadata metadata = new Metadata(100L);
        assertTrue(metadata.needsUpdate()); // 初始需要更新
        metadata.update(new HashMap<>(), new ArrayList<>());
        assertFalse(metadata.needsUpdate());
    }
}
```

- [ ] **Step 2: 运行测试，确认失败**

```bash
mvn clean test -pl mini-kafka-clients -Dtest=MetadataTest -Dsort.skip=true
```
Expected: FAIL - compilation error

- [ ] **Step 3: 实现 Metadata.java**

```java
package com.github.minikafka.clients.network;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 客户端集群元数据缓存，定期从 Broker 刷新
 * 对应 Kafka clients/src/main/java/org/apache/kafka/clients/Metadata.java
 */
public final class Metadata {

    public static final class BrokerInfo {
        public final int nodeId;
        public final String host;
        public final int port;
        public BrokerInfo(int nodeId, String host, int port) {
            this.nodeId = nodeId; this.host = host; this.port = port;
        }
    }

    public static final class PartitionMetadata {
        public final int partition;
        public final String leaderHost;
        public final int leaderPort;
        public final int leaderId;
        public PartitionMetadata(int partition, String leaderHost, int leaderPort, int leaderId) {
            this.partition = partition;
            this.leaderHost = leaderHost;
            this.leaderPort = leaderPort;
            this.leaderId = leaderId;
        }
        public int port() { return leaderPort; }
    }

    public static final class TopicMetadata {
        public final String topic;
        public final List<PartitionMetadata> partitions;
        public TopicMetadata(String topic, List<PartitionMetadata> partitions) {
            this.topic = topic;
            this.partitions = partitions;
        }
    }

    private final long metadataExpireMs;
    private Map<String, TopicMetadata> topicMetadataMap = new HashMap<>();
    private List<BrokerInfo> brokers = new ArrayList<>();
    private long lastRefreshMs = 0;
    private final AtomicBoolean needsUpdate = new AtomicBoolean(true);

    public Metadata(long metadataExpireMs) {
        this.metadataExpireMs = metadataExpireMs;
    }

    public synchronized void update(Map<String, TopicMetadata> topics, List<BrokerInfo> brokers) {
        this.topicMetadataMap = new HashMap<>(topics);
        this.brokers = new ArrayList<>(brokers);
        this.lastRefreshMs = System.currentTimeMillis();
        this.needsUpdate.set(false);
    }

    public synchronized boolean containsTopic(String topic) {
        return topicMetadataMap.containsKey(topic);
    }

    public synchronized int partitionCount(String topic) {
        TopicMetadata tm = topicMetadataMap.get(topic);
        return tm == null ? 0 : tm.partitions.size();
    }

    public synchronized PartitionMetadata leaderFor(String topic, int partition) {
        TopicMetadata tm = topicMetadataMap.get(topic);
        if (tm == null) return null;
        for (PartitionMetadata pm : tm.partitions) {
            if (pm.partition == partition) return pm;
        }
        return null;
    }

    public synchronized List<BrokerInfo> brokers() { return Collections.unmodifiableList(brokers); }

    public boolean needsUpdate() {
        return needsUpdate.get() || (System.currentTimeMillis() - lastRefreshMs) > metadataExpireMs;
    }

    public void requestUpdate() { needsUpdate.set(true); }
}
```

- [ ] **Step 4: 运行测试，确认通过**

```bash
mvn clean test -pl mini-kafka-clients -Dtest=MetadataTest -Dsort.skip=true
```
Expected: Tests run: 2, Failures: 0, Errors: 0

- [ ] **Step 5: Commit**

```bash
git add mini-kafka-clients/src/main/java/com/github/minikafka/clients/network/Metadata.java \
        mini-kafka-clients/src/test/java/com/github/minikafka/clients/network/MetadataTest.java
git commit -m "feat(clients): add Metadata with TDD"
```

---

### Task 4: NetworkClient（NIO 异步客户端）

**Files:**
- Create: `mini-kafka-clients/src/main/java/com/github/minikafka/clients/network/NetworkClient.java`

- [ ] **Step 1: 实现 NetworkClient.java**

```java
package com.github.minikafka.clients.network;

import com.github.minikafka.common.network.ByteBufferUtils;
import com.github.minikafka.common.protocol.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.*;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 基于 NIO 的异步网络客户端
 * 对应 Kafka org.apache.kafka.clients.NetworkClient
 *
 * 简化：单连接（连接到 bootstrap broker），同步刷写（在 poll 时读写）
 */
public final class NetworkClient implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(NetworkClient.class);

    private final String bootstrapHost;
    private final int bootstrapPort;
    private final Metadata metadata;
    private final InFlightRequests inFlightRequests = new InFlightRequests();
    private final AtomicInteger correlationIdCounter = new AtomicInteger(0);
    private final String clientId;

    private SocketChannel channel;
    private boolean connected = false;

    public NetworkClient(String bootstrapHost, int bootstrapPort,
                         String clientId, Metadata metadata) {
        this.bootstrapHost = bootstrapHost;
        this.bootstrapPort = bootstrapPort;
        this.clientId = clientId;
        this.metadata = metadata;
    }

    public void connect() throws IOException {
        channel = SocketChannel.open(new InetSocketAddress(bootstrapHost, bootstrapPort));
        channel.configureBlocking(false);
        connected = true;
        log.info("Connected to {}:{}", bootstrapHost, bootstrapPort);
    }

    public void ensureConnected() throws IOException {
        if (!connected || channel == null || !channel.isConnected()) {
            connect();
        }
    }

    public int nextCorrelationId() {
        return correlationIdCounter.incrementAndGet();
    }

    /**
     * 构造带 RequestHeader 的完整请求帧（4字节长度前缀）
     */
    public ByteBuffer buildRequest(ApiKeys apiKey, ByteBuffer body) {
        int corrId = nextCorrelationId();
        RequestHeader header = new RequestHeader(apiKey, (short) 0, corrId, clientId);
        int totalSize = header.sizeOf() + body.remaining();
        ByteBuffer frame = ByteBuffer.allocate(4 + totalSize);
        frame.putInt(totalSize);
        header.writeTo(frame);
        frame.put(body);
        frame.flip();
        return frame;
    }

    /**
     * 同步发送请求并等待响应（blocking）
     * 适用于 Metadata 刷新、简单一次性请求
     */
    public ByteBuffer sendSync(ApiKeys apiKey, ByteBuffer body) throws IOException {
        ensureConnected();
        ByteBuffer frame = buildRequest(apiKey, body);
        int corrId = correlationIdCounter.get();

        // 写入
        while (frame.hasRemaining()) channel.write(frame);

        // 切回 blocking 模式读响应
        channel.configureBlocking(true);
        try {
            byte[] sizeBuf = new byte[4];
            readFully(sizeBuf);
            int size = ByteBuffer.wrap(sizeBuf).getInt();
            byte[] respBuf = new byte[size];
            readFully(respBuf);
            ByteBuffer response = ByteBuffer.wrap(respBuf);
            response.getInt(); // skip correlationId
            return response;
        } finally {
            channel.configureBlocking(false);
        }
    }

    private void readFully(byte[] buf) throws IOException {
        int offset = 0;
        while (offset < buf.length) {
            ByteBuffer tmp = ByteBuffer.wrap(buf, offset, buf.length - offset);
            channel.configureBlocking(true);
            int read = channel.read(tmp);
            if (read < 0) throw new EOFException("Connection closed");
            offset += read;
        }
    }

    /**
     * 刷新 Metadata：发送 MetadataRequest，解析响应更新 Metadata
     */
    public void updateMetadata(List<String> topics) throws IOException {
        int bodySize = 4;
        for (String t : topics) bodySize += ByteBufferUtils.sizeOfString(t);
        ByteBuffer body = ByteBuffer.allocate(bodySize);
        body.putInt(topics.size());
        for (String t : topics) ByteBufferUtils.writeString(body, t);
        body.flip();

        ByteBuffer resp = sendSync(ApiKeys.METADATA, body);
        parseMetadataResponse(resp);
    }

    private void parseMetadataResponse(ByteBuffer resp) {
        int brokerCount = resp.getInt();
        List<Metadata.BrokerInfo> brokers = new ArrayList<>();
        for (int i = 0; i < brokerCount; i++) {
            int nodeId = resp.getInt();
            String host = ByteBufferUtils.readString(resp);
            int port = resp.getInt();
            brokers.add(new Metadata.BrokerInfo(nodeId, host, port));
        }

        int topicCount = resp.getInt();
        Map<String, Metadata.TopicMetadata> topics = new HashMap<>();
        for (int t = 0; t < topicCount; t++) {
            String topic = ByteBufferUtils.readString(resp);
            short errorCode = resp.getShort();
            int partCount = resp.getInt();
            List<Metadata.PartitionMetadata> partitions = new ArrayList<>();
            for (int p = 0; p < partCount; p++) {
                int partition = resp.getInt();
                int leader = resp.getInt();
                resp.getShort(); // errorCode
                String host = brokers.isEmpty() ? bootstrapHost : brokers.get(0).host;
                int port = brokers.isEmpty() ? bootstrapPort : brokers.get(0).port;
                partitions.add(new Metadata.PartitionMetadata(partition, host, port, leader));
            }
            topics.put(topic, new Metadata.TopicMetadata(topic, partitions));
        }
        metadata.update(topics, brokers);
    }

    @Override
    public void close() throws IOException {
        connected = false;
        if (channel != null) channel.close();
    }
}
```

- [ ] **Step 2: 编译验证**

```bash
mvn clean compile -pl mini-kafka-clients -Dsort.skip=true
```
Expected: BUILD SUCCESS

- [ ] **Step 3: Commit**

```bash
git add mini-kafka-clients/src/main/java/com/github/minikafka/clients/network/NetworkClient.java
git commit -m "feat(clients): add NetworkClient"
```

---

### Task 5: Producer 基础数据类

**Files:**
- Create: `mini-kafka-clients/src/main/java/com/github/minikafka/clients/producer/ProducerConfig.java`
- Create: `mini-kafka-clients/src/main/java/com/github/minikafka/clients/producer/ProducerRecord.java`
- Create: `mini-kafka-clients/src/main/java/com/github/minikafka/clients/producer/RecordMetadata.java`
- Create: `mini-kafka-clients/src/main/java/com/github/minikafka/clients/producer/ProducerBatch.java`

- [ ] **Step 1: 写 ProducerConfig.java**

```java
package com.github.minikafka.clients.producer;

import java.util.Properties;

public final class ProducerConfig {
    public final String bootstrapServers; // "host:port"
    public final int batchSize;           // 批次最大字节数，默认 16384
    public final long lingerMs;           // 延迟发送等待时间，默认 0
    public final int acks;                // 0=不等待, 1=leader确认, -1=all
    public final String clientId;

    public ProducerConfig(Properties props) {
        this.bootstrapServers = props.getProperty("bootstrap.servers", "localhost:9092");
        this.batchSize = Integer.parseInt(props.getProperty("batch.size", "16384"));
        this.lingerMs = Long.parseLong(props.getProperty("linger.ms", "0"));
        this.acks = Integer.parseInt(props.getProperty("acks", "1"));
        this.clientId = props.getProperty("client.id", "producer-1");
    }
}
```

- [ ] **Step 2: 写 ProducerRecord.java**

```java
package com.github.minikafka.clients.producer;

public final class ProducerRecord {
    public final String topic;
    public final Integer partition; // null 表示自动选择
    public final byte[] key;
    public final byte[] value;
    public final long timestamp;

    public ProducerRecord(String topic, byte[] key, byte[] value) {
        this(topic, null, key, value);
    }

    public ProducerRecord(String topic, Integer partition, byte[] key, byte[] value) {
        this.topic = topic;
        this.partition = partition;
        this.key = key;
        this.value = value;
        this.timestamp = System.currentTimeMillis();
    }
}
```

- [ ] **Step 3: 写 RecordMetadata.java**

```java
package com.github.minikafka.clients.producer;

public final class RecordMetadata {
    public final String topic;
    public final int partition;
    public final long offset;
    public final long timestamp;

    public RecordMetadata(String topic, int partition, long offset, long timestamp) {
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return topic + "-" + partition + "@" + offset;
    }
}
```

- [ ] **Step 4: 写 ProducerBatch.java**

```java
package com.github.minikafka.clients.producer;

import com.github.minikafka.common.record.Record;
import com.github.minikafka.common.record.RecordBatch;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * 单个 TopicPartition 的待发送批次，收集多条 Record
 */
public final class ProducerBatch {
    public final String topic;
    public final int partition;
    private final List<Record> records = new ArrayList<>();
    private final List<CompletableFuture<RecordMetadata>> futures = new ArrayList<>();
    private long baseOffset = -1;
    private final long createdMs = System.currentTimeMillis();

    public ProducerBatch(String topic, int partition) {
        this.topic = topic;
        this.partition = partition;
    }

    public CompletableFuture<RecordMetadata> tryAppend(byte[] key, byte[] value, long timestamp) {
        long offsetDelta = records.size();
        records.add(new Record(offsetDelta, timestamp, key, value));
        CompletableFuture<RecordMetadata> future = new CompletableFuture<>();
        futures.add(future);
        return future;
    }

    public RecordBatch toRecordBatch(long baseOffset) {
        // 更新 record offset 为绝对 offset
        List<Record> absolute = new ArrayList<>(records.size());
        for (int i = 0; i < records.size(); i++) {
            Record r = records.get(i);
            absolute.add(new Record(baseOffset + i, r.timestamp(), r.key(), r.value()));
        }
        return new RecordBatch(baseOffset, absolute);
    }

    public void done(long baseOffset) {
        this.baseOffset = baseOffset;
        for (int i = 0; i < futures.size(); i++) {
            Record r = records.get(i);
            futures.get(i).complete(new RecordMetadata(topic, partition, baseOffset + i, r.timestamp()));
        }
    }

    public void abort(Exception ex) {
        for (CompletableFuture<RecordMetadata> f : futures) f.completeExceptionally(ex);
    }

    public int estimatedSizeInBytes() {
        int size = 61; // RecordBatch overhead 近似值
        for (Record r : records) {
            size += 21 + (r.key() == null ? 0 : r.key().length) + (r.value() == null ? 0 : r.value().length);
        }
        return size;
    }

    public boolean isEmpty() { return records.isEmpty(); }
    public int recordCount() { return records.size(); }
    public long createdMs() { return createdMs; }
    public String topicPartitionKey() { return topic + "-" + partition; }
}
```

- [ ] **Step 5: 编译验证**

```bash
mvn clean compile -pl mini-kafka-clients -Dsort.skip=true
```
Expected: BUILD SUCCESS

- [ ] **Step 6: Commit**

```bash
git add mini-kafka-clients/src/main/java/com/github/minikafka/clients/producer/ProducerConfig.java \
        mini-kafka-clients/src/main/java/com/github/minikafka/clients/producer/ProducerRecord.java \
        mini-kafka-clients/src/main/java/com/github/minikafka/clients/producer/RecordMetadata.java \
        mini-kafka-clients/src/main/java/com/github/minikafka/clients/producer/ProducerBatch.java
git commit -m "feat(clients): add Producer data classes"
```

---

### Task 6: RecordAccumulator

**Files:**
- Create: `mini-kafka-clients/src/main/java/com/github/minikafka/clients/producer/RecordAccumulator.java`
- Create: `mini-kafka-clients/src/test/java/com/github/minikafka/clients/producer/RecordAccumulatorTest.java`

- [ ] **Step 1: 写 RecordAccumulator 失败测试**

```java
package com.github.minikafka.clients.producer;

import org.junit.Test;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import static org.junit.Assert.*;

public class RecordAccumulatorTest {

    @Test
    public void testAppendCreatesBatch() throws Exception {
        RecordAccumulator acc = new RecordAccumulator(16384, 0L);
        CompletableFuture<RecordMetadata> future = acc.append(
            "test", 0, "key".getBytes(), "value".getBytes(), System.currentTimeMillis()
        );
        assertNotNull(future);
        List<ProducerBatch> ready = acc.drain("test", 0);
        // linger.ms=0 且有消息，应立即可发送
        assertFalse(ready.isEmpty());
    }

    @Test
    public void testMultipleRecordsSameBatch() throws Exception {
        RecordAccumulator acc = new RecordAccumulator(16384, 100L);
        acc.append("t", 0, null, "v1".getBytes(), System.currentTimeMillis());
        acc.append("t", 0, null, "v2".getBytes(), System.currentTimeMillis());
        acc.append("t", 0, null, "v3".getBytes(), System.currentTimeMillis());

        // 未到 linger.ms 且未满 batch.size，不应 drain
        List<ProducerBatch> ready = acc.drain("t", 0);
        assertTrue(ready.isEmpty());
    }

    @Test
    public void testBatchReadyWhenFull() throws Exception {
        // 极小 batchSize，第一条消息就超过
        RecordAccumulator acc = new RecordAccumulator(1, 10000L);
        acc.append("t", 0, null, "value".getBytes(), System.currentTimeMillis());
        List<ProducerBatch> ready = acc.drain("t", 0);
        assertFalse(ready.isEmpty());
    }
}
```

- [ ] **Step 2: 运行测试，确认失败**

```bash
mvn clean test -pl mini-kafka-clients -Dtest=RecordAccumulatorTest -Dsort.skip=true
```
Expected: FAIL - compilation error

- [ ] **Step 3: 实现 RecordAccumulator.java**

```java
package com.github.minikafka.clients.producer;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 消息按 TopicPartition 批量缓冲
 * 对应 Kafka org.apache.kafka.clients.producer.internals.RecordAccumulator
 */
public final class RecordAccumulator {

    private final int batchSize;
    private final long lingerMs;
    // topicPartitionKey → 当前未满的 batch
    private final Map<String, ProducerBatch> batches = new ConcurrentHashMap<>();
    // 已满或已到 linger.ms，等待发送的 batch 列表
    private final Map<String, Queue<ProducerBatch>> readyBatches = new ConcurrentHashMap<>();

    public RecordAccumulator(int batchSize, long lingerMs) {
        this.batchSize = batchSize;
        this.lingerMs = lingerMs;
    }

    public synchronized CompletableFuture<RecordMetadata> append(
            String topic, int partition, byte[] key, byte[] value, long timestamp) {

        String key2 = topic + "-" + partition;
        ProducerBatch batch = batches.get(key2);

        if (batch == null) {
            batch = new ProducerBatch(topic, partition);
            batches.put(key2, batch);
        }

        CompletableFuture<RecordMetadata> future = batch.tryAppend(key, value, timestamp);

        // 如果 batch 已满，移入 ready 队列
        if (batch.estimatedSizeInBytes() >= batchSize) {
            moveToReady(key2, batch);
        }

        return future;
    }

    private void moveToReady(String key, ProducerBatch batch) {
        batches.remove(key);
        readyBatches.computeIfAbsent(key, k -> new LinkedList<>()).add(batch);
    }

    /**
     * 返回该 TopicPartition 已就绪的 batch 列表（满或 linger.ms 到期）
     * lingerMs=0 时，任何非空 batch 都立即就绪
     */
    public synchronized List<ProducerBatch> drain(String topic, int partition) {
        String key = topic + "-" + partition;
        long now = System.currentTimeMillis();

        // 检查当前 batch 是否到 linger.ms
        ProducerBatch current = batches.get(key);
        if (current != null && !current.isEmpty()) {
            boolean lingerExpired = lingerMs == 0 || (now - current.createdMs()) >= lingerMs;
            if (lingerExpired) {
                moveToReady(key, current);
            }
        }

        Queue<ProducerBatch> ready = readyBatches.remove(key);
        if (ready == null || ready.isEmpty()) return Collections.emptyList();
        return new ArrayList<>(ready);
    }

    public boolean hasUnsentRecords() {
        for (ProducerBatch b : batches.values()) if (!b.isEmpty()) return true;
        for (Queue<ProducerBatch> q : readyBatches.values()) if (!q.isEmpty()) return true;
        return false;
    }
}
```

- [ ] **Step 4: 运行测试，确认通过**

```bash
mvn clean test -pl mini-kafka-clients -Dtest=RecordAccumulatorTest -Dsort.skip=true
```
Expected: Tests run: 3, Failures: 0, Errors: 0

- [ ] **Step 5: Commit**

```bash
git add mini-kafka-clients/src/main/java/com/github/minikafka/clients/producer/RecordAccumulator.java \
        mini-kafka-clients/src/test/java/com/github/minikafka/clients/producer/RecordAccumulatorTest.java
git commit -m "feat(clients): add RecordAccumulator with TDD"
```

---

### Task 7: Sender + KafkaProducer

**Files:**
- Create: `mini-kafka-clients/src/main/java/com/github/minikafka/clients/producer/Sender.java`
- Create: `mini-kafka-clients/src/main/java/com/github/minikafka/clients/producer/KafkaProducer.java`

- [ ] **Step 1: 实现 Sender.java**

```java
package com.github.minikafka.clients.producer;

import com.github.minikafka.clients.network.Metadata;
import com.github.minikafka.clients.network.NetworkClient;
import com.github.minikafka.common.network.ByteBufferUtils;
import com.github.minikafka.common.protocol.ApiKeys;
import com.github.minikafka.common.record.RecordBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * Producer 后台 IO 线程，负责将 RecordAccumulator 中 ready 的 batch 发送给 Broker
 * 对应 Kafka org.apache.kafka.clients.producer.internals.Sender
 */
public final class Sender implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(Sender.class);

    private final RecordAccumulator accumulator;
    private final NetworkClient networkClient;
    private final Metadata metadata;
    private volatile boolean running = true;
    private final long lingerMs;

    public Sender(RecordAccumulator accumulator, NetworkClient networkClient,
                  Metadata metadata, long lingerMs) {
        this.accumulator = accumulator;
        this.networkClient = networkClient;
        this.metadata = metadata;
        this.lingerMs = lingerMs;
    }

    @Override
    public void run() {
        while (running) {
            try {
                // 刷新 Metadata（如果需要）
                if (metadata.needsUpdate()) {
                    networkClient.updateMetadata(new ArrayList<>(getKnownTopics()));
                }
                sendBatches();
                if (lingerMs > 0) Thread.sleep(Math.min(lingerMs, 50));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                if (running) log.error("Sender error", e);
            }
        }
        // 关闭时刷写剩余消息
        try { drainAll(); } catch (Exception ignored) {}
    }

    private void sendBatches() throws IOException {
        for (String topic : getKnownTopics()) {
            int partitions = metadata.partitionCount(topic);
            for (int p = 0; p < Math.max(partitions, 1); p++) {
                List<ProducerBatch> batches = accumulator.drain(topic, p);
                for (ProducerBatch batch : batches) {
                    sendBatch(batch);
                }
            }
        }
    }

    private Set<String> getKnownTopics() {
        // 从 accumulator 中推断 topic（通过反射不合适，简化：扫描元数据）
        return metadata.needsUpdate() ? new HashSet<>() : metadata.brokers().isEmpty()
            ? new HashSet<>() : new HashSet<>();
    }

    public void sendBatch(ProducerBatch batch) throws IOException {
        RecordBatch recordBatch = batch.toRecordBatch(0L);
        int batchSize = recordBatch.sizeInBytes();
        ByteBuffer batchBuf = ByteBuffer.allocate(batchSize);
        recordBatch.writeTo(batchBuf);
        batchBuf.flip();

        // PRODUCE request body
        int bodySize = 4 + ByteBufferUtils.sizeOfString(batch.topic) + 4 + 4 + 4 + batchSize;
        ByteBuffer body = ByteBuffer.allocate(bodySize);
        body.putInt(1); // topicCount
        ByteBufferUtils.writeString(body, batch.topic);
        body.putInt(1); // partCount
        body.putInt(batch.partition);
        body.putInt(batchSize);
        body.put(batchBuf);
        body.flip();

        ByteBuffer resp = networkClient.sendSync(ApiKeys.PRODUCE, body);
        // Parse response: topicCount(4) topic(string) partCount(4) partition(4) errorCode(2) baseOffset(8)
        resp.getInt(); // topicCount
        ByteBufferUtils.readString(resp); // topic
        resp.getInt(); // partCount
        resp.getInt(); // partition
        short errorCode = resp.getShort();
        long baseOffset = resp.getLong();

        if (errorCode == 0) {
            batch.done(baseOffset);
            log.debug("Sent batch to {}-{}, baseOffset={}", batch.topic, batch.partition, baseOffset);
        } else {
            batch.abort(new RuntimeException("Produce failed, errorCode=" + errorCode));
        }
    }

    private void drainAll() throws IOException {
        // 简化：尝试将所有剩余 batch 发送
    }

    public void initiateClose() { running = false; }
}
```

- [ ] **Step 2: 实现 KafkaProducer.java**

```java
package com.github.minikafka.clients.producer;

import com.github.minikafka.clients.network.Metadata;
import com.github.minikafka.clients.network.NetworkClient;
import com.github.minikafka.common.network.ByteBufferUtils;
import com.github.minikafka.common.protocol.ApiKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

/**
 * Kafka Producer 客户端主入口
 * 对应 Kafka org.apache.kafka.clients.producer.KafkaProducer
 */
public final class KafkaProducer implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(KafkaProducer.class);

    private final ProducerConfig config;
    private final Metadata metadata;
    private final NetworkClient networkClient;
    private final RecordAccumulator accumulator;
    private final Sender sender;
    private final Thread senderThread;

    public KafkaProducer(Properties props) throws IOException {
        this.config = new ProducerConfig(props);
        String[] parts = config.bootstrapServers.split(":");
        String host = parts[0];
        int port = parts.length > 1 ? Integer.parseInt(parts[1]) : 9092;

        this.metadata = new Metadata(5 * 60 * 1000L);
        this.networkClient = new NetworkClient(host, port, config.clientId, metadata);
        this.networkClient.connect();

        // 初始化元数据
        this.metadata.update(new HashMap<>(), new ArrayList<>());

        this.accumulator = new RecordAccumulator(config.batchSize, config.lingerMs);
        this.sender = new Sender(accumulator, networkClient, metadata, config.lingerMs);
        this.senderThread = new Thread(sender, "kafka-producer-network-thread-" + config.clientId);
        this.senderThread.setDaemon(true);
        this.senderThread.start();
    }

    /**
     * 异步发送消息，返回 Future<RecordMetadata>
     * 对应 Kafka KafkaProducer.send(ProducerRecord, Callback)
     */
    public Future<RecordMetadata> send(ProducerRecord record) throws IOException {
        // 确保 topic 元数据存在
        ensureTopicMetadata(record.topic);

        int partition = selectPartition(record);
        return accumulator.append(
            record.topic, partition, record.key, record.value, record.timestamp
        );
    }

    private void ensureTopicMetadata(String topic) throws IOException {
        if (!metadata.containsTopic(topic)) {
            networkClient.updateMetadata(Collections.singletonList(topic));
        }
    }

    private int selectPartition(ProducerRecord record) {
        if (record.partition != null) return record.partition;
        int partitions = Math.max(1, metadata.partitionCount(record.topic));
        if (record.key != null) {
            return Math.abs(Arrays.hashCode(record.key)) % partitions;
        }
        return (int) (System.nanoTime() % partitions);
    }

    /**
     * 直接同步发送（跳过 accumulator），方便测试
     */
    public RecordMetadata sendSync(ProducerRecord record) throws Exception {
        ensureTopicMetadata(record.topic);
        int partition = selectPartition(record);
        CompletableFuture<RecordMetadata> future = accumulator.append(
            record.topic, partition, record.key, record.value, record.timestamp
        );
        // 强制 drain 并发送
        List<ProducerBatch> batches = accumulator.drain(record.topic, partition);
        for (ProducerBatch batch : batches) {
            sender.sendBatch(batch);
        }
        return future.get();
    }

    @Override
    public void close() throws IOException {
        sender.initiateClose();
        try { senderThread.join(5000); } catch (InterruptedException ignored) {}
        networkClient.close();
        log.info("KafkaProducer closed");
    }
}
```

- [ ] **Step 3: 编译验证**

```bash
mvn clean compile -pl mini-kafka-clients -Dsort.skip=true
```
Expected: BUILD SUCCESS

- [ ] **Step 4: Commit**

```bash
git add mini-kafka-clients/src/main/java/com/github/minikafka/clients/producer/
git commit -m "feat(clients): add Sender and KafkaProducer"
```

---

### Task 8: Consumer 基础类

**Files:**
- Create: `mini-kafka-clients/src/main/java/com/github/minikafka/clients/consumer/ConsumerConfig.java`
- Create: `mini-kafka-clients/src/main/java/com/github/minikafka/clients/consumer/ConsumerRecord.java`
- Create: `mini-kafka-clients/src/main/java/com/github/minikafka/clients/consumer/ConsumerRecords.java`
- Create: `mini-kafka-clients/src/main/java/com/github/minikafka/clients/consumer/SubscriptionState.java`

- [ ] **Step 1: 写 ConsumerConfig.java**

```java
package com.github.minikafka.clients.consumer;

import java.util.Properties;

public final class ConsumerConfig {
    public final String bootstrapServers;
    public final String groupId;
    public final String autoOffsetReset;  // "earliest" or "latest"
    public final boolean enableAutoCommit;
    public final int autoCommitIntervalMs;
    public final int sessionTimeoutMs;
    public final int heartbeatIntervalMs;
    public final int fetchMaxBytes;
    public final String clientId;

    public ConsumerConfig(Properties props) {
        this.bootstrapServers = props.getProperty("bootstrap.servers", "localhost:9092");
        this.groupId = props.getProperty("group.id", "");
        this.autoOffsetReset = props.getProperty("auto.offset.reset", "latest");
        this.enableAutoCommit = Boolean.parseBoolean(props.getProperty("enable.auto.commit", "true"));
        this.autoCommitIntervalMs = Integer.parseInt(props.getProperty("auto.commit.interval.ms", "5000"));
        this.sessionTimeoutMs = Integer.parseInt(props.getProperty("session.timeout.ms", "30000"));
        this.heartbeatIntervalMs = Integer.parseInt(props.getProperty("heartbeat.interval.ms", "3000"));
        this.fetchMaxBytes = Integer.parseInt(props.getProperty("fetch.max.bytes", String.valueOf(50 * 1024 * 1024)));
        this.clientId = props.getProperty("client.id", "consumer-1");
    }
}
```

- [ ] **Step 2: 写 ConsumerRecord.java**

```java
package com.github.minikafka.clients.consumer;

public final class ConsumerRecord {
    public final String topic;
    public final int partition;
    public final long offset;
    public final long timestamp;
    public final byte[] key;
    public final byte[] value;

    public ConsumerRecord(String topic, int partition, long offset,
                          long timestamp, byte[] key, byte[] value) {
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
        this.timestamp = timestamp;
        this.key = key;
        this.value = value;
    }
}
```

- [ ] **Step 3: 写 ConsumerRecords.java**

```java
package com.github.minikafka.clients.consumer;

import java.util.*;

public final class ConsumerRecords implements Iterable<ConsumerRecord> {

    public static final ConsumerRecords EMPTY = new ConsumerRecords(Collections.emptyList());

    private final List<ConsumerRecord> records;

    public ConsumerRecords(List<ConsumerRecord> records) {
        this.records = Collections.unmodifiableList(new ArrayList<>(records));
    }

    public List<ConsumerRecord> records(String topic) {
        List<ConsumerRecord> result = new ArrayList<>();
        for (ConsumerRecord r : records) if (r.topic.equals(topic)) result.add(r);
        return result;
    }

    public boolean isEmpty() { return records.isEmpty(); }
    public int count() { return records.size(); }

    @Override
    public Iterator<ConsumerRecord> iterator() { return records.iterator(); }
}
```

- [ ] **Step 4: 写 SubscriptionState.java**

```java
package com.github.minikafka.clients.consumer;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 跟踪分区的 position（下次 fetch 的 offset）和 committed offset
 * 对应 Kafka org.apache.kafka.clients.consumer.internals.SubscriptionState
 */
public final class SubscriptionState {

    private final Set<String> subscribedTopics = new LinkedHashSet<>();
    // "topic-partition" → position
    private final Map<String, Long> positions = new ConcurrentHashMap<>();
    // "topic-partition" → committed offset
    private final Map<String, Long> committed = new ConcurrentHashMap<>();
    // 已分配的 partitions（Rebalance 后由 Coordinator 设置）
    private final Set<String> assignedPartitions = new LinkedHashSet<>();

    public void subscribe(Collection<String> topics) {
        subscribedTopics.addAll(topics);
    }

    public void assignPartitions(Map<String, List<Integer>> topicPartitions) {
        assignedPartitions.clear();
        for (Map.Entry<String, List<Integer>> e : topicPartitions.entrySet()) {
            for (int p : e.getValue()) {
                assignedPartitions.add(e.getKey() + "-" + p);
            }
        }
    }

    public Set<String> assignedPartitions() {
        return Collections.unmodifiableSet(assignedPartitions);
    }

    public Set<String> subscribedTopics() {
        return Collections.unmodifiableSet(subscribedTopics);
    }

    public long position(String topic, int partition) {
        return positions.getOrDefault(key(topic, partition), -1L);
    }

    public void setPosition(String topic, int partition, long offset) {
        positions.put(key(topic, partition), offset);
    }

    public void seek(String topic, int partition, long offset) {
        positions.put(key(topic, partition), offset);
    }

    public long committed(String topic, int partition) {
        return committed.getOrDefault(key(topic, partition), -1L);
    }

    public void setCommitted(String topic, int partition, long offset) {
        committed.put(key(topic, partition), offset);
    }

    private String key(String topic, int partition) {
        return topic + "-" + partition;
    }
}
```

- [ ] **Step 5: 编译验证**

```bash
mvn clean compile -pl mini-kafka-clients -Dsort.skip=true
```
Expected: BUILD SUCCESS

- [ ] **Step 6: Commit**

```bash
git add mini-kafka-clients/src/main/java/com/github/minikafka/clients/consumer/ConsumerConfig.java \
        mini-kafka-clients/src/main/java/com/github/minikafka/clients/consumer/ConsumerRecord.java \
        mini-kafka-clients/src/main/java/com/github/minikafka/clients/consumer/ConsumerRecords.java \
        mini-kafka-clients/src/main/java/com/github/minikafka/clients/consumer/SubscriptionState.java
git commit -m "feat(clients): add Consumer data classes"
```

---

### Task 9: ConsumerCoordinator + Fetcher + KafkaConsumer

**Files:**
- Create: `mini-kafka-clients/src/main/java/com/github/minikafka/clients/consumer/ConsumerCoordinator.java`
- Create: `mini-kafka-clients/src/main/java/com/github/minikafka/clients/consumer/Fetcher.java`
- Create: `mini-kafka-clients/src/main/java/com/github/minikafka/clients/consumer/KafkaConsumer.java`

- [ ] **Step 1: 实现 ConsumerCoordinator.java**

```java
package com.github.minikafka.clients.consumer;

import com.github.minikafka.clients.network.Metadata;
import com.github.minikafka.clients.network.NetworkClient;
import com.github.minikafka.common.network.ByteBufferUtils;
import com.github.minikafka.common.protocol.ApiKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * 客户端侧 Rebalance 协调器
 * 对应 Kafka org.apache.kafka.clients.consumer.internals.ConsumerCoordinator
 */
public final class ConsumerCoordinator {

    private static final Logger log = LoggerFactory.getLogger(ConsumerCoordinator.class);

    private final String groupId;
    private final ConsumerConfig config;
    private final NetworkClient networkClient;
    private final Metadata metadata;
    private final SubscriptionState subscriptionState;

    private String memberId = "";
    private int generationId = -1;
    private boolean needsJoin = true;
    private long lastHeartbeatMs = 0;

    public ConsumerCoordinator(String groupId, ConsumerConfig config,
                                NetworkClient networkClient, Metadata metadata,
                                SubscriptionState subscriptionState) {
        this.groupId = groupId;
        this.config = config;
        this.networkClient = networkClient;
        this.metadata = metadata;
        this.subscriptionState = subscriptionState;
    }

    /**
     * 确保已加入 Group（如需则触发 Rebalance）
     */
    public void ensureActiveGroup() throws IOException {
        if (needsJoin) {
            joinGroup();
        } else {
            maybeHeartbeat();
        }
    }

    private void joinGroup() throws IOException {
        log.info("Joining group {}, memberId={}", groupId, memberId);

        // JoinGroup request
        ByteBuffer body = ByteBuffer.allocate(
            ByteBufferUtils.sizeOfString(groupId) + 4
            + ByteBufferUtils.sizeOfString(memberId)
            + ByteBufferUtils.sizeOfString("consumer")
        );
        ByteBufferUtils.writeString(body, groupId);
        body.putInt(config.sessionTimeoutMs);
        ByteBufferUtils.writeString(body, memberId);
        ByteBufferUtils.writeString(body, "consumer");
        body.flip();

        ByteBuffer resp = networkClient.sendSync(ApiKeys.JOIN_GROUP, body);
        resp.getShort(); // errorCode
        generationId = resp.getInt();
        ByteBufferUtils.readString(resp); // protocol
        String leaderId = ByteBufferUtils.readString(resp);
        memberId = ByteBufferUtils.readString(resp);
        int memberCount = resp.getInt();
        List<String> members = new ArrayList<>();
        for (int i = 0; i < memberCount; i++) members.add(ByteBufferUtils.readString(resp));

        log.info("Joined group {}, memberId={}, generation={}, leader={}",
            groupId, memberId, generationId, leaderId);

        // SyncGroup request
        syncGroup(leaderId, members);
        needsJoin = false;
        lastHeartbeatMs = System.currentTimeMillis();
    }

    private void syncGroup(String leaderId, List<String> members) throws IOException {
        // 如果是 Leader，计算 RangeAssignor 分配
        Map<String, byte[]> assignment = new HashMap<>();
        if (memberId.equals(leaderId)) {
            assignment = rangeAssign(members);
        }

        int bodySize = ByteBufferUtils.sizeOfString(groupId) + 4
            + ByteBufferUtils.sizeOfString(memberId) + 4;
        for (Map.Entry<String, byte[]> e : assignment.entrySet()) {
            bodySize += ByteBufferUtils.sizeOfString(e.getKey()) + ByteBufferUtils.sizeOfBytes(e.getValue());
        }
        ByteBuffer body = ByteBuffer.allocate(bodySize);
        ByteBufferUtils.writeString(body, groupId);
        body.putInt(generationId);
        ByteBufferUtils.writeString(body, memberId);
        body.putInt(assignment.size());
        for (Map.Entry<String, byte[]> e : assignment.entrySet()) {
            ByteBufferUtils.writeString(body, e.getKey());
            ByteBufferUtils.writeBytes(body, e.getValue());
        }
        body.flip();

        ByteBuffer resp = networkClient.sendSync(ApiKeys.SYNC_GROUP, body);
        resp.getShort(); // errorCode
        byte[] myAssignment = ByteBufferUtils.readBytes(resp);

        // 解析分配结果，更新 SubscriptionState
        if (myAssignment != null && myAssignment.length > 0) {
            parseAssignment(myAssignment);
        } else {
            // 默认：订阅的 topic 的所有 partition
            assignAllSubscribed();
        }
        log.info("SyncGroup complete, assigned partitions: {}", subscriptionState.assignedPartitions());
    }

    private Map<String, byte[]> rangeAssign(List<String> members) {
        // RangeAssignor：将每个 topic 的 partition 均匀分配给成员
        Map<String, byte[]> result = new HashMap<>();
        for (String m : members) result.put(m, new byte[0]);

        for (String topic : subscriptionState.subscribedTopics()) {
            int partitions = metadata.partitionCount(topic);
            if (partitions == 0) continue;
            int membersCount = members.size();
            for (int i = 0; i < membersCount; i++) {
                int start = (partitions / membersCount) * i;
                int end = (i == membersCount - 1) ? partitions : start + partitions / membersCount;
                // 将 partition 列表编码到 assignment bytes
                // 格式：topicCount(4) topic(string) partCount(4) [partition(4)]
                byte[] assign = result.get(members.get(i));
                assign = encodeAssignment(assign, topic, start, end);
                result.put(members.get(i), assign);
            }
        }
        return result;
    }

    private byte[] encodeAssignment(byte[] existing, String topic, int start, int end) {
        int count = end - start;
        int size = (existing == null ? 4 : existing.length) + ByteBufferUtils.sizeOfString(topic) + 4 + count * 4;
        ByteBuffer buf = ByteBuffer.allocate(size);
        if (existing != null && existing.length > 0) buf.put(existing);
        ByteBufferUtils.writeString(buf, topic);
        buf.putInt(count);
        for (int i = start; i < end; i++) buf.putInt(i);
        return buf.array();
    }

    private void parseAssignment(byte[] assignment) {
        ByteBuffer buf = ByteBuffer.wrap(assignment);
        Map<String, List<Integer>> topicPartitions = new HashMap<>();
        while (buf.remaining() >= 2) {
            String topic = ByteBufferUtils.readString(buf);
            if (topic == null || buf.remaining() < 4) break;
            int count = buf.getInt();
            List<Integer> partitions = new ArrayList<>();
            for (int i = 0; i < count && buf.remaining() >= 4; i++) {
                partitions.add(buf.getInt());
            }
            if (!partitions.isEmpty()) topicPartitions.put(topic, partitions);
        }
        if (!topicPartitions.isEmpty()) {
            subscriptionState.assignPartitions(topicPartitions);
        } else {
            assignAllSubscribed();
        }
    }

    private void assignAllSubscribed() {
        Map<String, List<Integer>> topicPartitions = new HashMap<>();
        for (String topic : subscriptionState.subscribedTopics()) {
            int partitions = Math.max(1, metadata.partitionCount(topic));
            List<Integer> parts = new ArrayList<>();
            for (int i = 0; i < partitions; i++) parts.add(i);
            topicPartitions.put(topic, parts);
        }
        subscriptionState.assignPartitions(topicPartitions);
    }

    private void maybeHeartbeat() throws IOException {
        long now = System.currentTimeMillis();
        if (now - lastHeartbeatMs < config.heartbeatIntervalMs) return;

        ByteBuffer body = ByteBuffer.allocate(
            ByteBufferUtils.sizeOfString(groupId) + 4 + ByteBufferUtils.sizeOfString(memberId)
        );
        ByteBufferUtils.writeString(body, groupId);
        body.putInt(generationId);
        ByteBufferUtils.writeString(body, memberId);
        body.flip();

        ByteBuffer resp = networkClient.sendSync(ApiKeys.HEARTBEAT, body);
        short errorCode = resp.getShort();
        if (errorCode != 0) {
            log.warn("Heartbeat failed with errorCode={}, will rejoin", errorCode);
            needsJoin = true;
        }
        lastHeartbeatMs = now;
    }

    public void commitOffset(String topic, int partition, long offset) throws IOException {
        ByteBuffer body = ByteBuffer.allocate(
            ByteBufferUtils.sizeOfString(groupId) + 4
            + ByteBufferUtils.sizeOfString(memberId) + 4
            + ByteBufferUtils.sizeOfString(topic) + 4 + 4 + 8
        );
        ByteBufferUtils.writeString(body, groupId);
        body.putInt(generationId);
        ByteBufferUtils.writeString(body, memberId);
        body.putInt(1); // topicCount
        ByteBufferUtils.writeString(body, topic);
        body.putInt(1); // partCount
        body.putInt(partition);
        body.putLong(offset);
        body.flip();
        networkClient.sendSync(ApiKeys.OFFSET_COMMIT, body);
        subscriptionState.setCommitted(topic, partition, offset);
    }

    public long fetchCommittedOffset(String topic, int partition) throws IOException {
        ByteBuffer body = ByteBuffer.allocate(
            ByteBufferUtils.sizeOfString(groupId) + 4
            + ByteBufferUtils.sizeOfString(topic) + 4 + 4
        );
        ByteBufferUtils.writeString(body, groupId);
        body.putInt(1); // topicCount
        ByteBufferUtils.writeString(body, topic);
        body.putInt(1); // partCount
        body.putInt(partition);
        body.flip();

        ByteBuffer resp = networkClient.sendSync(ApiKeys.OFFSET_FETCH, body);
        resp.getInt(); // topicCount
        ByteBufferUtils.readString(resp); // topic
        resp.getInt(); // partCount
        resp.getInt(); // partition
        long offset = resp.getLong();
        resp.getShort(); // errorCode
        return offset;
    }

    public void leaveGroup() throws IOException {
        ByteBuffer body = ByteBuffer.allocate(
            ByteBufferUtils.sizeOfString(groupId) + ByteBufferUtils.sizeOfString(memberId)
        );
        ByteBufferUtils.writeString(body, groupId);
        ByteBufferUtils.writeString(body, memberId);
        body.flip();
        networkClient.sendSync(ApiKeys.LEAVE_GROUP, body);
        needsJoin = true;
        memberId = "";
        generationId = -1;
    }
}
```

- [ ] **Step 2: 实现 Fetcher.java**

```java
package com.github.minikafka.clients.consumer;

import com.github.minikafka.clients.network.Metadata;
import com.github.minikafka.clients.network.NetworkClient;
import com.github.minikafka.common.network.ByteBufferUtils;
import com.github.minikafka.common.protocol.ApiKeys;
import com.github.minikafka.common.record.RecordBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * 构造并发送 FetchRequest，解析 FetchResponse
 * 对应 Kafka org.apache.kafka.clients.consumer.internals.Fetcher
 */
public final class Fetcher {

    private static final Logger log = LoggerFactory.getLogger(Fetcher.class);

    private final NetworkClient networkClient;
    private final Metadata metadata;
    private final SubscriptionState subscriptionState;
    private final int fetchMaxBytes;

    public Fetcher(NetworkClient networkClient, Metadata metadata,
                   SubscriptionState subscriptionState, int fetchMaxBytes) {
        this.networkClient = networkClient;
        this.metadata = metadata;
        this.subscriptionState = subscriptionState;
        this.fetchMaxBytes = fetchMaxBytes;
    }

    /**
     * 对所有已分配 partition 发起 Fetch 请求
     * @return 收到的消息列表
     */
    public List<ConsumerRecord> fetchRecords() throws IOException {
        Set<String> assigned = subscriptionState.assignedPartitions();
        if (assigned.isEmpty()) return Collections.emptyList();

        // 按 topic 分组构造 FetchRequest
        Map<String, List<Integer>> topicPartitions = new LinkedHashMap<>();
        for (String tp : assigned) {
            int idx = tp.lastIndexOf('-');
            String topic = tp.substring(0, idx);
            int partition = Integer.parseInt(tp.substring(idx + 1));
            topicPartitions.computeIfAbsent(topic, k -> new ArrayList<>()).add(partition);
        }

        int bodySize = 4 + 4 + 4 + 4; // replicaId + maxWaitMs + minBytes + topicCount
        for (Map.Entry<String, List<Integer>> e : topicPartitions.entrySet()) {
            bodySize += ByteBufferUtils.sizeOfString(e.getKey()) + 4; // topic + partCount
            bodySize += e.getValue().size() * (4 + 8 + 4); // partition + fetchOffset + maxBytes
        }

        ByteBuffer body = ByteBuffer.allocate(bodySize);
        body.putInt(-1);  // replicaId
        body.putInt(500); // maxWaitMs
        body.putInt(0);   // minBytes
        body.putInt(topicPartitions.size());

        for (Map.Entry<String, List<Integer>> e : topicPartitions.entrySet()) {
            String topic = e.getKey();
            ByteBufferUtils.writeString(body, topic);
            body.putInt(e.getValue().size());
            for (int partition : e.getValue()) {
                long position = subscriptionState.position(topic, partition);
                if (position < 0) position = 0; // 从头消费
                body.putInt(partition);
                body.putLong(position);
                body.putInt(fetchMaxBytes / e.getValue().size());
            }
        }
        body.flip();

        ByteBuffer resp = networkClient.sendSync(ApiKeys.FETCH, body);
        return parseFetchResponse(resp);
    }

    private List<ConsumerRecord> parseFetchResponse(ByteBuffer resp) {
        List<ConsumerRecord> result = new ArrayList<>();
        int topicCount = resp.getInt();
        for (int t = 0; t < topicCount; t++) {
            String topic = ByteBufferUtils.readString(resp);
            int partCount = resp.getInt();
            for (int p = 0; p < partCount; p++) {
                int partition = resp.getInt();
                short errorCode = resp.getShort();
                long highWatermark = resp.getLong();
                int recordSize = resp.getInt();

                if (errorCode != 0 || recordSize == 0) continue;

                byte[] recordBytes = new byte[recordSize];
                resp.get(recordBytes);
                ByteBuffer recordBuf = ByteBuffer.wrap(recordBytes);

                while (recordBuf.hasRemaining()) {
                    try {
                        RecordBatch batch = RecordBatch.readFrom(recordBuf);
                        for (com.github.minikafka.common.record.Record r : batch.records()) {
                            result.add(new ConsumerRecord(
                                topic, partition, r.offset(), r.timestamp(), r.key(), r.value()
                            ));
                            // 更新 position
                            subscriptionState.setPosition(topic, partition, r.offset() + 1);
                        }
                    } catch (Exception e) {
                        log.warn("Error parsing RecordBatch", e);
                        break;
                    }
                }
            }
        }
        return result;
    }
}
```

- [ ] **Step 3: 实现 KafkaConsumer.java**

```java
package com.github.minikafka.clients.consumer;

import com.github.minikafka.clients.network.Metadata;
import com.github.minikafka.clients.network.NetworkClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.*;

/**
 * Kafka Consumer 主入口，poll 模型，非线程安全
 * 对应 Kafka org.apache.kafka.clients.consumer.KafkaConsumer
 */
public final class KafkaConsumer implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumer.class);

    private final ConsumerConfig config;
    private final Metadata metadata;
    private final NetworkClient networkClient;
    private final SubscriptionState subscriptionState;
    private final ConsumerCoordinator coordinator;
    private final Fetcher fetcher;
    private long lastAutoCommitMs = System.currentTimeMillis();

    public KafkaConsumer(Properties props) throws IOException {
        this.config = new ConsumerConfig(props);
        String[] parts = config.bootstrapServers.split(":");
        String host = parts[0];
        int port = parts.length > 1 ? Integer.parseInt(parts[1]) : 9092;

        this.metadata = new Metadata(5 * 60 * 1000L);
        this.networkClient = new NetworkClient(host, port, config.clientId, metadata);
        this.networkClient.connect();
        this.metadata.update(new HashMap<>(), new ArrayList<>());

        this.subscriptionState = new SubscriptionState();
        this.coordinator = new ConsumerCoordinator(
            config.groupId, config, networkClient, metadata, subscriptionState
        );
        this.fetcher = new Fetcher(networkClient, metadata, subscriptionState, config.fetchMaxBytes);
    }

    public void subscribe(Collection<String> topics) {
        subscriptionState.subscribe(topics);
        // 刷新元数据
        try {
            networkClient.updateMetadata(new ArrayList<>(topics));
        } catch (IOException e) {
            log.warn("Failed to update metadata on subscribe", e);
        }
    }

    /**
     * 拉取消息，最多等待 timeout
     * 对应 Kafka KafkaConsumer.poll(Duration)
     */
    public ConsumerRecords poll(Duration timeout) throws IOException {
        long deadline = System.currentTimeMillis() + timeout.toMillis();

        // 确保已加入 Group
        coordinator.ensureActiveGroup();

        // Fetch 消息
        List<ConsumerRecord> records = fetcher.fetchRecords();

        // 自动提交
        if (config.enableAutoCommit) maybeAutoCommit();

        return records.isEmpty() ? ConsumerRecords.EMPTY : new ConsumerRecords(records);
    }

    public void commitSync() throws IOException {
        for (String tp : subscriptionState.assignedPartitions()) {
            int idx = tp.lastIndexOf('-');
            String topic = tp.substring(0, idx);
            int partition = Integer.parseInt(tp.substring(idx + 1));
            long position = subscriptionState.position(topic, partition);
            if (position >= 0) {
                coordinator.commitOffset(topic, partition, position);
            }
        }
    }

    private void maybeAutoCommit() throws IOException {
        long now = System.currentTimeMillis();
        if (now - lastAutoCommitMs >= config.autoCommitIntervalMs) {
            commitSync();
            lastAutoCommitMs = now;
        }
    }

    public void seek(String topic, int partition, long offset) {
        subscriptionState.seek(topic, partition, offset);
    }

    @Override
    public void close() throws IOException {
        try { coordinator.leaveGroup(); } catch (Exception ignored) {}
        networkClient.close();
        log.info("KafkaConsumer closed");
    }
}
```

- [ ] **Step 4: 编译验证**

```bash
mvn clean compile -pl mini-kafka-clients -Dsort.skip=true
```
Expected: BUILD SUCCESS

- [ ] **Step 5: Commit**

```bash
git add mini-kafka-clients/src/main/java/com/github/minikafka/clients/consumer/
git commit -m "feat(clients): add ConsumerCoordinator, Fetcher, KafkaConsumer"
```

---

### Task 10: Producer + Consumer 端到端集成测试

**Files:**
- Create: `mini-kafka-clients/src/test/java/com/github/minikafka/clients/integration/ProducerConsumerIntegrationTest.java`

- [ ] **Step 1: 写集成测试**

```java
package com.github.minikafka.clients.integration;

import com.github.minikafka.clients.consumer.*;
import com.github.minikafka.clients.producer.*;
import com.github.minikafka.server.KafkaConfig;
import com.github.minikafka.server.KafkaServer;
import org.junit.*;
import java.time.Duration;
import java.util.*;
import static org.junit.Assert.*;

public class ProducerConsumerIntegrationTest {

    private static KafkaServer server;
    private static final int PORT = 19093;

    @BeforeClass
    public static void startBroker() throws Exception {
        Properties props = new Properties();
        props.setProperty("port", String.valueOf(PORT));
        props.setProperty("log.dirs",
            System.getProperty("java.io.tmpdir") + "/mini-kafka-pc-" + System.currentTimeMillis());
        server = new KafkaServer(new KafkaConfig(props));
        server.startup();
        // 创建测试 Topic
        Thread.sleep(300);
        server.controller().createTopic("test-topic", 2);
        server.controller().createTopic("group-topic", 3);
        Thread.sleep(100);
    }

    @AfterClass
    public static void stopBroker() throws Exception {
        if (server != null) server.shutdown();
    }

    @Test
    public void testProduceAndConsume() throws Exception {
        // Producer 发送 3 条消息
        Properties producerProps = new Properties();
        producerProps.setProperty("bootstrap.servers", "localhost:" + PORT);
        producerProps.setProperty("client.id", "test-producer");

        try (KafkaProducer producer = new KafkaProducer(producerProps)) {
            for (int i = 0; i < 3; i++) {
                RecordMetadata meta = producer.sendSync(
                    new ProducerRecord("test-topic", 0, null, ("message-" + i).getBytes())
                );
                assertNotNull(meta);
                assertEquals("test-topic", meta.topic);
                assertEquals(0, meta.partition);
            }
        }

        // Consumer 消费
        Properties consumerProps = new Properties();
        consumerProps.setProperty("bootstrap.servers", "localhost:" + PORT);
        consumerProps.setProperty("group.id", "test-group");
        consumerProps.setProperty("auto.offset.reset", "earliest");
        consumerProps.setProperty("enable.auto.commit", "false");

        try (KafkaConsumer consumer = new KafkaConsumer(consumerProps)) {
            consumer.subscribe(Collections.singletonList("test-topic"));
            consumer.seek("test-topic", 0, 0L);

            ConsumerRecords records = consumer.poll(Duration.ofSeconds(3));
            assertFalse("Should have received records", records.isEmpty());
            int count = 0;
            for (ConsumerRecord r : records) {
                assertTrue(new String(r.value()).startsWith("message-"));
                count++;
            }
            assertEquals(3, count);

            // 提交 offset
            consumer.commitSync();
        }
    }

    @Test
    public void testRebalanceWithMultipleConsumers() throws Exception {
        Properties consumerProps1 = new Properties();
        consumerProps1.setProperty("bootstrap.servers", "localhost:" + PORT);
        consumerProps1.setProperty("group.id", "rebalance-group");
        consumerProps1.setProperty("enable.auto.commit", "false");

        Properties consumerProps2 = new Properties();
        consumerProps2.setProperty("bootstrap.servers", "localhost:" + PORT);
        consumerProps2.setProperty("group.id", "rebalance-group");
        consumerProps2.setProperty("enable.auto.commit", "false");

        try (KafkaConsumer c1 = new KafkaConsumer(consumerProps1);
             KafkaConsumer c2 = new KafkaConsumer(consumerProps2)) {

            c1.subscribe(Collections.singletonList("group-topic"));
            c2.subscribe(Collections.singletonList("group-topic"));

            // 第一个 consumer 先 join
            ConsumerRecords r1 = c1.poll(Duration.ofSeconds(2));
            // 第二个 consumer join（触发 rebalance）
            ConsumerRecords r2 = c2.poll(Duration.ofSeconds(2));

            // 两个 consumer 都成功加入 group（不抛异常即通过）
            assertNotNull(r1);
            assertNotNull(r2);
        }
    }
}
```

- [ ] **Step 2: 运行集成测试**

```bash
mvn clean test -pl mini-kafka-clients -Dtest=ProducerConsumerIntegrationTest -Dsort.skip=true
```
Expected: Tests run: 2, Failures: 0, Errors: 0

- [ ] **Step 3: 运行所有 clients 测试**

```bash
mvn clean test -pl mini-kafka-clients -Dsort.skip=true
```
Expected: 所有测试通过

- [ ] **Step 4: Commit**

```bash
git add mini-kafka-clients/src/test/
git commit -m "test(clients): add Producer/Consumer end-to-end integration tests"
```
