# mini-kafka-common Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 构建 mini-kafka-common 模块，包含 Wire Protocol 帧、ApiKeys、Errors、RecordBatch 编解码，是 server 和 clients 的共同依赖。

**Architecture:** 纯 Java 8 数据模型 + ByteBuffer 序列化，无外部依赖。所有网络字节序操作封装在 ByteBufferUtils，RecordBatch 实现 magic=2 格式（对齐 Kafka DefaultRecordBatch）。

**Tech Stack:** Java 8, Maven, JUnit4

---

## File Map

```
mini-kafka-common/
├── pom.xml
└── src/
    ├── main/java/com/github/minikafka/common/
    │   ├── protocol/
    │   │   ├── ApiKeys.java
    │   │   ├── Errors.java
    │   │   ├── RequestHeader.java
    │   │   └── ResponseHeader.java
    │   ├── record/
    │   │   ├── CompressionType.java
    │   │   ├── Record.java
    │   │   └── RecordBatch.java
    │   └── network/
    │       └── ByteBufferUtils.java
    └── test/java/com/github/minikafka/common/
        ├── protocol/
        │   ├── RequestHeaderTest.java
        │   └── ResponseHeaderTest.java
        ├── record/
        │   └── RecordBatchTest.java
        └── network/
            └── ByteBufferUtilsTest.java
```

---

### Task 1: Maven 父 pom + common 模块骨架

**Files:**
- Create: `pom.xml`（父 pom）
- Create: `mini-kafka-common/pom.xml`

- [ ] **Step 1: 写父 pom.xml**

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0
         http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <groupId>com.github.minikafka</groupId>
    <artifactId>mini-kafka</artifactId>
    <version>0.1.0-SNAPSHOT</version>
    <packaging>pom</packaging>
    <name>mini-kafka</name>

    <modules>
        <module>mini-kafka-common</module>
        <module>mini-kafka-server</module>
        <module>mini-kafka-clients</module>
        <module>mini-kafka-examples</module>
    </modules>

    <properties>
        <java.version>1.8</java.version>
        <maven.compiler.source>1.8</maven.compiler.source>
        <maven.compiler.target>1.8</maven.compiler.target>
        <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
        <junit.version>4.13.2</junit.version>
        <slf4j.version>1.7.36</slf4j.version>
        <logback.version>1.2.12</logback.version>
    </properties>

    <dependencyManagement>
        <dependencies>
            <dependency>
                <groupId>junit</groupId>
                <artifactId>junit</artifactId>
                <version>${junit.version}</version>
                <scope>test</scope>
            </dependency>
            <dependency>
                <groupId>org.slf4j</groupId>
                <artifactId>slf4j-api</artifactId>
                <version>${slf4j.version}</version>
            </dependency>
            <dependency>
                <groupId>ch.qos.logback</groupId>
                <artifactId>logback-classic</artifactId>
                <version>${logback.version}</version>
            </dependency>
            <dependency>
                <groupId>com.github.minikafka</groupId>
                <artifactId>mini-kafka-common</artifactId>
                <version>${project.version}</version>
            </dependency>
        </dependencies>
    </dependencyManagement>
</project>
```

- [ ] **Step 2: 写 mini-kafka-common/pom.xml**

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

    <artifactId>mini-kafka-common</artifactId>
    <name>mini-kafka-common</name>

    <dependencies>
        <dependency>
            <groupId>junit</groupId>
            <artifactId>junit</artifactId>
        </dependency>
    </dependencies>
</project>
```

- [ ] **Step 3: 创建目录结构**

```bash
mkdir -p mini-kafka-common/src/main/java/com/github/minikafka/common/protocol
mkdir -p mini-kafka-common/src/main/java/com/github/minikafka/common/record
mkdir -p mini-kafka-common/src/main/java/com/github/minikafka/common/network
mkdir -p mini-kafka-common/src/test/java/com/github/minikafka/common/protocol
mkdir -p mini-kafka-common/src/test/java/com/github/minikafka/common/record
mkdir -p mini-kafka-common/src/test/java/com/github/minikafka/common/network
```

- [ ] **Step 4: 验证编译**

```bash
mvn clean compile -pl mini-kafka-common -Dsort.skip=true
```
Expected: BUILD SUCCESS

- [ ] **Step 5: Commit**

```bash
git add pom.xml mini-kafka-common/pom.xml
git commit -m "chore: init multi-module maven structure"
```

---

### Task 2: ApiKeys 和 Errors 枚举

**Files:**
- Create: `mini-kafka-common/src/main/java/com/github/minikafka/common/protocol/ApiKeys.java`
- Create: `mini-kafka-common/src/main/java/com/github/minikafka/common/protocol/Errors.java`

- [ ] **Step 1: 写 ApiKeys.java**

```java
package com.github.minikafka.common.protocol;

public enum ApiKeys {
    PRODUCE(0, "Produce"),
    FETCH(1, "Fetch"),
    METADATA(3, "Metadata"),
    OFFSET_COMMIT(8, "OffsetCommit"),
    OFFSET_FETCH(9, "OffsetFetch"),
    JOIN_GROUP(11, "JoinGroup"),
    HEARTBEAT(12, "Heartbeat"),
    LEAVE_GROUP(13, "LeaveGroup"),
    SYNC_GROUP(14, "SyncGroup"),
    CREATE_TOPICS(19, "CreateTopics");

    public final short id;
    public final String name;

    ApiKeys(int id, String name) {
        this.id = (short) id;
        this.name = name;
    }

    public static ApiKeys forId(short id) {
        for (ApiKeys key : values()) {
            if (key.id == id) return key;
        }
        throw new IllegalArgumentException("Unknown api key: " + id);
    }
}
```

- [ ] **Step 2: 写 Errors.java**

```java
package com.github.minikafka.common.protocol;

public enum Errors {
    NONE(0, ""),
    UNKNOWN_SERVER_ERROR(-1, "The server experienced an unexpected error when processing the request."),
    UNKNOWN_TOPIC_OR_PARTITION(3, "This server does not host this topic-partition."),
    LEADER_NOT_AVAILABLE(5, "There is no leader for this topic-partition as we are in the middle of a leadership election."),
    REQUEST_TIMED_OUT(7, "The request timed out."),
    GROUP_LOAD_IN_PROGRESS(14, "The coordinator is loading and hence can't process requests for this group."),
    GROUP_COORDINATOR_NOT_AVAILABLE(15, "The group coordinator is not available."),
    NOT_COORDINATOR(16, "This is not the correct coordinator for this group."),
    INVALID_TOPIC_EXCEPTION(17, "The request attempted to perform an operation on an invalid topic."),
    REBALANCE_IN_PROGRESS(27, "The group is rebalancing, so a rejoin is needed."),
    UNKNOWN_MEMBER_ID(25, "The coordinator is not aware of this member."),
    ILLEGAL_GENERATION(22, "The provided generation id is not the current generation."),
    INCONSISTENT_GROUP_PROTOCOL(23, "The group member's supported protocols are incompatible with those of existing members.");

    public final short code;
    public final String message;

    Errors(int code, String message) {
        this.code = (short) code;
        this.message = message;
    }

    public static Errors forCode(short code) {
        for (Errors e : values()) {
            if (e.code == code) return e;
        }
        return UNKNOWN_SERVER_ERROR;
    }
}
```

- [ ] **Step 3: 编译验证**

```bash
mvn clean compile -pl mini-kafka-common -Dsort.skip=true
```
Expected: BUILD SUCCESS

- [ ] **Step 4: Commit**

```bash
git add mini-kafka-common/src/main/java/com/github/minikafka/common/protocol/
git commit -m "feat(common): add ApiKeys and Errors enums"
```

---

### Task 3: ByteBufferUtils

**Files:**
- Create: `mini-kafka-common/src/main/java/com/github/minikafka/common/network/ByteBufferUtils.java`
- Create: `mini-kafka-common/src/test/java/com/github/minikafka/common/network/ByteBufferUtilsTest.java`

- [ ] **Step 1: 写失败测试**

```java
package com.github.minikafka.common.network;

import org.junit.Test;
import java.nio.ByteBuffer;
import static org.junit.Assert.*;

public class ByteBufferUtilsTest {

    @Test
    public void testWriteAndReadString() {
        ByteBuffer buf = ByteBuffer.allocate(64);
        ByteBufferUtils.writeString(buf, "hello");
        buf.flip();
        assertEquals("hello", ByteBufferUtils.readString(buf));
    }

    @Test
    public void testWriteAndReadNullString() {
        ByteBuffer buf = ByteBuffer.allocate(64);
        ByteBufferUtils.writeString(buf, null);
        buf.flip();
        assertNull(ByteBufferUtils.readString(buf));
    }

    @Test
    public void testWriteAndReadBytes() {
        ByteBuffer buf = ByteBuffer.allocate(64);
        byte[] data = {1, 2, 3, 4};
        ByteBufferUtils.writeBytes(buf, data);
        buf.flip();
        assertArrayEquals(data, ByteBufferUtils.readBytes(buf));
    }

    @Test
    public void testWriteAndReadNullBytes() {
        ByteBuffer buf = ByteBuffer.allocate(64);
        ByteBufferUtils.writeBytes(buf, null);
        buf.flip();
        assertNull(ByteBufferUtils.readBytes(buf));
    }

    @Test
    public void testSizeOfString() {
        assertEquals(2 + 5, ByteBufferUtils.sizeOfString("hello"));
        assertEquals(2, ByteBufferUtils.sizeOfString(null));
    }
}
```

- [ ] **Step 2: 运行测试，确认失败**

```bash
mvn clean test -pl mini-kafka-common -Dtest=ByteBufferUtilsTest -Dsort.skip=true
```
Expected: FAIL - compilation error (ByteBufferUtils not found)

- [ ] **Step 3: 实现 ByteBufferUtils.java**

```java
package com.github.minikafka.common.network;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public final class ByteBufferUtils {

    private ByteBufferUtils() {}

    /** 写 nullable string: 2字节长度（-1表示null）+ UTF-8 bytes */
    public static void writeString(ByteBuffer buffer, String value) {
        if (value == null) {
            buffer.putShort((short) -1);
        } else {
            byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
            buffer.putShort((short) bytes.length);
            buffer.put(bytes);
        }
    }

    /** 读 nullable string */
    public static String readString(ByteBuffer buffer) {
        short length = buffer.getShort();
        if (length < 0) return null;
        byte[] bytes = new byte[length];
        buffer.get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    /** 写 nullable bytes: 4字节长度（-1表示null）+ bytes */
    public static void writeBytes(ByteBuffer buffer, byte[] value) {
        if (value == null) {
            buffer.putInt(-1);
        } else {
            buffer.putInt(value.length);
            buffer.put(value);
        }
    }

    /** 读 nullable bytes */
    public static byte[] readBytes(ByteBuffer buffer) {
        int length = buffer.getInt();
        if (length < 0) return null;
        byte[] bytes = new byte[length];
        buffer.get(bytes);
        return bytes;
    }

    /** 计算 string 序列化后的字节数 */
    public static int sizeOfString(String value) {
        if (value == null) return 2;
        return 2 + value.getBytes(StandardCharsets.UTF_8).length;
    }

    /** 计算 bytes 序列化后的字节数 */
    public static int sizeOfBytes(byte[] value) {
        if (value == null) return 4;
        return 4 + value.length;
    }
}
```

- [ ] **Step 4: 运行测试，确认通过**

```bash
mvn clean test -pl mini-kafka-common -Dtest=ByteBufferUtilsTest -Dsort.skip=true
```
Expected: Tests run: 5, Failures: 0, Errors: 0

- [ ] **Step 5: Commit**

```bash
git add mini-kafka-common/src/main/java/com/github/minikafka/common/network/ \
        mini-kafka-common/src/test/java/com/github/minikafka/common/network/
git commit -m "feat(common): add ByteBufferUtils with TDD"
```

---

### Task 4: RequestHeader / ResponseHeader

**Files:**
- Create: `mini-kafka-common/src/main/java/com/github/minikafka/common/protocol/RequestHeader.java`
- Create: `mini-kafka-common/src/main/java/com/github/minikafka/common/protocol/ResponseHeader.java`
- Create: `mini-kafka-common/src/test/java/com/github/minikafka/common/protocol/RequestHeaderTest.java`
- Create: `mini-kafka-common/src/test/java/com/github/minikafka/common/protocol/ResponseHeaderTest.java`

- [ ] **Step 1: 写 RequestHeader 失败测试**

```java
package com.github.minikafka.common.protocol;

import org.junit.Test;
import java.nio.ByteBuffer;
import static org.junit.Assert.*;

public class RequestHeaderTest {

    @Test
    public void testSerializeDeserialize() {
        RequestHeader header = new RequestHeader(ApiKeys.PRODUCE, (short) 0, 42, "test-client");
        ByteBuffer buf = ByteBuffer.allocate(header.sizeOf());
        header.writeTo(buf);
        buf.flip();
        RequestHeader decoded = RequestHeader.parse(buf);
        assertEquals(ApiKeys.PRODUCE.id, decoded.apiKey());
        assertEquals(0, decoded.apiVersion());
        assertEquals(42, decoded.correlationId());
        assertEquals("test-client", decoded.clientId());
    }

    @Test
    public void testSizeOf() {
        RequestHeader header = new RequestHeader(ApiKeys.FETCH, (short) 1, 1, "client");
        // 2(apiKey) + 2(apiVersion) + 4(correlationId) + 2(clientIdLen) + 6(clientId)
        assertEquals(16, header.sizeOf());
    }
}
```

- [ ] **Step 2: 运行测试，确认失败**

```bash
mvn clean test -pl mini-kafka-common -Dtest=RequestHeaderTest -Dsort.skip=true
```
Expected: FAIL - compilation error

- [ ] **Step 3: 实现 RequestHeader.java**

```java
package com.github.minikafka.common.protocol;

import com.github.minikafka.common.network.ByteBufferUtils;
import java.nio.ByteBuffer;

public final class RequestHeader {
    private final short apiKey;
    private final short apiVersion;
    private final int correlationId;
    private final String clientId;

    public RequestHeader(ApiKeys apiKey, short apiVersion, int correlationId, String clientId) {
        this.apiKey = apiKey.id;
        this.apiVersion = apiVersion;
        this.correlationId = correlationId;
        this.clientId = clientId;
    }

    private RequestHeader(short apiKey, short apiVersion, int correlationId, String clientId) {
        this.apiKey = apiKey;
        this.apiVersion = apiVersion;
        this.correlationId = correlationId;
        this.clientId = clientId;
    }

    public short apiKey() { return apiKey; }
    public short apiVersion() { return apiVersion; }
    public int correlationId() { return correlationId; }
    public String clientId() { return clientId; }

    public int sizeOf() {
        return 2 + 2 + 4 + ByteBufferUtils.sizeOfString(clientId);
    }

    public void writeTo(ByteBuffer buffer) {
        buffer.putShort(apiKey);
        buffer.putShort(apiVersion);
        buffer.putInt(correlationId);
        ByteBufferUtils.writeString(buffer, clientId);
    }

    public static RequestHeader parse(ByteBuffer buffer) {
        short apiKey = buffer.getShort();
        short apiVersion = buffer.getShort();
        int correlationId = buffer.getInt();
        String clientId = ByteBufferUtils.readString(buffer);
        return new RequestHeader(apiKey, apiVersion, correlationId, clientId);
    }
}
```

- [ ] **Step 4: 写 ResponseHeader 失败测试**

```java
package com.github.minikafka.common.protocol;

import org.junit.Test;
import java.nio.ByteBuffer;
import static org.junit.Assert.*;

public class ResponseHeaderTest {

    @Test
    public void testSerializeDeserialize() {
        ResponseHeader header = new ResponseHeader(42);
        ByteBuffer buf = ByteBuffer.allocate(ResponseHeader.SIZE);
        header.writeTo(buf);
        buf.flip();
        ResponseHeader decoded = ResponseHeader.parse(buf);
        assertEquals(42, decoded.correlationId());
    }

    @Test
    public void testSize() {
        assertEquals(4, ResponseHeader.SIZE);
    }
}
```

- [ ] **Step 5: 实现 ResponseHeader.java**

```java
package com.github.minikafka.common.protocol;

import java.nio.ByteBuffer;

public final class ResponseHeader {
    public static final int SIZE = 4;

    private final int correlationId;

    public ResponseHeader(int correlationId) {
        this.correlationId = correlationId;
    }

    public int correlationId() { return correlationId; }

    public void writeTo(ByteBuffer buffer) {
        buffer.putInt(correlationId);
    }

    public static ResponseHeader parse(ByteBuffer buffer) {
        return new ResponseHeader(buffer.getInt());
    }
}
```

- [ ] **Step 6: 运行所有协议测试，确认通过**

```bash
mvn clean test -pl mini-kafka-common -Dtest=RequestHeaderTest,ResponseHeaderTest -Dsort.skip=true
```
Expected: Tests run: 4, Failures: 0, Errors: 0

- [ ] **Step 7: Commit**

```bash
git add mini-kafka-common/src/main/java/com/github/minikafka/common/protocol/ \
        mini-kafka-common/src/test/java/com/github/minikafka/common/protocol/
git commit -m "feat(common): add RequestHeader and ResponseHeader with TDD"
```

---

### Task 5: Record 和 RecordBatch 编解码

**Files:**
- Create: `mini-kafka-common/src/main/java/com/github/minikafka/common/record/CompressionType.java`
- Create: `mini-kafka-common/src/main/java/com/github/minikafka/common/record/Record.java`
- Create: `mini-kafka-common/src/main/java/com/github/minikafka/common/record/RecordBatch.java`
- Create: `mini-kafka-common/src/test/java/com/github/minikafka/common/record/RecordBatchTest.java`

- [ ] **Step 1: 写 CompressionType.java**

```java
package com.github.minikafka.common.record;

public enum CompressionType {
    NONE(0, "none");

    public final int id;
    public final String name;

    CompressionType(int id, String name) {
        this.id = id;
        this.name = name;
    }
}
```

- [ ] **Step 2: 写 Record.java**

```java
package com.github.minikafka.common.record;

/** 单条消息，对应 Kafka Record（magic=2 格式内的条目） */
public final class Record {
    private final long offset;
    private final long timestamp;
    private final byte[] key;
    private final byte[] value;

    public Record(long offset, long timestamp, byte[] key, byte[] value) {
        this.offset = offset;
        this.timestamp = timestamp;
        this.key = key;
        this.value = value;
    }

    public long offset() { return offset; }
    public long timestamp() { return timestamp; }
    public byte[] key() { return key; }
    public byte[] value() { return value; }
}
```

- [ ] **Step 3: 写 RecordBatch 失败测试**

```java
package com.github.minikafka.common.record;

import org.junit.Test;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import static org.junit.Assert.*;

public class RecordBatchTest {

    @Test
    public void testWriteAndReadSingleRecord() {
        List<Record> records = Arrays.asList(
            new Record(0, 1000L, "key1".getBytes(), "value1".getBytes())
        );
        RecordBatch batch = new RecordBatch(0L, records);

        ByteBuffer buf = ByteBuffer.allocate(batch.sizeInBytes());
        batch.writeTo(buf);
        buf.flip();

        RecordBatch decoded = RecordBatch.readFrom(buf);
        assertEquals(0L, decoded.baseOffset());
        assertEquals(1, decoded.records().size());

        Record r = decoded.records().get(0);
        assertArrayEquals("key1".getBytes(), r.key());
        assertArrayEquals("value1".getBytes(), r.value());
        assertEquals(1000L, r.timestamp());
    }

    @Test
    public void testWriteAndReadMultipleRecords() {
        List<Record> records = Arrays.asList(
            new Record(0, 1000L, null, "msg1".getBytes()),
            new Record(1, 1001L, null, "msg2".getBytes()),
            new Record(2, 1002L, null, "msg3".getBytes())
        );
        RecordBatch batch = new RecordBatch(100L, records);

        ByteBuffer buf = ByteBuffer.allocate(batch.sizeInBytes());
        batch.writeTo(buf);
        buf.flip();

        RecordBatch decoded = RecordBatch.readFrom(buf);
        assertEquals(100L, decoded.baseOffset());
        assertEquals(3, decoded.records().size());
        assertArrayEquals("msg3".getBytes(), decoded.records().get(2).value());
    }

    @Test
    public void testLastOffset() {
        List<Record> records = Arrays.asList(
            new Record(0, 1000L, null, "a".getBytes()),
            new Record(1, 1001L, null, "b".getBytes())
        );
        RecordBatch batch = new RecordBatch(5L, records);
        assertEquals(6L, batch.lastOffset());
    }
}
```

- [ ] **Step 4: 运行测试，确认失败**

```bash
mvn clean test -pl mini-kafka-common -Dtest=RecordBatchTest -Dsort.skip=true
```
Expected: FAIL - compilation error

- [ ] **Step 5: 实现 RecordBatch.java**

```java
package com.github.minikafka.common.record;

import com.github.minikafka.common.network.ByteBufferUtils;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.zip.CRC32;

/**
 * Kafka magic=2 RecordBatch 格式（简化版）：
 *   8  baseOffset
 *   4  batchLength（从 partitionLeaderEpoch 到末尾的字节数）
 *   4  partitionLeaderEpoch（固定0）
 *   1  magic（固定2）
 *   4  crc（覆盖 attributes 到末尾）
 *   2  attributes（固定0=NONE compression, timestamp type=CREATE）
 *   4  lastOffsetDelta
 *   8  firstTimestamp
 *   8  maxTimestamp
 *   8  producerId（固定-1）
 *   2  producerEpoch（固定-1）
 *   4  baseSequence（固定-1）
 *   4  recordCount
 *   [records...]
 *     每条 record：
 *       4  length（key+value+timestamp的字节数，含后面所有字段）
 *       1  attributes（固定0）
 *       8  timestampDelta
 *       4  offsetDelta
 *       4  keyLength（-1表示null）
 *       [key bytes]
 *       4  valueLength（-1表示null）
 *       [value bytes]
 */
public final class RecordBatch {

    // 固定头部大小（不含 batchLength 自身前的 baseOffset+batchLength 8+4=12 字节）
    // partitionLeaderEpoch(4) + magic(1) + crc(4) + attributes(2) + lastOffsetDelta(4)
    // + firstTimestamp(8) + maxTimestamp(8) + producerId(8) + producerEpoch(2)
    // + baseSequence(4) + recordCount(4) = 49
    private static final int BATCH_OVERHEAD = 4 + 1 + 4 + 2 + 4 + 8 + 8 + 8 + 2 + 4 + 4;

    private final long baseOffset;
    private final List<Record> records;

    public RecordBatch(long baseOffset, List<Record> records) {
        this.baseOffset = baseOffset;
        this.records = Collections.unmodifiableList(new ArrayList<>(records));
    }

    public long baseOffset() { return baseOffset; }
    public List<Record> records() { return records; }
    public long lastOffset() { return baseOffset + records.size() - 1; }

    public int sizeInBytes() {
        // 8(baseOffset) + 4(batchLength) + BATCH_OVERHEAD + recordsSize
        return 8 + 4 + BATCH_OVERHEAD + recordsSize();
    }

    private int recordsSize() {
        int size = 0;
        for (Record r : records) {
            size += recordSize(r);
        }
        return size;
    }

    private int recordSize(Record r) {
        // 4(length) + 1(attributes) + 8(timestampDelta) + 4(offsetDelta)
        // + 4(keyLen) + key + 4(valueLen) + value
        return 4 + 1 + 8 + 4
            + ByteBufferUtils.sizeOfBytes(r.key())
            + ByteBufferUtils.sizeOfBytes(r.value());
    }

    public void writeTo(ByteBuffer buffer) {
        long firstTimestamp = records.isEmpty() ? 0L : records.get(0).timestamp();
        long maxTimestamp = records.isEmpty() ? 0L : records.get(records.size() - 1).timestamp();
        int lastOffsetDelta = records.isEmpty() ? 0 : records.size() - 1;

        buffer.putLong(baseOffset);
        int batchLengthPos = buffer.position();
        buffer.putInt(0); // placeholder for batchLength
        int batchStart = buffer.position();

        buffer.putInt(0);           // partitionLeaderEpoch
        buffer.put((byte) 2);       // magic

        int crcPos = buffer.position();
        buffer.putInt(0);           // placeholder for CRC

        int crcStart = buffer.position();
        buffer.putShort((short) 0); // attributes
        buffer.putInt(lastOffsetDelta);
        buffer.putLong(firstTimestamp);
        buffer.putLong(maxTimestamp);
        buffer.putLong(-1L);        // producerId
        buffer.putShort((short) -1);// producerEpoch
        buffer.putInt(-1);          // baseSequence
        buffer.putInt(records.size());

        for (int i = 0; i < records.size(); i++) {
            Record r = records.get(i);
            int rSize = recordSize(r) - 4; // 不含 length 字段本身
            buffer.putInt(rSize);
            buffer.put((byte) 0);   // attributes
            buffer.putLong(r.timestamp() - firstTimestamp); // timestampDelta
            buffer.putInt(i);       // offsetDelta
            ByteBufferUtils.writeBytes(buffer, r.key());
            ByteBufferUtils.writeBytes(buffer, r.value());
        }

        int end = buffer.position();

        // 填 batchLength
        buffer.putInt(batchLengthPos, end - batchStart);

        // 计算并填 CRC（覆盖 crcStart 到 end）
        byte[] crcData = new byte[end - crcStart];
        int savedPos = buffer.position();
        buffer.position(crcStart);
        buffer.get(crcData);
        buffer.position(savedPos);

        CRC32 crc32 = new CRC32();
        crc32.update(crcData);
        buffer.putInt(crcPos, (int) crc32.getValue());
    }

    public static RecordBatch readFrom(ByteBuffer buffer) {
        long baseOffset = buffer.getLong();
        int batchLength = buffer.getInt();
        buffer.getInt(); // partitionLeaderEpoch
        buffer.get();    // magic
        buffer.getInt(); // crc (skip validation for now)
        buffer.getShort(); // attributes
        buffer.getInt(); // lastOffsetDelta
        long firstTimestamp = buffer.getLong();
        buffer.getLong(); // maxTimestamp
        buffer.getLong(); // producerId
        buffer.getShort(); // producerEpoch
        buffer.getInt(); // baseSequence
        int recordCount = buffer.getInt();

        List<Record> records = new ArrayList<>(recordCount);
        for (int i = 0; i < recordCount; i++) {
            buffer.getInt(); // length
            buffer.get();    // attributes
            long timestampDelta = buffer.getLong();
            int offsetDelta = buffer.getInt();
            byte[] key = ByteBufferUtils.readBytes(buffer);
            byte[] value = ByteBufferUtils.readBytes(buffer);
            records.add(new Record(baseOffset + offsetDelta, firstTimestamp + timestampDelta, key, value));
        }

        return new RecordBatch(baseOffset, records);
    }
}
```

- [ ] **Step 6: 运行测试，确认通过**

```bash
mvn clean test -pl mini-kafka-common -Dtest=RecordBatchTest -Dsort.skip=true
```
Expected: Tests run: 3, Failures: 0, Errors: 0

- [ ] **Step 7: Commit**

```bash
git add mini-kafka-common/src/main/java/com/github/minikafka/common/record/ \
        mini-kafka-common/src/test/java/com/github/minikafka/common/record/
git commit -m "feat(common): add Record and RecordBatch with TDD"
```

---

### Task 6: 运行全模块测试并 install

**Files:** 无新文件

- [ ] **Step 1: 运行 common 所有测试**

```bash
mvn clean test -pl mini-kafka-common -Dsort.skip=true
```
Expected: Tests run: 12+, Failures: 0, Errors: 0

- [ ] **Step 2: install 到本地仓库供后续模块使用**

```bash
mvn clean install -pl mini-kafka-common -DskipTests -Dsort.skip=true
```
Expected: BUILD SUCCESS, mini-kafka-common-0.1.0-SNAPSHOT.jar installed

- [ ] **Step 3: Commit**

```bash
git add -A
git commit -m "chore(common): all tests pass, module ready for server/clients"
```
