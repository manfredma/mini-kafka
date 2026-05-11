# mini-kafka-server Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 构建 Broker 服务端，包含 NIO 网络层、Log 持久化、KafkaController、GroupCoordinator，对齐 Kafka 源码结构。

**Architecture:** Reactor 模式网络层（Acceptor + Processor + RequestChannel + Handler 线程池）；Log 层对应磁盘 .log/.index 文件对；GroupCoordinator 维护 JoinGroup/SyncGroup/Heartbeat 状态机；KafkaController 单节点管理 Topic/Partition 元数据。

**Tech Stack:** Java 8, Maven, JUnit4, SLF4J+Logback

**依赖:** mini-kafka-common 需先 `mvn install`

---

## File Map

```
mini-kafka-server/
├── pom.xml
└── src/
    ├── main/java/com/github/minikafka/server/
    │   ├── KafkaServer.java
    │   ├── KafkaConfig.java
    │   ├── network/
    │   │   ├── SocketServer.java
    │   │   ├── Acceptor.java
    │   │   ├── Processor.java
    │   │   ├── RequestChannel.java
    │   │   └── NetworkReceive.java
    │   ├── api/
    │   │   ├── KafkaApis.java
    │   │   └── KafkaRequestHandler.java
    │   ├── log/
    │   │   ├── LogConfig.java
    │   │   ├── LogManager.java
    │   │   ├── Log.java
    │   │   ├── LogSegment.java
    │   │   └── OffsetIndex.java
    │   ├── controller/
    │   │   ├── KafkaController.java
    │   │   └── ControllerContext.java
    │   └── coordinator/
    │       ├── GroupCoordinator.java
    │       ├── GroupMetadata.java
    │       └── MemberMetadata.java
    └── test/java/com/github/minikafka/server/
        ├── log/
        │   ├── LogSegmentTest.java
        │   ├── OffsetIndexTest.java
        │   └── LogTest.java
        ├── coordinator/
        │   └── GroupCoordinatorTest.java
        └── integration/
            └── BrokerIntegrationTest.java
```

---

### Task 1: server 模块 pom + 基础骨架

**Files:**
- Create: `mini-kafka-server/pom.xml`
- Create: `mini-kafka-server/src/main/java/com/github/minikafka/server/KafkaConfig.java`
- Create: `mini-kafka-server/src/main/resources/logback.xml`

- [ ] **Step 1: 写 mini-kafka-server/pom.xml**

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

    <artifactId>mini-kafka-server</artifactId>
    <name>mini-kafka-server</name>

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
    </dependencies>
</project>
```

- [ ] **Step 2: 写 KafkaConfig.java**

```java
package com.github.minikafka.server;

import java.util.Properties;

public final class KafkaConfig {
    public final int port;
    public final String logDirs;
    public final int numNetworkThreads;       // Processor 线程数
    public final int numIoThreads;            // KafkaRequestHandler 线程数
    public final int logSegmentBytes;         // 单 Segment 最大字节数
    public final long logSegmentMs;           // 单 Segment 最大时间（ms）
    public final int logIndexIntervalBytes;   // 稀疏索引间隔
    public final int sessionTimeoutMs;        // Consumer 心跳超时

    public KafkaConfig(Properties props) {
        this.port = Integer.parseInt(props.getProperty("port", "9092"));
        this.logDirs = props.getProperty("log.dirs", "/tmp/mini-kafka-logs");
        this.numNetworkThreads = Integer.parseInt(props.getProperty("num.network.threads", "3"));
        this.numIoThreads = Integer.parseInt(props.getProperty("num.io.threads", "8"));
        this.logSegmentBytes = Integer.parseInt(props.getProperty("log.segment.bytes", String.valueOf(1024 * 1024 * 1024)));
        this.logSegmentMs = Long.parseLong(props.getProperty("log.segment.ms", String.valueOf(7 * 24 * 60 * 60 * 1000L)));
        this.logIndexIntervalBytes = Integer.parseInt(props.getProperty("log.index.interval.bytes", "4096"));
        this.sessionTimeoutMs = Integer.parseInt(props.getProperty("session.timeout.ms", "30000"));
    }

    public static KafkaConfig defaultConfig() {
        return new KafkaConfig(new Properties());
    }
}
```

- [ ] **Step 3: 写 logback.xml（放到 src/main/resources/）**

```xml
<configuration>
    <appender name="CONSOLE" class="ch.qos.logback.core.ConsoleAppender">
        <encoder>
            <pattern>%d{HH:mm:ss.SSS} [%thread] %-5level %logger{36} - %msg%n</pattern>
        </encoder>
    </appender>
    <root level="INFO">
        <appender-ref ref="CONSOLE"/>
    </root>
</configuration>
```

- [ ] **Step 4: 编译验证**

```bash
mvn clean compile -pl mini-kafka-server -Dsort.skip=true
```
Expected: BUILD SUCCESS

- [ ] **Step 5: Commit**

```bash
git add mini-kafka-server/
git commit -m "chore(server): init server module with KafkaConfig"
```

---

### Task 2: Log 持久化 — OffsetIndex

**Files:**
- Create: `mini-kafka-server/src/main/java/com/github/minikafka/server/log/OffsetIndex.java`
- Create: `mini-kafka-server/src/test/java/com/github/minikafka/server/log/OffsetIndexTest.java`

- [ ] **Step 1: 写 OffsetIndex 失败测试**

```java
package com.github.minikafka.server.log;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import java.io.File;
import java.io.IOException;
import static org.junit.Assert.*;

public class OffsetIndexTest {

    private File indexFile;
    private OffsetIndex index;

    @Before
    public void setUp() throws IOException {
        indexFile = File.createTempFile("test", ".index");
        indexFile.deleteOnExit();
        index = new OffsetIndex(indexFile, 0L, 1024 * 1024);
    }

    @After
    public void tearDown() throws IOException {
        index.close();
    }

    @Test
    public void testAppendAndLookup() throws IOException {
        index.append(0, 0);
        index.append(10, 500);
        index.append(20, 1200);

        // 精确匹配
        assertEquals(500, index.lookup(10));
        // 找不到精确匹配时返回最近的小于等于的索引项对应的 position
        assertEquals(500, index.lookup(15));
        // 小于第一个索引项时返回0
        assertEquals(0, index.lookup(5));
    }

    @Test
    public void testEntries() throws IOException {
        assertEquals(0, index.entries());
        index.append(0, 0);
        index.append(10, 500);
        assertEquals(2, index.entries());
    }

    @Test
    public void testEmpty() throws IOException {
        assertEquals(0, index.lookup(100));
    }
}
```

- [ ] **Step 2: 运行测试，确认失败**

```bash
mvn clean test -pl mini-kafka-server -Dtest=OffsetIndexTest -Dsort.skip=true
```
Expected: FAIL - compilation error

- [ ] **Step 3: 实现 OffsetIndex.java**

```java
package com.github.minikafka.server.log;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 * 稀疏 offset 索引，每条 entry = (relativeOffset:4bytes, position:4bytes)
 * baseOffset 存储相对 offset 以节省空间（对齐 Kafka OffsetIndex）
 */
public final class OffsetIndex implements Closeable {

    private static final int ENTRY_SIZE = 8; // 4(relativeOffset) + 4(position)

    private final File file;
    private final long baseOffset;
    private MappedByteBuffer mmap;
    private int entries;

    public OffsetIndex(File file, long baseOffset, int maxIndexSize) throws IOException {
        this.file = file;
        this.baseOffset = baseOffset;
        try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
            raf.setLength(maxIndexSize);
            this.mmap = raf.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, maxIndexSize);
        }
        // 计算已有 entries（文件重新打开时恢复）
        this.entries = (int) (file.length() / ENTRY_SIZE);
        // 如果是新文件，entries 为 0
        if (file.length() == maxIndexSize) {
            // mmap 了整个文件，需找到真实写入位置
            this.entries = countEntries();
        }
    }

    private int countEntries() {
        int count = 0;
        mmap.position(0);
        while (mmap.remaining() >= ENTRY_SIZE) {
            int relOffset = mmap.getInt();
            mmap.getInt(); // position
            if (relOffset == 0 && count > 0) break;
            if (relOffset != 0 || count == 0) count++;
            else break;
        }
        return count;
    }

    public void append(long offset, int position) throws IOException {
        int relativeOffset = (int) (offset - baseOffset);
        mmap.position(entries * ENTRY_SIZE);
        mmap.putInt(relativeOffset);
        mmap.putInt(position);
        entries++;
    }

    /**
     * 二分查找：返回 <= targetOffset 的最大索引项对应的 filePosition
     * 若无任何索引项，返回 0
     */
    public int lookup(long targetOffset) {
        if (entries == 0) return 0;
        int relTarget = (int) (targetOffset - baseOffset);
        int lo = 0, hi = entries - 1, result = 0;
        while (lo <= hi) {
            int mid = (lo + hi) >>> 1;
            int relOffset = relativeOffset(mid);
            int pos = position(mid);
            if (relOffset <= relTarget) {
                result = pos;
                lo = mid + 1;
            } else {
                hi = mid - 1;
            }
        }
        return result;
    }

    private int relativeOffset(int n) {
        return mmap.getInt(n * ENTRY_SIZE);
    }

    private int position(int n) {
        return mmap.getInt(n * ENTRY_SIZE + 4);
    }

    public int entries() { return entries; }

    public void flush() {
        mmap.force();
    }

    @Override
    public void close() {
        flush();
    }
}
```

- [ ] **Step 4: 运行测试，确认通过**

```bash
mvn clean test -pl mini-kafka-server -Dtest=OffsetIndexTest -Dsort.skip=true
```
Expected: Tests run: 3, Failures: 0, Errors: 0

- [ ] **Step 5: Commit**

```bash
git add mini-kafka-server/src/main/java/com/github/minikafka/server/log/OffsetIndex.java \
        mini-kafka-server/src/test/java/com/github/minikafka/server/log/OffsetIndexTest.java
git commit -m "feat(server): add OffsetIndex with TDD"
```

---

### Task 3: Log 持久化 — LogConfig + LogSegment

**Files:**
- Create: `mini-kafka-server/src/main/java/com/github/minikafka/server/log/LogConfig.java`
- Create: `mini-kafka-server/src/main/java/com/github/minikafka/server/log/LogSegment.java`
- Create: `mini-kafka-server/src/test/java/com/github/minikafka/server/log/LogSegmentTest.java`

- [ ] **Step 1: 写 LogConfig.java**

```java
package com.github.minikafka.server.log;

public final class LogConfig {
    public final int segmentBytes;
    public final long segmentMs;
    public final int indexIntervalBytes;
    public final int maxIndexSize;

    public LogConfig(int segmentBytes, long segmentMs, int indexIntervalBytes) {
        this.segmentBytes = segmentBytes;
        this.segmentMs = segmentMs;
        this.indexIntervalBytes = indexIntervalBytes;
        this.maxIndexSize = 10 * 1024 * 1024; // 10MB index
    }

    public static LogConfig defaultConfig() {
        return new LogConfig(1024 * 1024 * 1024, 7 * 24 * 3600 * 1000L, 4096);
    }

    /** 测试用小配置，便于触发 segment 滚动 */
    public static LogConfig testConfig(int segmentBytes) {
        return new LogConfig(segmentBytes, Long.MAX_VALUE, 64);
    }
}
```

- [ ] **Step 2: 写 LogSegment 失败测试**

```java
package com.github.minikafka.server.log;

import com.github.minikafka.common.record.Record;
import com.github.minikafka.common.record.RecordBatch;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import static org.junit.Assert.*;

public class LogSegmentTest {

    private File dir;
    private LogSegment segment;

    @Before
    public void setUp() throws IOException {
        dir = createTempDir();
        segment = new LogSegment(dir, 0L, LogConfig.testConfig(1024 * 1024));
    }

    @After
    public void tearDown() throws IOException {
        segment.close();
        deleteDir(dir);
    }

    @Test
    public void testAppendAndRead() throws IOException {
        List<Record> records = Arrays.asList(
            new Record(0, System.currentTimeMillis(), "k1".getBytes(), "v1".getBytes())
        );
        RecordBatch batch = new RecordBatch(0L, records);
        segment.append(batch);

        ByteBuffer result = segment.read(0L, 4096);
        assertNotNull(result);
        RecordBatch decoded = RecordBatch.readFrom(result);
        assertEquals(0L, decoded.baseOffset());
        assertArrayEquals("v1".getBytes(), decoded.records().get(0).value());
    }

    @Test
    public void testSize() throws IOException {
        assertEquals(0, segment.size());
        List<Record> records = Collections.singletonList(
            new Record(0, System.currentTimeMillis(), null, "hello".getBytes())
        );
        segment.append(new RecordBatch(0L, records));
        assertTrue(segment.size() > 0);
    }

    @Test
    public void testBaseOffset() {
        assertEquals(0L, segment.baseOffset());
    }

    private File createTempDir() throws IOException {
        File f = File.createTempFile("log-segment-test", "");
        f.delete();
        f.mkdirs();
        return f;
    }

    private void deleteDir(File dir) {
        if (dir.isDirectory()) {
            for (File f : dir.listFiles()) f.delete();
        }
        dir.delete();
    }
}
```

- [ ] **Step 3: 运行测试，确认失败**

```bash
mvn clean test -pl mini-kafka-server -Dtest=LogSegmentTest -Dsort.skip=true
```
Expected: FAIL - compilation error

- [ ] **Step 4: 实现 LogSegment.java**

```java
package com.github.minikafka.server.log;

import com.github.minikafka.common.record.RecordBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * 对应磁盘上的一对 .log + .index 文件
 * 文件名 = baseOffset（20位补零，如 00000000000000000000.log）
 */
public final class LogSegment implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(LogSegment.class);

    private final long baseOffset;
    private final LogConfig config;
    private final FileChannel logChannel;
    private final OffsetIndex offsetIndex;
    private long size;
    private long bytesSinceLastIndex;
    private long createdMs;

    public LogSegment(File dir, long baseOffset, LogConfig config) throws IOException {
        this.baseOffset = baseOffset;
        this.config = config;
        this.createdMs = System.currentTimeMillis();

        File logFile = new File(dir, filenamePrefixed(baseOffset) + ".log");
        File indexFile = new File(dir, filenamePrefixed(baseOffset) + ".index");

        RandomAccessFile raf = new RandomAccessFile(logFile, "rw");
        this.logChannel = raf.getChannel();
        this.size = logFile.length();
        this.logChannel.position(this.size);

        this.offsetIndex = new OffsetIndex(indexFile, baseOffset, config.maxIndexSize);
        this.bytesSinceLastIndex = 0;
    }

    public long baseOffset() { return baseOffset; }
    public long size() { return size; }
    public long createdMs() { return createdMs; }

    public void append(RecordBatch batch) throws IOException {
        ByteBuffer buf = ByteBuffer.allocate(batch.sizeInBytes());
        batch.writeTo(buf);
        buf.flip();

        // 写索引（稀疏索引：每隔 indexIntervalBytes 写一条）
        if (bytesSinceLastIndex >= config.indexIntervalBytes || offsetIndex.entries() == 0) {
            offsetIndex.append(batch.baseOffset(), (int) size);
            bytesSinceLastIndex = 0;
        }

        int written = buf.remaining();
        logChannel.write(buf);
        size += written;
        bytesSinceLastIndex += written;
    }

    /**
     * 读取从 startOffset 开始、最多 maxLength 字节的数据
     * 通过 OffsetIndex 定位物理位置，然后顺序扫描找到目标 offset
     */
    public ByteBuffer read(long startOffset, int maxLength) throws IOException {
        int startPosition = offsetIndex.lookup(startOffset);
        long readableSize = size - startPosition;
        if (readableSize <= 0) return null;

        int toRead = (int) Math.min(maxLength, readableSize);
        ByteBuffer buf = ByteBuffer.allocate(toRead);
        logChannel.read(buf, startPosition);
        buf.flip();
        return buf;
    }

    public boolean isFull() {
        return size >= config.segmentBytes;
    }

    public boolean isExpired(long now) {
        return (now - createdMs) >= config.segmentMs;
    }

    public void flush() throws IOException {
        logChannel.force(true);
        offsetIndex.flush();
    }

    @Override
    public void close() throws IOException {
        flush();
        logChannel.close();
        offsetIndex.close();
    }

    static String filenamePrefixed(long offset) {
        return String.format("%020d", offset);
    }
}
```

- [ ] **Step 5: 运行测试，确认通过**

```bash
mvn clean test -pl mini-kafka-server -Dtest=LogSegmentTest -Dsort.skip=true
```
Expected: Tests run: 3, Failures: 0, Errors: 0

- [ ] **Step 6: Commit**

```bash
git add mini-kafka-server/src/main/java/com/github/minikafka/server/log/ \
        mini-kafka-server/src/test/java/com/github/minikafka/server/log/
git commit -m "feat(server): add LogConfig and LogSegment with TDD"
```

---

### Task 4: Log（Partition 级别）

**Files:**
- Create: `mini-kafka-server/src/main/java/com/github/minikafka/server/log/Log.java`
- Create: `mini-kafka-server/src/test/java/com/github/minikafka/server/log/LogTest.java`

- [ ] **Step 1: 写 Log 失败测试**

```java
package com.github.minikafka.server.log;

import com.github.minikafka.common.record.Record;
import com.github.minikafka.common.record.RecordBatch;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import static org.junit.Assert.*;

public class LogTest {

    private File dir;
    private Log log;

    @Before
    public void setUp() throws IOException {
        dir = createTempDir();
        log = new Log(dir, LogConfig.testConfig(512)); // 小 segment，便于测试滚动
    }

    @After
    public void tearDown() throws IOException {
        log.close();
        deleteDir(dir);
    }

    @Test
    public void testAppendAndNextOffset() throws IOException {
        assertEquals(0L, log.logEndOffset());

        List<Record> records = Arrays.asList(
            new Record(0, System.currentTimeMillis(), null, "msg1".getBytes()),
            new Record(1, System.currentTimeMillis(), null, "msg2".getBytes())
        );
        long nextOffset = log.append(new RecordBatch(0L, records));
        assertEquals(2L, nextOffset);
        assertEquals(2L, log.logEndOffset());
    }

    @Test
    public void testReadBack() throws IOException {
        List<Record> records = Collections.singletonList(
            new Record(0, System.currentTimeMillis(), "k".getBytes(), "hello".getBytes())
        );
        log.append(new RecordBatch(0L, records));

        ByteBuffer result = log.read(0L, 4096);
        assertNotNull(result);
        RecordBatch decoded = RecordBatch.readFrom(result);
        assertArrayEquals("hello".getBytes(), decoded.records().get(0).value());
    }

    @Test
    public void testSegmentRolling() throws IOException {
        // 反复 append 直到触发 segment 滚动
        int count = 0;
        while (log.numberOfSegments() < 2 && count < 1000) {
            byte[] value = new byte[64];
            List<Record> records = Collections.singletonList(
                new Record(count, System.currentTimeMillis(), null, value)
            );
            log.append(new RecordBatch(count, records));
            count++;
        }
        assertTrue("Should have rolled to a new segment", log.numberOfSegments() >= 2);
    }

    private File createTempDir() throws IOException {
        File f = File.createTempFile("log-test", "");
        f.delete();
        f.mkdirs();
        return f;
    }

    private void deleteDir(File dir) {
        if (dir.isDirectory()) {
            for (File f : dir.listFiles()) f.delete();
        }
        dir.delete();
    }
}
```

- [ ] **Step 2: 运行测试，确认失败**

```bash
mvn clean test -pl mini-kafka-server -Dtest=LogTest -Dsort.skip=true
```
Expected: FAIL - compilation error

- [ ] **Step 3: 实现 Log.java**

```java
package com.github.minikafka.server.log;

import com.github.minikafka.common.record.RecordBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 单个 TopicPartition 的 Log，管理有序的 LogSegment 列表
 * 对应 Kafka kafka.log.Log
 */
public final class Log implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(Log.class);

    private final File dir;
    private final LogConfig config;
    private final List<LogSegment> segments = new ArrayList<>();
    private final AtomicLong logEndOffset = new AtomicLong(0);

    public Log(File dir, LogConfig config) throws IOException {
        this.dir = dir;
        this.config = config;
        dir.mkdirs();
        loadSegments();
    }

    private void loadSegments() throws IOException {
        File[] logFiles = dir.listFiles((d, name) -> name.endsWith(".log"));
        if (logFiles == null || logFiles.length == 0) {
            // 新分区：创建第一个 segment
            segments.add(new LogSegment(dir, 0L, config));
            return;
        }
        Arrays.sort(logFiles, Comparator.comparing(File::getName));
        for (File f : logFiles) {
            String name = f.getName().replace(".log", "");
            long baseOffset = Long.parseLong(name);
            segments.add(new LogSegment(dir, baseOffset, config));
        }
        // 恢复 logEndOffset（从最后一个 segment 扫描）
        // 简化：从最后一个 segment 的 baseOffset 开始往前估算
        recoverLogEndOffset();
    }

    private void recoverLogEndOffset() throws IOException {
        if (segments.isEmpty()) return;
        LogSegment last = activeSegment();
        // 读完最后一个 segment 累计 offset
        long offset = last.baseOffset();
        ByteBuffer buf;
        while ((buf = last.read(offset, 1024 * 1024)) != null && buf.hasRemaining()) {
            try {
                RecordBatch batch = RecordBatch.readFrom(buf);
                offset = batch.lastOffset() + 1;
            } catch (Exception e) {
                break;
            }
        }
        logEndOffset.set(offset);
    }

    /**
     * 追加 RecordBatch，返回下一条消息的 offset（logEndOffset）
     */
    public long append(RecordBatch batch) throws IOException {
        maybeRoll();
        activeSegment().append(batch);
        long nextOffset = batch.lastOffset() + 1;
        logEndOffset.set(nextOffset);
        return nextOffset;
    }

    /**
     * 从 startOffset 读取最多 maxLength 字节，跨 segment 查找
     */
    public ByteBuffer read(long startOffset, int maxLength) throws IOException {
        LogSegment segment = findSegment(startOffset);
        if (segment == null) return null;
        return segment.read(startOffset, maxLength);
    }

    private LogSegment findSegment(long offset) {
        // 二分查找 baseOffset <= offset 的最大 segment
        int lo = 0, hi = segments.size() - 1, result = -1;
        while (lo <= hi) {
            int mid = (lo + hi) >>> 1;
            if (segments.get(mid).baseOffset() <= offset) {
                result = mid;
                lo = mid + 1;
            } else {
                hi = mid - 1;
            }
        }
        return result >= 0 ? segments.get(result) : null;
    }

    private void maybeRoll() throws IOException {
        LogSegment active = activeSegment();
        long now = System.currentTimeMillis();
        if (active.isFull() || active.isExpired(now)) {
            log.info("Rolling log segment at offset {}", logEndOffset.get());
            active.flush();
            segments.add(new LogSegment(dir, logEndOffset.get(), config));
        }
    }

    private LogSegment activeSegment() {
        return segments.get(segments.size() - 1);
    }

    public long logEndOffset() { return logEndOffset.get(); }
    public int numberOfSegments() { return segments.size(); }

    public void flush() throws IOException {
        for (LogSegment seg : segments) seg.flush();
    }

    @Override
    public void close() throws IOException {
        for (LogSegment seg : segments) seg.close();
    }
}
```

- [ ] **Step 4: 运行测试，确认通过**

```bash
mvn clean test -pl mini-kafka-server -Dtest=LogTest -Dsort.skip=true
```
Expected: Tests run: 3, Failures: 0, Errors: 0

- [ ] **Step 5: Commit**

```bash
git add mini-kafka-server/src/main/java/com/github/minikafka/server/log/Log.java \
        mini-kafka-server/src/test/java/com/github/minikafka/server/log/LogTest.java
git commit -m "feat(server): add Log with segment rolling and TDD"
```

---

### Task 5: LogManager

**Files:**
- Create: `mini-kafka-server/src/main/java/com/github/minikafka/server/log/LogManager.java`

- [ ] **Step 1: 实现 LogManager.java**

```java
package com.github.minikafka.server.log;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;

/**
 * 管理所有 TopicPartition 的 Log 实例
 * 对应 Kafka kafka.log.LogManager
 */
public final class LogManager implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(LogManager.class);

    private final File baseDir;
    private final LogConfig defaultConfig;
    private final Map<String, Log> logs = new ConcurrentHashMap<>();

    public LogManager(File baseDir, LogConfig defaultConfig) throws IOException {
        this.baseDir = baseDir;
        this.defaultConfig = defaultConfig;
        baseDir.mkdirs();
        loadLogs();
    }

    private void loadLogs() throws IOException {
        File[] partitionDirs = baseDir.listFiles(File::isDirectory);
        if (partitionDirs == null) return;
        for (File dir : partitionDirs) {
            String key = dir.getName(); // format: topicName-partitionId
            logs.put(key, new Log(dir, defaultConfig));
            log.info("Loaded log for {}", key);
        }
    }

    public Log getOrCreateLog(String topic, int partition) throws IOException {
        String key = logKey(topic, partition);
        Log existing = logs.get(key);
        if (existing != null) return existing;
        synchronized (this) {
            existing = logs.get(key);
            if (existing != null) return existing;
            File dir = new File(baseDir, key);
            Log newLog = new Log(dir, defaultConfig);
            logs.put(key, newLog);
            log.info("Created new log for {}", key);
            return newLog;
        }
    }

    public Log getLog(String topic, int partition) {
        return logs.get(logKey(topic, partition));
    }

    private String logKey(String topic, int partition) {
        return topic + "-" + partition;
    }

    public void flush() throws IOException {
        for (Log l : logs.values()) l.flush();
    }

    @Override
    public void close() throws IOException {
        for (Log l : logs.values()) l.close();
    }
}
```

- [ ] **Step 2: 编译验证**

```bash
mvn clean compile -pl mini-kafka-server -Dsort.skip=true
```
Expected: BUILD SUCCESS

- [ ] **Step 3: Commit**

```bash
git add mini-kafka-server/src/main/java/com/github/minikafka/server/log/LogManager.java
git commit -m "feat(server): add LogManager"
```

---

### Task 6: GroupCoordinator 状态机

**Files:**
- Create: `mini-kafka-server/src/main/java/com/github/minikafka/server/coordinator/MemberMetadata.java`
- Create: `mini-kafka-server/src/main/java/com/github/minikafka/server/coordinator/GroupMetadata.java`
- Create: `mini-kafka-server/src/main/java/com/github/minikafka/server/coordinator/GroupCoordinator.java`
- Create: `mini-kafka-server/src/test/java/com/github/minikafka/server/coordinator/GroupCoordinatorTest.java`

- [ ] **Step 1: 写 MemberMetadata.java**

```java
package com.github.minikafka.server.coordinator;

public final class MemberMetadata {
    public final String memberId;
    public final String clientId;
    public final int sessionTimeoutMs;
    public byte[] assignment; // SyncGroup 后填充

    public MemberMetadata(String memberId, String clientId, int sessionTimeoutMs) {
        this.memberId = memberId;
        this.clientId = clientId;
        this.sessionTimeoutMs = sessionTimeoutMs;
    }
}
```

- [ ] **Step 2: 写 GroupMetadata.java**

```java
package com.github.minikafka.server.coordinator;

import java.util.*;

public final class GroupMetadata {

    public enum GroupState {
        Empty, PreparingRebalance, CompletingRebalance, Stable, Dead
    }

    private final String groupId;
    private GroupState state = GroupState.Empty;
    private int generationId = 0;
    private String leaderId;
    private String protocolName = "range";
    private final Map<String, MemberMetadata> members = new LinkedHashMap<>();
    private final Map<String, byte[]> committedOffsets = new HashMap<>(); // "topic-partition" -> offset bytes

    // JoinGroup 等待计数
    private int awaitingJoinCount = 0;
    private int awaitingSyncCount = 0;

    public GroupMetadata(String groupId) {
        this.groupId = groupId;
    }

    public String groupId() { return groupId; }
    public GroupState state() { return state; }
    public void setState(GroupState state) { this.state = state; }
    public int generationId() { return generationId; }
    public void incrementGeneration() { generationId++; }
    public String leaderId() { return leaderId; }
    public void setLeaderId(String leaderId) { this.leaderId = leaderId; }
    public Map<String, MemberMetadata> members() { return members; }

    public void addMember(MemberMetadata member) {
        if (members.isEmpty()) leaderId = member.memberId;
        members.put(member.memberId, member);
    }

    public void removeMember(String memberId) {
        members.remove(memberId);
        if (memberId.equals(leaderId) && !members.isEmpty()) {
            leaderId = members.keySet().iterator().next();
        }
    }

    public boolean hasMember(String memberId) {
        return members.containsKey(memberId);
    }

    public void commitOffset(String topic, int partition, long offset) {
        committedOffsets.put(topic + "-" + partition, longToBytes(offset));
    }

    public Long fetchOffset(String topic, int partition) {
        byte[] bytes = committedOffsets.get(topic + "-" + partition);
        return bytes == null ? -1L : bytesToLong(bytes);
    }

    private byte[] longToBytes(long v) {
        return new byte[]{
            (byte)(v >> 56), (byte)(v >> 48), (byte)(v >> 40), (byte)(v >> 32),
            (byte)(v >> 24), (byte)(v >> 16), (byte)(v >> 8), (byte) v
        };
    }

    private long bytesToLong(byte[] b) {
        long v = 0;
        for (byte x : b) v = (v << 8) | (x & 0xFF);
        return v;
    }
}
```

- [ ] **Step 3: 写 GroupCoordinator 失败测试**

```java
package com.github.minikafka.server.coordinator;

import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

public class GroupCoordinatorTest {

    private GroupCoordinator coordinator;

    @Before
    public void setUp() {
        coordinator = new GroupCoordinator(30000);
    }

    @Test
    public void testJoinGroupCreatesGroup() {
        GroupCoordinator.JoinResult result = coordinator.handleJoinGroup(
            "test-group", "", "client-1", 30000, "range"
        );
        assertEquals(GroupMetadata.GroupState.CompletingRebalance, result.state);
        assertNotNull(result.memberId);
        assertFalse(result.memberId.isEmpty());
    }

    @Test
    public void testSecondMemberTriggersRebalance() {
        coordinator.handleJoinGroup("test-group", "", "client-1", 30000, "range");
        GroupCoordinator.JoinResult r2 = coordinator.handleJoinGroup(
            "test-group", "", "client-2", 30000, "range"
        );
        assertEquals(GroupMetadata.GroupState.CompletingRebalance, r2.state);
    }

    @Test
    public void testSyncGroupStabilizes() {
        GroupCoordinator.JoinResult r1 = coordinator.handleJoinGroup(
            "g1", "", "c1", 30000, "range"
        );
        // leader 提交分配
        Map<String, byte[]> assignment = new java.util.HashMap<>();
        assignment.put(r1.memberId, new byte[]{0}); // partition 0 分配给 member
        GroupCoordinator.SyncResult sync = coordinator.handleSyncGroup(
            "g1", r1.generationId, r1.memberId, assignment
        );
        assertEquals(GroupMetadata.GroupState.Stable, sync.state);
        assertNotNull(sync.assignment);
    }

    @Test
    public void testHeartbeat() {
        GroupCoordinator.JoinResult r = coordinator.handleJoinGroup(
            "hb-group", "", "c1", 30000, "range"
        );
        coordinator.handleSyncGroup("hb-group", r.generationId, r.memberId, new java.util.HashMap<>());
        boolean ok = coordinator.handleHeartbeat("hb-group", r.generationId, r.memberId);
        assertTrue(ok);
    }

    @Test
    public void testLeaveGroup() {
        GroupCoordinator.JoinResult r = coordinator.handleJoinGroup(
            "leave-group", "", "c1", 30000, "range"
        );
        coordinator.handleLeaveGroup("leave-group", r.memberId);
        GroupMetadata meta = coordinator.getGroup("leave-group");
        assertFalse(meta.hasMember(r.memberId));
    }
}
```

- [ ] **Step 4: 运行测试，确认失败**

```bash
mvn clean test -pl mini-kafka-server -Dtest=GroupCoordinatorTest -Dsort.skip=true
```
Expected: FAIL - compilation error

- [ ] **Step 5: 实现 GroupCoordinator.java**

```java
package com.github.minikafka.server.coordinator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 服务端 GroupCoordinator，管理 ConsumerGroup 的 Rebalance 状态机
 * 简化：顺序处理，单组并发
 */
public final class GroupCoordinator {

    private static final Logger log = LoggerFactory.getLogger(GroupCoordinator.class);

    private final Map<String, GroupMetadata> groups = new ConcurrentHashMap<>();
    private final int defaultSessionTimeoutMs;
    private final AtomicInteger memberIdCounter = new AtomicInteger(0);

    public GroupCoordinator(int defaultSessionTimeoutMs) {
        this.defaultSessionTimeoutMs = defaultSessionTimeoutMs;
    }

    public static final class JoinResult {
        public final String memberId;
        public final int generationId;
        public final String leaderId;
        public final List<String> members; // only non-empty for leader
        public final GroupMetadata.GroupState state;

        JoinResult(String memberId, int generationId, String leaderId,
                   List<String> members, GroupMetadata.GroupState state) {
            this.memberId = memberId;
            this.generationId = generationId;
            this.leaderId = leaderId;
            this.members = members;
            this.state = state;
        }
    }

    public static final class SyncResult {
        public final byte[] assignment;
        public final GroupMetadata.GroupState state;

        SyncResult(byte[] assignment, GroupMetadata.GroupState state) {
            this.assignment = assignment;
            this.state = state;
        }
    }

    public synchronized JoinResult handleJoinGroup(
            String groupId, String memberId, String clientId,
            int sessionTimeoutMs, String protocol) {

        GroupMetadata group = groups.computeIfAbsent(groupId, GroupMetadata::new);

        // 新成员分配 memberId
        if (memberId == null || memberId.isEmpty()) {
            memberId = clientId + "-" + memberIdCounter.incrementAndGet();
        }

        // 状态转换：任何新成员加入都触发 Rebalance
        if (group.state() == GroupMetadata.GroupState.Stable
                || group.state() == GroupMetadata.GroupState.Empty) {
            group.setState(GroupMetadata.GroupState.PreparingRebalance);
            group.incrementGeneration();
        }

        MemberMetadata member = new MemberMetadata(memberId, clientId, sessionTimeoutMs);
        group.addMember(member);
        group.setState(GroupMetadata.GroupState.CompletingRebalance);

        // Leader 收到成员列表，Follower 收到空列表
        List<String> memberList = Collections.emptyList();
        if (memberId.equals(group.leaderId())) {
            memberList = new ArrayList<>(group.members().keySet());
        }

        log.info("Member {} joined group {}, generation={}", memberId, groupId, group.generationId());
        return new JoinResult(memberId, group.generationId(), group.leaderId(), memberList, group.state());
    }

    public synchronized SyncResult handleSyncGroup(
            String groupId, int generationId, String memberId,
            Map<String, byte[]> assignment) {

        GroupMetadata group = groups.get(groupId);
        if (group == null) {
            return new SyncResult(null, GroupMetadata.GroupState.Dead);
        }

        // Leader 提交分配结果，分发给所有成员
        if (memberId.equals(group.leaderId()) && !assignment.isEmpty()) {
            for (Map.Entry<String, byte[]> e : assignment.entrySet()) {
                MemberMetadata m = group.members().get(e.getKey());
                if (m != null) m.assignment = e.getValue();
            }
            group.setState(GroupMetadata.GroupState.Stable);
            log.info("Group {} stabilized at generation {}", groupId, group.generationId());
        }

        byte[] myAssignment = new byte[0];
        MemberMetadata me = group.members().get(memberId);
        if (me != null && me.assignment != null) {
            myAssignment = me.assignment;
        }

        return new SyncResult(myAssignment, group.state());
    }

    public synchronized boolean handleHeartbeat(String groupId, int generationId, String memberId) {
        GroupMetadata group = groups.get(groupId);
        if (group == null || !group.hasMember(memberId)) return false;
        if (group.generationId() != generationId) return false;
        return group.state() == GroupMetadata.GroupState.Stable;
    }

    public synchronized void handleLeaveGroup(String groupId, String memberId) {
        GroupMetadata group = groups.get(groupId);
        if (group == null) return;
        group.removeMember(memberId);
        if (group.members().isEmpty()) {
            group.setState(GroupMetadata.GroupState.Empty);
        } else {
            group.setState(GroupMetadata.GroupState.PreparingRebalance);
        }
        log.info("Member {} left group {}", memberId, groupId);
    }

    public void commitOffset(String groupId, String topic, int partition, long offset) {
        GroupMetadata group = groups.get(groupId);
        if (group != null) group.commitOffset(topic, partition, offset);
    }

    public long fetchOffset(String groupId, String topic, int partition) {
        GroupMetadata group = groups.get(groupId);
        return group == null ? -1L : group.fetchOffset(topic, partition);
    }

    public GroupMetadata getGroup(String groupId) {
        return groups.get(groupId);
    }
}
```

- [ ] **Step 6: 运行测试，确认通过**

```bash
mvn clean test -pl mini-kafka-server -Dtest=GroupCoordinatorTest -Dsort.skip=true
```
Expected: Tests run: 5, Failures: 0, Errors: 0

- [ ] **Step 7: Commit**

```bash
git add mini-kafka-server/src/main/java/com/github/minikafka/server/coordinator/ \
        mini-kafka-server/src/test/java/com/github/minikafka/server/coordinator/
git commit -m "feat(server): add GroupCoordinator with Rebalance state machine and TDD"
```

---

### Task 7: KafkaController + ControllerContext

**Files:**
- Create: `mini-kafka-server/src/main/java/com/github/minikafka/server/controller/ControllerContext.java`
- Create: `mini-kafka-server/src/main/java/com/github/minikafka/server/controller/KafkaController.java`

- [ ] **Step 1: 写 ControllerContext.java**

```java
package com.github.minikafka.server.controller;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 集群元数据内存视图
 * 存储 topic → partition count → leader 映射
 */
public final class ControllerContext {

    // topic → partitionCount
    private final Map<String, Integer> topicPartitions = new ConcurrentHashMap<>();
    // "topic-partition" → leaderId (单节点固定为 0)
    private final Map<String, Integer> partitionLeaders = new ConcurrentHashMap<>();

    public void createTopic(String topic, int partitions) {
        topicPartitions.put(topic, partitions);
        for (int i = 0; i < partitions; i++) {
            partitionLeaders.put(topic + "-" + i, 0); // 单节点，broker id = 0
        }
    }

    public boolean topicExists(String topic) {
        return topicPartitions.containsKey(topic);
    }

    public int partitionCount(String topic) {
        return topicPartitions.getOrDefault(topic, 0);
    }

    public Integer leaderFor(String topic, int partition) {
        return partitionLeaders.get(topic + "-" + partition);
    }

    public Map<String, Integer> allTopics() {
        return Collections.unmodifiableMap(topicPartitions);
    }
}
```

- [ ] **Step 2: 写 KafkaController.java**

```java
package com.github.minikafka.server.controller;

import com.github.minikafka.server.log.LogManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;

/**
 * 单节点 Controller，负责 Topic 创建和 Leader 分配
 * 简化版：同步调用，无事件队列
 */
public final class KafkaController {

    private static final Logger log = LoggerFactory.getLogger(KafkaController.class);

    private final ControllerContext context;
    private final LogManager logManager;

    public KafkaController(ControllerContext context, LogManager logManager) {
        this.context = context;
        this.logManager = logManager;
    }

    public synchronized void createTopic(String topic, int partitions) throws IOException {
        if (context.topicExists(topic)) {
            log.warn("Topic {} already exists", topic);
            return;
        }
        // 创建每个 partition 的 Log
        for (int i = 0; i < partitions; i++) {
            logManager.getOrCreateLog(topic, i);
        }
        context.createTopic(topic, partitions);
        log.info("Created topic {} with {} partitions", topic, partitions);
    }

    public ControllerContext context() { return context; }
}
```

- [ ] **Step 3: 编译验证**

```bash
mvn clean compile -pl mini-kafka-server -Dsort.skip=true
```
Expected: BUILD SUCCESS

- [ ] **Step 4: Commit**

```bash
git add mini-kafka-server/src/main/java/com/github/minikafka/server/controller/
git commit -m "feat(server): add KafkaController and ControllerContext"
```

---

### Task 8: 网络层 — RequestChannel + NetworkReceive

**Files:**
- Create: `mini-kafka-server/src/main/java/com/github/minikafka/server/network/NetworkReceive.java`
- Create: `mini-kafka-server/src/main/java/com/github/minikafka/server/network/RequestChannel.java`

- [ ] **Step 1: 写 NetworkReceive.java**

```java
package com.github.minikafka.server.network;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

/**
 * 从 Channel 读取一个完整的带4字节长度前缀的消息帧
 * 对应 Kafka kafka.network.NetworkReceive
 */
public final class NetworkReceive {

    private final ByteBuffer sizeBuffer = ByteBuffer.allocate(4);
    private ByteBuffer payload;
    private boolean complete = false;

    public boolean readFrom(ReadableByteChannel channel) throws IOException {
        if (!sizeBuffer.hasRemaining()) {
            // size 已读完，读 payload
            if (payload == null) {
                sizeBuffer.flip();
                int size = sizeBuffer.getInt();
                payload = ByteBuffer.allocate(size);
            }
            channel.read(payload);
            if (!payload.hasRemaining()) {
                payload.flip();
                complete = true;
            }
        } else {
            channel.read(sizeBuffer);
            if (!sizeBuffer.hasRemaining()) {
                sizeBuffer.flip();
                int size = sizeBuffer.getInt();
                payload = ByteBuffer.allocate(size);
                channel.read(payload);
                if (!payload.hasRemaining()) {
                    payload.flip();
                    complete = true;
                }
            }
        }
        return complete;
    }

    public ByteBuffer payload() { return payload; }
    public boolean complete() { return complete; }
}
```

- [ ] **Step 2: 写 RequestChannel.java**

```java
package com.github.minikafka.server.network;

import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * Processor 和 KafkaRequestHandler 之间的请求/响应通道
 * 对应 Kafka kafka.network.RequestChannel
 */
public final class RequestChannel {

    public static final class Request {
        public final int processorId;
        public final SocketChannel channel;
        public final ByteBuffer buffer;
        public final long startTimeMs;

        public Request(int processorId, SocketChannel channel, ByteBuffer buffer) {
            this.processorId = processorId;
            this.channel = channel;
            this.buffer = buffer;
            this.startTimeMs = System.currentTimeMillis();
        }
    }

    public static final class Response {
        public final int processorId;
        public final SocketChannel channel;
        public final ByteBuffer buffer;

        public Response(int processorId, SocketChannel channel, ByteBuffer buffer) {
            this.processorId = processorId;
            this.channel = channel;
            this.buffer = buffer;
        }
    }

    private final BlockingQueue<Request> requestQueue;
    private final BlockingQueue<Response> responseQueue;

    public RequestChannel(int queueSize) {
        this.requestQueue = new ArrayBlockingQueue<>(queueSize);
        this.responseQueue = new ArrayBlockingQueue<>(queueSize * 2);
    }

    public void sendRequest(Request request) throws InterruptedException {
        requestQueue.put(request);
    }

    public Request receiveRequest(long timeoutMs) throws InterruptedException {
        return requestQueue.poll(timeoutMs, java.util.concurrent.TimeUnit.MILLISECONDS);
    }

    public void sendResponse(Response response) throws InterruptedException {
        responseQueue.put(response);
    }

    public Response receiveResponse() {
        return responseQueue.poll();
    }
}
```

- [ ] **Step 3: 编译验证**

```bash
mvn clean compile -pl mini-kafka-server -Dsort.skip=true
```
Expected: BUILD SUCCESS

- [ ] **Step 4: Commit**

```bash
git add mini-kafka-server/src/main/java/com/github/minikafka/server/network/
git commit -m "feat(server): add NetworkReceive and RequestChannel"
```

---

### Task 9: 网络层 — Acceptor + Processor + SocketServer

**Files:**
- Create: `mini-kafka-server/src/main/java/com/github/minikafka/server/network/Acceptor.java`
- Create: `mini-kafka-server/src/main/java/com/github/minikafka/server/network/Processor.java`
- Create: `mini-kafka-server/src/main/java/com/github/minikafka/server/network/SocketServer.java`

- [ ] **Step 1: 实现 Processor.java**

```java
package com.github.minikafka.server.network;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * 管理一批 SocketChannel，读取完整请求帧后放入 RequestChannel
 * 同时将 ResponseQueue 中的响应写回客户端
 */
public final class Processor implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(Processor.class);

    private final int id;
    private final RequestChannel requestChannel;
    private final Selector selector;
    private final Queue<SocketChannel> newConnections = new ConcurrentLinkedQueue<>();
    private final Map<SocketChannel, NetworkReceive> inflightReads = new HashMap<>();
    private volatile boolean running = true;

    public Processor(int id, RequestChannel requestChannel) throws IOException {
        this.id = id;
        this.requestChannel = requestChannel;
        this.selector = Selector.open();
    }

    public void accept(SocketChannel channel) {
        newConnections.add(channel);
        selector.wakeup();
    }

    @Override
    public void run() {
        while (running) {
            try {
                // 注册新连接
                SocketChannel newConn;
                while ((newConn = newConnections.poll()) != null) {
                    newConn.configureBlocking(false);
                    newConn.register(selector, SelectionKey.OP_READ);
                    inflightReads.put(newConn, new NetworkReceive());
                }

                // 处理就绪事件
                int ready = selector.select(300);
                if (ready > 0) {
                    Iterator<SelectionKey> iter = selector.selectedKeys().iterator();
                    while (iter.hasNext()) {
                        SelectionKey key = iter.next();
                        iter.remove();
                        if (key.isReadable()) {
                            handleRead(key);
                        } else if (key.isWritable()) {
                            handleWrite(key);
                        }
                    }
                }

                // 写回响应
                sendPendingResponses();

            } catch (Exception e) {
                if (running) log.error("Processor {} error", id, e);
            }
        }
    }

    private void handleRead(SelectionKey key) throws IOException, InterruptedException {
        SocketChannel channel = (SocketChannel) key.channel();
        NetworkReceive receive = inflightReads.get(channel);
        if (receive == null) return;

        boolean complete = receive.readFrom(channel);
        if (complete) {
            ByteBuffer payload = receive.payload().duplicate();
            requestChannel.sendRequest(new RequestChannel.Request(id, channel, payload));
            inflightReads.put(channel, new NetworkReceive()); // 重置，准备下次读
        }
    }

    private void handleWrite(SelectionKey key) throws IOException {
        SocketChannel channel = (SocketChannel) key.channel();
        ByteBuffer buf = (ByteBuffer) key.attachment();
        channel.write(buf);
        if (!buf.hasRemaining()) {
            key.interestOps(SelectionKey.OP_READ);
        }
    }

    private void sendPendingResponses() throws IOException {
        RequestChannel.Response response;
        while ((response = requestChannel.receiveResponse()) != null) {
            if (response.processorId != id) {
                // 不是这个 Processor 的响应，放回去（简化处理）
                try { requestChannel.sendResponse(response); } catch (InterruptedException ignored) {}
                break;
            }
            SocketChannel channel = response.channel;
            SelectionKey key = channel.keyFor(selector);
            if (key != null && key.isValid()) {
                key.attach(response.buffer);
                key.interestOps(SelectionKey.OP_WRITE);
                selector.wakeup();
                // 直接写
                channel.write(response.buffer);
                if (!response.buffer.hasRemaining()) {
                    key.interestOps(SelectionKey.OP_READ);
                }
            }
        }
    }

    public void shutdown() {
        running = false;
        selector.wakeup();
    }
}
```

- [ ] **Step 2: 实现 Acceptor.java**

```java
package com.github.minikafka.server.network;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.*;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 监听端口，accept 新连接并轮询分发给 Processor
 */
public final class Acceptor implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(Acceptor.class);

    private final int port;
    private final Processor[] processors;
    private final ServerSocketChannel serverChannel;
    private final Selector selector;
    private final AtomicInteger nextProcessor = new AtomicInteger(0);
    private volatile boolean running = true;

    public Acceptor(int port, Processor[] processors) throws IOException {
        this.port = port;
        this.processors = processors;
        this.serverChannel = ServerSocketChannel.open();
        this.serverChannel.configureBlocking(false);
        this.serverChannel.bind(new InetSocketAddress(port));
        this.selector = Selector.open();
        this.serverChannel.register(selector, SelectionKey.OP_ACCEPT);
    }

    @Override
    public void run() {
        log.info("Acceptor listening on port {}", port);
        while (running) {
            try {
                int ready = selector.select(500);
                if (ready > 0) {
                    Iterator<SelectionKey> iter = selector.selectedKeys().iterator();
                    while (iter.hasNext()) {
                        SelectionKey key = iter.next();
                        iter.remove();
                        if (key.isAcceptable()) {
                            SocketChannel client = serverChannel.accept();
                            if (client != null) {
                                int idx = nextProcessor.getAndIncrement() % processors.length;
                                processors[idx].accept(client);
                                log.debug("Accepted connection, assigned to processor {}", idx);
                            }
                        }
                    }
                }
            } catch (Exception e) {
                if (running) log.error("Acceptor error", e);
            }
        }
    }

    public void shutdown() throws IOException {
        running = false;
        selector.wakeup();
        serverChannel.close();
    }
}
```

- [ ] **Step 3: 实现 SocketServer.java**

```java
package com.github.minikafka.server.network;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;

/**
 * 组装并启动 Acceptor + Processor 线程
 * 对应 Kafka kafka.network.SocketServer
 */
public final class SocketServer {

    private static final Logger log = LoggerFactory.getLogger(SocketServer.class);

    private final RequestChannel requestChannel;
    private final Acceptor acceptor;
    private final Processor[] processors;
    private final Thread acceptorThread;
    private final Thread[] processorThreads;

    public SocketServer(int port, int numProcessors, RequestChannel requestChannel) throws IOException {
        this.requestChannel = requestChannel;
        this.processors = new Processor[numProcessors];
        this.processorThreads = new Thread[numProcessors];

        for (int i = 0; i < numProcessors; i++) {
            processors[i] = new Processor(i, requestChannel);
            processorThreads[i] = new Thread(processors[i], "kafka-network-thread-" + i);
            processorThreads[i].setDaemon(true);
        }
        this.acceptor = new Acceptor(port, processors);
        this.acceptorThread = new Thread(acceptor, "kafka-acceptor");
        this.acceptorThread.setDaemon(true);
    }

    public void startup() {
        for (Thread t : processorThreads) t.start();
        acceptorThread.start();
        log.info("SocketServer started");
    }

    public void shutdown() throws IOException, InterruptedException {
        acceptor.shutdown();
        for (Processor p : processors) p.shutdown();
        acceptorThread.join(5000);
        for (Thread t : processorThreads) t.join(5000);
        log.info("SocketServer stopped");
    }
}
```

- [ ] **Step 4: 编译验证**

```bash
mvn clean compile -pl mini-kafka-server -Dsort.skip=true
```
Expected: BUILD SUCCESS

- [ ] **Step 5: Commit**

```bash
git add mini-kafka-server/src/main/java/com/github/minikafka/server/network/
git commit -m "feat(server): add NIO network layer (Acceptor, Processor, SocketServer)"
```

---

### Task 10: KafkaApis + KafkaRequestHandler + KafkaServer

**Files:**
- Create: `mini-kafka-server/src/main/java/com/github/minikafka/server/api/KafkaApis.java`
- Create: `mini-kafka-server/src/main/java/com/github/minikafka/server/api/KafkaRequestHandler.java`
- Create: `mini-kafka-server/src/main/java/com/github/minikafka/server/KafkaServer.java`

- [ ] **Step 1: 实现 KafkaApis.java**

```java
package com.github.minikafka.server.api;

import com.github.minikafka.common.network.ByteBufferUtils;
import com.github.minikafka.common.protocol.*;
import com.github.minikafka.common.record.RecordBatch;
import com.github.minikafka.server.controller.KafkaController;
import com.github.minikafka.server.coordinator.GroupCoordinator;
import com.github.minikafka.server.coordinator.GroupMetadata;
import com.github.minikafka.server.log.Log;
import com.github.minikafka.server.log.LogManager;
import com.github.minikafka.server.network.RequestChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * 请求分发器，处理所有 ApiKeys 对应的逻辑
 * 对应 Kafka kafka.server.KafkaApis
 */
public final class KafkaApis {

    private static final Logger log = LoggerFactory.getLogger(KafkaApis.class);

    private final LogManager logManager;
    private final KafkaController controller;
    private final GroupCoordinator groupCoordinator;

    public KafkaApis(LogManager logManager, KafkaController controller,
                     GroupCoordinator groupCoordinator) {
        this.logManager = logManager;
        this.controller = controller;
        this.groupCoordinator = groupCoordinator;
    }

    public ByteBuffer handle(RequestChannel.Request request) {
        ByteBuffer buf = request.buffer.duplicate();
        RequestHeader header = RequestHeader.parse(buf);
        ApiKeys apiKey = ApiKeys.forId(header.apiKey());
        log.debug("Handling {} request correlationId={}", apiKey.name, header.correlationId());

        ByteBuffer responseBody;
        try {
            switch (apiKey) {
                case PRODUCE:       responseBody = handleProduce(buf); break;
                case FETCH:         responseBody = handleFetch(buf); break;
                case METADATA:      responseBody = handleMetadata(buf); break;
                case CREATE_TOPICS: responseBody = handleCreateTopics(buf); break;
                case JOIN_GROUP:    responseBody = handleJoinGroup(buf); break;
                case SYNC_GROUP:    responseBody = handleSyncGroup(buf); break;
                case HEARTBEAT:     responseBody = handleHeartbeat(buf); break;
                case LEAVE_GROUP:   responseBody = handleLeaveGroup(buf); break;
                case OFFSET_COMMIT: responseBody = handleOffsetCommit(buf); break;
                case OFFSET_FETCH:  responseBody = handleOffsetFetch(buf); break;
                default:
                    log.warn("Unsupported api key: {}", apiKey);
                    responseBody = buildErrorResponse(Errors.UNKNOWN_SERVER_ERROR);
            }
        } catch (Exception e) {
            log.error("Error handling {}", apiKey, e);
            responseBody = buildErrorResponse(Errors.UNKNOWN_SERVER_ERROR);
        }

        return buildResponse(header.correlationId(), responseBody);
    }

    // ---- PRODUCE ----
    // Request: topicCount(4) [topic(string) partitionCount(4) [partition(4) recordBatchSize(4) recordBatch(bytes)]]
    // Response: topicCount(4) [topic(string) partitionCount(4) [partition(4) errorCode(2) baseOffset(8)]]
    private ByteBuffer handleProduce(ByteBuffer buf) throws IOException {
        int topicCount = buf.getInt();
        Map<String, Map<Integer, Long>> results = new LinkedHashMap<>();

        for (int t = 0; t < topicCount; t++) {
            String topic = ByteBufferUtils.readString(buf);
            int partCount = buf.getInt();
            Map<Integer, Long> partResults = new LinkedHashMap<>();
            for (int p = 0; p < partCount; p++) {
                int partition = buf.getInt();
                int batchSize = buf.getInt();
                ByteBuffer batchBuf = buf.slice();
                batchBuf.limit(batchSize);
                buf.position(buf.position() + batchSize);

                RecordBatch batch = RecordBatch.readFrom(batchBuf);
                Log partLog = logManager.getOrCreateLog(topic, partition);
                long nextOffset = partLog.append(batch);
                partResults.put(partition, batch.baseOffset());
            }
            results.put(topic, partResults);
        }

        // 构建 response body
        int size = 4; // topicCount
        for (Map.Entry<String, Map<Integer, Long>> e : results.entrySet()) {
            size += ByteBufferUtils.sizeOfString(e.getKey()) + 4; // partCount
            size += e.getValue().size() * (4 + 2 + 8); // partition + errorCode + offset
        }
        ByteBuffer resp = ByteBuffer.allocate(size);
        resp.putInt(results.size());
        for (Map.Entry<String, Map<Integer, Long>> e : results.entrySet()) {
            ByteBufferUtils.writeString(resp, e.getKey());
            resp.putInt(e.getValue().size());
            for (Map.Entry<Integer, Long> pe : e.getValue().entrySet()) {
                resp.putInt(pe.getKey());
                resp.putShort(Errors.NONE.code);
                resp.putLong(pe.getValue());
            }
        }
        resp.flip();
        return resp;
    }

    // ---- FETCH ----
    // Request: topicCount(4) [topic(string) partCount(4) [partition(4) fetchOffset(8) maxBytes(4)]]
    // Response: topicCount(4) [topic(string) partCount(4) [partition(4) errorCode(2) highWatermark(8) recordSize(4) records(bytes)]]
    private ByteBuffer handleFetch(ByteBuffer buf) throws IOException {
        buf.getInt(); // replicaId（固定-1）
        buf.getInt(); // maxWaitMs
        buf.getInt(); // minBytes
        int topicCount = buf.getInt();

        ByteBuffer respBody = ByteBuffer.allocate(1024 * 1024);
        respBody.putInt(topicCount);

        for (int t = 0; t < topicCount; t++) {
            String topic = ByteBufferUtils.readString(buf);
            int partCount = buf.getInt();
            ByteBufferUtils.writeString(respBody, topic);
            respBody.putInt(partCount);

            for (int p = 0; p < partCount; p++) {
                int partition = buf.getInt();
                long fetchOffset = buf.getLong();
                int maxBytes = buf.getInt();

                respBody.putInt(partition);
                Log partLog = logManager.getLog(topic, partition);
                if (partLog == null) {
                    respBody.putShort(Errors.UNKNOWN_TOPIC_OR_PARTITION.code);
                    respBody.putLong(0L); // highWatermark
                    respBody.putInt(0);  // recordSize
                } else {
                    ByteBuffer records = partLog.read(fetchOffset, maxBytes);
                    respBody.putShort(Errors.NONE.code);
                    respBody.putLong(partLog.logEndOffset()); // highWatermark
                    if (records != null && records.hasRemaining()) {
                        respBody.putInt(records.remaining());
                        respBody.put(records);
                    } else {
                        respBody.putInt(0);
                    }
                }
            }
        }
        respBody.flip();
        return respBody;
    }

    // ---- METADATA ----
    // Request: topicCount(4) [topic(string)]
    // Response: brokerCount(4) [nodeId(4) host(string) port(4)]
    //           topicCount(4) [topic(string) errorCode(2) partCount(4) [partition(4) leader(4) errorCode(2)]]
    private ByteBuffer handleMetadata(ByteBuffer buf) {
        int topicCount = buf.getInt();
        List<String> requestedTopics = new ArrayList<>();
        for (int i = 0; i < topicCount; i++) requestedTopics.add(ByteBufferUtils.readString(buf));

        if (requestedTopics.isEmpty()) {
            requestedTopics.addAll(controller.context().allTopics().keySet());
        }

        ByteBuffer resp = ByteBuffer.allocate(4096);
        resp.putInt(1); // 1 broker
        resp.putInt(0); // nodeId = 0
        ByteBufferUtils.writeString(resp, "localhost");
        resp.putInt(9092);

        resp.putInt(requestedTopics.size());
        for (String topic : requestedTopics) {
            ByteBufferUtils.writeString(resp, topic);
            if (!controller.context().topicExists(topic)) {
                resp.putShort(Errors.UNKNOWN_TOPIC_OR_PARTITION.code);
                resp.putInt(0);
            } else {
                resp.putShort(Errors.NONE.code);
                int partitions = controller.context().partitionCount(topic);
                resp.putInt(partitions);
                for (int i = 0; i < partitions; i++) {
                    resp.putInt(i);
                    resp.putInt(0); // leader = broker 0
                    resp.putShort(Errors.NONE.code);
                }
            }
        }
        resp.flip();
        return resp;
    }

    // ---- CREATE_TOPICS ----
    // Request: topicCount(4) [topic(string) partitions(4)]
    // Response: topicCount(4) [topic(string) errorCode(2)]
    private ByteBuffer handleCreateTopics(ByteBuffer buf) throws IOException {
        int topicCount = buf.getInt();
        Map<String, Short> results = new LinkedHashMap<>();
        for (int i = 0; i < topicCount; i++) {
            String topic = ByteBufferUtils.readString(buf);
            int partitions = buf.getInt();
            try {
                controller.createTopic(topic, partitions);
                results.put(topic, Errors.NONE.code);
            } catch (Exception e) {
                results.put(topic, Errors.UNKNOWN_SERVER_ERROR.code);
            }
        }
        int size = 4;
        for (String t : results.keySet()) size += ByteBufferUtils.sizeOfString(t) + 2;
        ByteBuffer resp = ByteBuffer.allocate(size);
        resp.putInt(results.size());
        for (Map.Entry<String, Short> e : results.entrySet()) {
            ByteBufferUtils.writeString(resp, e.getKey());
            resp.putShort(e.getValue());
        }
        resp.flip();
        return resp;
    }

    // ---- JOIN_GROUP ----
    // Request: groupId(string) sessionTimeoutMs(4) memberId(string) protocolType(string)
    // Response: errorCode(2) generationId(4) protocol(string) leaderId(string) memberId(string) memberCount(4) [memberId(string)]
    private ByteBuffer handleJoinGroup(ByteBuffer buf) {
        String groupId = ByteBufferUtils.readString(buf);
        int sessionTimeoutMs = buf.getInt();
        String memberId = ByteBufferUtils.readString(buf);
        String protocolType = ByteBufferUtils.readString(buf);

        GroupCoordinator.JoinResult result = groupCoordinator.handleJoinGroup(
            groupId, memberId, "client", sessionTimeoutMs, "range"
        );

        int size = 2 + 4
            + ByteBufferUtils.sizeOfString("range")
            + ByteBufferUtils.sizeOfString(result.leaderId)
            + ByteBufferUtils.sizeOfString(result.memberId)
            + 4;
        for (String m : result.members) size += ByteBufferUtils.sizeOfString(m);

        ByteBuffer resp = ByteBuffer.allocate(size);
        resp.putShort(Errors.NONE.code);
        resp.putInt(result.generationId);
        ByteBufferUtils.writeString(resp, "range");
        ByteBufferUtils.writeString(resp, result.leaderId);
        ByteBufferUtils.writeString(resp, result.memberId);
        resp.putInt(result.members.size());
        for (String m : result.members) ByteBufferUtils.writeString(resp, m);
        resp.flip();
        return resp;
    }

    // ---- SYNC_GROUP ----
    // Request: groupId(string) generationId(4) memberId(string) assignmentCount(4) [memberId(string) assignment(bytes)]
    // Response: errorCode(2) assignment(bytes)
    private ByteBuffer handleSyncGroup(ByteBuffer buf) {
        String groupId = ByteBufferUtils.readString(buf);
        int generationId = buf.getInt();
        String memberId = ByteBufferUtils.readString(buf);
        int count = buf.getInt();
        Map<String, byte[]> assignment = new HashMap<>();
        for (int i = 0; i < count; i++) {
            String mid = ByteBufferUtils.readString(buf);
            byte[] assign = ByteBufferUtils.readBytes(buf);
            assignment.put(mid, assign);
        }

        GroupCoordinator.SyncResult result = groupCoordinator.handleSyncGroup(
            groupId, generationId, memberId, assignment
        );

        ByteBuffer resp = ByteBuffer.allocate(2 + ByteBufferUtils.sizeOfBytes(result.assignment));
        resp.putShort(Errors.NONE.code);
        ByteBufferUtils.writeBytes(resp, result.assignment);
        resp.flip();
        return resp;
    }

    // ---- HEARTBEAT ----
    // Request: groupId(string) generationId(4) memberId(string)
    // Response: errorCode(2)
    private ByteBuffer handleHeartbeat(ByteBuffer buf) {
        String groupId = ByteBufferUtils.readString(buf);
        int generationId = buf.getInt();
        String memberId = ByteBufferUtils.readString(buf);
        boolean ok = groupCoordinator.handleHeartbeat(groupId, generationId, memberId);
        ByteBuffer resp = ByteBuffer.allocate(2);
        resp.putShort(ok ? Errors.NONE.code : Errors.REBALANCE_IN_PROGRESS.code);
        resp.flip();
        return resp;
    }

    // ---- LEAVE_GROUP ----
    // Request: groupId(string) memberId(string)
    // Response: errorCode(2)
    private ByteBuffer handleLeaveGroup(ByteBuffer buf) {
        String groupId = ByteBufferUtils.readString(buf);
        String memberId = ByteBufferUtils.readString(buf);
        groupCoordinator.handleLeaveGroup(groupId, memberId);
        ByteBuffer resp = ByteBuffer.allocate(2);
        resp.putShort(Errors.NONE.code);
        resp.flip();
        return resp;
    }

    // ---- OFFSET_COMMIT ----
    // Request: groupId(string) generationId(4) memberId(string) topicCount(4) [topic(string) partCount(4) [partition(4) offset(8)]]
    // Response: topicCount(4) [topic(string) partCount(4) [partition(4) errorCode(2)]]
    private ByteBuffer handleOffsetCommit(ByteBuffer buf) {
        String groupId = ByteBufferUtils.readString(buf);
        buf.getInt(); // generationId
        ByteBufferUtils.readString(buf); // memberId
        int topicCount = buf.getInt();
        for (int t = 0; t < topicCount; t++) {
            String topic = ByteBufferUtils.readString(buf);
            int partCount = buf.getInt();
            for (int p = 0; p < partCount; p++) {
                int partition = buf.getInt();
                long offset = buf.getLong();
                groupCoordinator.commitOffset(groupId, topic, partition, offset);
            }
        }
        ByteBuffer resp = ByteBuffer.allocate(4);
        resp.putInt(0);
        resp.flip();
        return resp;
    }

    // ---- OFFSET_FETCH ----
    // Request: groupId(string) topicCount(4) [topic(string) partCount(4) [partition(4)]]
    // Response: topicCount(4) [topic(string) partCount(4) [partition(4) offset(8) errorCode(2)]]
    private ByteBuffer handleOffsetFetch(ByteBuffer buf) {
        String groupId = ByteBufferUtils.readString(buf);
        int topicCount = buf.getInt();
        ByteBuffer resp = ByteBuffer.allocate(65536);
        resp.putInt(topicCount);
        for (int t = 0; t < topicCount; t++) {
            String topic = ByteBufferUtils.readString(buf);
            int partCount = buf.getInt();
            ByteBufferUtils.writeString(resp, topic);
            resp.putInt(partCount);
            for (int p = 0; p < partCount; p++) {
                int partition = buf.getInt();
                long offset = groupCoordinator.fetchOffset(groupId, topic, partition);
                resp.putInt(partition);
                resp.putLong(offset);
                resp.putShort(Errors.NONE.code);
            }
        }
        resp.flip();
        return resp;
    }

    private ByteBuffer buildErrorResponse(Errors error) {
        ByteBuffer buf = ByteBuffer.allocate(2);
        buf.putShort(error.code);
        buf.flip();
        return buf;
    }

    private ByteBuffer buildResponse(int correlationId, ByteBuffer body) {
        int totalSize = ResponseHeader.SIZE + body.remaining();
        ByteBuffer response = ByteBuffer.allocate(4 + totalSize);
        response.putInt(totalSize); // length prefix
        new ResponseHeader(correlationId).writeTo(response);
        response.put(body);
        response.flip();
        return response;
    }
}
```

- [ ] **Step 2: 实现 KafkaRequestHandler.java**

```java
package com.github.minikafka.server.api;

import com.github.minikafka.server.network.RequestChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.concurrent.*;

/**
 * IO 线程池，从 RequestChannel 取请求，调用 KafkaApis 处理后写回响应
 */
public final class KafkaRequestHandler {

    private static final Logger log = LoggerFactory.getLogger(KafkaRequestHandler.class);

    private final ExecutorService executor;
    private final RequestChannel requestChannel;
    private final KafkaApis apis;
    private volatile boolean running = true;
    private Thread dispatchThread;

    public KafkaRequestHandler(int numThreads, RequestChannel requestChannel, KafkaApis apis) {
        this.executor = Executors.newFixedThreadPool(numThreads,
            r -> new Thread(r, "kafka-request-handler-" + System.nanoTime()));
        this.requestChannel = requestChannel;
        this.apis = apis;
    }

    public void startup() {
        dispatchThread = new Thread(this::dispatchLoop, "kafka-request-dispatcher");
        dispatchThread.setDaemon(true);
        dispatchThread.start();
    }

    private void dispatchLoop() {
        while (running) {
            try {
                RequestChannel.Request request = requestChannel.receiveRequest(100);
                if (request == null) continue;
                executor.submit(() -> {
                    try {
                        ByteBuffer response = apis.handle(request);
                        requestChannel.sendResponse(new RequestChannel.Response(
                            request.processorId, request.channel, response
                        ));
                    } catch (Exception e) {
                        log.error("Error processing request", e);
                    }
                });
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    public void shutdown() throws InterruptedException {
        running = false;
        executor.shutdown();
        executor.awaitTermination(5, TimeUnit.SECONDS);
        if (dispatchThread != null) dispatchThread.join(3000);
    }
}
```

- [ ] **Step 3: 实现 KafkaServer.java**

```java
package com.github.minikafka.server;

import com.github.minikafka.server.api.KafkaApis;
import com.github.minikafka.server.api.KafkaRequestHandler;
import com.github.minikafka.server.controller.ControllerContext;
import com.github.minikafka.server.controller.KafkaController;
import com.github.minikafka.server.coordinator.GroupCoordinator;
import com.github.minikafka.server.log.LogConfig;
import com.github.minikafka.server.log.LogManager;
import com.github.minikafka.server.network.RequestChannel;
import com.github.minikafka.server.network.SocketServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.File;

public final class KafkaServer {

    private static final Logger log = LoggerFactory.getLogger(KafkaServer.class);

    private final KafkaConfig config;
    private LogManager logManager;
    private KafkaController controller;
    private GroupCoordinator groupCoordinator;
    private RequestChannel requestChannel;
    private SocketServer socketServer;
    private KafkaRequestHandler requestHandler;

    public KafkaServer(KafkaConfig config) {
        this.config = config;
    }

    public void startup() throws Exception {
        log.info("Starting KafkaServer on port {}", config.port);

        LogConfig logConfig = new LogConfig(
            config.logSegmentBytes, config.logSegmentMs, config.logIndexIntervalBytes
        );
        logManager = new LogManager(new File(config.logDirs), logConfig);
        controller = new KafkaController(new ControllerContext(), logManager);
        groupCoordinator = new GroupCoordinator(config.sessionTimeoutMs);

        requestChannel = new RequestChannel(500);
        KafkaApis apis = new KafkaApis(logManager, controller, groupCoordinator);
        requestHandler = new KafkaRequestHandler(config.numIoThreads, requestChannel, apis);
        socketServer = new SocketServer(config.port, config.numNetworkThreads, requestChannel);

        socketServer.startup();
        requestHandler.startup();

        log.info("KafkaServer started successfully");
    }

    public void shutdown() throws Exception {
        log.info("Shutting down KafkaServer");
        if (socketServer != null) socketServer.shutdown();
        if (requestHandler != null) requestHandler.shutdown();
        if (logManager != null) logManager.close();
        log.info("KafkaServer stopped");
    }

    public LogManager logManager() { return logManager; }
    public KafkaController controller() { return controller; }
    public GroupCoordinator groupCoordinator() { return groupCoordinator; }
    public int port() { return config.port; }

    public static void main(String[] args) throws Exception {
        KafkaServer server = new KafkaServer(KafkaConfig.defaultConfig());
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try { server.shutdown(); } catch (Exception e) { e.printStackTrace(); }
        }));
        server.startup();
        Thread.currentThread().join();
    }
}
```

- [ ] **Step 4: 编译验证**

```bash
mvn clean compile -pl mini-kafka-server -Dsort.skip=true
```
Expected: BUILD SUCCESS

- [ ] **Step 5: Commit**

```bash
git add mini-kafka-server/src/main/java/com/github/minikafka/server/api/ \
        mini-kafka-server/src/main/java/com/github/minikafka/server/KafkaServer.java
git commit -m "feat(server): add KafkaApis, KafkaRequestHandler, KafkaServer"
```

---

### Task 11: Broker 集成测试

**Files:**
- Create: `mini-kafka-server/src/test/java/com/github/minikafka/server/integration/BrokerIntegrationTest.java`

- [ ] **Step 1: 写集成测试**

```java
package com.github.minikafka.server.integration;

import com.github.minikafka.server.KafkaConfig;
import com.github.minikafka.server.KafkaServer;
import com.github.minikafka.common.network.ByteBufferUtils;
import com.github.minikafka.common.protocol.*;
import com.github.minikafka.common.record.*;
import org.junit.*;
import java.io.*;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.*;
import static org.junit.Assert.*;

/**
 * 启动内嵌 Broker，通过 TCP 发送原始协议请求，验证 Produce/Fetch/Metadata/CreateTopics
 */
public class BrokerIntegrationTest {

    private static KafkaServer server;
    private static final int PORT = 19092;

    @BeforeClass
    public static void startBroker() throws Exception {
        Properties props = new Properties();
        props.setProperty("port", String.valueOf(PORT));
        props.setProperty("log.dirs", System.getProperty("java.io.tmpdir") + "/mini-kafka-it-" + System.currentTimeMillis());
        server = new KafkaServer(new KafkaConfig(props));
        server.startup();
        Thread.sleep(300); // 等待 Acceptor 就绪
    }

    @AfterClass
    public static void stopBroker() throws Exception {
        if (server != null) server.shutdown();
    }

    @Test
    public void testCreateTopicAndProduceAndFetch() throws Exception {
        // 1. CreateTopics
        sendCreateTopics("integration-test", 1);
        Thread.sleep(100);

        // 2. Produce
        List<Record> records = Arrays.asList(
            new Record(0, System.currentTimeMillis(), "key1".getBytes(), "value1".getBytes())
        );
        RecordBatch batch = new RecordBatch(0L, records);
        long baseOffset = sendProduce("integration-test", 0, batch);
        assertEquals(0L, baseOffset);

        // 3. Fetch
        byte[] fetched = sendFetch("integration-test", 0, 0L, 1024 * 1024);
        assertNotNull(fetched);
        assertTrue(fetched.length > 0);
        RecordBatch fetchedBatch = RecordBatch.readFrom(ByteBuffer.wrap(fetched));
        assertEquals(1, fetchedBatch.records().size());
        assertArrayEquals("value1".getBytes(), fetchedBatch.records().get(0).value());
    }

    @Test
    public void testMetadata() throws Exception {
        sendCreateTopics("meta-test", 2);
        Thread.sleep(100);
        int partitions = sendMetadataGetPartitions("meta-test");
        assertEquals(2, partitions);
    }

    // ---------- 辅助方法：构造并发送原始请求 ----------

    private void sendCreateTopics(String topic, int partitions) throws Exception {
        // Body: topicCount(4) topic(string) partitions(4)
        int bodySize = 4 + ByteBufferUtils.sizeOfString(topic) + 4;
        ByteBuffer body = ByteBuffer.allocate(bodySize);
        body.putInt(1);
        ByteBufferUtils.writeString(body, topic);
        body.putInt(partitions);
        body.flip();
        sendRequest(ApiKeys.CREATE_TOPICS, 1, body);
    }

    private long sendProduce(String topic, int partition, RecordBatch batch) throws Exception {
        int batchSize = batch.sizeInBytes();
        ByteBuffer batchBuf = ByteBuffer.allocate(batchSize);
        batch.writeTo(batchBuf);
        batchBuf.flip();

        int bodySize = 4 + ByteBufferUtils.sizeOfString(topic) + 4 + 4 + 4 + batchSize;
        ByteBuffer body = ByteBuffer.allocate(bodySize);
        body.putInt(1); // topicCount
        ByteBufferUtils.writeString(body, topic);
        body.putInt(1); // partCount
        body.putInt(partition);
        body.putInt(batchSize);
        body.put(batchBuf);
        body.flip();

        ByteBuffer response = sendRequest(ApiKeys.PRODUCE, 2, body);
        int topicCount = response.getInt();
        ByteBufferUtils.readString(response); // topic
        response.getInt(); // partCount
        response.getInt(); // partition
        response.getShort(); // errorCode
        return response.getLong(); // baseOffset
    }

    private byte[] sendFetch(String topic, int partition, long offset, int maxBytes) throws Exception {
        int bodySize = 4 + 4 + 4 + 4 + ByteBufferUtils.sizeOfString(topic) + 4 + 4 + 8 + 4;
        ByteBuffer body = ByteBuffer.allocate(bodySize);
        body.putInt(-1); // replicaId
        body.putInt(100); // maxWaitMs
        body.putInt(0);  // minBytes
        body.putInt(1);  // topicCount
        ByteBufferUtils.writeString(body, topic);
        body.putInt(1);  // partCount
        body.putInt(partition);
        body.putLong(offset);
        body.putInt(maxBytes);
        body.flip();

        ByteBuffer response = sendRequest(ApiKeys.FETCH, 3, body);
        response.getInt(); // topicCount
        ByteBufferUtils.readString(response); // topic
        response.getInt(); // partCount
        response.getInt(); // partition
        short errorCode = response.getShort();
        assertEquals(0, errorCode);
        response.getLong(); // highWatermark
        int recordSize = response.getInt();
        if (recordSize == 0) return null;
        byte[] records = new byte[recordSize];
        response.get(records);
        return records;
    }

    private int sendMetadataGetPartitions(String topic) throws Exception {
        int bodySize = 4 + ByteBufferUtils.sizeOfString(topic);
        ByteBuffer body = ByteBuffer.allocate(bodySize);
        body.putInt(1);
        ByteBufferUtils.writeString(body, topic);
        body.flip();

        ByteBuffer response = sendRequest(ApiKeys.METADATA, 4, body);
        int brokerCount = response.getInt();
        for (int i = 0; i < brokerCount; i++) {
            response.getInt(); ByteBufferUtils.readString(response); response.getInt();
        }
        int topicCount = response.getInt();
        ByteBufferUtils.readString(response); // topic
        response.getShort(); // errorCode
        return response.getInt(); // partitions
    }

    private ByteBuffer sendRequest(ApiKeys apiKey, int correlationId, ByteBuffer body) throws Exception {
        RequestHeader header = new RequestHeader(apiKey, (short) 0, correlationId, "test");
        int totalSize = header.sizeOf() + body.remaining();
        ByteBuffer request = ByteBuffer.allocate(4 + totalSize);
        request.putInt(totalSize);
        header.writeTo(request);
        request.put(body);
        request.flip();

        try (Socket socket = new Socket("localhost", PORT)) {
            OutputStream out = socket.getOutputStream();
            InputStream in = socket.getInputStream();
            out.write(request.array(), request.position(), request.remaining());
            out.flush();

            // 读 4字节长度
            byte[] sizeBuf = new byte[4];
            readFully(in, sizeBuf);
            int responseSize = ByteBuffer.wrap(sizeBuf).getInt();

            byte[] respBuf = new byte[responseSize];
            readFully(in, respBuf);
            ByteBuffer response = ByteBuffer.wrap(respBuf);
            response.getInt(); // correlationId
            return response;
        }
    }

    private void readFully(InputStream in, byte[] buf) throws IOException {
        int offset = 0;
        while (offset < buf.length) {
            int read = in.read(buf, offset, buf.length - offset);
            if (read < 0) throw new EOFException();
            offset += read;
        }
    }
}
```

- [ ] **Step 2: 运行集成测试**

```bash
mvn clean test -pl mini-kafka-server -Dtest=BrokerIntegrationTest -Dsort.skip=true
```
Expected: Tests run: 2, Failures: 0, Errors: 0

- [ ] **Step 3: 运行所有 server 测试**

```bash
mvn clean test -pl mini-kafka-server -Dsort.skip=true
```
Expected: 所有测试通过

- [ ] **Step 4: Commit**

```bash
git add mini-kafka-server/src/test/
git commit -m "test(server): add broker integration tests"
```
