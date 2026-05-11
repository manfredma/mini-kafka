# mini-kafka-examples Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 提供可运行的端到端演示，展示 mini-kafka 完整的 Broker + Producer + Consumer 链路。

**Architecture:** 独立 main 方法，启动内嵌 Broker，通过 KafkaProducer/KafkaConsumer API 演示典型用法。

**Tech Stack:** Java 8, Maven

**依赖:** mini-kafka-server + mini-kafka-clients

---

### Task 1: examples 模块 pom

**Files:**
- Create: `mini-kafka-examples/pom.xml`

- [ ] **Step 1: 写 mini-kafka-examples/pom.xml**

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

    <artifactId>mini-kafka-examples</artifactId>
    <name>mini-kafka-examples</name>

    <dependencies>
        <dependency>
            <groupId>com.github.minikafka</groupId>
            <artifactId>mini-kafka-server</artifactId>
            <version>${project.version}</version>
        </dependency>
        <dependency>
            <groupId>com.github.minikafka</groupId>
            <artifactId>mini-kafka-clients</artifactId>
            <version>${project.version}</version>
        </dependency>
    </dependencies>
</project>
```

- [ ] **Step 2: 创建目录**

```bash
mkdir -p mini-kafka-examples/src/main/java/com/github/minikafka/examples
```

- [ ] **Step 3: 编译验证**

```bash
mvn clean install -pl mini-kafka-common,mini-kafka-server,mini-kafka-clients -DskipTests -Dsort.skip=true
mvn clean compile -pl mini-kafka-examples -Dsort.skip=true
```
Expected: BUILD SUCCESS

- [ ] **Step 4: Commit**

```bash
git add mini-kafka-examples/pom.xml
git commit -m "chore(examples): init examples module"
```

---

### Task 2: QuickStartExample

**Files:**
- Create: `mini-kafka-examples/src/main/java/com/github/minikafka/examples/QuickStartExample.java`

- [ ] **Step 1: 写 QuickStartExample.java**

```java
package com.github.minikafka.examples;

import com.github.minikafka.clients.consumer.*;
import com.github.minikafka.clients.producer.*;
import com.github.minikafka.server.KafkaConfig;
import com.github.minikafka.server.KafkaServer;
import java.time.Duration;
import java.util.*;

/**
 * mini-kafka 快速上手演示
 * 启动内嵌 Broker，Producer 发送 5 条消息，Consumer 消费并打印
 */
public class QuickStartExample {

    public static void main(String[] args) throws Exception {
        // 1. 启动 Broker
        Properties serverProps = new Properties();
        serverProps.setProperty("port", "9092");
        serverProps.setProperty("log.dirs", "/tmp/mini-kafka-quickstart");
        KafkaServer server = new KafkaServer(new KafkaConfig(serverProps));
        server.startup();
        Thread.sleep(500);

        // 2. 创建 Topic
        server.controller().createTopic("quickstart-events", 1);
        Thread.sleep(100);

        // 3. Producer 发送 5 条消息
        Properties producerProps = new Properties();
        producerProps.setProperty("bootstrap.servers", "localhost:9092");
        producerProps.setProperty("client.id", "quickstart-producer");

        System.out.println("=== Producer: Sending 5 messages ===");
        try (KafkaProducer producer = new KafkaProducer(producerProps)) {
            for (int i = 1; i <= 5; i++) {
                String value = "Event #" + i + " from mini-kafka";
                RecordMetadata meta = producer.sendSync(
                    new ProducerRecord("quickstart-events", 0,
                        ("key-" + i).getBytes(), value.getBytes())
                );
                System.out.printf("  Sent: topic=%s, partition=%d, offset=%d, value=%s%n",
                    meta.topic, meta.partition, meta.offset, value);
            }
        }

        // 4. Consumer 消费
        Properties consumerProps = new Properties();
        consumerProps.setProperty("bootstrap.servers", "localhost:9092");
        consumerProps.setProperty("group.id", "quickstart-group");
        consumerProps.setProperty("enable.auto.commit", "false");

        System.out.println("\n=== Consumer: Polling messages ===");
        try (KafkaConsumer consumer = new KafkaConsumer(consumerProps)) {
            consumer.subscribe(Collections.singletonList("quickstart-events"));
            consumer.seek("quickstart-events", 0, 0L); // 从头消费

            ConsumerRecords records = consumer.poll(Duration.ofSeconds(3));
            System.out.printf("  Received %d records%n", records.count());
            for (ConsumerRecord record : records) {
                System.out.printf("  Consumed: topic=%s, partition=%d, offset=%d, value=%s%n",
                    record.topic, record.partition, record.offset, new String(record.value));
            }
            consumer.commitSync();
            System.out.println("  Committed offsets");
        }

        // 5. 停止 Broker
        server.shutdown();
        System.out.println("\n=== Done ===");
    }
}
```

- [ ] **Step 2: 编译验证**

```bash
mvn clean compile -pl mini-kafka-examples -Dsort.skip=true
```
Expected: BUILD SUCCESS

- [ ] **Step 3: Commit**

```bash
git add mini-kafka-examples/src/main/java/com/github/minikafka/examples/QuickStartExample.java
git commit -m "feat(examples): add QuickStartExample"
```

---

### Task 3: 全量构建 + 全量测试

**Files:** 无新文件

- [ ] **Step 1: 全量构建**

```bash
mvn clean install -Dsort.skip=true
```
Expected: BUILD SUCCESS，所有模块打包成功

- [ ] **Step 2: 全量测试**

```bash
mvn clean test -Dsort.skip=true
```
Expected: 所有测试通过，无 FAIL

- [ ] **Step 3: Commit**

```bash
git add -A
git commit -m "chore: full build and all tests passing"
```
