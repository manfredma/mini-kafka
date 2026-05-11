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
            consumer.seek("quickstart-events", 0, 0L);

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
