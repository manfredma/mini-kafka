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
                assertTrue(new String(r.value).startsWith("message-"));
                count++;
            }
            assertEquals(3, count);

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

            ConsumerRecords r1 = c1.poll(Duration.ofSeconds(2));
            ConsumerRecords r2 = c2.poll(Duration.ofSeconds(2));

            assertNotNull(r1);
            assertNotNull(r2);
        }
    }
}
