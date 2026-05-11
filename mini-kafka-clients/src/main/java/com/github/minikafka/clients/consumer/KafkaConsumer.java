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
 * 对齐 Kafka org.apache.kafka.clients.consumer.KafkaConsumer
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
        this.fetcher = new Fetcher(networkClient, subscriptionState, config.fetchMaxBytes);
    }

    public void subscribe(Collection<String> topics) {
        subscriptionState.subscribe(topics);
        try {
            networkClient.updateMetadata(new ArrayList<>(topics));
        } catch (IOException e) {
            log.warn("Failed to update metadata on subscribe", e);
        }
    }

    public ConsumerRecords poll(Duration timeout) throws IOException {
        coordinator.ensureActiveGroup();
        List<ConsumerRecord> records = fetcher.fetchRecords();
        if (config.enableAutoCommit) maybeAutoCommit();
        return records.isEmpty() ? ConsumerRecords.EMPTY : new ConsumerRecords(records);
    }

    public void commitSync() throws IOException {
        for (String tp : subscriptionState.assignedPartitions()) {
            int idx = tp.lastIndexOf('-');
            String topic = tp.substring(0, idx);
            int partition = Integer.parseInt(tp.substring(idx + 1));
            long position = subscriptionState.position(topic, partition);
            if (position >= 0) coordinator.commitOffset(topic, partition, position);
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
