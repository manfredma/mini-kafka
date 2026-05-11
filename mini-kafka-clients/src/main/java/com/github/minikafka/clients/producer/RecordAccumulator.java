package com.github.minikafka.clients.producer;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 消息按 TopicPartition 批量缓冲
 * 对齐 Kafka org.apache.kafka.clients.producer.internals.RecordAccumulator
 */
public final class RecordAccumulator {

    private final int batchSize;
    private final long lingerMs;
    private final Map<String, ProducerBatch> batches = new ConcurrentHashMap<>();
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

        if (batch.estimatedSizeInBytes() >= batchSize) {
            moveToReady(key2, batch);
        }

        return future;
    }

    private void moveToReady(String key, ProducerBatch batch) {
        batches.remove(key);
        readyBatches.computeIfAbsent(key, k -> new LinkedList<>()).add(batch);
    }

    public synchronized List<ProducerBatch> drain(String topic, int partition) {
        String key = topic + "-" + partition;
        long now = System.currentTimeMillis();

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
