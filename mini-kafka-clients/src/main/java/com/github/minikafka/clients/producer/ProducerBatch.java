package com.github.minikafka.clients.producer;

import com.github.minikafka.common.record.Record;
import com.github.minikafka.common.record.RecordBatch;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * 单个 TopicPartition 的待发送批次
 */
public final class ProducerBatch {
    public final String topic;
    public final int partition;
    private final List<Record> records = new ArrayList<>();
    private final List<CompletableFuture<RecordMetadata>> futures = new ArrayList<>();
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
        List<Record> absolute = new ArrayList<>(records.size());
        for (int i = 0; i < records.size(); i++) {
            Record r = records.get(i);
            absolute.add(new Record(baseOffset + i, r.timestamp(), r.key(), r.value()));
        }
        return new RecordBatch(baseOffset, absolute);
    }

    public void done(long baseOffset) {
        for (int i = 0; i < futures.size(); i++) {
            Record r = records.get(i);
            futures.get(i).complete(new RecordMetadata(topic, partition, baseOffset + i, r.timestamp()));
        }
    }

    public void abort(Exception ex) {
        for (CompletableFuture<RecordMetadata> f : futures) f.completeExceptionally(ex);
    }

    public int estimatedSizeInBytes() {
        int size = 61;
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
