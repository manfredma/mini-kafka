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
