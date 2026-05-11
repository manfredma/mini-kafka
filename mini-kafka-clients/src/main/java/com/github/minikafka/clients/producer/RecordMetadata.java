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
