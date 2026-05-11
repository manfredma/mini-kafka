package com.github.minikafka.clients.producer;

public final class ProducerRecord {
    public final String topic;
    public final Integer partition;
    public final byte[] key;
    public final byte[] value;
    public final long timestamp;

    public ProducerRecord(String topic, byte[] key, byte[] value) {
        this(topic, null, key, value);
    }

    public ProducerRecord(String topic, Integer partition, byte[] key, byte[] value) {
        this.topic = topic;
        this.partition = partition;
        this.key = key;
        this.value = value;
        this.timestamp = System.currentTimeMillis();
    }
}
