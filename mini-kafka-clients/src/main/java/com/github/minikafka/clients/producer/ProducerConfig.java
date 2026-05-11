package com.github.minikafka.clients.producer;

import java.util.Properties;

public final class ProducerConfig {
    public final String bootstrapServers;
    public final int batchSize;
    public final long lingerMs;
    public final int acks;
    public final String clientId;

    public ProducerConfig(Properties props) {
        this.bootstrapServers = props.getProperty("bootstrap.servers", "localhost:9092");
        this.batchSize = Integer.parseInt(props.getProperty("batch.size", "16384"));
        this.lingerMs = Long.parseLong(props.getProperty("linger.ms", "0"));
        this.acks = Integer.parseInt(props.getProperty("acks", "1"));
        this.clientId = props.getProperty("client.id", "producer-1");
    }
}
