package com.github.minikafka.clients.consumer;

import java.util.Properties;

public final class ConsumerConfig {
    public final String bootstrapServers;
    public final String groupId;
    public final String autoOffsetReset;
    public final boolean enableAutoCommit;
    public final int autoCommitIntervalMs;
    public final int sessionTimeoutMs;
    public final int heartbeatIntervalMs;
    public final int fetchMaxBytes;
    public final String clientId;

    public ConsumerConfig(Properties props) {
        this.bootstrapServers = props.getProperty("bootstrap.servers", "localhost:9092");
        this.groupId = props.getProperty("group.id", "");
        this.autoOffsetReset = props.getProperty("auto.offset.reset", "latest");
        this.enableAutoCommit = Boolean.parseBoolean(props.getProperty("enable.auto.commit", "true"));
        this.autoCommitIntervalMs = Integer.parseInt(props.getProperty("auto.commit.interval.ms", "5000"));
        this.sessionTimeoutMs = Integer.parseInt(props.getProperty("session.timeout.ms", "30000"));
        this.heartbeatIntervalMs = Integer.parseInt(props.getProperty("heartbeat.interval.ms", "3000"));
        this.fetchMaxBytes = Integer.parseInt(props.getProperty("fetch.max.bytes", String.valueOf(50 * 1024 * 1024)));
        this.clientId = props.getProperty("client.id", "consumer-1");
    }
}
