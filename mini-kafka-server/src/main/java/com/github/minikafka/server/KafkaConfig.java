package com.github.minikafka.server;

import java.util.Properties;

public final class KafkaConfig {
    public final int port;
    public final String logDirs;
    public final int numNetworkThreads;
    public final int numIoThreads;
    public final int logSegmentBytes;
    public final long logSegmentMs;
    public final int logIndexIntervalBytes;
    public final int sessionTimeoutMs;

    public KafkaConfig(Properties props) {
        this.port = Integer.parseInt(props.getProperty("port", "9092"));
        this.logDirs = props.getProperty("log.dirs", "/tmp/mini-kafka-logs");
        this.numNetworkThreads = Integer.parseInt(props.getProperty("num.network.threads", "3"));
        this.numIoThreads = Integer.parseInt(props.getProperty("num.io.threads", "8"));
        this.logSegmentBytes = Integer.parseInt(props.getProperty("log.segment.bytes", String.valueOf(1024 * 1024 * 1024)));
        this.logSegmentMs = Long.parseLong(props.getProperty("log.segment.ms", String.valueOf(7 * 24 * 60 * 60 * 1000L)));
        this.logIndexIntervalBytes = Integer.parseInt(props.getProperty("log.index.interval.bytes", "4096"));
        this.sessionTimeoutMs = Integer.parseInt(props.getProperty("session.timeout.ms", "30000"));
    }

    public static KafkaConfig defaultConfig() {
        return new KafkaConfig(new Properties());
    }
}
