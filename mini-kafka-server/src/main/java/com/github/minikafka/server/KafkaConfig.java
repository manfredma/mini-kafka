package com.github.minikafka.server;

import java.util.Properties;

/**
 * Broker 配置持有类，从 {@link Properties} 中读取并解析所有运行时参数。
 * <p>
 * 对应 Kafka 原版 {@code kafka.server.KafkaConfig}（大幅简化版）。
 * 所有字段在构造后不可变（{@code final}），可安全跨线程读取。
 * 未在 Properties 中指定的配置项均使用内置默认值，与 Kafka 默认值保持一致。
 * </p>
 */
public final class KafkaConfig {
    /** Broker 监听端口，默认 9092。 */
    public final int port;
    /** 日志数据根目录，默认 {@code /tmp/mini-kafka-logs}。 */
    public final String logDirs;
    /** 网络 IO 线程数（Processor 数量），默认 3。 */
    public final int numNetworkThreads;
    /** 请求处理线程数（KafkaRequestHandler 线程池大小），默认 8。 */
    public final int numIoThreads;
    /** 单个 LogSegment 的最大字节数，超过后滚动新 Segment，默认 1 GiB。 */
    public final int logSegmentBytes;
    /** 单个 LogSegment 的最大存活时长（毫秒），超过后滚动新 Segment，默认 7 天。 */
    public final long logSegmentMs;
    /** 稀疏索引写入间隔（字节），每写入该字节数后追加一条索引条目，默认 4096。 */
    public final int logIndexIntervalBytes;
    /** Consumer 会话超时时间（毫秒），超时未收到心跳则触发 Rebalance，默认 30000。 */
    public final int sessionTimeoutMs;

    /**
     * 从 {@link Properties} 中解析配置。
     * <p>
     * 所有配置项均有默认值，传入空 {@code Properties} 等价于 {@link #defaultConfig()}。
     * </p>
     *
     * @param props 配置来源，不得为 {@code null}；缺失的 key 使用默认值
     */
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

    /**
     * 使用所有默认值创建配置实例，等价于 {@code new KafkaConfig(new Properties())}。
     *
     * @return 全默认值的 {@link KafkaConfig} 实例
     */
    public static KafkaConfig defaultConfig() {
        return new KafkaConfig(new Properties());
    }
}
