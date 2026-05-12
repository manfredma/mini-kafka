package com.github.minikafka.clients.consumer;

import java.util.Properties;

/**
 * Consumer 配置参数容器，从 {@link Properties} 中读取并解析各项配置。
 *
 * <p>对齐 Kafka {@code org.apache.kafka.clients.consumer.ConsumerConfig}（简化版）。
 * 所有字段均为 {@code final}，构造后不可修改，可安全在多线程间共享。
 *
 * <p>支持的配置项及默认值：
 * <ul>
 *   <li>{@code bootstrap.servers}：Broker 地址，格式 {@code host:port}，默认 {@code localhost:9092}</li>
 *   <li>{@code group.id}：Consumer Group ID，用于 Rebalance 协调，默认空字符串</li>
 *   <li>{@code auto.offset.reset}：无已提交 offset 时的起始策略（{@code latest}/{@code earliest}），默认 {@code latest}</li>
 *   <li>{@code enable.auto.commit}：是否开启自动提交 offset，默认 true</li>
 *   <li>{@code auto.commit.interval.ms}：自动提交间隔（毫秒），默认 5000</li>
 *   <li>{@code session.timeout.ms}：Consumer 会话超时时间（毫秒），默认 30000</li>
 *   <li>{@code heartbeat.interval.ms}：心跳发送间隔（毫秒），默认 3000</li>
 *   <li>{@code fetch.max.bytes}：单次 FetchRequest 最大返回字节数，默认 50MB</li>
 *   <li>{@code client.id}：客户端标识，写入请求头，默认 {@code consumer-1}</li>
 * </ul>
 */
public final class ConsumerConfig {

    /** Bootstrap Broker 地址，格式 {@code host:port}。 */
    public final String bootstrapServers;

    /**
     * Consumer Group ID。同一 Group 内的多个 Consumer 共同消费订阅 Topic 的分区，
     * 每个分区在同一时刻只分配给 Group 内一个 Consumer。
     */
    public final String groupId;

    /**
     * 无已提交 offset 时（首次消费或 offset 过期）的起始策略：
     * <ul>
     *   <li>{@code latest}：从最新消息开始消费（跳过历史消息）</li>
     *   <li>{@code earliest}：从最早消息开始消费（读取全部历史）</li>
     * </ul>
     */
    public final String autoOffsetReset;

    /**
     * 是否开启自动提交 offset。开启时，{@link KafkaConsumer#poll} 会按
     * {@link #autoCommitIntervalMs} 间隔自动调用 {@link KafkaConsumer#commitSync}。
     */
    public final boolean enableAutoCommit;

    /** 自动提交 offset 的时间间隔（毫秒），仅在 {@link #enableAutoCommit} 为 true 时生效。 */
    public final int autoCommitIntervalMs;

    /**
     * Consumer 会话超时时间（毫秒）。若 Coordinator 在此时间内未收到心跳，
     * 则认为该 Consumer 已死亡并触发 Rebalance。
     */
    public final int sessionTimeoutMs;

    /**
     * 心跳发送间隔（毫秒）。{@link ConsumerCoordinator} 按此间隔向 Coordinator 发送
     * HeartbeatRequest，维持会话存活。推荐设置为 {@link #sessionTimeoutMs} 的 1/3 以下。
     */
    public final int heartbeatIntervalMs;

    /**
     * 单次 FetchRequest 允许服务端返回的最大字节数。
     * 实际每个分区的 fetch 配额为 {@code fetchMaxBytes / 分区数}。
     */
    public final int fetchMaxBytes;

    /** 客户端 ID，写入每个请求的 RequestHeader，便于服务端日志追踪。 */
    public final String clientId;

    /**
     * 从 Properties 构造 ConsumerConfig，缺失的 key 使用默认值。
     *
     * @param props 配置属性；允许为空（全部使用默认值）
     */
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
