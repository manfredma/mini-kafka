package com.github.minikafka.clients.producer;

import java.util.Properties;

/**
 * Producer 配置参数容器，从 {@link Properties} 中读取并解析各项配置。
 *
 * <p>对齐 Kafka {@code org.apache.kafka.clients.producer.ProducerConfig}（简化版）。
 * 所有字段均为 {@code final}，构造后不可修改，可安全在多线程间共享。
 *
 * <p>支持的配置项及默认值：
 * <ul>
 *   <li>{@code bootstrap.servers}：Broker 地址，格式 {@code host:port}，默认 {@code localhost:9092}</li>
 *   <li>{@code batch.size}：单个 ProducerBatch 的最大字节数，默认 16384（16KB）</li>
 *   <li>{@code linger.ms}：批次等待时间（毫秒），0 表示立即发送，默认 0</li>
 *   <li>{@code acks}：发送确认级别（0/1/-1），默认 1</li>
 *   <li>{@code client.id}：客户端标识，写入请求头，默认 {@code producer-1}</li>
 * </ul>
 */
public final class ProducerConfig {

    /** Bootstrap Broker 地址，格式 {@code host:port}。 */
    public final String bootstrapServers;

    /**
     * 批次最大字节数（字节）。当 {@link ProducerBatch} 的估算大小超过此值时，
     * 该批次会立即被移入就绪队列等待发送。
     */
    public final int batchSize;

    /**
     * 批次等待时间（毫秒）。即使批次未满，等待超过此时间后也会被发送。
     * 设为 0 表示不等待，追加消息后立即触发发送（低延迟模式）。
     */
    public final long lingerMs;

    /**
     * 消息确认级别：
     * <ul>
     *   <li>0：不等待 Broker 确认（最低延迟，可能丢失）</li>
     *   <li>1：等待 leader 写入确认（默认，平衡延迟与可靠性）</li>
     *   <li>-1：等待所有 ISR 副本确认（最高可靠性）</li>
     * </ul>
     */
    public final int acks;

    /** 客户端 ID，写入每个请求的 RequestHeader，便于服务端日志追踪。 */
    public final String clientId;

    /**
     * 从 Properties 构造 ProducerConfig，缺失的 key 使用默认值。
     *
     * @param props 配置属性；允许为空（全部使用默认值）
     */
    public ProducerConfig(Properties props) {
        this.bootstrapServers = props.getProperty("bootstrap.servers", "localhost:9092");
        this.batchSize = Integer.parseInt(props.getProperty("batch.size", "16384"));
        this.lingerMs = Long.parseLong(props.getProperty("linger.ms", "0"));
        this.acks = Integer.parseInt(props.getProperty("acks", "1"));
        this.clientId = props.getProperty("client.id", "producer-1");
    }
}
