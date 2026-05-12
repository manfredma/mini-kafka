package com.github.minikafka.clients.producer;

/**
 * 消息发送成功后服务端返回的元数据，包含消息在 Kafka 中的精确位置信息。
 *
 * <p>对齐 Kafka {@code org.apache.kafka.clients.producer.RecordMetadata}。
 * 本类为不可变值对象，由 {@link ProducerBatch#done(long)} 在批次发送成功后构造，
 * 通过 {@link java.util.concurrent.CompletableFuture} 返回给调用方。
 */
public final class RecordMetadata {

    /** 消息所在的 Topic 名称。 */
    public final String topic;

    /** 消息所在的分区编号（从 0 开始）。 */
    public final int partition;

    /** 消息在分区内的绝对偏移量，由服务端分配，全局单调递增。 */
    public final long offset;

    /** 消息的时间戳（毫秒），与 {@link ProducerRecord#timestamp} 一致。 */
    public final long timestamp;

    /**
     * 构造发送结果元数据。
     *
     * @param topic     消息所在 Topic
     * @param partition 消息所在分区
     * @param offset    服务端分配的消息偏移量
     * @param timestamp 消息时间戳（毫秒）
     */
    public RecordMetadata(String topic, int partition, long offset, long timestamp) {
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
        this.timestamp = timestamp;
    }

    /**
     * 返回可读的消息位置字符串，格式为 {@code topic-partition@offset}。
     *
     * @return 例如 {@code my-topic-0@42}
     */
    @Override
    public String toString() {
        return topic + "-" + partition + "@" + offset;
    }
}
