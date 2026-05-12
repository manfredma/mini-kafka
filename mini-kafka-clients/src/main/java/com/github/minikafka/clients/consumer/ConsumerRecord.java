package com.github.minikafka.clients.consumer;

/**
 * Consumer 从 Broker 拉取到的单条消息记录。
 *
 * <p>对齐 Kafka {@code org.apache.kafka.clients.consumer.ConsumerRecord}（简化版，
 * 不含 headers、leaderEpoch 等扩展字段）。
 * 本类为不可变值对象，由 {@link Fetcher} 解析 FetchResponse 时构造。
 */
public final class ConsumerRecord {

    /** 消息所在的 Topic 名称。 */
    public final String topic;

    /** 消息所在的分区编号（从 0 开始）。 */
    public final int partition;

    /** 消息在分区内的绝对偏移量，全局单调递增，可用于 seek 和 commit。 */
    public final long offset;

    /** 消息的时间戳（毫秒），由 Producer 在发送时写入。 */
    public final long timestamp;

    /**
     * 消息 Key 的字节数组，可为 null。
     * 通常用于业务标识（如用户 ID、订单 ID），也决定了 Producer 侧的分区路由。
     */
    public final byte[] key;

    /**
     * 消息 Value 的字节数组，可为 null（墓碑消息，用于日志压缩删除语义）。
     * 业务层需自行对字节数组进行反序列化。
     */
    public final byte[] value;

    /**
     * 构造一条消费记录。
     *
     * @param topic     消息所在 Topic
     * @param partition 消息所在分区
     * @param offset    消息在分区内的绝对偏移量
     * @param timestamp 消息时间戳（毫秒）
     * @param key       消息 Key，可为 null
     * @param value     消息 Value，可为 null
     */
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
