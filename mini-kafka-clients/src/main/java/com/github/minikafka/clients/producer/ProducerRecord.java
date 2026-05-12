package com.github.minikafka.clients.producer;

/**
 * 待发送的消息记录，包含 Topic、可选分区、Key、Value 和时间戳。
 *
 * <p>对齐 Kafka {@code org.apache.kafka.clients.producer.ProducerRecord}。
 * 本类为不可变值对象，构造后所有字段均不可修改。
 *
 * <p>分区选择规则（由 {@link KafkaProducer#selectPartition} 实现）：
 * <ol>
 *   <li>若 {@link #partition} 不为 null，使用指定分区。</li>
 *   <li>若 {@link #key} 不为 null，对 key 字节数组取 hash 后对分区数取模。</li>
 *   <li>否则，使用 {@code System.nanoTime()} 对分区数取模（轮询效果）。</li>
 * </ol>
 */
public final class ProducerRecord {

    /** 目标 Topic 名称，不允许为 null。 */
    public final String topic;

    /**
     * 指定的目标分区编号（从 0 开始）。
     * 为 null 表示由 Producer 根据 key 或轮询策略自动选择分区。
     */
    public final Integer partition;

    /**
     * 消息 Key 的字节数组，可为 null。
     * Key 用于分区路由（相同 key 路由到相同分区）和业务语义标识。
     */
    public final byte[] key;

    /**
     * 消息 Value 的字节数组，可为 null（表示墓碑消息，用于日志压缩删除语义）。
     */
    public final byte[] value;

    /** 消息创建时的系统时间戳（毫秒），写入 RecordBatch 的 timestamp 字段。 */
    public final long timestamp;

    /**
     * 构造消息记录，由 Producer 自动选择分区。
     *
     * @param topic Topic 名称
     * @param key   消息 Key，可为 null
     * @param value 消息 Value，可为 null
     */
    public ProducerRecord(String topic, byte[] key, byte[] value) {
        this(topic, null, key, value);
    }

    /**
     * 构造消息记录，指定目标分区。
     *
     * @param topic     Topic 名称
     * @param partition 目标分区编号；为 null 时由 Producer 自动选择
     * @param key       消息 Key，可为 null
     * @param value     消息 Value，可为 null
     */
    public ProducerRecord(String topic, Integer partition, byte[] key, byte[] value) {
        this.topic = topic;
        this.partition = partition;
        this.key = key;
        this.value = value;
        this.timestamp = System.currentTimeMillis();
    }
}
