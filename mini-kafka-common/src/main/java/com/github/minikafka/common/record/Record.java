package com.github.minikafka.common.record;

/**
 * 单条 Kafka 消息记录，对应 Kafka 原版 {@code org.apache.kafka.common.record.Record}（magic=2 格式）。
 *
 * <p>一条 Record 是 {@link RecordBatch} 的基本组成单元，持有消息的绝对 offset、时间戳、
 * 可选的 key 和 value 字节数组。
 *
 * <p>关键设计决策：
 * <ul>
 *   <li>所有字段均为 {@code final}，对象创建后不可变，保证线程安全和调试友好性。</li>
 *   <li>{@code key} 和 {@code value} 均允许为 null，对应 Kafka 协议中 NULLABLE_BYTES 的 null 语义
 *       （序列化时长度字段写 -1）。</li>
 *   <li>getter 均为单行方法，不加注释（规则第 4 条）。</li>
 * </ul>
 */
public final class Record {
    private final long offset;
    private final long timestamp;
    private final byte[] key;
    private final byte[] value;

    /**
     * 构造一条消息记录。
     *
     * @param offset    消息在分区中的绝对 offset，由所属 {@link RecordBatch} 的 baseOffset 加上
     *                  批内 offsetDelta 计算得出
     * @param timestamp 消息时间戳（毫秒，CreateTime 语义），由生产者在发送时设置
     * @param key       消息 key 字节数组，允许为 null（表示无 key）
     * @param value     消息 value 字节数组，允许为 null（表示墓碑消息，用于 compaction 删除）
     */
    public Record(long offset, long timestamp, byte[] key, byte[] value) {
        this.offset = offset;
        this.timestamp = timestamp;
        this.key = key;
        this.value = value;
    }

    public long offset() { return offset; }
    public long timestamp() { return timestamp; }
    public byte[] key() { return key; }
    public byte[] value() { return value; }
}
