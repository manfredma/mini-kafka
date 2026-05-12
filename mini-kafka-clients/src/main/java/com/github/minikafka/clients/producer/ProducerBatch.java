package com.github.minikafka.clients.producer;

import com.github.minikafka.common.record.Record;
import com.github.minikafka.common.record.RecordBatch;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * 单个 TopicPartition 的待发送消息批次。
 *
 * <p>对齐 Kafka {@code org.apache.kafka.clients.producer.internals.ProducerBatch}。
 * 每个批次归属于固定的 topic + partition，在 {@link RecordAccumulator} 中积累消息，
 * 满足 batchSize 或 lingerMs 条件后由 {@link Sender} 发送。
 *
 * <p>批次内消息的 offset 在追加时以相对偏移量（offsetDelta）存储，
 * 发送成功后由 {@link #done(long)} 结合服务端返回的 baseOffset 转换为绝对 offset。
 *
 * <p>本类不是线程安全的，由 {@link RecordAccumulator} 持有锁后访问。
 */
public final class ProducerBatch {

    /** 该批次所属的 Topic 名称。 */
    public final String topic;
    /** 该批次所属的分区编号。 */
    public final int partition;

    /** 批次内的消息列表，offset 为批次内相对偏移量（0, 1, 2, ...）。 */
    private final List<Record> records = new ArrayList<>();
    /**
     * 与 records 一一对应的 Future 列表。
     * 批次发送成功后由 {@link #done(long)} 完成；失败后由 {@link #abort(Exception)} 异常完成。
     */
    private final List<CompletableFuture<RecordMetadata>> futures = new ArrayList<>();
    /** 批次创建时的系统时间戳（毫秒），用于 lingerMs 过期判断。 */
    private final long createdMs = System.currentTimeMillis();

    /**
     * 构造一个空批次。
     *
     * @param topic     所属 Topic
     * @param partition 所属分区
     */
    public ProducerBatch(String topic, int partition) {
        this.topic = topic;
        this.partition = partition;
    }

    /**
     * 向批次末尾追加一条消息，返回对应的发送 Future。
     *
     * <p>消息以批次内相对偏移量（{@code offsetDelta = records.size()}）存储，
     * 待批次发送成功后转换为绝对 offset。
     *
     * @param key       消息 Key 字节数组，可为 null
     * @param value     消息 Value 字节数组，可为 null
     * @param timestamp 消息时间戳（毫秒）
     * @return 发送结果 Future，成功时 complete 为 {@link RecordMetadata}，失败时 completeExceptionally
     */
    public CompletableFuture<RecordMetadata> tryAppend(byte[] key, byte[] value, long timestamp) {
        long offsetDelta = records.size();
        records.add(new Record(offsetDelta, timestamp, key, value));
        CompletableFuture<RecordMetadata> future = new CompletableFuture<>();
        futures.add(future);
        return future;
    }

    /**
     * 将批次内所有消息转换为使用绝对 offset 的 {@link RecordBatch}，用于网络序列化发送。
     *
     * @param baseOffset 服务端为该批次分配的起始绝对 offset（发送前通常传 0，服务端响应后才知道真实值）
     * @return 包含绝对 offset 消息的 RecordBatch 对象
     */
    public RecordBatch toRecordBatch(long baseOffset) {
        List<Record> absolute = new ArrayList<>(records.size());
        for (int i = 0; i < records.size(); i++) {
            Record r = records.get(i);
            absolute.add(new Record(baseOffset + i, r.timestamp(), r.key(), r.value()));
        }
        return new RecordBatch(baseOffset, absolute);
    }

    /**
     * 批次发送成功后，完成所有消息的 Future。
     *
     * <p>每个 Future 以 {@link RecordMetadata} 完成，offset 为 {@code baseOffset + i}。
     *
     * @param baseOffset 服务端在 ProduceResponse 中返回的批次起始绝对 offset
     */
    public void done(long baseOffset) {
        for (int i = 0; i < futures.size(); i++) {
            Record r = records.get(i);
            futures.get(i).complete(new RecordMetadata(topic, partition, baseOffset + i, r.timestamp()));
        }
    }

    /**
     * 批次发送失败后，以异常完成所有消息的 Future。
     *
     * <p>调用此方法后，所有等待该批次结果的 {@code Future.get()} 将抛出
     * {@link java.util.concurrent.ExecutionException}。
     *
     * @param ex 导致发送失败的异常（如网络错误、服务端错误码非零）
     */
    public void abort(Exception ex) {
        for (CompletableFuture<RecordMetadata> f : futures) f.completeExceptionally(ex);
    }

    /**
     * 估算当前批次序列化后的字节大小，用于判断是否超过 batchSize 阈值。
     *
     * <p>计算方式：RecordBatch 固定头部（61 字节）+ 每条 Record 固定开销（21 字节）
     * + key 长度 + value 长度。此为近似值，实际序列化大小可能略有差异。
     *
     * @return 估算的字节数
     */
    public int estimatedSizeInBytes() {
        int size = 61;
        for (Record r : records) {
            size += 21 + (r.key() == null ? 0 : r.key().length) + (r.value() == null ? 0 : r.value().length);
        }
        return size;
    }

    /** 返回批次是否为空（尚未追加任何消息）。 */
    public boolean isEmpty() { return records.isEmpty(); }

    /** 返回批次内当前消息数量。 */
    public int recordCount() { return records.size(); }

    /** 返回批次创建时的系统时间戳（毫秒），用于 lingerMs 过期判断。 */
    public long createdMs() { return createdMs; }

    /**
     * 返回该批次的 TopicPartition 字符串 key，格式为 {@code topic-partition}。
     * 与 {@link RecordAccumulator} 内部 Map 的 key 格式一致。
     *
     * @return 例如 {@code my-topic-0}
     */
    public String topicPartitionKey() { return topic + "-" + partition; }
}
