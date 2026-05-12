package com.github.minikafka.clients.producer;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 消息按 TopicPartition 批量缓冲的积累器。
 *
 * <p>对齐 Kafka {@code org.apache.kafka.clients.producer.internals.RecordAccumulator}。
 * 主要职责：
 * <ol>
 *   <li>将消息追加到对应 TopicPartition 的当前活跃 {@link ProducerBatch} 中。</li>
 *   <li>当批次估算大小超过 {@code batchSize} 时，立即将其移入就绪队列。</li>
 *   <li>当 lingerMs 到期时，{@link #drain} 将活跃批次移入就绪队列并返回。</li>
 * </ol>
 *
 * <p>内部维护两个 Map：
 * <ul>
 *   <li>{@code batches}：每个 TopicPartition 当前正在积累的活跃批次（最多一个）。</li>
 *   <li>{@code readyBatches}：已满或已过期、等待 Sender 发送的批次队列（可多个）。</li>
 * </ul>
 *
 * <p>所有公开方法均为 {@code synchronized}，线程安全。
 */
public final class RecordAccumulator {

    /** 单个批次的最大字节数，超过后立即移入就绪队列。 */
    private final int batchSize;
    /** 批次等待时间（毫秒），0 表示不等待；到期后 {@link #drain} 会将批次移入就绪队列。 */
    private final long lingerMs;

    /**
     * TopicPartition key -> 当前活跃批次（正在积累消息）。
     * key 格式为 {@code topic-partition}。
     */
    private final Map<String, ProducerBatch> batches = new ConcurrentHashMap<>();

    /**
     * TopicPartition key -> 就绪批次队列（已满或已过期，等待发送）。
     * 使用 Queue 保证 FIFO 发送顺序。
     */
    private final Map<String, Queue<ProducerBatch>> readyBatches = new ConcurrentHashMap<>();

    /**
     * 构造消息积累器。
     *
     * @param batchSize 单批次最大字节数
     * @param lingerMs  批次最大等待时间（毫秒），0 表示立即发送
     */
    public RecordAccumulator(int batchSize, long lingerMs) {
        this.batchSize = batchSize;
        this.lingerMs = lingerMs;
    }

    /**
     * 将一条消息追加到指定 TopicPartition 的当前批次中。
     *
     * <p>若该 TopicPartition 尚无活跃批次，则创建新批次。
     * 追加后若批次估算大小 >= {@code batchSize}，立即调用 {@link #moveToReady} 将其移入就绪队列。
     *
     * <p>此方法为 {@code synchronized}，线程安全。
     *
     * @param topic     目标 Topic 名称
     * @param partition 目标分区编号
     * @param key       消息 Key，可为 null
     * @param value     消息 Value，可为 null
     * @param timestamp 消息时间戳（毫秒）
     * @return 发送结果 Future，批次发送成功后 complete，失败后 completeExceptionally
     */
    public synchronized CompletableFuture<RecordMetadata> append(
            String topic, int partition, byte[] key, byte[] value, long timestamp) {

        String key2 = topic + "-" + partition;
        ProducerBatch batch = batches.get(key2);
        if (batch == null) {
            batch = new ProducerBatch(topic, partition);
            batches.put(key2, batch);
        }

        CompletableFuture<RecordMetadata> future = batch.tryAppend(key, value, timestamp);

        if (batch.estimatedSizeInBytes() >= batchSize) {
            moveToReady(key2, batch);
        }

        return future;
    }

    /**
     * 将指定 TopicPartition 的活跃批次从 {@code batches} 移入 {@code readyBatches} 就绪队列。
     *
     * <p>移动后该 TopicPartition 的活跃批次槽位清空，下次 {@link #append} 时会创建新批次。
     *
     * @param key   TopicPartition key，格式 {@code topic-partition}
     * @param batch 要移入就绪队列的批次
     */
    private void moveToReady(String key, ProducerBatch batch) {
        batches.remove(key);
        readyBatches.computeIfAbsent(key, k -> new LinkedList<>()).add(batch);
    }

    /**
     * 检查指定 TopicPartition 的 lingerMs 是否到期，若到期则将活跃批次移入就绪队列，
     * 并返回所有就绪批次列表（清空就绪队列）。
     *
     * <p>lingerMs == 0 时视为立即过期，每次调用都会将非空活跃批次移入就绪队列。
     *
     * <p>此方法为 {@code synchronized}，线程安全。
     *
     * @param topic     Topic 名称
     * @param partition 分区编号
     * @return 当前就绪的批次列表（按加入顺序排列）；若无就绪批次则返回空列表
     */
    public synchronized List<ProducerBatch> drain(String topic, int partition) {
        String key = topic + "-" + partition;
        long now = System.currentTimeMillis();

        ProducerBatch current = batches.get(key);
        if (current != null && !current.isEmpty()) {
            boolean lingerExpired = lingerMs == 0 || (now - current.createdMs()) >= lingerMs;
            if (lingerExpired) {
                moveToReady(key, current);
            }
        }

        Queue<ProducerBatch> ready = readyBatches.remove(key);
        if (ready == null || ready.isEmpty()) return Collections.emptyList();
        return new ArrayList<>(ready);
    }

    /**
     * 判断是否存在尚未发送的消息（包括活跃批次和就绪队列中的批次）。
     *
     * <p>此方法为 {@code synchronized}，线程安全。
     * 通常由 {@link KafkaProducer#close()} 在关闭前用于等待消息全部发送完毕。
     *
     * @return true 表示仍有待发消息；false 表示所有消息已发送或队列为空
     */
    public synchronized boolean hasUnsentRecords() {
        for (ProducerBatch b : batches.values()) if (!b.isEmpty()) return true;
        for (Queue<ProducerBatch> q : readyBatches.values()) if (!q.isEmpty()) return true;
        return false;
    }
}
