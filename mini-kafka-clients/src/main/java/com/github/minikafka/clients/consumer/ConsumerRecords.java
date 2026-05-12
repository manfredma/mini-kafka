package com.github.minikafka.clients.consumer;

import java.util.*;

/**
 * 一次 {@link KafkaConsumer#poll} 调用返回的消息集合，可能包含多个 Topic 的消息。
 *
 * <p>对齐 Kafka {@code org.apache.kafka.clients.consumer.ConsumerRecords}（简化版）。
 * 内部以不可修改列表存储，线程安全（只读）。
 *
 * <p>{@link #EMPTY} 单例用于无消息时的零分配返回，避免每次创建空集合对象。
 */
public final class ConsumerRecords implements Iterable<ConsumerRecord> {

    /** 空消息集合单例，用于 poll 无消息时的返回值，避免重复创建空对象。 */
    public static final ConsumerRecords EMPTY = new ConsumerRecords(Collections.emptyList());

    /** 内部存储的消息列表，不可修改，按 Fetcher 返回顺序排列。 */
    private final List<ConsumerRecord> records;

    /**
     * 构造消息集合，对传入列表做防御性拷贝并包装为不可修改视图。
     *
     * @param records 消息列表，可为空列表但不可为 null
     */
    public ConsumerRecords(List<ConsumerRecord> records) {
        this.records = Collections.unmodifiableList(new ArrayList<>(records));
    }

    /**
     * 按 Topic 名称过滤，返回属于指定 Topic 的所有消息。
     *
     * <p>返回的列表保持原有顺序（按 partition + offset 排列）。
     *
     * @param topic 要过滤的 Topic 名称
     * @return 属于该 Topic 的消息列表；若该 Topic 无消息则返回空列表（不返回 null）
     */
    public List<ConsumerRecord> records(String topic) {
        List<ConsumerRecord> result = new ArrayList<>();
        for (ConsumerRecord r : records) if (r.topic.equals(topic)) result.add(r);
        return result;
    }

    /**
     * 返回此次 poll 是否没有拉取到任何消息。
     *
     * @return true 表示无消息（可继续 poll 等待）；false 表示有消息可处理
     */
    public boolean isEmpty() { return records.isEmpty(); }

    /**
     * 返回此次 poll 拉取到的消息总数（所有 Topic 合计）。
     *
     * @return 消息总条数，>= 0
     */
    public int count() { return records.size(); }

    /**
     * 返回所有消息的迭代器，按 Fetcher 返回顺序（Topic + Partition 顺序）遍历。
     *
     * @return 消息迭代器，不支持 {@code remove} 操作
     */
    @Override
    public Iterator<ConsumerRecord> iterator() { return records.iterator(); }
}
