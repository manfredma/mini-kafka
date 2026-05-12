package com.github.minikafka.clients.consumer;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 跟踪 Consumer 的订阅状态、已分配分区及各分区的消费进度（position 和 committed offset）。
 *
 * <p>对齐 Kafka {@code org.apache.kafka.clients.consumer.internals.SubscriptionState}（简化版）。
 *
 * <p>内部 key 格式统一为 {@code "topic-partition"}（由私有方法 {@link #key} 生成）。
 *
 * <p>线程安全性：{@code positions} 和 {@code committed} 使用 {@link ConcurrentHashMap}，
 * 可在多线程中安全读写。{@code subscribedTopics} 和 {@code assignedPartitions} 使用
 * {@link LinkedHashSet}，需由调用方（通常为单线程 Consumer 主线程）保证访问安全。
 */
public final class SubscriptionState {

    /** 已订阅的 Topic 集合，保持插入顺序，由 {@link KafkaConsumer#subscribe} 填充。 */
    private final Set<String> subscribedTopics = new LinkedHashSet<>();

    /**
     * 各分区的当前消费位置（下一条待拉取消息的 offset）。
     * key 格式：{@code "topic-partition"}；value 为 offset；-1 表示尚未初始化。
     */
    private final Map<String, Long> positions = new ConcurrentHashMap<>();

    /**
     * 各分区已提交到 Coordinator 的 offset。
     * key 格式：{@code "topic-partition"}；value 为 offset；-1 表示尚未提交。
     */
    private final Map<String, Long> committed = new ConcurrentHashMap<>();

    /**
     * Rebalance 后分配给本 Consumer 的分区集合。
     * key 格式：{@code "topic-partition"}，由 {@link ConsumerCoordinator} 在 SyncGroup 后更新。
     */
    private final Set<String> assignedPartitions = new LinkedHashSet<>();

    /**
     * 订阅指定的 Topic 列表。允许多次调用（追加语义，不覆盖已有订阅）。
     *
     * <p>订阅后需等待下次 Rebalance 完成才能分配到具体分区。
     *
     * @param topics 要订阅的 Topic 名称集合，不允许为 null
     */
    public void subscribe(Collection<String> topics) {
        subscribedTopics.addAll(topics);
    }

    /**
     * Rebalance 完成后，更新本 Consumer 被分配的分区集合（全量替换）。
     *
     * <p>此方法会清空原有分配，以新分配完整替换。
     * 通常由 {@link ConsumerCoordinator} 在 SyncGroup 响应解析后调用。
     *
     * @param topicPartitions 新的分区分配结果，key 为 Topic 名称，value 为分区编号列表
     */
    public void assignPartitions(Map<String, List<Integer>> topicPartitions) {
        assignedPartitions.clear();
        for (Map.Entry<String, List<Integer>> e : topicPartitions.entrySet()) {
            for (int p : e.getValue()) assignedPartitions.add(e.getKey() + "-" + p);
        }
    }

    /**
     * 返回当前已分配分区的只读集合，元素格式为 {@code "topic-partition"}。
     *
     * @return 不可修改的分区 key 集合；Rebalance 前为空集合
     */
    public Set<String> assignedPartitions() { return Collections.unmodifiableSet(assignedPartitions); }

    /**
     * 返回已订阅 Topic 的只读集合。
     *
     * @return 不可修改的 Topic 名称集合
     */
    public Set<String> subscribedTopics() { return Collections.unmodifiableSet(subscribedTopics); }

    /**
     * 返回指定分区的当前消费位置（下一条待拉取消息的 offset）。
     *
     * @param topic     Topic 名称
     * @param partition 分区编号
     * @return 当前 position；-1 表示该分区尚未初始化（未拉取过任何消息）
     */
    public long position(String topic, int partition) {
        return positions.getOrDefault(key(topic, partition), -1L);
    }

    /**
     * 设置指定分区的消费位置，通常由 {@link Fetcher} 在成功拉取消息后调用。
     *
     * @param topic     Topic 名称
     * @param partition 分区编号
     * @param offset    新的 position（下一条待拉取消息的 offset = 已拉取最大 offset + 1）
     */
    public void setPosition(String topic, int partition, long offset) {
        positions.put(key(topic, partition), offset);
    }

    /**
     * 手动定位消费位置，跳过或回退到指定 offset。
     *
     * <p>与 {@link #setPosition} 语义相同，提供更符合 Kafka API 习惯的命名。
     * 下次 {@link Fetcher#fetchRecords} 将从此 offset 开始拉取。
     *
     * @param topic     Topic 名称
     * @param partition 分区编号
     * @param offset    目标 offset（包含，即下次拉取从此 offset 开始）
     */
    public void seek(String topic, int partition, long offset) {
        positions.put(key(topic, partition), offset);
    }

    /**
     * 返回指定分区已提交到 Coordinator 的 offset。
     *
     * @param topic     Topic 名称
     * @param partition 分区编号
     * @return 已提交的 offset；-1 表示尚未提交过（首次消费）
     */
    public long committed(String topic, int partition) {
        return committed.getOrDefault(key(topic, partition), -1L);
    }

    /**
     * 更新指定分区的已提交 offset，由 {@link ConsumerCoordinator#commitOffset} 在
     * OffsetCommitRequest 成功后调用。
     *
     * @param topic     Topic 名称
     * @param partition 分区编号
     * @param offset    已提交的 offset 值
     */
    public void setCommitted(String topic, int partition, long offset) {
        committed.put(key(topic, partition), offset);
    }

    /**
     * 生成内部 Map 使用的 TopicPartition key。
     *
     * @param topic     Topic 名称
     * @param partition 分区编号
     * @return 格式为 {@code "topic-partition"} 的 key 字符串
     */
    private String key(String topic, int partition) { return topic + "-" + partition; }
}
