package com.github.minikafka.server.controller;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 集群元数据的内存视图，存储 topic 分区数和各分区的 Leader broker ID。
 * <p>
 * 对应 Kafka 原版 {@code kafka.controller.ControllerContext}（大幅简化版）。
 * mini-kafka 为单节点架构，所有分区的 Leader 固定为 broker 0。
 * 使用 {@link ConcurrentHashMap} 存储，支持并发读取；写操作由外层
 * {@link KafkaController} 的 {@code synchronized} 方法保证互斥。
 * </p>
 */
public final class ControllerContext {

    private final Map<String, Integer> topicPartitions = new ConcurrentHashMap<>();
    private final Map<String, Integer> partitionLeaders = new ConcurrentHashMap<>();

    /**
     * 注册 topic 元数据，并将所有分区的 Leader 设置为 broker 0（单节点固定值）。
     * <p>
     * 若 topic 已存在，此方法会覆盖原有分区数和 Leader 信息（幂等写入）。
     * 通常由 {@link KafkaController#createTopic} 在创建 Log 后调用。
     * </p>
     *
     * @param topic      topic 名称，不得为 {@code null}
     * @param partitions 分区数量，正整数
     */
    public void createTopic(String topic, int partitions) {
        topicPartitions.put(topic, partitions);
        for (int i = 0; i < partitions; i++) {
            partitionLeaders.put(topic + "-" + i, 0);
        }
    }

    /**
     * 判断指定 topic 是否已在元数据中注册。
     *
     * @param topic topic 名称
     * @return {@code true} 表示 topic 已存在
     */
    public boolean topicExists(String topic) {
        return topicPartitions.containsKey(topic);
    }

    /**
     * 返回指定 topic 的分区数量。
     *
     * @param topic topic 名称
     * @return 分区数量；若 topic 不存在，返回 0
     */
    public int partitionCount(String topic) {
        return topicPartitions.getOrDefault(topic, 0);
    }

    /**
     * 返回指定 topic-partition 的 Leader broker ID。
     *
     * @param topic     topic 名称
     * @param partition 分区编号
     * @return Leader broker ID（mini-kafka 中固定为 0）；
     *         若该分区元数据不存在，返回 {@code null}
     */
    public Integer leaderFor(String topic, int partition) {
        return partitionLeaders.get(topic + "-" + partition);
    }

    /**
     * 返回所有已注册 topic 及其分区数的只读视图。
     *
     * @return 不可修改的 topic → partitionCount 映射
     */
    public Map<String, Integer> allTopics() {
        return Collections.unmodifiableMap(topicPartitions);
    }
}
