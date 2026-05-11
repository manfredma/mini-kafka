package com.github.minikafka.server.controller;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 集群元数据内存视图
 */
public final class ControllerContext {

    private final Map<String, Integer> topicPartitions = new ConcurrentHashMap<>();
    private final Map<String, Integer> partitionLeaders = new ConcurrentHashMap<>();

    public void createTopic(String topic, int partitions) {
        topicPartitions.put(topic, partitions);
        for (int i = 0; i < partitions; i++) {
            partitionLeaders.put(topic + "-" + i, 0);
        }
    }

    public boolean topicExists(String topic) {
        return topicPartitions.containsKey(topic);
    }

    public int partitionCount(String topic) {
        return topicPartitions.getOrDefault(topic, 0);
    }

    public Integer leaderFor(String topic, int partition) {
        return partitionLeaders.get(topic + "-" + partition);
    }

    public Map<String, Integer> allTopics() {
        return Collections.unmodifiableMap(topicPartitions);
    }
}
