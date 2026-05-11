package com.github.minikafka.clients.consumer;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 跟踪分区的 position 和 committed offset
 * 对齐 Kafka org.apache.kafka.clients.consumer.internals.SubscriptionState
 */
public final class SubscriptionState {

    private final Set<String> subscribedTopics = new LinkedHashSet<>();
    private final Map<String, Long> positions = new ConcurrentHashMap<>();
    private final Map<String, Long> committed = new ConcurrentHashMap<>();
    private final Set<String> assignedPartitions = new LinkedHashSet<>();

    public void subscribe(Collection<String> topics) {
        subscribedTopics.addAll(topics);
    }

    public void assignPartitions(Map<String, List<Integer>> topicPartitions) {
        assignedPartitions.clear();
        for (Map.Entry<String, List<Integer>> e : topicPartitions.entrySet()) {
            for (int p : e.getValue()) assignedPartitions.add(e.getKey() + "-" + p);
        }
    }

    public Set<String> assignedPartitions() { return Collections.unmodifiableSet(assignedPartitions); }
    public Set<String> subscribedTopics() { return Collections.unmodifiableSet(subscribedTopics); }

    public long position(String topic, int partition) {
        return positions.getOrDefault(key(topic, partition), -1L);
    }

    public void setPosition(String topic, int partition, long offset) {
        positions.put(key(topic, partition), offset);
    }

    public void seek(String topic, int partition, long offset) {
        positions.put(key(topic, partition), offset);
    }

    public long committed(String topic, int partition) {
        return committed.getOrDefault(key(topic, partition), -1L);
    }

    public void setCommitted(String topic, int partition, long offset) {
        committed.put(key(topic, partition), offset);
    }

    private String key(String topic, int partition) { return topic + "-" + partition; }
}
