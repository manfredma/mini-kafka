package com.github.minikafka.clients.network;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 客户端集群元数据缓存
 * 对齐 Kafka org.apache.kafka.clients.Metadata
 */
public final class Metadata {

    public static final class BrokerInfo {
        public final int nodeId;
        public final String host;
        public final int port;
        public BrokerInfo(int nodeId, String host, int port) {
            this.nodeId = nodeId; this.host = host; this.port = port;
        }
    }

    public static final class PartitionMetadata {
        public final int partition;
        public final String leaderHost;
        public final int leaderPort;
        public final int leaderId;
        public PartitionMetadata(int partition, String leaderHost, int leaderPort, int leaderId) {
            this.partition = partition;
            this.leaderHost = leaderHost;
            this.leaderPort = leaderPort;
            this.leaderId = leaderId;
        }
        public int port() { return leaderPort; }
    }

    public static final class TopicMetadata {
        public final String topic;
        public final List<PartitionMetadata> partitions;
        public TopicMetadata(String topic, List<PartitionMetadata> partitions) {
            this.topic = topic;
            this.partitions = partitions;
        }
    }

    private final long metadataExpireMs;
    private Map<String, TopicMetadata> topicMetadataMap = new HashMap<>();
    private List<BrokerInfo> brokers = new ArrayList<>();
    private long lastRefreshMs = 0;
    private final AtomicBoolean needsUpdate = new AtomicBoolean(true);

    public Metadata(long metadataExpireMs) {
        this.metadataExpireMs = metadataExpireMs;
    }

    public synchronized void update(Map<String, TopicMetadata> topics, List<BrokerInfo> brokers) {
        this.topicMetadataMap = new HashMap<>(topics);
        this.brokers = new ArrayList<>(brokers);
        this.lastRefreshMs = System.currentTimeMillis();
        this.needsUpdate.set(false);
    }

    public synchronized boolean containsTopic(String topic) {
        return topicMetadataMap.containsKey(topic);
    }

    public synchronized int partitionCount(String topic) {
        TopicMetadata tm = topicMetadataMap.get(topic);
        return tm == null ? 0 : tm.partitions.size();
    }

    public synchronized PartitionMetadata leaderFor(String topic, int partition) {
        TopicMetadata tm = topicMetadataMap.get(topic);
        if (tm == null) return null;
        for (PartitionMetadata pm : tm.partitions) {
            if (pm.partition == partition) return pm;
        }
        return null;
    }

    public synchronized List<BrokerInfo> brokers() { return Collections.unmodifiableList(brokers); }

    public boolean needsUpdate() {
        return needsUpdate.get() || (System.currentTimeMillis() - lastRefreshMs) > metadataExpireMs;
    }

    public void requestUpdate() { needsUpdate.set(true); }
}
