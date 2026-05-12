package com.github.minikafka.clients.network;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 客户端集群元数据缓存，保存 Broker 列表与 Topic/Partition 的 leader 信息。
 *
 * <p>对齐 Kafka {@code org.apache.kafka.clients.Metadata}。
 *
 * <p>元数据刷新由两种机制触发：
 * <ol>
 *   <li><b>时间过期</b>：距上次刷新超过 {@code metadataExpireMs} 毫秒后，
 *       {@link #needsUpdate()} 返回 true。</li>
 *   <li><b>主动请求</b>：调用 {@link #requestUpdate()} 后，{@link #needsUpdate()}
 *       在下次 poll 前立即返回 true。</li>
 * </ol>
 *
 * <p>对外暴露的读写方法均为 {@code synchronized}，可在多线程环境下安全使用。
 * {@link #needsUpdate()} 和 {@link #requestUpdate()} 使用 {@link AtomicBoolean}，
 * 无需持有对象锁即可安全调用。
 */
public final class Metadata {

    /**
     * Broker 节点信息。
     */
    public static final class BrokerInfo {
        /** Broker 的节点 ID，对应 Kafka 配置中的 {@code broker.id}。 */
        public final int nodeId;
        /** Broker 的主机名或 IP 地址。 */
        public final String host;
        /** Broker 的监听端口。 */
        public final int port;

        public BrokerInfo(int nodeId, String host, int port) {
            this.nodeId = nodeId; this.host = host; this.port = port;
        }
    }

    /**
     * 单个分区的元数据，包含 leader 节点的连接信息。
     */
    public static final class PartitionMetadata {
        /** 分区编号，从 0 开始。 */
        public final int partition;
        /** 该分区 leader 所在 Broker 的主机名。 */
        public final String leaderHost;
        /** 该分区 leader 所在 Broker 的端口。 */
        public final int leaderPort;
        /** 该分区 leader 的 Broker 节点 ID。 */
        public final int leaderId;

        public PartitionMetadata(int partition, String leaderHost, int leaderPort, int leaderId) {
            this.partition = partition;
            this.leaderHost = leaderHost;
            this.leaderPort = leaderPort;
            this.leaderId = leaderId;
        }

        /** 返回 leader 端口，等同于 {@link #leaderPort}，提供更短的调用路径。 */
        public int port() { return leaderPort; }
    }

    /**
     * 单个 Topic 的元数据，包含其所有分区的 leader 信息。
     */
    public static final class TopicMetadata {
        /** Topic 名称。 */
        public final String topic;
        /** 该 Topic 的所有分区元数据列表，顺序与分区编号不保证一致。 */
        public final List<PartitionMetadata> partitions;

        public TopicMetadata(String topic, List<PartitionMetadata> partitions) {
            this.topic = topic;
            this.partitions = partitions;
        }
    }

    /** 元数据的有效期（毫秒），超过此时间后 {@link #needsUpdate()} 返回 true。 */
    private final long metadataExpireMs;
    /** Topic 名称到 TopicMetadata 的映射，每次 {@link #update} 时整体替换。 */
    private Map<String, TopicMetadata> topicMetadataMap = new HashMap<>();
    /** 当前已知的 Broker 列表，每次 {@link #update} 时整体替换。 */
    private List<BrokerInfo> brokers = new ArrayList<>();
    /** 上次成功刷新元数据的时间戳（毫秒）。 */
    private long lastRefreshMs = 0;
    /**
     * 主动刷新标志。{@link #requestUpdate()} 将其置为 true；
     * {@link #update} 成功后将其置为 false。
     */
    private final AtomicBoolean needsUpdate = new AtomicBoolean(true);

    /**
     * 构造元数据缓存。
     *
     * @param metadataExpireMs 元数据过期时间（毫秒），超过后自动触发刷新
     */
    public Metadata(long metadataExpireMs) {
        this.metadataExpireMs = metadataExpireMs;
    }

    /**
     * 原子替换全部元数据，并重置刷新标志和时间戳。
     *
     * <p>此方法为 {@code synchronized}，线程安全。通常由 {@link NetworkClient#updateMetadata}
     * 在收到服务端 MetadataResponse 后调用。
     *
     * @param topics  新的 Topic 元数据映射，key 为 topic 名称
     * @param brokers 新的 Broker 列表；传入空列表表示无已知 Broker（仍保留 bootstrap 地址）
     */
    public synchronized void update(Map<String, TopicMetadata> topics, List<BrokerInfo> brokers) {
        this.topicMetadataMap = new HashMap<>(topics);
        this.brokers = new ArrayList<>(brokers);
        this.lastRefreshMs = System.currentTimeMillis();
        this.needsUpdate.set(false);
    }

    /**
     * 判断缓存中是否已包含指定 Topic 的元数据。
     *
     * @param topic Topic 名称
     * @return true 表示已有该 Topic 的分区信息；false 表示需要先刷新元数据
     */
    public synchronized boolean containsTopic(String topic) {
        return topicMetadataMap.containsKey(topic);
    }

    /**
     * 返回指定 Topic 的分区数量。
     *
     * @param topic Topic 名称
     * @return 分区数量；若 Topic 不存在于缓存中则返回 0
     */
    public synchronized int partitionCount(String topic) {
        TopicMetadata tm = topicMetadataMap.get(topic);
        return tm == null ? 0 : tm.partitions.size();
    }

    /**
     * 返回指定 Topic 指定分区的 leader 元数据。
     *
     * @param topic     Topic 名称
     * @param partition 分区编号（从 0 开始）
     * @return 对应分区的 {@link PartitionMetadata}；
     *         若 Topic 不存在或该分区编号不存在则返回 null
     */
    public synchronized PartitionMetadata leaderFor(String topic, int partition) {
        TopicMetadata tm = topicMetadataMap.get(topic);
        if (tm == null) return null;
        for (PartitionMetadata pm : tm.partitions) {
            if (pm.partition == partition) return pm;
        }
        return null;
    }

    /**
     * 返回当前已知的 Broker 列表（只读视图）。
     *
     * @return 不可修改的 Broker 列表；列表可能为空（尚未刷新时）
     */
    public synchronized List<BrokerInfo> brokers() { return Collections.unmodifiableList(brokers); }

    /**
     * 判断元数据是否需要刷新。
     *
     * <p>满足以下任一条件时返回 true：
     * <ul>
     *   <li>调用过 {@link #requestUpdate()}（主动触发）</li>
     *   <li>距上次刷新已超过 {@code metadataExpireMs} 毫秒（时间过期）</li>
     * </ul>
     * 此方法不加锁，可在任意线程中安全调用。
     *
     * @return true 表示需要立即刷新元数据
     */
    public boolean needsUpdate() {
        return needsUpdate.get() || (System.currentTimeMillis() - lastRefreshMs) > metadataExpireMs;
    }

    /**
     * 主动标记元数据需要刷新，下次 {@link #needsUpdate()} 调用将立即返回 true。
     *
     * <p>通常在发现 Topic 不存在或收到 LEADER_NOT_AVAILABLE 错误时调用。
     * 此方法不加锁，可在任意线程中安全调用。
     */
    public void requestUpdate() { needsUpdate.set(true); }
}
