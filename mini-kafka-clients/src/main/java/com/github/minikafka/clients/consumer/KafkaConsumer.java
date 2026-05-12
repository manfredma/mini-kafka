package com.github.minikafka.clients.consumer;

import com.github.minikafka.clients.network.Metadata;
import com.github.minikafka.clients.network.NetworkClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.*;

/**
 * Kafka Consumer 主入口，提供 poll 模型的消息消费 API。
 *
 * <p>对齐 Kafka {@code org.apache.kafka.clients.consumer.KafkaConsumer}（简化版）。
 *
 * <p>初始化流程：
 * <ol>
 *   <li>解析 {@link ConsumerConfig}，建立到 Bootstrap Broker 的 TCP 连接。</li>
 *   <li>初始化 {@link Metadata}、{@link SubscriptionState}、{@link ConsumerCoordinator}
 *       和 {@link Fetcher}。</li>
 * </ol>
 *
 * <p>典型使用流程：
 * <pre>{@code
 * consumer.subscribe(List.of("my-topic"));
 * while (true) {
 *     ConsumerRecords records = consumer.poll(Duration.ofMillis(100));
 *     for (ConsumerRecord r : records) { ... }
 * }
 * }</pre>
 *
 * <p>线程安全性：本类不是线程安全的，所有方法应由单一线程调用（Kafka 官方同款约束）。
 */
public final class KafkaConsumer implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumer.class);

    private final ConsumerConfig config;
    private final Metadata metadata;
    private final NetworkClient networkClient;
    private final SubscriptionState subscriptionState;
    private final ConsumerCoordinator coordinator;
    private final Fetcher fetcher;
    /** 上次自动提交 offset 的时间戳（毫秒），用于 {@link #maybeAutoCommit} 限流。 */
    private long lastAutoCommitMs = System.currentTimeMillis();

    /**
     * 从 Properties 构造 Consumer，并立即建立到 Bootstrap Broker 的 TCP 连接。
     *
     * @param props Consumer 配置属性，支持的 key 见 {@link ConsumerConfig}
     * @throws IOException 建立 TCP 连接失败时抛出
     */
    public KafkaConsumer(Properties props) throws IOException {
        this.config = new ConsumerConfig(props);
        String[] parts = config.bootstrapServers.split(":");
        String host = parts[0];
        int port = parts.length > 1 ? Integer.parseInt(parts[1]) : 9092;

        this.metadata = new Metadata(5 * 60 * 1000L);
        this.networkClient = new NetworkClient(host, port, config.clientId, metadata);
        this.networkClient.connect();
        this.metadata.update(new HashMap<>(), new ArrayList<>());

        this.subscriptionState = new SubscriptionState();
        this.coordinator = new ConsumerCoordinator(
            config.groupId, config, networkClient, metadata, subscriptionState
        );
        this.fetcher = new Fetcher(networkClient, subscriptionState, config.fetchMaxBytes);
    }

    /**
     * 订阅指定的 Topic 列表，并立即触发一次元数据刷新。
     *
     * <p>订阅后不会立即分配分区，分区分配在下次 {@link #poll} 调用时通过
     * {@link ConsumerCoordinator#ensureActiveGroup} 触发 Rebalance 完成。
     * 元数据刷新失败时仅打印警告日志，不抛出异常（Rebalance 时会再次尝试）。
     *
     * @param topics 要订阅的 Topic 名称集合，不允许为 null 或包含 null 元素
     */
    public void subscribe(Collection<String> topics) {
        subscriptionState.subscribe(topics);
        try {
            networkClient.updateMetadata(new ArrayList<>(topics));
        } catch (IOException e) {
            log.warn("Failed to update metadata on subscribe", e);
        }
    }

    /**
     * 拉取消息，确保入组，执行 Fetch，并按需自动提交 offset。
     *
     * <p>执行流程：
     * <ol>
     *   <li>调用 {@link ConsumerCoordinator#ensureActiveGroup} 确保已加入 Group（首次或心跳失败后触发 Rebalance）。</li>
     *   <li>调用 {@link Fetcher#fetchRecords} 拉取所有已分配分区的消息。</li>
     *   <li>若 {@link ConsumerConfig#enableAutoCommit} 为 true，检查是否需要自动提交。</li>
     * </ol>
     *
     * <p>注意：当前实现忽略 {@code timeout} 参数（不做超时控制），每次调用均为同步阻塞。
     *
     * @param timeout 本次 poll 的最长等待时间（当前实现未使用，保留以对齐 Kafka API 签名）
     * @return 拉取到的消息集合；无消息时返回 {@link ConsumerRecords#EMPTY}，不返回 null
     * @throws IOException 网络通信失败时抛出
     */
    public ConsumerRecords poll(Duration timeout) throws IOException {
        coordinator.ensureActiveGroup();
        List<ConsumerRecord> records = fetcher.fetchRecords();
        if (config.enableAutoCommit) maybeAutoCommit();
        return records.isEmpty() ? ConsumerRecords.EMPTY : new ConsumerRecords(records);
    }

    /**
     * 同步提交所有已分配分区的当前消费位置（position）到 Coordinator。
     *
     * <p>遍历 {@link SubscriptionState#assignedPartitions}，对每个 position >= 0 的分区
     * 调用 {@link ConsumerCoordinator#commitOffset}。position 为 -1 的分区（尚未拉取过消息）跳过。
     *
     * @throws IOException 网络通信失败时抛出
     */
    public void commitSync() throws IOException {
        for (String tp : subscriptionState.assignedPartitions()) {
            int idx = tp.lastIndexOf('-');
            String topic = tp.substring(0, idx);
            int partition = Integer.parseInt(tp.substring(idx + 1));
            long position = subscriptionState.position(topic, partition);
            if (position >= 0) coordinator.commitOffset(topic, partition, position);
        }
    }

    /**
     * 按 {@link ConsumerConfig#autoCommitIntervalMs} 间隔自动提交 offset（限流）。
     *
     * <p>距上次提交时间未超过间隔则直接返回；超过后调用 {@link #commitSync} 提交。
     *
     * @throws IOException 网络通信失败时抛出
     */
    private void maybeAutoCommit() throws IOException {
        long now = System.currentTimeMillis();
        if (now - lastAutoCommitMs >= config.autoCommitIntervalMs) {
            commitSync();
            lastAutoCommitMs = now;
        }
    }

    /**
     * 手动定位指定分区的消费位置，下次 {@link #poll} 将从此 offset 开始拉取消息。
     *
     * <p>可用于：
     * <ul>
     *   <li>回退重消费（seek 到更早的 offset）</li>
     *   <li>跳过异常消息（seek 到当前 offset + 1）</li>
     *   <li>从头消费（seek 到 0）</li>
     * </ul>
     * 此方法仅更新本地 {@link SubscriptionState}，不向 Broker 发送任何请求。
     *
     * @param topic     Topic 名称
     * @param partition 分区编号
     * @param offset    目标 offset（包含，即下次拉取从此 offset 开始）
     */
    public void seek(String topic, int partition, long offset) {
        subscriptionState.seek(topic, partition, offset);
    }

    /**
     * 关闭 Consumer，主动退出 Consumer Group 并释放网络连接。
     *
     * <p>关闭流程：
     * <ol>
     *   <li>调用 {@link ConsumerCoordinator#leaveGroup} 发送 LeaveGroupRequest，
     *       触发服务端立即 Rebalance（而非等待 sessionTimeoutMs 超时）。</li>
     *   <li>关闭 {@link NetworkClient} 释放 TCP 连接。</li>
     * </ol>
     * leaveGroup 失败时静默忽略异常，确保 close 流程不因网络问题中断。
     *
     * @throws IOException 关闭网络连接失败时抛出
     */
    @Override
    public void close() throws IOException {
        try { coordinator.leaveGroup(); } catch (Exception ignored) {}
        networkClient.close();
        log.info("KafkaConsumer closed");
    }
}
