package com.github.minikafka.clients.producer;

import com.github.minikafka.clients.network.Metadata;
import com.github.minikafka.clients.network.NetworkClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.Closeable;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

/**
 * Kafka Producer 主入口，提供消息发送的核心 API。
 *
 * <p>对齐 Kafka {@code org.apache.kafka.clients.producer.KafkaProducer}（简化版）。
 *
 * <p>初始化流程：
 * <ol>
 *   <li>解析 {@link ProducerConfig}，建立到 Bootstrap Broker 的 TCP 连接。</li>
 *   <li>初始化 {@link Metadata}（空元数据），待首次发送时按需刷新。</li>
 *   <li>创建 {@link RecordAccumulator} 和 {@link Sender}，启动后台 IO 线程。</li>
 * </ol>
 *
 * <p>线程安全性：{@link #send} 和 {@link #sendSync} 可在多线程中并发调用，
 * 内部 {@link RecordAccumulator#append} 已加锁。但 {@link NetworkClient} 不支持并发，
 * {@link #sendSync} 的实际网络发送需由调用方保证串行（或通过外部锁）。
 */
public final class KafkaProducer implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(KafkaProducer.class);

    private final ProducerConfig config;
    private final Metadata metadata;
    private final NetworkClient networkClient;
    private final RecordAccumulator accumulator;
    private final Sender sender;
    private final Thread senderThread;

    /**
     * 从 Properties 构造 Producer，并立即建立到 Bootstrap Broker 的 TCP 连接。
     *
     * @param props Producer 配置属性，支持的 key 见 {@link ProducerConfig}
     * @throws IOException 建立 TCP 连接失败时抛出
     */
    public KafkaProducer(Properties props) throws IOException {
        this.config = new ProducerConfig(props);
        String[] parts = config.bootstrapServers.split(":");
        String host = parts[0];
        int port = parts.length > 1 ? Integer.parseInt(parts[1]) : 9092;

        this.metadata = new Metadata(5 * 60 * 1000L);
        this.networkClient = new NetworkClient(host, port, config.clientId, metadata);
        this.networkClient.connect();
        this.metadata.update(new HashMap<>(), new ArrayList<>());

        this.accumulator = new RecordAccumulator(config.batchSize, config.lingerMs);
        this.sender = new Sender(accumulator, networkClient, config.lingerMs);
        this.senderThread = new Thread(sender, "kafka-producer-network-thread-" + config.clientId);
        this.senderThread.setDaemon(true);
        this.senderThread.start();
    }

    /**
     * 异步发送消息，将消息追加到 {@link RecordAccumulator} 后立即返回。
     *
     * <p>消息不会立即发送到 Broker，而是在批次满足 batchSize 或 lingerMs 条件后由
     * Sender 发送。若需要立即发送并等待结果，请使用 {@link #sendSync}。
     *
     * @param record 待发送的消息，{@link ProducerRecord#partition} 为 null 时自动选择分区
     * @return 发送结果 Future；批次成功发送后 complete 为 {@link RecordMetadata}，
     *         发送失败后 completeExceptionally
     * @throws IOException 刷新元数据时网络失败（仅在 Topic 首次发送时可能发生）
     */
    public Future<RecordMetadata> send(ProducerRecord record) throws IOException {
        ensureTopicMetadata(record.topic);
        int partition = selectPartition(record);
        return accumulator.append(record.topic, partition, record.key, record.value, record.timestamp);
    }

    /**
     * 同步发送消息，强制 drain 当前批次并立即调用 {@link Sender#sendBatch} 发送，
     * 阻塞直到收到 Broker 响应后返回 {@link RecordMetadata}。
     *
     * <p>适合测试场景或对延迟要求不高、需要确认发送结果的场景。
     * 注意：此方法在调用线程上执行网络 IO，不经过后台 Sender 线程。
     *
     * @param record 待发送的消息
     * @return 发送成功后的消息元数据，包含 topic、partition、offset、timestamp
     * @throws Exception 网络失败、Broker 返回错误码或 Future 执行异常时抛出
     */
    public RecordMetadata sendSync(ProducerRecord record) throws Exception {
        ensureTopicMetadata(record.topic);
        int partition = selectPartition(record);
        CompletableFuture<RecordMetadata> future = accumulator.append(
            record.topic, partition, record.key, record.value, record.timestamp
        );
        List<ProducerBatch> batches = accumulator.drain(record.topic, partition);
        for (ProducerBatch batch : batches) {
            sender.sendBatch(batch);
        }
        return future.get();
    }

    /**
     * 确保 {@link Metadata} 中包含指定 Topic 的分区信息。
     *
     * <p>若 Topic 不在缓存中，则向 Broker 发送 MetadataRequest 并等待响应更新缓存。
     * 每次 {@link #send} 和 {@link #sendSync} 调用前都会触发此检查。
     *
     * @param topic 需要确认元数据存在的 Topic 名称
     * @throws IOException 网络请求失败时抛出
     */
    private void ensureTopicMetadata(String topic) throws IOException {
        if (!metadata.containsTopic(topic)) {
            networkClient.updateMetadata(Collections.singletonList(topic));
        }
    }

    /**
     * 为消息选择目标分区。
     *
     * <p>选择策略（按优先级）：
     * <ol>
     *   <li>若 {@link ProducerRecord#partition} 不为 null，直接使用指定分区。</li>
     *   <li>若 {@link ProducerRecord#key} 不为 null，对 key 字节数组取 hash 后对分区数取模（保证相同 key 路由到相同分区）。</li>
     *   <li>否则，使用 {@code System.nanoTime()} 对分区数取模（近似轮询，分散负载）。</li>
     * </ol>
     *
     * @param record 待发送的消息
     * @return 选中的分区编号（0 到 partitionCount-1）
     */
    private int selectPartition(ProducerRecord record) {
        if (record.partition != null) return record.partition;
        int partitions = Math.max(1, metadata.partitionCount(record.topic));
        if (record.key != null) {
            return Math.abs(Arrays.hashCode(record.key)) % partitions;
        }
        return (int) (System.nanoTime() % partitions);
    }

    /**
     * 关闭 Producer，停止后台 Sender 线程并释放网络连接。
     *
     * <p>关闭流程：
     * <ol>
     *   <li>调用 {@link Sender#initiateClose()} 通知后台线程停止。</li>
     *   <li>等待后台线程最多 5 秒退出。</li>
     *   <li>关闭 {@link NetworkClient} 释放 TCP 连接。</li>
     * </ol>
     * 注意：关闭时不保证 {@link RecordAccumulator} 中的待发消息全部发送完毕。
     * 如需确保消息发送，应在 close 前调用 {@link #sendSync} 或等待所有 Future 完成。
     *
     * @throws IOException 关闭网络连接失败时抛出
     */
    @Override
    public void close() throws IOException {
        sender.initiateClose();
        try { senderThread.join(5000); } catch (InterruptedException ignored) {}
        networkClient.close();
        log.info("KafkaProducer closed");
    }
}
