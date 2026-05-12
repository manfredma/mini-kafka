package com.github.minikafka.clients.consumer;

import com.github.minikafka.clients.network.Metadata;
import com.github.minikafka.clients.network.NetworkClient;
import com.github.minikafka.common.network.ByteBufferUtils;
import com.github.minikafka.common.protocol.ApiKeys;
import com.github.minikafka.common.record.RecordBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * 构造 FetchRequest、发送并解析 FetchResponse，将消息转换为 {@link ConsumerRecord} 列表。
 *
 * <p>对齐 Kafka {@code org.apache.kafka.clients.consumer.internals.Fetcher}（简化版）。
 * 每次 {@link #fetchRecords()} 调用会针对所有已分配分区构造一个批量 FetchRequest，
 * 并在同一次网络往返中获取所有分区的消息。
 *
 * <p>线程安全性：本类不是线程安全的，应由单一 Consumer 主线程调用。
 */
public final class Fetcher {

    private static final Logger log = LoggerFactory.getLogger(Fetcher.class);

    /** 网络客户端，用于发送 FetchRequest 和接收 FetchResponse。 */
    private final NetworkClient networkClient;
    /** 订阅状态，提供已分配分区列表和各分区当前 position，拉取后更新 position。 */
    private final SubscriptionState subscriptionState;
    /** 单次 FetchRequest 允许返回的最大字节数，均摊到每个分区。 */
    private final int fetchMaxBytes;

    /**
     * 构造 Fetcher。
     *
     * @param networkClient     网络客户端
     * @param subscriptionState 订阅状态，提供分区列表和 position
     * @param fetchMaxBytes     单次 fetch 最大字节数
     */
    public Fetcher(NetworkClient networkClient, SubscriptionState subscriptionState, int fetchMaxBytes) {
        this.networkClient = networkClient;
        this.subscriptionState = subscriptionState;
        this.fetchMaxBytes = fetchMaxBytes;
    }

    /**
     * 针对所有已分配分区构造并发送 FetchRequest，解析响应后返回消息列表。
     *
     * <p>FetchRequest 帧格式（简化版）：
     * <pre>
     * [4字节replicaId=-1][4字节maxWaitMs=500][4字节minBytes=0]
     * [4字节topicCount]
     *   ([string topic][4字节partCount]
     *     ([4字节partition][8字节fetchOffset][4字节maxBytes]) * partCount
     *   ) * topicCount
     * </pre>
     * 每个分区的 fetchOffset 取自 {@link SubscriptionState#position}，
     * 未初始化时（position == -1）从 offset 0 开始拉取。
     * 每个分区的 maxBytes 均摊为 {@code fetchMaxBytes / 分区数}。
     *
     * <p>若当前无已分配分区，直接返回空列表。
     *
     * @return 本次 fetch 拉取到的消息列表，按 topic + partition 顺序排列；无消息时返回空列表
     * @throws IOException 网络通信失败时抛出
     */
    public List<ConsumerRecord> fetchRecords() throws IOException {
        Set<String> assigned = subscriptionState.assignedPartitions();
        if (assigned.isEmpty()) return Collections.emptyList();

        Map<String, List<Integer>> topicPartitions = new LinkedHashMap<>();
        for (String tp : assigned) {
            int idx = tp.lastIndexOf('-');
            String topic = tp.substring(0, idx);
            int partition = Integer.parseInt(tp.substring(idx + 1));
            topicPartitions.computeIfAbsent(topic, k -> new ArrayList<>()).add(partition);
        }

        int bodySize = 4 + 4 + 4 + 4;
        for (Map.Entry<String, List<Integer>> e : topicPartitions.entrySet()) {
            bodySize += ByteBufferUtils.sizeOfString(e.getKey()) + 4;
            bodySize += e.getValue().size() * (4 + 8 + 4);
        }

        ByteBuffer body = ByteBuffer.allocate(bodySize);
        body.putInt(-1); body.putInt(500); body.putInt(0);
        body.putInt(topicPartitions.size());

        for (Map.Entry<String, List<Integer>> e : topicPartitions.entrySet()) {
            String topic = e.getKey();
            ByteBufferUtils.writeString(body, topic);
            body.putInt(e.getValue().size());
            for (int partition : e.getValue()) {
                long position = subscriptionState.position(topic, partition);
                if (position < 0) position = 0;
                body.putInt(partition);
                body.putLong(position);
                body.putInt(fetchMaxBytes / e.getValue().size());
            }
        }
        body.flip();

        ByteBuffer resp = networkClient.sendSync(ApiKeys.FETCH, body);
        return parseFetchResponse(resp);
    }

    /**
     * 解析 FetchResponse 帧，将 RecordBatch 反序列化为 {@link ConsumerRecord} 列表，
     * 并同步更新 {@link SubscriptionState} 中各分区的 position。
     *
     * <p>FetchResponse 帧格式：
     * <pre>
     * [4字节topicCount]
     *   ([string topic][4字节partCount]
     *     ([4字节partition][2字节errorCode][8字节highWatermark][4字节recordSize][recordBytes]) * partCount
     *   ) * topicCount
     * </pre>
     * errorCode 非零或 recordSize 为 0 时跳过该分区。
     * 每条消息拉取成功后，position 更新为 {@code record.offset() + 1}（下一条待拉取 offset）。
     * RecordBatch 解析异常时打印警告日志并跳过剩余数据，不中断整体流程。
     *
     * @param resp 已跳过 correlationId 的响应体 buffer
     * @return 解析出的消息列表；解析异常的分区消息被跳过
     */
    private List<ConsumerRecord> parseFetchResponse(ByteBuffer resp) {
        List<ConsumerRecord> result = new ArrayList<>();
        int topicCount = resp.getInt();
        for (int t = 0; t < topicCount; t++) {
            String topic = ByteBufferUtils.readString(resp);
            int partCount = resp.getInt();
            for (int p = 0; p < partCount; p++) {
                int partition = resp.getInt();
                short errorCode = resp.getShort();
                resp.getLong(); // highWatermark
                int recordSize = resp.getInt();

                if (errorCode != 0 || recordSize == 0) continue;

                byte[] recordBytes = new byte[recordSize];
                resp.get(recordBytes);
                ByteBuffer recordBuf = ByteBuffer.wrap(recordBytes);

                while (recordBuf.hasRemaining()) {
                    try {
                        RecordBatch batch = RecordBatch.readFrom(recordBuf);
                        for (com.github.minikafka.common.record.Record r : batch.records()) {
                            result.add(new ConsumerRecord(
                                topic, partition, r.offset(), r.timestamp(), r.key(), r.value()
                            ));
                            subscriptionState.setPosition(topic, partition, r.offset() + 1);
                        }
                    } catch (Exception e) {
                        log.warn("Error parsing RecordBatch", e);
                        break;
                    }
                }
            }
        }
        return result;
    }
}
