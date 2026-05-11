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
 * 构造并发送 FetchRequest，解析 FetchResponse
 * 对齐 Kafka org.apache.kafka.clients.consumer.internals.Fetcher
 */
public final class Fetcher {

    private static final Logger log = LoggerFactory.getLogger(Fetcher.class);

    private final NetworkClient networkClient;
    private final SubscriptionState subscriptionState;
    private final int fetchMaxBytes;

    public Fetcher(NetworkClient networkClient, SubscriptionState subscriptionState, int fetchMaxBytes) {
        this.networkClient = networkClient;
        this.subscriptionState = subscriptionState;
        this.fetchMaxBytes = fetchMaxBytes;
    }

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
