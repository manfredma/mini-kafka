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
 * Kafka Producer 主入口
 * 对齐 Kafka org.apache.kafka.clients.producer.KafkaProducer
 */
public final class KafkaProducer implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(KafkaProducer.class);

    private final ProducerConfig config;
    private final Metadata metadata;
    private final NetworkClient networkClient;
    private final RecordAccumulator accumulator;
    private final Sender sender;
    private final Thread senderThread;

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

    public Future<RecordMetadata> send(ProducerRecord record) throws IOException {
        ensureTopicMetadata(record.topic);
        int partition = selectPartition(record);
        return accumulator.append(record.topic, partition, record.key, record.value, record.timestamp);
    }

    /**
     * 同步发送（强制 drain 并立即发送），适合测试和低延迟场景
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

    private void ensureTopicMetadata(String topic) throws IOException {
        if (!metadata.containsTopic(topic)) {
            networkClient.updateMetadata(Collections.singletonList(topic));
        }
    }

    private int selectPartition(ProducerRecord record) {
        if (record.partition != null) return record.partition;
        int partitions = Math.max(1, metadata.partitionCount(record.topic));
        if (record.key != null) {
            return Math.abs(Arrays.hashCode(record.key)) % partitions;
        }
        return (int) (System.nanoTime() % partitions);
    }

    @Override
    public void close() throws IOException {
        sender.initiateClose();
        try { senderThread.join(5000); } catch (InterruptedException ignored) {}
        networkClient.close();
        log.info("KafkaProducer closed");
    }
}
