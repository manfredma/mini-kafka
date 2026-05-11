package com.github.minikafka.clients.producer;

import com.github.minikafka.clients.network.Metadata;
import com.github.minikafka.clients.network.NetworkClient;
import com.github.minikafka.common.network.ByteBufferUtils;
import com.github.minikafka.common.protocol.ApiKeys;
import com.github.minikafka.common.record.RecordBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * Producer 后台 IO 线程
 * 对齐 Kafka org.apache.kafka.clients.producer.internals.Sender
 */
public final class Sender implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(Sender.class);

    private final RecordAccumulator accumulator;
    private final NetworkClient networkClient;
    private volatile boolean running = true;
    private final long lingerMs;

    public Sender(RecordAccumulator accumulator, NetworkClient networkClient, long lingerMs) {
        this.accumulator = accumulator;
        this.networkClient = networkClient;
        this.lingerMs = lingerMs;
    }

    @Override
    public void run() {
        while (running) {
            try {
                if (lingerMs > 0) Thread.sleep(Math.min(lingerMs, 50));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                if (running) log.error("Sender error", e);
            }
        }
    }

    public void sendBatch(ProducerBatch batch) throws IOException {
        RecordBatch recordBatch = batch.toRecordBatch(0L);
        int batchSize = recordBatch.sizeInBytes();
        ByteBuffer batchBuf = ByteBuffer.allocate(batchSize);
        recordBatch.writeTo(batchBuf);
        batchBuf.flip();

        int bodySize = 4 + ByteBufferUtils.sizeOfString(batch.topic) + 4 + 4 + 4 + batchSize;
        ByteBuffer body = ByteBuffer.allocate(bodySize);
        body.putInt(1);
        ByteBufferUtils.writeString(body, batch.topic);
        body.putInt(1);
        body.putInt(batch.partition);
        body.putInt(batchSize);
        body.put(batchBuf);
        body.flip();

        ByteBuffer resp = networkClient.sendSync(ApiKeys.PRODUCE, body);
        resp.getInt(); // topicCount
        ByteBufferUtils.readString(resp); // topic
        resp.getInt(); // partCount
        resp.getInt(); // partition
        short errorCode = resp.getShort();
        long baseOffset = resp.getLong();

        if (errorCode == 0) {
            batch.done(baseOffset);
            log.debug("Sent batch to {}-{}, baseOffset={}", batch.topic, batch.partition, baseOffset);
        } else {
            batch.abort(new RuntimeException("Produce failed, errorCode=" + errorCode));
        }
    }

    public void initiateClose() { running = false; }
}
