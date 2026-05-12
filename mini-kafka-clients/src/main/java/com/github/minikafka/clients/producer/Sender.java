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
 * Producer 后台 IO 线程，负责将就绪批次发送到 Broker。
 *
 * <p>对齐 Kafka {@code org.apache.kafka.clients.producer.internals.Sender}（简化版）。
 *
 * <p>简化说明：在本实现中，后台线程（{@link #run()}）仅按 lingerMs 周期休眠，
 * 并不主动 drain 和发送批次。实际发送由 {@link KafkaProducer#sendSync} 在调用线程
 * 上同步触发，通过直接调用 {@link #sendBatch(ProducerBatch)} 完成。
 * 这与 Kafka 原版的异步 IO 循环不同，简化了并发模型。
 *
 * <p>线程安全性：{@link #sendBatch} 和 {@link #run} 运行在不同线程，
 * 但由于 {@link #sendBatch} 仅在 {@link KafkaProducer#sendSync} 的调用线程上执行，
 * 且 {@link NetworkClient} 不支持并发，调用方需保证同一时刻只有一个线程调用 {@link #sendBatch}。
 */
public final class Sender implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(Sender.class);

    /** 消息积累器，用于在 drain 场景下获取就绪批次（当前后台线程未使用）。 */
    private final RecordAccumulator accumulator;
    /** 网络客户端，用于实际发送请求和接收响应。 */
    private final NetworkClient networkClient;
    /** 后台线程运行标志，{@link #initiateClose()} 将其置为 false 以停止循环。 */
    private volatile boolean running = true;
    /** lingerMs 配置，后台线程据此决定每次循环的休眠时长。 */
    private final long lingerMs;

    /**
     * 构造 Sender。
     *
     * @param accumulator   消息积累器
     * @param networkClient 网络客户端
     * @param lingerMs      批次等待时间（毫秒），后台线程以此为休眠间隔
     */
    public Sender(RecordAccumulator accumulator, NetworkClient networkClient, long lingerMs) {
        this.accumulator = accumulator;
        this.networkClient = networkClient;
        this.lingerMs = lingerMs;
    }

    /**
     * 后台 IO 线程主循环。
     *
     * <p>当前实现仅按 {@code min(lingerMs, 50)} 毫秒周期休眠，不主动发送批次。
     * 实际消息发送由 {@link KafkaProducer#sendSync} 在调用线程上同步完成。
     * 循环在 {@link #initiateClose()} 被调用后退出。
     */
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

    /**
     * 将一个 {@link ProducerBatch} 序列化并通过 {@link NetworkClient} 同步发送到 Broker。
     *
     * <p>发送流程：
     * <ol>
     *   <li>将批次转换为 {@link RecordBatch}（使用 baseOffset=0，服务端会分配真实 offset）。</li>
     *   <li>构造 ProduceRequest 帧：{@code [topicCount=1][topic][partCount=1][partition][batchSize][batchBytes]}</li>
     *   <li>调用 {@link NetworkClient#sendSync} 发送并阻塞等待响应。</li>
     *   <li>解析 ProduceResponse：读取 errorCode 和 baseOffset。</li>
     *   <li>errorCode == 0 时调用 {@link ProducerBatch#done(long)}，否则调用 {@link ProducerBatch#abort(Exception)}。</li>
     * </ol>
     *
     * @param batch 待发送的批次，发送后其所有 Future 将被完成（成功或失败）
     * @throws IOException 网络通信失败时抛出
     */
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

    /**
     * 通知后台线程停止运行。
     *
     * <p>将 {@link #running} 置为 false，后台线程在当次休眠结束后退出循环。
     * 调用方通常还需要调用 {@code senderThread.join()} 等待线程实际退出。
     */
    public void initiateClose() { running = false; }
}
