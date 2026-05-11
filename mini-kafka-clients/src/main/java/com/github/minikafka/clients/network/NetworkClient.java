package com.github.minikafka.clients.network;

import com.github.minikafka.common.network.ByteBufferUtils;
import com.github.minikafka.common.protocol.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.*;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 基于 NIO 的网络客户端（简化版：同步发送）
 * 对齐 Kafka org.apache.kafka.clients.NetworkClient
 */
public final class NetworkClient implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(NetworkClient.class);

    private final String bootstrapHost;
    private final int bootstrapPort;
    private final Metadata metadata;
    private final String clientId;
    private final AtomicInteger correlationIdCounter = new AtomicInteger(0);

    private volatile SocketChannel channel;
    private volatile boolean connected = false;

    public NetworkClient(String bootstrapHost, int bootstrapPort,
                         String clientId, Metadata metadata) {
        this.bootstrapHost = bootstrapHost;
        this.bootstrapPort = bootstrapPort;
        this.clientId = clientId;
        this.metadata = metadata;
    }

    public void connect() throws IOException {
        channel = SocketChannel.open(new InetSocketAddress(bootstrapHost, bootstrapPort));
        channel.configureBlocking(true);
        connected = true;
        log.info("Connected to {}:{}", bootstrapHost, bootstrapPort);
    }

    public void ensureConnected() throws IOException {
        if (!connected || channel == null || !channel.isConnected()) {
            connect();
        }
    }

    /**
     * 同步发送请求并等待响应
     */
    public ByteBuffer sendSync(ApiKeys apiKey, ByteBuffer body) throws IOException {
        ensureConnected();
        int corrId = correlationIdCounter.incrementAndGet();
        RequestHeader header = new RequestHeader(apiKey, (short) 0, corrId, clientId);
        int totalSize = header.sizeOf() + body.remaining();

        ByteBuffer frame = ByteBuffer.allocate(4 + totalSize);
        frame.putInt(totalSize);
        header.writeTo(frame);
        frame.put(body);
        frame.flip();

        while (frame.hasRemaining()) channel.write(frame);

        byte[] sizeBuf = new byte[4];
        readFully(sizeBuf);
        int size = ByteBuffer.wrap(sizeBuf).getInt();
        byte[] respBuf = new byte[size];
        readFully(respBuf);

        ByteBuffer response = ByteBuffer.wrap(respBuf);
        response.getInt(); // skip correlationId
        return response;
    }

    /**
     * 刷新 Metadata
     */
    public void updateMetadata(List<String> topics) throws IOException {
        int bodySize = 4;
        for (String t : topics) bodySize += ByteBufferUtils.sizeOfString(t);
        ByteBuffer body = ByteBuffer.allocate(bodySize);
        body.putInt(topics.size());
        for (String t : topics) ByteBufferUtils.writeString(body, t);
        body.flip();

        ByteBuffer resp = sendSync(ApiKeys.METADATA, body);
        parseMetadataResponse(resp);
    }

    private void parseMetadataResponse(ByteBuffer resp) {
        int brokerCount = resp.getInt();
        List<Metadata.BrokerInfo> brokers = new ArrayList<>();
        for (int i = 0; i < brokerCount; i++) {
            int nodeId = resp.getInt();
            String host = ByteBufferUtils.readString(resp);
            int port = resp.getInt();
            brokers.add(new Metadata.BrokerInfo(nodeId, host, port));
        }

        int topicCount = resp.getInt();
        Map<String, Metadata.TopicMetadata> topics = new HashMap<>();
        for (int t = 0; t < topicCount; t++) {
            String topic = ByteBufferUtils.readString(resp);
            resp.getShort(); // errorCode
            int partCount = resp.getInt();
            List<Metadata.PartitionMetadata> partitions = new ArrayList<>();
            for (int p = 0; p < partCount; p++) {
                int partition = resp.getInt();
                resp.getInt(); // leader
                resp.getShort(); // errorCode
                String host = brokers.isEmpty() ? bootstrapHost : brokers.get(0).host;
                int port = brokers.isEmpty() ? bootstrapPort : brokers.get(0).port;
                partitions.add(new Metadata.PartitionMetadata(partition, host, port, 0));
            }
            topics.put(topic, new Metadata.TopicMetadata(topic, partitions));
        }
        metadata.update(topics, brokers);
    }

    private void readFully(byte[] buf) throws IOException {
        int offset = 0;
        while (offset < buf.length) {
            ByteBuffer tmp = ByteBuffer.wrap(buf, offset, buf.length - offset);
            int read = channel.read(tmp);
            if (read < 0) throw new EOFException("Connection closed");
            offset += read;
        }
    }

    @Override
    public void close() throws IOException {
        connected = false;
        if (channel != null) channel.close();
    }
}
