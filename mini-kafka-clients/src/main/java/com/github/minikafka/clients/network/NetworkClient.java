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
 * 基于 NIO SocketChannel 的同步网络客户端（简化版）。
 *
 * <p>对齐 Kafka {@code org.apache.kafka.clients.NetworkClient}，但做了大幅简化：
 * <ul>
 *   <li>仅支持单连接（Bootstrap 地址），不支持多 Broker 连接池。</li>
 *   <li>采用同步阻塞 IO（{@code configureBlocking(true)}），无 Selector 事件循环。</li>
 *   <li>每次 {@link #sendSync} 都在调用线程上完整执行写帧 + 读帧，适合低并发测试场景。</li>
 * </ul>
 *
 * <p>线程安全性：本类不是线程安全的，同一时刻只应由单一线程调用。
 */
public final class NetworkClient implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(NetworkClient.class);

    /** Bootstrap Broker 的主机名或 IP。 */
    private final String bootstrapHost;
    /** Bootstrap Broker 的端口。 */
    private final int bootstrapPort;
    /** 元数据缓存，由 {@link #updateMetadata} 刷新后写入。 */
    private final Metadata metadata;
    /** 客户端标识，写入每个请求的 RequestHeader.clientId 字段。 */
    private final String clientId;
    /** 自增 correlationId 生成器，保证每个请求的 ID 唯一。 */
    private final AtomicInteger correlationIdCounter = new AtomicInteger(0);

    /** 当前 TCP 连接的 NIO 通道；未连接时为 null。 */
    private volatile SocketChannel channel;
    /** 连接状态标志；{@link #connect()} 成功后置为 true，{@link #close()} 后置为 false。 */
    private volatile boolean connected = false;

    /**
     * 构造网络客户端（不立即建立连接）。
     *
     * @param bootstrapHost Bootstrap Broker 主机名
     * @param bootstrapPort Bootstrap Broker 端口
     * @param clientId      客户端 ID，写入请求头
     * @param metadata      元数据缓存，用于 {@link #updateMetadata} 写入解析结果
     */
    public NetworkClient(String bootstrapHost, int bootstrapPort,
                         String clientId, Metadata metadata) {
        this.bootstrapHost = bootstrapHost;
        this.bootstrapPort = bootstrapPort;
        this.clientId = clientId;
        this.metadata = metadata;
    }

    /**
     * 同步建立 TCP 连接，并将 channel 设置为阻塞模式。
     *
     * <p>若已存在旧连接，将被新连接覆盖（不会自动关闭旧 channel，调用方应先调用
     * {@link #close()}）。
     *
     * @throws IOException 连接失败时抛出（如目标不可达、端口未监听）
     */
    public void connect() throws IOException {
        channel = SocketChannel.open(new InetSocketAddress(bootstrapHost, bootstrapPort));
        channel.configureBlocking(true);
        connected = true;
        log.info("Connected to {}:{}", bootstrapHost, bootstrapPort);
    }

    /**
     * 幂等连接：若当前已连接则直接返回，否则调用 {@link #connect()} 建立新连接。
     *
     * @throws IOException 建立新连接失败时抛出
     */
    public void ensureConnected() throws IOException {
        if (!connected || channel == null || !channel.isConnected()) {
            connect();
        }
    }

    /**
     * 同步发送一个 API 请求并阻塞等待响应，返回响应体（已跳过 correlationId）。
     *
     * <p>帧格式（发送）：{@code [4字节总长度][RequestHeader][body]}
     * <br>帧格式（接收）：{@code [4字节响应长度][4字节correlationId][响应体]}
     *
     * <p>方法内部自动调用 {@link #ensureConnected()} 保证连接存在。
     * correlationId 由内部计数器自增生成，响应中的 correlationId 字段被跳过（不做校验）。
     *
     * @param apiKey 请求类型，写入 RequestHeader
     * @param body   已序列化且已 flip 的请求体 buffer（不含请求头）
     * @return 响应体 buffer，position 已跳过 correlationId，指向实际业务数据起始位置
     * @throws IOException 网络读写失败或连接断开时抛出
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
     * 向 Broker 发送 MetadataRequest，并将解析结果写入 {@link Metadata}。
     *
     * <p>请求体格式：{@code [4字节topicCount][topic字符串列表]}
     *
     * @param topics 需要获取元数据的 Topic 名称列表；传入空列表表示获取所有 Topic
     * @throws IOException 网络通信失败时抛出
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

    /**
     * 解析 MetadataResponse 帧，更新 {@link Metadata} 缓存。
     *
     * <p>响应帧格式：
     * <pre>
     * [4字节brokerCount]
     *   ([4字节nodeId][string host][4字节port]) * brokerCount
     * [4字节topicCount]
     *   ([string topic][2字节errorCode][4字节partCount]
     *     ([4字节partition][4字节leader][2字节errorCode]) * partCount
     *   ) * topicCount
     * </pre>
     * 简化实现：分区的 leader 主机直接取 brokers 列表第一个节点（或 bootstrap 地址）。
     *
     * @param resp 已跳过 correlationId 的响应体 buffer
     */
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

    /**
     * 从 channel 中读取恰好 {@code buf.length} 字节，循环读直到填满缓冲区。
     *
     * @param buf 目标字节数组，读取完成后 buf 中包含完整数据
     * @throws IOException  网络读取失败时抛出
     * @throws EOFException 连接在读取完成前被对端关闭时抛出
     */
    private void readFully(byte[] buf) throws IOException {
        int offset = 0;
        while (offset < buf.length) {
            ByteBuffer tmp = ByteBuffer.wrap(buf, offset, buf.length - offset);
            int read = channel.read(tmp);
            if (read < 0) throw new EOFException("Connection closed");
            offset += read;
        }
    }

    /**
     * 关闭网络连接，释放底层 SocketChannel 资源。
     *
     * @throws IOException 关闭 channel 失败时抛出（通常可忽略）
     */
    @Override
    public void close() throws IOException {
        connected = false;
        if (channel != null) channel.close();
    }
}
