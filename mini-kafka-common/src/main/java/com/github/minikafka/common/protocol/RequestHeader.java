package com.github.minikafka.common.protocol;

import com.github.minikafka.common.network.ByteBufferUtils;
import java.nio.ByteBuffer;

/**
 * Kafka 请求帧头部，对应 Kafka 原版 {@code org.apache.kafka.common.requests.RequestHeader}。
 *
 * <p>请求头部在每个 Kafka 请求帧的消息体最前面，格式（按字节顺序）：
 * <pre>
 *   2 bytes  api_key
 *   2 bytes  api_version
 *   4 bytes  correlation_id
 *   2+N bytes client_id（Kafka NULLABLE_STRING 编码：2 字节长度 + UTF-8 字节）
 * </pre>
 *
 * <p>关键设计决策：
 * <ul>
 *   <li>提供两个构造器：面向调用方的公开构造器接受类型安全的 {@link ApiKeys} 枚举；
 *       私有构造器接受原始 {@code short}，专供 {@link #parse(ByteBuffer)} 反序列化使用，
 *       避免在反序列化路径上进行枚举查找（可能抛出异常）。</li>
 *   <li>类声明为 {@code final}，不允许继承，与 Kafka 原版设计保持一致。</li>
 *   <li>{@code clientId} 允许为 {@code null}，序列化时写入 {@code -1} 长度标记。</li>
 * </ul>
 */
public final class RequestHeader {
    private final short apiKey;
    private final short apiVersion;
    private final int correlationId;
    private final String clientId;

    /**
     * 构造请求头部（面向调用方的公开入口）。
     *
     * @param apiKey      请求对应的 API 类型，不能为 null
     * @param apiVersion  API 版本号，由调用方根据协商结果传入
     * @param correlationId 请求关联 ID，用于将响应与请求配对；由发送方自行管理唯一性
     * @param clientId    客户端标识字符串，可为 null（序列化为 -1 长度的空字符串标记）
     */
    public RequestHeader(ApiKeys apiKey, short apiVersion, int correlationId, String clientId) {
        this.apiKey = apiKey.id;
        this.apiVersion = apiVersion;
        this.correlationId = correlationId;
        this.clientId = clientId;
    }

    /**
     * 反序列化专用私有构造器，直接接受原始 short 类型的 apiKey，
     * 避免在解析路径上触发 {@link ApiKeys#forId(short)} 的枚举查找开销和异常风险。
     */
    private RequestHeader(short apiKey, short apiVersion, int correlationId, String clientId) {
        this.apiKey = apiKey;
        this.apiVersion = apiVersion;
        this.correlationId = correlationId;
        this.clientId = clientId;
    }

    public short apiKey() { return apiKey; }
    public short apiVersion() { return apiVersion; }
    public int correlationId() { return correlationId; }
    public String clientId() { return clientId; }

    /**
     * 计算此请求头部序列化后占用的字节数。
     *
     * <p>固定部分：api_key(2) + api_version(2) + correlation_id(4) = 8 字节；
     * 可变部分：{@code clientId} 的 Kafka NULLABLE_STRING 编码长度（含 2 字节长度前缀）。
     *
     * @return 序列化字节数，始终大于 0
     */
    public int sizeOf() {
        return 2 + 2 + 4 + ByteBufferUtils.sizeOfString(clientId);
    }

    /**
     * 将请求头部按 Kafka 协议格式序列化写入 {@code buffer}。
     *
     * <p>调用方须确保 {@code buffer} 剩余容量不小于 {@link #sizeOf()} 字节，
     * 否则将抛出 {@link java.nio.BufferOverflowException}。
     *
     * @param buffer 目标 ByteBuffer，写入后 position 向后移动 {@link #sizeOf()} 个字节
     */
    public void writeTo(ByteBuffer buffer) {
        buffer.putShort(apiKey);
        buffer.putShort(apiVersion);
        buffer.putInt(correlationId);
        ByteBufferUtils.writeString(buffer, clientId);
    }

    /**
     * 从 {@code buffer} 当前位置反序列化出一个 {@code RequestHeader}。
     *
     * <p>读取后 {@code buffer} 的 position 向后移动相应字节数。
     * 此方法不校验 apiKey 是否为已知值，因此不会抛出 {@link IllegalArgumentException}。
     *
     * @param buffer 包含请求头部字节的 ByteBuffer，position 需指向头部起始位置
     * @return 反序列化得到的 {@code RequestHeader}，保证非 null
     */
    public static RequestHeader parse(ByteBuffer buffer) {
        short apiKey = buffer.getShort();
        short apiVersion = buffer.getShort();
        int correlationId = buffer.getInt();
        String clientId = ByteBufferUtils.readString(buffer);
        return new RequestHeader(apiKey, apiVersion, correlationId, clientId);
    }
}
