package com.github.minikafka.common.protocol;

import java.nio.ByteBuffer;

/**
 * Kafka 响应帧头部，对应 Kafka 原版 {@code org.apache.kafka.common.requests.ResponseHeader}。
 *
 * <p>响应头部位于每个 Kafka 响应帧的消息体最前面，格式极为简单：
 * <pre>
 *   4 bytes  correlation_id
 * </pre>
 *
 * <p>关键设计决策：
 * <ul>
 *   <li>响应头部仅含 {@code correlationId} 一个字段，因此将固定大小 {@link #SIZE} 定义为常量，
 *       方便调用方在分配 ByteBuffer 时直接使用，无需实例化对象。</li>
 *   <li>类声明为 {@code final}，不允许继承。</li>
 * </ul>
 */
public final class ResponseHeader {

    /** 响应头部的固定序列化字节数（仅含 4 字节 correlation_id）。 */
    public static final int SIZE = 4;

    private final int correlationId;

    /**
     * 构造响应头部。
     *
     * @param correlationId 与对应请求的 {@code correlationId} 相同，用于客户端将响应与请求配对
     */
    public ResponseHeader(int correlationId) {
        this.correlationId = correlationId;
    }

    public int correlationId() { return correlationId; }

    /**
     * 将响应头部序列化写入 {@code buffer}。
     *
     * <p>调用方须确保 {@code buffer} 剩余容量不小于 {@link #SIZE}（4 字节），
     * 否则将抛出 {@link java.nio.BufferOverflowException}。
     *
     * @param buffer 目标 ByteBuffer，写入后 position 向后移动 {@link #SIZE} 个字节
     */
    public void writeTo(ByteBuffer buffer) {
        buffer.putInt(correlationId);
    }

    /**
     * 从 {@code buffer} 当前位置反序列化出一个 {@code ResponseHeader}。
     *
     * <p>读取后 {@code buffer} 的 position 向后移动 {@link #SIZE}（4）字节。
     *
     * @param buffer 包含响应头部字节的 ByteBuffer，position 需指向头部起始位置
     * @return 反序列化得到的 {@code ResponseHeader}，保证非 null
     */
    public static ResponseHeader parse(ByteBuffer buffer) {
        return new ResponseHeader(buffer.getInt());
    }
}
