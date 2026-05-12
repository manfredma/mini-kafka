package com.github.minikafka.server.network;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

/**
 * 从非阻塞 Channel 读取一个完整的带 4 字节长度前缀的消息帧。
 * <p>
 * 对应 Kafka 原版 {@code kafka.network.NetworkReceive}。
 * 消息帧格式：[4字节大端序长度][payload字节]。
 * 每次调用 {@link #readFrom(ReadableByteChannel)} 可能只读取部分数据（非阻塞 IO），
 * 需多次调用直到 {@link #complete()} 返回 {@code true}。
 * </p>
 * <p>
 * 非线程安全，每个 {@link Processor} 为每个连接维护独立的 NetworkReceive 实例。
 * 读取完成后（complete=true），{@link Processor} 应将此实例替换为新实例，以便接收下一帧。
 * </p>
 */
public final class NetworkReceive {

    private final ByteBuffer sizeBuffer = ByteBuffer.allocate(4);
    private ByteBuffer payload;
    private boolean complete = false;

    /**
     * 从 Channel 中尽可能多地读取数据，推进帧读取状态机。
     * <p>
     * 内部分两阶段：
     * <ol>
     *   <li>读取 4 字节长度前缀到 sizeBuffer</li>
     *   <li>根据长度分配 payload buffer，读取实际消息体</li>
     * </ol>
     * 每次调用均为非阻塞，Channel 无数据时立即返回 {@code false}。
     * </p>
     *
     * @param channel 可读的非阻塞 SocketChannel
     * @return {@code true} 表示完整帧已读取完毕，可通过 {@link #payload()} 获取消息体；
     *         {@code false} 表示帧尚未读完，需继续调用
     * @throws IOException Channel 读取失败（如连接断开）时抛出
     */
    public boolean readFrom(ReadableByteChannel channel) throws IOException {
        if (sizeBuffer.hasRemaining()) {
            channel.read(sizeBuffer);
        }
        if (!sizeBuffer.hasRemaining() && payload == null) {
            sizeBuffer.flip();
            int size = sizeBuffer.getInt();
            payload = ByteBuffer.allocate(size);
        }
        if (payload != null && payload.hasRemaining()) {
            channel.read(payload);
        }
        if (payload != null && !payload.hasRemaining()) {
            payload.flip();
            complete = true;
        }
        return complete;
    }

    /**
     * 返回已读取完毕的消息体 ByteBuffer（position=0，limit=消息体长度）。
     *
     * @return 消息体 ByteBuffer；若帧尚未读完（{@link #complete()} 为 {@code false}），返回 {@code null}
     */
    public ByteBuffer payload() { return payload; }

    /**
     * 判断是否已读取到完整消息帧。
     *
     * @return {@code true} 表示完整帧已就绪
     */
    public boolean complete() { return complete; }
}
