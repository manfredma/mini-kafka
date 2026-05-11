package com.github.minikafka.server.network;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

/**
 * 从 Channel 读取一个完整的带4字节长度前缀的消息帧
 * 对齐 Kafka kafka.network.NetworkReceive
 */
public final class NetworkReceive {

    private final ByteBuffer sizeBuffer = ByteBuffer.allocate(4);
    private ByteBuffer payload;
    private boolean complete = false;

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

    public ByteBuffer payload() { return payload; }
    public boolean complete() { return complete; }
}
