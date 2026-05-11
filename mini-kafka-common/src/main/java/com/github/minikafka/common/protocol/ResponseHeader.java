package com.github.minikafka.common.protocol;

import java.nio.ByteBuffer;

public final class ResponseHeader {
    public static final int SIZE = 4;

    private final int correlationId;

    public ResponseHeader(int correlationId) {
        this.correlationId = correlationId;
    }

    public int correlationId() { return correlationId; }

    public void writeTo(ByteBuffer buffer) {
        buffer.putInt(correlationId);
    }

    public static ResponseHeader parse(ByteBuffer buffer) {
        return new ResponseHeader(buffer.getInt());
    }
}
