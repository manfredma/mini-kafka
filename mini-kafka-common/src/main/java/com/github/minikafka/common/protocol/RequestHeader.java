package com.github.minikafka.common.protocol;

import com.github.minikafka.common.network.ByteBufferUtils;
import java.nio.ByteBuffer;

public final class RequestHeader {
    private final short apiKey;
    private final short apiVersion;
    private final int correlationId;
    private final String clientId;

    public RequestHeader(ApiKeys apiKey, short apiVersion, int correlationId, String clientId) {
        this.apiKey = apiKey.id;
        this.apiVersion = apiVersion;
        this.correlationId = correlationId;
        this.clientId = clientId;
    }

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

    public int sizeOf() {
        return 2 + 2 + 4 + ByteBufferUtils.sizeOfString(clientId);
    }

    public void writeTo(ByteBuffer buffer) {
        buffer.putShort(apiKey);
        buffer.putShort(apiVersion);
        buffer.putInt(correlationId);
        ByteBufferUtils.writeString(buffer, clientId);
    }

    public static RequestHeader parse(ByteBuffer buffer) {
        short apiKey = buffer.getShort();
        short apiVersion = buffer.getShort();
        int correlationId = buffer.getInt();
        String clientId = ByteBufferUtils.readString(buffer);
        return new RequestHeader(apiKey, apiVersion, correlationId, clientId);
    }
}
