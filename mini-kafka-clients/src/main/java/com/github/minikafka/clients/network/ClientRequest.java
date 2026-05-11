package com.github.minikafka.clients.network;

import com.github.minikafka.common.protocol.ApiKeys;
import java.nio.ByteBuffer;
import java.util.function.Consumer;

public final class ClientRequest {
    public final int correlationId;
    public final ApiKeys apiKey;
    public final ByteBuffer requestBuffer;
    public final Consumer<ClientResponse> callback;
    public final long createdMs;

    public ClientRequest(int correlationId, ApiKeys apiKey,
                         ByteBuffer requestBuffer, Consumer<ClientResponse> callback) {
        this.correlationId = correlationId;
        this.apiKey = apiKey;
        this.requestBuffer = requestBuffer;
        this.callback = callback;
        this.createdMs = System.currentTimeMillis();
    }
}
