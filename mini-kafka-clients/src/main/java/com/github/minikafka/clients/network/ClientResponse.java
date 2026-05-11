package com.github.minikafka.clients.network;

import java.nio.ByteBuffer;

public final class ClientResponse {
    public final ClientRequest request;
    public final ByteBuffer responseBody;
    public final Exception exception;

    public ClientResponse(ClientRequest request, ByteBuffer responseBody) {
        this.request = request;
        this.responseBody = responseBody;
        this.exception = null;
    }

    public ClientResponse(ClientRequest request, Exception exception) {
        this.request = request;
        this.responseBody = null;
        this.exception = exception;
    }

    public boolean hasError() { return exception != null; }
}
