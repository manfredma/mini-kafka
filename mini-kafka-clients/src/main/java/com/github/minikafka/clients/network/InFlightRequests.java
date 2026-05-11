package com.github.minikafka.clients.network;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 管理已发送但未收到响应的请求
 */
public final class InFlightRequests {

    private final Map<Integer, ClientRequest> inflightMap = new LinkedHashMap<>();

    public void add(ClientRequest request) {
        inflightMap.put(request.correlationId, request);
    }

    public ClientRequest remove(int correlationId) {
        return inflightMap.remove(correlationId);
    }

    public int size() { return inflightMap.size(); }
    public boolean isEmpty() { return inflightMap.isEmpty(); }
}
