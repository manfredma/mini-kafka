package com.github.minikafka.clients.network;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 已发出但尚未收到响应的在途请求管理器。
 *
 * <p>对齐 Kafka {@code org.apache.kafka.clients.InFlightRequests}（简化版，单连接）。
 * 内部使用 {@link LinkedHashMap} 保持插入顺序，以支持按发送顺序匹配响应（Kafka 协议
 * 要求同一连接上响应与请求顺序一致）。
 *
 * <p>本类不是线程安全的，调用方需自行保证并发安全。
 */
public final class InFlightRequests {

    /** correlationId -> 对应的 ClientRequest，按插入顺序排列。 */
    private final Map<Integer, ClientRequest> inflightMap = new LinkedHashMap<>();

    /**
     * 将请求加入在途队列。
     *
     * @param request 已发送的请求，以其 {@link ClientRequest#correlationId} 为 key 存储
     */
    public void add(ClientRequest request) {
        inflightMap.put(request.correlationId, request);
    }

    /**
     * 根据 correlationId 移除并返回对应的在途请求。
     *
     * @param correlationId 服务端响应中携带的关联 ID
     * @return 对应的 {@link ClientRequest}；若不存在（已超时或重复响应）则返回 null
     */
    public ClientRequest remove(int correlationId) {
        return inflightMap.remove(correlationId);
    }

    /** 返回当前在途请求的数量。 */
    public int size() { return inflightMap.size(); }

    /** 返回是否没有任何在途请求。 */
    public boolean isEmpty() { return inflightMap.isEmpty(); }
}
