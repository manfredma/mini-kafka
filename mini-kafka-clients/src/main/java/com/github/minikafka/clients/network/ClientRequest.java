package com.github.minikafka.clients.network;

import com.github.minikafka.common.protocol.ApiKeys;
import java.nio.ByteBuffer;
import java.util.function.Consumer;

/**
 * 客户端发出的一次 RPC 请求的封装对象。
 *
 * <p>对齐 Kafka {@code org.apache.kafka.clients.ClientRequest}。
 * 每个请求携带唯一的 correlationId，用于将异步响应与请求一一对应；
 * 同时持有响应回调，由 {@link NetworkClient} 在收到响应后触发。
 *
 * <p>本类为不可变值对象，构造后所有字段均不可修改。
 */
public final class ClientRequest {

    /** 请求的唯一序列号，由 {@link NetworkClient} 自增分配，用于响应匹配。 */
    public final int correlationId;

    /** 请求对应的 Kafka API 类型，决定服务端路由到哪个 Handler。 */
    public final ApiKeys apiKey;

    /**
     * 已序列化的请求体（不含请求头），position 已置为 0，limit 为有效数据末尾。
     * 调用方不得在请求发出后继续修改此 buffer。
     */
    public final ByteBuffer requestBuffer;

    /**
     * 响应回调。当响应到达（无论成功或失败）时，{@link NetworkClient} 将构造
     * {@link ClientResponse} 并调用此回调。回调在 IO 线程上执行，实现应尽量轻量。
     * 允许为 null（即不关心响应）。
     */
    public final Consumer<ClientResponse> callback;

    /** 请求创建时的系统时间戳（毫秒），可用于超时检测。 */
    public final long createdMs;

    /**
     * 构造一个客户端请求。
     *
     * @param correlationId  请求唯一序列号，由调用方（通常是 NetworkClient）分配
     * @param apiKey         请求类型，见 {@link ApiKeys}
     * @param requestBuffer  已序列化且已 flip 的请求体 buffer，不含请求头
     * @param callback       响应到达时的回调；传 null 表示不关心响应结果
     */
    public ClientRequest(int correlationId, ApiKeys apiKey,
                         ByteBuffer requestBuffer, Consumer<ClientResponse> callback) {
        this.correlationId = correlationId;
        this.apiKey = apiKey;
        this.requestBuffer = requestBuffer;
        this.callback = callback;
        this.createdMs = System.currentTimeMillis();
    }
}
