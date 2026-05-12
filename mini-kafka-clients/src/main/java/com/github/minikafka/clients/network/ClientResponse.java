package com.github.minikafka.clients.network;

import java.nio.ByteBuffer;

/**
 * 服务端对一次 RPC 请求的响应封装。
 *
 * <p>对齐 Kafka {@code org.apache.kafka.clients.ClientResponse}。
 * 响应分为两种状态：
 * <ul>
 *   <li>正常响应：{@link #responseBody} 不为 null，{@link #exception} 为 null</li>
 *   <li>异常响应：{@link #exception} 不为 null，{@link #responseBody} 为 null</li>
 * </ul>
 * 调用方应先通过 {@link #hasError()} 判断是否发生错误，再读取对应字段。
 *
 * <p>本类为不可变值对象。
 */
public final class ClientResponse {

    /** 触发此响应的原始请求，可用于获取 correlationId、apiKey 等上下文。 */
    public final ClientRequest request;

    /**
     * 响应体（已跳过 correlationId 头部，position 指向业务数据起始位置）。
     * 仅在请求成功时不为 null；发生异常时为 null。
     */
    public final ByteBuffer responseBody;

    /**
     * 发生网络或协议错误时的异常对象。
     * 请求成功时为 null；发生错误时不为 null。
     */
    public final Exception exception;

    /**
     * 构造正常响应。
     *
     * @param request      对应的原始请求
     * @param responseBody 已解析的响应体 buffer，不含 correlationId 头部
     */
    public ClientResponse(ClientRequest request, ByteBuffer responseBody) {
        this.request = request;
        this.responseBody = responseBody;
        this.exception = null;
    }

    /**
     * 构造异常响应（网络断开、超时、协议解析失败等）。
     *
     * @param request   对应的原始请求
     * @param exception 导致请求失败的异常
     */
    public ClientResponse(ClientRequest request, Exception exception) {
        this.request = request;
        this.responseBody = null;
        this.exception = exception;
    }

    /**
     * 判断此响应是否为错误响应。
     *
     * @return true 表示请求失败，{@link #exception} 不为 null；
     *         false 表示请求成功，{@link #responseBody} 可安全读取
     */
    public boolean hasError() { return exception != null; }
}
