package com.github.minikafka.server.network;

import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * {@link Processor} 和 {@link com.github.minikafka.server.api.KafkaRequestHandler} 之间
 * 的请求/响应解耦通道，基于有界阻塞队列实现背压。
 * <p>
 * 对应 Kafka 原版 {@code kafka.network.RequestChannel}。
 * 请求队列：Processor 生产，KafkaRequestHandler 消费；
 * 响应队列：KafkaRequestHandler 生产，Processor 消费。
 * processorId 字段用于响应路由——每个响应必须由生成对应请求的 Processor 来发送，
 * 以确保响应写回正确的 SocketChannel。
 * </p>
 */
public final class RequestChannel {

    /**
     * 封装一次完整的客户端请求，携带路由所需的 processorId。
     */
    public static final class Request {
        /**
         * 生成该请求的 Processor 编号（0 到 numNetworkThreads-1）。
         * 响应必须路由回同一 Processor，因为 SocketChannel 注册在该 Processor 的 Selector 上。
         */
        public final int processorId;
        /** 发送该请求的客户端 SocketChannel，用于响应路由。 */
        public final SocketChannel channel;
        /** 请求消息体（已去除 4 字节长度前缀），position=0，limit=消息体长度。 */
        public final ByteBuffer buffer;
        /** 请求进入队列的时间戳（毫秒），可用于延迟监控。 */
        public final long startTimeMs;

        /**
         * 构造请求对象，自动记录入队时间戳。
         *
         * @param processorId 生成该请求的 Processor 编号
         * @param channel     客户端 SocketChannel
         * @param buffer      请求消息体 ByteBuffer
         */
        public Request(int processorId, SocketChannel channel, ByteBuffer buffer) {
            this.processorId = processorId;
            this.channel = channel;
            this.buffer = buffer;
            this.startTimeMs = System.currentTimeMillis();
        }
    }

    /**
     * 封装一次完整的响应，携带路由所需的 processorId。
     */
    public static final class Response {
        /**
         * 应处理该响应的 Processor 编号，与对应 {@link Request#processorId} 相同。
         * Processor 在 {@code sendPendingResponses()} 中根据此字段过滤属于自己的响应。
         */
        public final int processorId;
        /** 目标客户端 SocketChannel。 */
        public final SocketChannel channel;
        /** 响应消息体（含 4 字节长度前缀），position=0，limit=总长度。 */
        public final ByteBuffer buffer;

        /**
         * 构造响应对象。
         *
         * @param processorId 应发送该响应的 Processor 编号
         * @param channel     目标客户端 SocketChannel
         * @param buffer      响应消息体 ByteBuffer
         */
        public Response(int processorId, SocketChannel channel, ByteBuffer buffer) {
            this.processorId = processorId;
            this.channel = channel;
            this.buffer = buffer;
        }
    }

    private final BlockingQueue<Request> requestQueue;
    private final BlockingQueue<Response> responseQueue;

    /**
     * 构造 RequestChannel，初始化有界阻塞队列。
     *
     * @param queueSize 请求队列容量；响应队列容量为其 2 倍（响应通常比请求多）
     */
    public RequestChannel(int queueSize) {
        this.requestQueue = new ArrayBlockingQueue<>(queueSize);
        this.responseQueue = new ArrayBlockingQueue<>(queueSize * 2);
    }

    /**
     * 将请求放入请求队列，阻塞直到队列有空间。
     * <p>由 Processor 在读取完整帧后调用。</p>
     *
     * @param request 要入队的请求，不得为 {@code null}
     * @throws InterruptedException 等待期间线程被中断时抛出
     */
    public void sendRequest(Request request) throws InterruptedException {
        requestQueue.put(request);
    }

    /**
     * 从请求队列取出一个请求，超时后返回 {@code null}。
     * <p>由 KafkaRequestHandler 的 dispatch 线程调用。</p>
     *
     * @param timeoutMs 等待超时时间（毫秒），超时后返回 {@code null}
     * @return 队列头部的请求；超时或队列为空时返回 {@code null}
     * @throws InterruptedException 等待期间线程被中断时抛出
     */
    public Request receiveRequest(long timeoutMs) throws InterruptedException {
        return requestQueue.poll(timeoutMs, TimeUnit.MILLISECONDS);
    }

    /**
     * 将响应放入响应队列，阻塞直到队列有空间。
     * <p>由 KafkaRequestHandler 的 IO 线程在处理完请求后调用。</p>
     *
     * @param response 要入队的响应，不得为 {@code null}
     * @throws InterruptedException 等待期间线程被中断时抛出
     */
    public void sendResponse(Response response) throws InterruptedException {
        responseQueue.put(response);
    }

    /**
     * 非阻塞地从响应队列取出一个响应。
     * <p>由 Processor 在每次 select 循环末尾调用，轮询属于自己的响应。</p>
     *
     * @return 队列头部的响应；队列为空时返回 {@code null}
     */
    public Response receiveResponse() {
        return responseQueue.poll();
    }
}
