package com.github.minikafka.server.network;

import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Processor 和 KafkaRequestHandler 之间的请求/响应通道
 * 对齐 Kafka kafka.network.RequestChannel
 */
public final class RequestChannel {

    public static final class Request {
        public final int processorId;
        public final SocketChannel channel;
        public final ByteBuffer buffer;
        public final long startTimeMs;

        public Request(int processorId, SocketChannel channel, ByteBuffer buffer) {
            this.processorId = processorId;
            this.channel = channel;
            this.buffer = buffer;
            this.startTimeMs = System.currentTimeMillis();
        }
    }

    public static final class Response {
        public final int processorId;
        public final SocketChannel channel;
        public final ByteBuffer buffer;

        public Response(int processorId, SocketChannel channel, ByteBuffer buffer) {
            this.processorId = processorId;
            this.channel = channel;
            this.buffer = buffer;
        }
    }

    private final BlockingQueue<Request> requestQueue;
    private final BlockingQueue<Response> responseQueue;

    public RequestChannel(int queueSize) {
        this.requestQueue = new ArrayBlockingQueue<>(queueSize);
        this.responseQueue = new ArrayBlockingQueue<>(queueSize * 2);
    }

    public void sendRequest(Request request) throws InterruptedException {
        requestQueue.put(request);
    }

    public Request receiveRequest(long timeoutMs) throws InterruptedException {
        return requestQueue.poll(timeoutMs, TimeUnit.MILLISECONDS);
    }

    public void sendResponse(Response response) throws InterruptedException {
        responseQueue.put(response);
    }

    public Response receiveResponse() {
        return responseQueue.poll();
    }
}
