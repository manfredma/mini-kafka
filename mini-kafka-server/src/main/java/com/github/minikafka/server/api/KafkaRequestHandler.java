package com.github.minikafka.server.api;

import com.github.minikafka.server.network.RequestChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.nio.ByteBuffer;
import java.util.concurrent.*;

/**
 * 请求分发与处理层：单一 dispatch 线程从 {@link RequestChannel} 取请求，
 * 提交到固定大小线程池由 {@link KafkaApis} 异步处理，处理完成后将响应写回 RequestChannel。
 * <p>
 * 对应 Kafka 原版 {@code kafka.server.KafkaRequestHandler}（精简版）。
 * 架构分层：
 * <pre>
 * Processor (NIO线程) → RequestChannel.requestQueue
 *   → dispatchLoop (单线程) → executor (IO线程池) → KafkaApis
 *   → RequestChannel.responseQueue → Processor (NIO线程)
 * </pre>
 * dispatch 线程负责串行取请求并提交任务，实际处理由线程池并发执行，
 * 避免慢请求阻塞 dispatch 线程。
 * </p>
 */
public final class KafkaRequestHandler {

    private static final Logger log = LoggerFactory.getLogger(KafkaRequestHandler.class);

    private final ExecutorService executor;
    private final RequestChannel requestChannel;
    private final KafkaApis apis;
    private volatile boolean running = true;
    private Thread dispatchThread;

    /**
     * 构造 KafkaRequestHandler，初始化固定大小的 IO 线程池。
     *
     * @param numThreads     IO 处理线程数，对应 {@code num.io.threads} 配置
     * @param requestChannel 与 Processor 共享的请求/响应通道
     * @param apis           请求处理逻辑入口，不得为 {@code null}
     */
    public KafkaRequestHandler(int numThreads, RequestChannel requestChannel, KafkaApis apis) {
        this.executor = Executors.newFixedThreadPool(numThreads,
            r -> new Thread(r, "kafka-request-handler-" + System.nanoTime()));
        this.requestChannel = requestChannel;
        this.apis = apis;
    }

    /**
     * 启动 dispatch 线程，开始从 RequestChannel 取请求并分发处理。
     * dispatch 线程设置为 daemon 线程，JVM 退出时自动终止。
     */
    public void startup() {
        dispatchThread = new Thread(this::dispatchLoop, "kafka-request-dispatcher");
        dispatchThread.setDaemon(true);
        dispatchThread.start();
    }

    /**
     * dispatch 主循环：以 100ms 超时轮询 RequestChannel，将请求提交到线程池处理。
     * <p>
     * 每个请求在线程池中独立执行：调用 {@link KafkaApis#handle(RequestChannel.Request)}
     * 获取响应 ByteBuffer，然后将响应放入 RequestChannel，由对应 Processor 写回客户端。
     * 处理异常时仅记录日志，不影响其他请求的处理。
     * </p>
     */
    private void dispatchLoop() {
        while (running) {
            try {
                RequestChannel.Request request = requestChannel.receiveRequest(100);
                if (request == null) continue;
                executor.submit(() -> {
                    try {
                        ByteBuffer response = apis.handle(request);
                        requestChannel.sendResponse(new RequestChannel.Response(
                            request.processorId, request.channel, response
                        ));
                    } catch (Exception e) {
                        log.error("Error processing request", e);
                    }
                });
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    /**
     * 优雅关闭：停止 dispatch 循环，等待线程池中已提交任务完成（最多 5 秒），
     * 再等待 dispatch 线程退出（最多 3 秒）。
     *
     * @throws InterruptedException 等待期间线程被中断时抛出
     */
    public void shutdown() throws InterruptedException {
        running = false;
        executor.shutdown();
        executor.awaitTermination(5, TimeUnit.SECONDS);
        if (dispatchThread != null) dispatchThread.join(3000);
    }
}
