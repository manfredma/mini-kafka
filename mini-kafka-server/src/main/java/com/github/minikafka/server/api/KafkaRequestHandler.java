package com.github.minikafka.server.api;

import com.github.minikafka.server.network.RequestChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.nio.ByteBuffer;
import java.util.concurrent.*;

/**
 * IO 线程池，从 RequestChannel 取请求，调用 KafkaApis 处理后写回响应
 */
public final class KafkaRequestHandler {

    private static final Logger log = LoggerFactory.getLogger(KafkaRequestHandler.class);

    private final ExecutorService executor;
    private final RequestChannel requestChannel;
    private final KafkaApis apis;
    private volatile boolean running = true;
    private Thread dispatchThread;

    public KafkaRequestHandler(int numThreads, RequestChannel requestChannel, KafkaApis apis) {
        this.executor = Executors.newFixedThreadPool(numThreads,
            r -> new Thread(r, "kafka-request-handler-" + System.nanoTime()));
        this.requestChannel = requestChannel;
        this.apis = apis;
    }

    public void startup() {
        dispatchThread = new Thread(this::dispatchLoop, "kafka-request-dispatcher");
        dispatchThread.setDaemon(true);
        dispatchThread.start();
    }

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

    public void shutdown() throws InterruptedException {
        running = false;
        executor.shutdown();
        executor.awaitTermination(5, TimeUnit.SECONDS);
        if (dispatchThread != null) dispatchThread.join(3000);
    }
}
