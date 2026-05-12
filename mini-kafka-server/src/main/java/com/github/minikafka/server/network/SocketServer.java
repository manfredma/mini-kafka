package com.github.minikafka.server.network;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;

/**
 * 组装并启动 {@link Acceptor} 和 {@link Processor} 线程，是网络层的统一管理入口。
 * <p>
 * 对应 Kafka 原版 {@code kafka.network.SocketServer}（精简版）。
 * 构造时创建指定数量的 Processor（对应 numNetworkThreads），以及一个 Acceptor。
 * 所有线程均设置为 daemon 线程，JVM 退出时自动终止。
 * </p>
 * <p>
 * 启动顺序：先启动所有 Processor 线程（确保 Selector 就绪），再启动 Acceptor（开始接受连接）。
 * 关闭顺序：先关闭 Acceptor（停止接受新连接），再关闭所有 Processor，最后等待线程退出。
 * </p>
 */
public final class SocketServer {

    private static final Logger log = LoggerFactory.getLogger(SocketServer.class);

    private final Acceptor acceptor;
    private final Processor[] processors;
    private final Thread acceptorThread;
    private final Thread[] processorThreads;

    /**
     * 构造 SocketServer：创建并初始化所有 Processor 和 Acceptor，但不启动线程。
     *
     * @param port           Broker 监听端口
     * @param numProcessors  网络 IO 线程数（Processor 数量）
     * @param requestChannel 与 KafkaRequestHandler 共享的请求/响应通道
     * @throws IOException Processor 或 Acceptor 初始化（端口绑定、Selector 创建）失败时抛出
     */
    public SocketServer(int port, int numProcessors, RequestChannel requestChannel) throws IOException {
        this.processors = new Processor[numProcessors];
        this.processorThreads = new Thread[numProcessors];

        for (int i = 0; i < numProcessors; i++) {
            processors[i] = new Processor(i, requestChannel);
            processorThreads[i] = new Thread(processors[i], "kafka-network-thread-" + i);
            processorThreads[i].setDaemon(true);
        }
        this.acceptor = new Acceptor(port, processors);
        this.acceptorThread = new Thread(acceptor, "kafka-acceptor");
        this.acceptorThread.setDaemon(true);
    }

    /**
     * 启动所有 Processor 线程和 Acceptor 线程，开始接受客户端连接。
     * <p>
     * 先启动 Processor 线程，确保 Selector 就绪后再启动 Acceptor，
     * 避免新连接在 Processor 尚未注册 Selector 时到达。
     * </p>
     */
    public void startup() {
        for (Thread t : processorThreads) t.start();
        acceptorThread.start();
        log.info("SocketServer started");
    }

    /**
     * 优雅关闭：停止 Acceptor 和所有 Processor，等待线程退出（最多 5 秒）。
     *
     * @throws IOException          Acceptor 关闭失败时抛出
     * @throws InterruptedException 等待线程退出时被中断时抛出
     */
    public void shutdown() throws IOException, InterruptedException {
        acceptor.shutdown();
        for (Processor p : processors) p.shutdown();
        acceptorThread.join(5000);
        for (Thread t : processorThreads) t.join(5000);
        log.info("SocketServer stopped");
    }
}
