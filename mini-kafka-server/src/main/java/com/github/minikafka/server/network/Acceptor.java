package com.github.minikafka.server.network;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.*;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 监听指定端口，接受新连接并以轮询方式分发给各 {@link Processor}。
 * <p>
 * 对应 Kafka 原版 {@code kafka.network.Acceptor}。
 * 运行在独立的 "kafka-acceptor" 线程中，通过 NIO {@link Selector} 监听 OP_ACCEPT 事件。
 * 新连接按 Round-Robin 策略分配给 Processor 数组，实现连接负载均衡。
 * </p>
 * <p>
 * 线程安全性：{@link #accept(SocketChannel)} 不对外暴露（由 run() 内部调用），
 * {@link #shutdown()} 可由外部线程安全调用。
 * </p>
 */
public final class Acceptor implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(Acceptor.class);

    private final int port;
    private final Processor[] processors;
    private final ServerSocketChannel serverChannel;
    private final Selector selector;
    /** 轮询计数器，用于 Round-Robin 分配 Processor；溢出时自然回绕，不影响正确性。 */
    private final AtomicInteger nextProcessor = new AtomicInteger(0);
    private volatile boolean running = true;

    /**
     * 构造 Acceptor：绑定端口并注册 OP_ACCEPT 事件。
     *
     * @param port       Broker 监听端口
     * @param processors 用于接收新连接的 Processor 数组，不得为空
     * @throws IOException 端口绑定或 Selector 创建失败时抛出
     */
    public Acceptor(int port, Processor[] processors) throws IOException {
        this.port = port;
        this.processors = processors;
        this.serverChannel = ServerSocketChannel.open();
        this.serverChannel.configureBlocking(false);
        this.serverChannel.bind(new InetSocketAddress(port));
        this.selector = Selector.open();
        this.serverChannel.register(selector, SelectionKey.OP_ACCEPT);
    }

    /**
     * Acceptor 主循环：每 500ms 超时 select 一次，处理所有 OP_ACCEPT 事件。
     * <p>
     * 每次接受新连接后，通过 Round-Robin 选取 Processor，
     * 调用 {@link Processor#accept(SocketChannel)} 将连接交给对应 Processor 管理。
     * </p>
     */
    @Override
    public void run() {
        log.info("Acceptor listening on port {}", port);
        while (running) {
            try {
                selector.select(500);
                Iterator<SelectionKey> iter = selector.selectedKeys().iterator();
                while (iter.hasNext()) {
                    SelectionKey key = iter.next();
                    iter.remove();
                    if (key.isAcceptable()) {
                        SocketChannel client = serverChannel.accept();
                        if (client != null) {
                            int idx = nextProcessor.getAndIncrement() % processors.length;
                            processors[idx].accept(client);
                            log.debug("Accepted connection, assigned to processor {}", idx);
                        }
                    }
                }
            } catch (Exception e) {
                if (running) log.error("Acceptor error", e);
            }
        }
    }

    /**
     * 停止 Acceptor：设置 running=false，唤醒 Selector，并关闭 ServerSocketChannel。
     *
     * @throws IOException ServerSocketChannel 关闭失败时抛出
     */
    public void shutdown() throws IOException {
        running = false;
        selector.wakeup();
        serverChannel.close();
    }
}
