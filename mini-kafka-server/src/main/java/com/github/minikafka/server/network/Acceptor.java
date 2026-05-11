package com.github.minikafka.server.network;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.*;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 监听端口，accept 新连接并轮询分发给 Processor
 * 对齐 Kafka kafka.network.Acceptor
 */
public final class Acceptor implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(Acceptor.class);

    private final int port;
    private final Processor[] processors;
    private final ServerSocketChannel serverChannel;
    private final Selector selector;
    private final AtomicInteger nextProcessor = new AtomicInteger(0);
    private volatile boolean running = true;

    public Acceptor(int port, Processor[] processors) throws IOException {
        this.port = port;
        this.processors = processors;
        this.serverChannel = ServerSocketChannel.open();
        this.serverChannel.configureBlocking(false);
        this.serverChannel.bind(new InetSocketAddress(port));
        this.selector = Selector.open();
        this.serverChannel.register(selector, SelectionKey.OP_ACCEPT);
    }

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

    public void shutdown() throws IOException {
        running = false;
        selector.wakeup();
        serverChannel.close();
    }
}
