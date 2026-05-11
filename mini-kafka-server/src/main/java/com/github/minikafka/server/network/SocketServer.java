package com.github.minikafka.server.network;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;

/**
 * 组装并启动 Acceptor + Processor 线程
 * 对齐 Kafka kafka.network.SocketServer
 */
public final class SocketServer {

    private static final Logger log = LoggerFactory.getLogger(SocketServer.class);

    private final Acceptor acceptor;
    private final Processor[] processors;
    private final Thread acceptorThread;
    private final Thread[] processorThreads;

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

    public void startup() {
        for (Thread t : processorThreads) t.start();
        acceptorThread.start();
        log.info("SocketServer started");
    }

    public void shutdown() throws IOException, InterruptedException {
        acceptor.shutdown();
        for (Processor p : processors) p.shutdown();
        acceptorThread.join(5000);
        for (Thread t : processorThreads) t.join(5000);
        log.info("SocketServer stopped");
    }
}
