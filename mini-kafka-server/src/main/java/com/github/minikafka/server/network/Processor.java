package com.github.minikafka.server.network;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * 管理一批 SocketChannel，读取完整请求帧放入 RequestChannel
 * 对齐 Kafka kafka.network.Processor
 */
public final class Processor implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(Processor.class);

    private final int id;
    private final RequestChannel requestChannel;
    private final Selector selector;
    private final Queue<SocketChannel> newConnections = new ConcurrentLinkedQueue<>();
    private final Map<SocketChannel, NetworkReceive> inflightReads = new HashMap<>();
    private volatile boolean running = true;

    public Processor(int id, RequestChannel requestChannel) throws IOException {
        this.id = id;
        this.requestChannel = requestChannel;
        this.selector = Selector.open();
    }

    public void accept(SocketChannel channel) {
        newConnections.add(channel);
        selector.wakeup();
    }

    @Override
    public void run() {
        while (running) {
            try {
                SocketChannel newConn;
                while ((newConn = newConnections.poll()) != null) {
                    newConn.configureBlocking(false);
                    newConn.register(selector, SelectionKey.OP_READ);
                    inflightReads.put(newConn, new NetworkReceive());
                }

                selector.select(300);
                Iterator<SelectionKey> iter = selector.selectedKeys().iterator();
                while (iter.hasNext()) {
                    SelectionKey key = iter.next();
                    iter.remove();
                    if (key.isValid() && key.isReadable()) {
                        handleRead(key);
                    } else if (key.isValid() && key.isWritable()) {
                        handleWrite(key);
                    }
                }

                sendPendingResponses();

            } catch (Exception e) {
                if (running) log.error("Processor {} error", id, e);
            }
        }
    }

    private void handleRead(SelectionKey key) throws IOException, InterruptedException {
        SocketChannel channel = (SocketChannel) key.channel();
        NetworkReceive receive = inflightReads.get(channel);
        if (receive == null) return;

        try {
            boolean complete = receive.readFrom(channel);
            if (complete) {
                ByteBuffer payload = receive.payload().duplicate();
                requestChannel.sendRequest(new RequestChannel.Request(id, channel, payload));
                inflightReads.put(channel, new NetworkReceive());
            }
        } catch (IOException e) {
            key.cancel();
            inflightReads.remove(channel);
            try { channel.close(); } catch (IOException ignored) {}
        }
    }

    private void handleWrite(SelectionKey key) throws IOException {
        SocketChannel channel = (SocketChannel) key.channel();
        ByteBuffer buf = (ByteBuffer) key.attachment();
        if (buf != null) {
            channel.write(buf);
            if (!buf.hasRemaining()) {
                key.interestOps(SelectionKey.OP_READ);
                key.attach(null);
            }
        }
    }

    private void sendPendingResponses() throws IOException {
        RequestChannel.Response response;
        List<RequestChannel.Response> deferred = new ArrayList<>();
        while ((response = requestChannel.receiveResponse()) != null) {
            if (response.processorId != id) {
                deferred.add(response);
                continue;
            }
            SocketChannel channel = response.channel;
            SelectionKey key = channel.keyFor(selector);
            if (key != null && key.isValid()) {
                key.attach(response.buffer);
                key.interestOps(SelectionKey.OP_WRITE);
                selector.wakeup();
                channel.write(response.buffer);
                if (!response.buffer.hasRemaining()) {
                    key.interestOps(SelectionKey.OP_READ);
                    key.attach(null);
                }
            }
        }
        for (RequestChannel.Response r : deferred) {
            try { requestChannel.sendResponse(r); } catch (InterruptedException ignored) {}
        }
    }

    public void shutdown() {
        running = false;
        selector.wakeup();
    }
}
