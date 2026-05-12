package com.github.minikafka.server.network;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * 管理一批 SocketChannel，通过 NIO Selector 事件驱动地读取完整请求帧并写回响应。
 * <p>
 * 对应 Kafka 原版 {@code kafka.network.Processor}。
 * 每个 Processor 运行在独立线程中，拥有自己的 NIO {@link Selector}，
 * 负责管理分配给它的所有客户端连接的读写事件。
 * </p>
 * <p>
 * 关键设计决策：
 * <ul>
 *   <li>新连接通过 {@link ConcurrentLinkedQueue} 传递（{@link #accept(SocketChannel)}），
 *       避免 Acceptor 线程与 Processor 线程直接竞争 Selector。</li>
 *   <li>写响应采用两阶段设计：{@link #sendPendingResponses()} 只向 Selector 注册 OP_WRITE
 *       并挂载 buffer，实际写入由 {@link #handleWrite(SelectionKey)} 在 OP_WRITE 事件触发时完成，
 *       避免在 select 循环外直接写 Channel（可能阻塞）。</li>
 *   <li>不属于本 Processor 的响应在 {@link #sendPendingResponses()} 中放回队列，
 *       由对应 Processor 处理，避免跨 Selector 写 Channel。</li>
 * </ul>
 * </p>
 */
public final class Processor implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(Processor.class);

    private final int id;
    private final RequestChannel requestChannel;
    private final Selector selector;
    private final Queue<SocketChannel> newConnections = new ConcurrentLinkedQueue<>();
    private final Map<SocketChannel, NetworkReceive> inflightReads = new HashMap<>();
    private volatile boolean running = true;

    /**
     * 构造 Processor，打开专属 NIO Selector。
     *
     * @param id             Processor 编号（0 到 numNetworkThreads-1），用于响应路由
     * @param requestChannel 请求/响应通道，与 KafkaRequestHandler 共享
     * @throws IOException Selector 打开失败时抛出
     */
    public Processor(int id, RequestChannel requestChannel) throws IOException {
        this.id = id;
        this.requestChannel = requestChannel;
        this.selector = Selector.open();
    }

    /**
     * 由 {@link Acceptor} 线程调用，将新接受的连接交给本 Processor 管理。
     * <p>
     * 通过 {@link ConcurrentLinkedQueue} 实现线程安全的连接传递，
     * 调用后立即唤醒 Selector，确保新连接在下一次 select 循环中被注册。
     * </p>
     *
     * @param channel 已 accept 但尚未配置为非阻塞的 SocketChannel
     */
    public void accept(SocketChannel channel) {
        newConnections.add(channel);
        selector.wakeup();
    }

    @Override
    public void run() {
        while (running) {
            try {
                // 注册新连接
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

    /**
     * 处理 OP_READ 事件：从 Channel 读取数据，帧完整后放入 RequestChannel。
     * <p>
     * 使用 {@link NetworkReceive} 维护每个连接的读取状态（支持分片读取）。
     * 帧读取完成后，duplicate payload（避免共享 position）放入 requestChannel，
     * 并为该连接创建新的 NetworkReceive 实例以接收下一帧。
     * 若 Channel 发生 IO 异常（如客户端断开），取消 SelectionKey 并关闭 Channel。
     * </p>
     *
     * @param key 触发 OP_READ 的 SelectionKey
     * @throws IOException          Channel 操作失败时抛出（已在方法内部捕获并处理）
     * @throws InterruptedException 放入 requestChannel 时线程被中断时抛出
     */
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

    /**
     * 处理 OP_WRITE 事件：将挂载在 SelectionKey attachment 上的响应 buffer 写入 Channel。
     * <p>
     * 写完后（buffer.hasRemaining() == false）将 interestOps 切回 OP_READ，
     * 并清除 attachment，等待下一次请求。
     * 若 buffer 尚未写完（Socket 发送缓冲区满），保持 OP_WRITE 注册，等待下次事件继续写。
     * </p>
     *
     * @param key 触发 OP_WRITE 的 SelectionKey，attachment 为待写入的响应 ByteBuffer
     * @throws IOException Channel 写入失败时抛出
     */
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

    /**
     * 将属于本 Processor 的响应通过 OP_WRITE 发出；
     * 不属于本 Processor 的响应放回队列一次后停止本轮处理，
     * 避免多 Processor 场景下无限轮转。
     */
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
                // 只注册 OP_WRITE，实际写入由 handleWrite 完成，避免双写
                key.attach(response.buffer);
                key.interestOps(SelectionKey.OP_WRITE);
                selector.wakeup();
            }
        }
        // 将不属于本 Processor 的响应放回队列，让各自的 Processor 处理
        for (RequestChannel.Response r : deferred) {
            try { requestChannel.sendResponse(r); } catch (InterruptedException ignored) {}
        }
    }

    /**
     * 通知 Processor 停止运行，唤醒阻塞中的 Selector 以使 run 循环尽快退出。
     */
    public void shutdown() {
        running = false;
        selector.wakeup();
    }
}
