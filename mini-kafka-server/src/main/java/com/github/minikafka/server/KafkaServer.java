package com.github.minikafka.server;

import com.github.minikafka.server.api.KafkaApis;
import com.github.minikafka.server.api.KafkaRequestHandler;
import com.github.minikafka.server.controller.ControllerContext;
import com.github.minikafka.server.controller.KafkaController;
import com.github.minikafka.server.coordinator.GroupCoordinator;
import com.github.minikafka.server.log.LogConfig;
import com.github.minikafka.server.log.LogManager;
import com.github.minikafka.server.network.RequestChannel;
import com.github.minikafka.server.network.SocketServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.File;

/**
 * mini-kafka Broker 的主入口，负责组装并启动所有子系统。
 * <p>
 * 对应 Kafka 原版 {@code kafka.server.KafkaServer}（大幅简化版）。
 * 启动顺序：LogManager → KafkaController → GroupCoordinator →
 * RequestChannel → KafkaApis → KafkaRequestHandler → SocketServer。
 * 关闭顺序与启动顺序相反，先停止网络接入再关闭存储，保证在途请求有机会处理完毕。
 * </p>
 * <p>
 * 线程安全性：{@link #startup()} 和 {@link #shutdown()} 不是线程安全的，
 * 应由同一线程（或外部同步）顺序调用。
 * </p>
 */
public final class KafkaServer {

    private static final Logger log = LoggerFactory.getLogger(KafkaServer.class);

    private final KafkaConfig config;
    private LogManager logManager;
    private KafkaController controller;
    private GroupCoordinator groupCoordinator;
    private RequestChannel requestChannel;
    private SocketServer socketServer;
    private KafkaRequestHandler requestHandler;

    /**
     * 创建 KafkaServer 实例，但不启动任何子系统。
     *
     * @param config Broker 配置，不得为 {@code null}
     */
    public KafkaServer(KafkaConfig config) {
        this.config = config;
    }

    /**
     * 按顺序初始化并启动所有子系统。
     * <p>
     * 方法返回后，Broker 即可接受客户端连接。若任一子系统初始化失败，
     * 已启动的组件不会自动回滚，调用方应捕获异常后调用 {@link #shutdown()} 清理。
     * </p>
     *
     * @throws Exception 任意子系统初始化或启动失败时抛出
     */
    public void startup() throws Exception {
        log.info("Starting KafkaServer on port {}", config.port);

        LogConfig logConfig = new LogConfig(
            config.logSegmentBytes, config.logSegmentMs, config.logIndexIntervalBytes
        );
        logManager = new LogManager(new File(config.logDirs), logConfig);
        controller = new KafkaController(new ControllerContext(), logManager);
        groupCoordinator = new GroupCoordinator(config.sessionTimeoutMs);

        requestChannel = new RequestChannel(500);
        KafkaApis apis = new KafkaApis(logManager, controller, groupCoordinator);
        requestHandler = new KafkaRequestHandler(config.numIoThreads, requestChannel, apis);
        socketServer = new SocketServer(config.port, config.numNetworkThreads, requestChannel);

        socketServer.startup();
        requestHandler.startup();

        log.info("KafkaServer started successfully");
    }

    /**
     * 按逆序优雅关闭所有子系统。
     * <p>
     * 先停止网络层（SocketServer），再停止请求处理层（KafkaRequestHandler），
     * 最后关闭存储层（LogManager）。各步骤均容忍 {@code null}（未完成初始化的情况）。
     * </p>
     *
     * @throws Exception 任意子系统关闭时抛出的异常
     */
    public void shutdown() throws Exception {
        log.info("Shutting down KafkaServer");
        if (socketServer != null) socketServer.shutdown();
        if (requestHandler != null) requestHandler.shutdown();
        if (logManager != null) logManager.close();
        log.info("KafkaServer stopped");
    }

    public LogManager logManager() { return logManager; }
    public KafkaController controller() { return controller; }
    public GroupCoordinator groupCoordinator() { return groupCoordinator; }
    public int port() { return config.port; }

    /**
     * 以默认配置启动 Broker，并阻塞主线程直至进程退出。
     * <p>
     * 注册 JVM ShutdownHook 以确保进程收到 SIGTERM/SIGINT 时优雅关闭。
     * </p>
     *
     * @param args 命令行参数（当前未使用）
     * @throws Exception 启动失败时抛出
     */
    public static void main(String[] args) throws Exception {
        KafkaServer server = new KafkaServer(KafkaConfig.defaultConfig());
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try { server.shutdown(); } catch (Exception e) { e.printStackTrace(); }
        }));
        server.startup();
        Thread.currentThread().join();
    }
}
