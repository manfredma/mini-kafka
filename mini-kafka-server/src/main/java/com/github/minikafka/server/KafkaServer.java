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

public final class KafkaServer {

    private static final Logger log = LoggerFactory.getLogger(KafkaServer.class);

    private final KafkaConfig config;
    private LogManager logManager;
    private KafkaController controller;
    private GroupCoordinator groupCoordinator;
    private RequestChannel requestChannel;
    private SocketServer socketServer;
    private KafkaRequestHandler requestHandler;

    public KafkaServer(KafkaConfig config) {
        this.config = config;
    }

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

    public static void main(String[] args) throws Exception {
        KafkaServer server = new KafkaServer(KafkaConfig.defaultConfig());
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try { server.shutdown(); } catch (Exception e) { e.printStackTrace(); }
        }));
        server.startup();
        Thread.currentThread().join();
    }
}
