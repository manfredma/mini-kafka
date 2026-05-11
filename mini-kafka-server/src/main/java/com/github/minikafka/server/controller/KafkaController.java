package com.github.minikafka.server.controller;

import com.github.minikafka.server.log.LogManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;

/**
 * 单节点 Controller，负责 Topic 创建和 Leader 分配
 * 对齐 Kafka kafka.controller.KafkaController（简化版：同步调用）
 */
public final class KafkaController {

    private static final Logger log = LoggerFactory.getLogger(KafkaController.class);

    private final ControllerContext context;
    private final LogManager logManager;

    public KafkaController(ControllerContext context, LogManager logManager) {
        this.context = context;
        this.logManager = logManager;
    }

    public synchronized void createTopic(String topic, int partitions) throws IOException {
        if (context.topicExists(topic)) {
            log.warn("Topic {} already exists", topic);
            return;
        }
        for (int i = 0; i < partitions; i++) {
            logManager.getOrCreateLog(topic, i);
        }
        context.createTopic(topic, partitions);
        log.info("Created topic {} with {} partitions", topic, partitions);
    }

    public ControllerContext context() { return context; }
}
