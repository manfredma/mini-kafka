package com.github.minikafka.server.controller;

import com.github.minikafka.server.log.LogManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;

/**
 * 单节点 Controller，负责 Topic 创建和 Leader 分配。
 * <p>
 * 对应 Kafka 原版 {@code kafka.controller.KafkaController}（大幅简化版：同步调用，无 ZooKeeper）。
 * mini-kafka 为单节点架构，Controller 与 Broker 运行在同一进程中，
 * 所有分区 Leader 固定为 broker 0，无需选举。
 * </p>
 * <p>
 * 线程安全性：{@link #createTopic} 使用 {@code synchronized} 保证幂等性，
 * 防止并发请求重复创建同一 topic。
 * </p>
 */
public final class KafkaController {

    private static final Logger log = LoggerFactory.getLogger(KafkaController.class);

    private final ControllerContext context;
    private final LogManager logManager;

    /**
     * 构造 KafkaController。
     *
     * @param context    集群元数据视图，不得为 {@code null}
     * @param logManager 存储层管理器，用于为新 topic 创建 Log，不得为 {@code null}
     */
    public KafkaController(ControllerContext context, LogManager logManager) {
        this.context = context;
        this.logManager = logManager;
    }

    /**
     * 创建 topic：为每个分区创建 Log 文件，并在 ControllerContext 中注册元数据。
     * <p>
     * 幂等操作：若 topic 已存在，直接返回不做任何修改，并输出 warn 日志。
     * 整个方法使用 {@code synchronized} 保证原子性，防止并发重复创建。
     * </p>
     *
     * @param topic      要创建的 topic 名称，不得为 {@code null}
     * @param partitions 分区数量，正整数
     * @throws IOException 创建 Log 文件失败时抛出
     */
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
