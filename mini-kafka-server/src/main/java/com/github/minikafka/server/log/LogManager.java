package com.github.minikafka.server.log;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.*;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 管理所有 TopicPartition 的 {@link Log} 实例，是 Broker 存储层的统一入口。
 * <p>
 * 对应 Kafka 原版 {@code kafka.log.LogManager}（大幅简化版）。
 * Log 实例以 {@code "topic-partition"} 为 key 存储于 {@link java.util.concurrent.ConcurrentHashMap}，
 * 支持并发读取；创建新 Log 时通过 {@code synchronized} 块保证不重复创建（双重检查锁）。
 * </p>
 * <p>
 * 启动时扫描 baseDir 下所有子目录，自动恢复已有 Log（重启场景）。
 * </p>
 */
public final class LogManager implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(LogManager.class);

    private final File baseDir;
    private final LogConfig defaultConfig;
    private final Map<String, Log> logs = new ConcurrentHashMap<>();

    /**
     * 初始化 LogManager：创建 baseDir（若不存在），并扫描加载已有 Log。
     *
     * @param baseDir       所有 Log 数据的根目录，不存在时自动创建
     * @param defaultConfig 新创建 Log 使用的默认配置
     * @throws IOException 目录创建或 Log 加载失败时抛出
     */
    public LogManager(File baseDir, LogConfig defaultConfig) throws IOException {
        this.baseDir = baseDir;
        this.defaultConfig = defaultConfig;
        baseDir.mkdirs();
        loadLogs();
    }

    /**
     * 扫描 baseDir 下所有子目录，将每个目录恢复为对应的 Log 实例。
     * 目录名即为 logKey（格式：{@code "topic-partition"}）。
     *
     * @throws IOException Log 恢复失败时抛出
     */
    private void loadLogs() throws IOException {
        File[] partitionDirs = baseDir.listFiles(File::isDirectory);
        if (partitionDirs == null) return;
        for (File dir : partitionDirs) {
            String key = dir.getName();
            logs.put(key, new Log(dir, defaultConfig));
            log.info("Loaded log for {}", key);
        }
    }

    /**
     * 获取指定 topic-partition 对应的 Log；若不存在则创建。
     * <p>
     * 采用双重检查锁（先无锁查，再加锁创建），兼顾读多写少场景下的性能与正确性。
     * </p>
     *
     * @param topic     topic 名称
     * @param partition 分区编号，从 0 开始
     * @return 对应的 {@link Log} 实例，永不为 {@code null}
     * @throws IOException 创建新 Log 时目录或文件操作失败
     */
    public Log getOrCreateLog(String topic, int partition) throws IOException {
        String key = logKey(topic, partition);
        Log existing = logs.get(key);
        if (existing != null) return existing;
        synchronized (this) {
            existing = logs.get(key);
            if (existing != null) return existing;
            File dir = new File(baseDir, key);
            Log newLog = new Log(dir, defaultConfig);
            logs.put(key, newLog);
            log.info("Created new log for {}", key);
            return newLog;
        }
    }

    /**
     * 获取指定 topic-partition 对应的 Log，仅查找不创建。
     *
     * @param topic     topic 名称
     * @param partition 分区编号
     * @return 对应的 {@link Log} 实例；若该 topic-partition 的 Log 尚未创建，返回 {@code null}
     */
    public Log getLog(String topic, int partition) {
        return logs.get(logKey(topic, partition));
    }

    /**
     * 生成 topic-partition 的 Map key，格式为 {@code "topic-partition"}。
     *
     * @param topic     topic 名称
     * @param partition 分区编号
     * @return Map key 字符串
     */
    private String logKey(String topic, int partition) {
        return topic + "-" + partition;
    }

    public void flush() throws IOException {
        for (Log l : logs.values()) l.flush();
    }

    @Override
    public void close() throws IOException {
        for (Log l : logs.values()) l.close();
    }
}
