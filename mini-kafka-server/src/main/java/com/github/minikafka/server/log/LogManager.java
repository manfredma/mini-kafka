package com.github.minikafka.server.log;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.*;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 管理所有 TopicPartition 的 Log 实例
 * 对齐 Kafka kafka.log.LogManager
 */
public final class LogManager implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(LogManager.class);

    private final File baseDir;
    private final LogConfig defaultConfig;
    private final Map<String, Log> logs = new ConcurrentHashMap<>();

    public LogManager(File baseDir, LogConfig defaultConfig) throws IOException {
        this.baseDir = baseDir;
        this.defaultConfig = defaultConfig;
        baseDir.mkdirs();
        loadLogs();
    }

    private void loadLogs() throws IOException {
        File[] partitionDirs = baseDir.listFiles(File::isDirectory);
        if (partitionDirs == null) return;
        for (File dir : partitionDirs) {
            String key = dir.getName();
            logs.put(key, new Log(dir, defaultConfig));
            log.info("Loaded log for {}", key);
        }
    }

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

    public Log getLog(String topic, int partition) {
        return logs.get(logKey(topic, partition));
    }

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
