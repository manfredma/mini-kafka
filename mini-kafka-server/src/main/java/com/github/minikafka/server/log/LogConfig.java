package com.github.minikafka.server.log;

public final class LogConfig {
    public final int segmentBytes;
    public final long segmentMs;
    public final int indexIntervalBytes;
    public final int maxIndexSize;

    public LogConfig(int segmentBytes, long segmentMs, int indexIntervalBytes) {
        this.segmentBytes = segmentBytes;
        this.segmentMs = segmentMs;
        this.indexIntervalBytes = indexIntervalBytes;
        this.maxIndexSize = 10 * 1024 * 1024;
    }

    public static LogConfig defaultConfig() {
        return new LogConfig(1024 * 1024 * 1024, 7 * 24 * 3600 * 1000L, 4096);
    }

    public static LogConfig testConfig(int segmentBytes) {
        return new LogConfig(segmentBytes, Long.MAX_VALUE, 64);
    }
}
