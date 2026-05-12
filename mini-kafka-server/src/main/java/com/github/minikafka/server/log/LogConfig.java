package com.github.minikafka.server.log;

/**
 * LogSegment 及 OffsetIndex 的配置参数持有类。
 * <p>
 * 对应 Kafka 原版 {@code kafka.log.LogConfig}（精简版，仅保留核心字段）。
 * 所有字段均为 {@code final}，实例创建后不可变，可安全跨线程共享。
 * </p>
 */
public final class LogConfig {
    /** 单个 LogSegment .log 文件的最大字节数，超过后触发 Segment 滚动。 */
    public final int segmentBytes;
    /**
     * 单个 LogSegment 的最大存活时长（毫秒）。
     * 从 Segment 创建时刻起计算，超过后即使未达到 {@link #segmentBytes} 也会滚动。
     * 设为 {@link Long#MAX_VALUE} 可禁用基于时间的滚动（常用于测试）。
     */
    public final long segmentMs;
    /**
     * 稀疏索引写入间隔（字节）。
     * 每向 .log 文件写入 indexIntervalBytes 字节后，向 OffsetIndex 追加一条索引条目。
     * 值越小索引越密集，查找越快，但索引文件越大。
     */
    public final int indexIntervalBytes;
    /**
     * 单个 OffsetIndex 文件的最大字节数，决定索引最多可存储的条目数。
     * 固定为 10 MiB（10 * 1024 * 1024），与 Kafka 默认值一致。
     */
    public final int maxIndexSize;

    /**
     * 构造 LogConfig。
     *
     * @param segmentBytes       单个 Segment 的最大字节数，正整数
     * @param segmentMs          单个 Segment 的最大存活时长（毫秒），正数；
     *                           {@link Long#MAX_VALUE} 表示不限时间
     * @param indexIntervalBytes 稀疏索引写入间隔（字节），正整数
     */
    public LogConfig(int segmentBytes, long segmentMs, int indexIntervalBytes) {
        this.segmentBytes = segmentBytes;
        this.segmentMs = segmentMs;
        this.indexIntervalBytes = indexIntervalBytes;
        this.maxIndexSize = 10 * 1024 * 1024;
    }

    /**
     * 返回与 Kafka 默认值对齐的生产环境配置：
     * 1 GiB Segment、7 天滚动、4 KiB 索引间隔。
     *
     * @return 默认 {@link LogConfig} 实例
     */
    public static LogConfig defaultConfig() {
        return new LogConfig(1024 * 1024 * 1024, 7 * 24 * 3600 * 1000L, 4096);
    }

    /**
     * 返回适合单元测试的小 Segment 配置：指定 segmentBytes、禁用时间滚动、64 字节索引间隔。
     * <p>
     * 使用极小的 segmentBytes 可在测试中快速触发 Segment 滚动，验证多 Segment 逻辑。
     * </p>
     *
     * @param segmentBytes 测试用 Segment 大小（字节），通常设置为几百到几千字节
     * @return 测试用 {@link LogConfig} 实例
     */
    public static LogConfig testConfig(int segmentBytes) {
        return new LogConfig(segmentBytes, Long.MAX_VALUE, 64);
    }
}
