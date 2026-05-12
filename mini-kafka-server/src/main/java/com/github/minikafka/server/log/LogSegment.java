package com.github.minikafka.server.log;

import com.github.minikafka.common.record.RecordBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;

/**
 * 对应磁盘上一对 .log + .index 文件，是 Log 的基本存储单元。
 * <p>
 * 对应 Kafka 原版 {@code kafka.log.LogSegment}。
 * .log 文件顺序追加 {@link RecordBatch}，.index 文件按 {@code indexIntervalBytes}
 * 间隔维护稀疏 offset 索引，用于加速随机读取定位。
 * </p>
 * <p>
 * 设计决策：
 * <ul>
 *   <li>文件名以 20 位零填充的 baseOffset 命名（如 {@code 00000000000000000000.log}），
 *       方便按名称排序恢复 Segment 列表。</li>
 *   <li>使用 {@link FileChannel} 而非 {@link java.io.RandomAccessFile}，
 *       避免句柄泄漏且支持 NIO 直接写。</li>
 *   <li>非线程安全，由外层 {@code Log} 持有锁后调用。</li>
 * </ul>
 * </p>
 */
public final class LogSegment implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(LogSegment.class);

    private final long baseOffset;
    private final LogConfig config;
    private final FileChannel logChannel;
    private final OffsetIndex offsetIndex;
    private long size;
    private long bytesSinceLastIndex;
    private final long createdMs;

    /**
     * 打开或创建一个 LogSegment（.log + .index 文件对）。
     * <p>
     * 若文件已存在（重启场景），FileChannel 定位到文件末尾以便继续追加；
     * OffsetIndex 通过 {@code recoverEntries()} 恢复已有条目数。
     * </p>
     *
     * @param dir        Segment 所在目录（对应 topic-partition 目录）
     * @param baseOffset 本 Segment 的起始 offset，同时用于生成文件名
     * @param config     日志配置，提供 maxIndexSize 和 indexIntervalBytes 等参数
     * @throws IOException 文件创建或内存映射失败时抛出
     */
    public LogSegment(File dir, long baseOffset, LogConfig config) throws IOException {
        this.baseOffset = baseOffset;
        this.config = config;
        this.createdMs = System.currentTimeMillis();

        File logFile = new File(dir, filenamePrefixed(baseOffset) + ".log");
        File indexFile = new File(dir, filenamePrefixed(baseOffset) + ".index");

        // 用 FileChannel.open 避免 RandomAccessFile 句柄泄漏
        this.logChannel = FileChannel.open(logFile.toPath(),
            StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
        this.size = this.logChannel.size();
        this.logChannel.position(this.size);

        this.offsetIndex = new OffsetIndex(indexFile, baseOffset, config.maxIndexSize);
        this.bytesSinceLastIndex = 0;
    }

    public long baseOffset() { return baseOffset; }
    public long size() { return size; }
    public long createdMs() { return createdMs; }

    /**
     * 将 RecordBatch 顺序追加到 .log 文件末尾，并按需更新稀疏索引。
     * <p>
     * 稀疏索引写入策略：当自上次写索引以来已积累的字节数达到 {@code indexIntervalBytes}，
     * 或当前索引为空（第一条 batch），则向 OffsetIndex 追加一条条目，记录
     * (batch.baseOffset, 当前文件大小)。
     * </p>
     *
     * @param batch 要追加的消息批次，不得为 {@code null}；batch 的 baseOffset 必须
     *              单调递增（由上层 Log 保证）
     * @throws IOException 写文件失败时抛出
     */
    public void append(RecordBatch batch) throws IOException {
        ByteBuffer buf = ByteBuffer.allocate(batch.sizeInBytes());
        batch.writeTo(buf);
        buf.flip();

        if (bytesSinceLastIndex >= config.indexIntervalBytes || offsetIndex.entries() == 0) {
            offsetIndex.append(batch.baseOffset(), (int) size);
            bytesSinceLastIndex = 0;
        }

        int written = buf.remaining();
        logChannel.write(buf);
        size += written;
        bytesSinceLastIndex += written;
    }

    /**
     * 从 startOffset 开始读取最多 maxLength 字节。
     * 通过 OffsetIndex 定位起始物理位置，然后顺序扫描跳过 baseOffset < startOffset 的 batch，
     * 只返回从 startOffset 开始的有效数据。
     */
    public ByteBuffer read(long startOffset, int maxLength) throws IOException {
        int startPosition = offsetIndex.lookup(startOffset);
        long readableSize = size - startPosition;
        if (readableSize <= 0) return null;

        // 读取候选区域
        int toRead = (int) Math.min(maxLength, readableSize);
        ByteBuffer raw = ByteBuffer.allocate(toRead);
        logChannel.read(raw, startPosition);
        raw.flip();

        // 跳过 baseOffset < startOffset 的 batch（稀疏索引可能指向更早的位置）
        while (raw.hasRemaining()) {
            raw.mark();
            if (raw.remaining() < 12) break; // 不足以读 baseOffset+batchLength
            long batchBaseOffset = raw.getLong();
            int batchLength = raw.getInt();
            if (batchBaseOffset + batchLength < 0 || batchLength < 0) {
                raw.reset();
                break;
            }
            if (batchBaseOffset >= startOffset) {
                // 找到正确起始点，reset 回 batch 开头并返回
                raw.reset();
                return raw.slice();
            }
            // 跳过这个 batch：batchLength 不含自身前面的 baseOffset(8)+batchLength(4)
            int skipBytes = batchLength;
            if (raw.remaining() < skipBytes) {
                raw.reset();
                break;
            }
            raw.position(raw.position() + skipBytes);
        }

        if (!raw.hasRemaining()) return null;
        return raw.slice();
    }

    /**
     * 暴露底层 FileChannel，供 {@code Log.recoverLogEndOffset()} 在重启时直接按字节位置扫描。
     * 包级别可见，不对外部模块开放。
     *
     * @return 本 Segment 对应 .log 文件的 FileChannel
     */
    FileChannel logChannel() { return logChannel; }

    /**
     * 判断本 Segment 是否已达到字节容量上限，需要滚动新 Segment。
     *
     * @return {@code true} 表示已满（size >= segmentBytes），应立即滚动
     */
    public boolean isFull() { return size >= config.segmentBytes; }

    /**
     * 判断本 Segment 是否已超过最大存活时长，需要基于时间滚动。
     *
     * @param now 当前时间戳（毫秒，通常为 {@code System.currentTimeMillis()}）
     * @return {@code true} 表示已过期（now - createdMs >= segmentMs），应滚动新 Segment
     */
    public boolean isExpired(long now) { return (now - createdMs) >= config.segmentMs; }

    /**
     * 将 .log 文件和 .index 文件的内容强制刷盘（fsync）。
     *
     * @throws IOException 刷盘失败时抛出
     */
    public void flush() throws IOException {
        logChannel.force(true);
        offsetIndex.flush();
    }

    /**
     * 刷盘后关闭 .log 和 .index 文件，释放文件句柄。
     *
     * @throws IOException 刷盘或关闭失败时抛出
     */
    @Override
    public void close() throws IOException {
        flush();
        logChannel.close();
        offsetIndex.close();
    }

    /**
     * 将 offset 格式化为 20 位零填充字符串，作为 .log/.index 文件名前缀。
     * 例如 offset=0 → {@code "00000000000000000000"}。
     *
     * @param offset 文件名对应的 baseOffset
     * @return 20 位零填充字符串
     */
    static String filenamePrefixed(long offset) {
        return String.format("%020d", offset);
    }
}
