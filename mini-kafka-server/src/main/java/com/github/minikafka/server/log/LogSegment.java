package com.github.minikafka.server.log;

import com.github.minikafka.common.record.RecordBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;

/**
 * 对应磁盘上一对 .log + .index 文件
 * 对齐 Kafka kafka.log.LogSegment
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

    /** 暴露给 Log.recoverLogEndOffset 使用，包级别可见 */
    FileChannel logChannel() { return logChannel; }

    public boolean isFull() { return size >= config.segmentBytes; }

    public boolean isExpired(long now) { return (now - createdMs) >= config.segmentMs; }

    public void flush() throws IOException {
        logChannel.force(true);
        offsetIndex.flush();
    }

    @Override
    public void close() throws IOException {
        flush();
        logChannel.close();
        offsetIndex.close();
    }

    static String filenamePrefixed(long offset) {
        return String.format("%020d", offset);
    }
}
