package com.github.minikafka.server.log;

import com.github.minikafka.common.record.RecordBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

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

        RandomAccessFile raf = new RandomAccessFile(logFile, "rw");
        this.logChannel = raf.getChannel();
        this.size = logFile.length();
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

    public ByteBuffer read(long startOffset, int maxLength) throws IOException {
        int startPosition = offsetIndex.lookup(startOffset);
        long readableSize = size - startPosition;
        if (readableSize <= 0) return null;

        int toRead = (int) Math.min(maxLength, readableSize);
        ByteBuffer buf = ByteBuffer.allocate(toRead);
        logChannel.read(buf, startPosition);
        buf.flip();
        return buf;
    }

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
