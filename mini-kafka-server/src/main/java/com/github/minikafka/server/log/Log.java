package com.github.minikafka.server.log;

import com.github.minikafka.common.record.RecordBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 单个 TopicPartition 的 Log，管理有序的 LogSegment 列表
 * 对齐 Kafka kafka.log.Log
 *
 * append/maybeRoll/read/findSegment 均在 lock 下执行，保证多线程安全。
 */
public final class Log implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(Log.class);

    private final File dir;
    private final LogConfig config;
    private final List<LogSegment> segments = new ArrayList<>();
    private final AtomicLong logEndOffset = new AtomicLong(0);
    private final ReentrantLock lock = new ReentrantLock();

    public Log(File dir, LogConfig config) throws IOException {
        this.dir = dir;
        this.config = config;
        dir.mkdirs();
        loadSegments();
    }

    private void loadSegments() throws IOException {
        File[] logFiles = dir.listFiles((d, name) -> name.endsWith(".log"));
        if (logFiles == null || logFiles.length == 0) {
            segments.add(new LogSegment(dir, 0L, config));
            return;
        }
        Arrays.sort(logFiles, Comparator.comparing(File::getName));
        for (File f : logFiles) {
            String name = f.getName().replace(".log", "");
            long baseOffset = Long.parseLong(name);
            segments.add(new LogSegment(dir, baseOffset, config));
        }
        recoverLogEndOffset();
    }

    /**
     * 从最后一个 segment 顺序扫描，使用 segment 内绝对 position 游标避免重复解析。
     */
    private void recoverLogEndOffset() throws IOException {
        if (segments.isEmpty()) return;
        LogSegment last = activeSegment();
        long offset = last.baseOffset();
        // 使用 position 游标直接读，不依赖 offsetIndex（避免稀疏索引导致重复解析）
        long position = 0;
        while (position < last.size()) {
            int toRead = (int) Math.min(1024 * 1024, last.size() - position);
            ByteBuffer buf = ByteBuffer.allocate(toRead);
            last.logChannel().read(buf, position);
            buf.flip();
            long consumed = 0;
            while (buf.remaining() >= 12) {
                buf.mark();
                long batchBaseOffset = buf.getLong();
                int batchLength = buf.getInt();
                if (batchLength <= 0 || buf.remaining() < batchLength) {
                    buf.reset();
                    break;
                }
                buf.position(buf.position() + batchLength);
                offset = batchBaseOffset + /* lastOffsetDelta 从 batch 内部读 */ 1; // 保守估计
                consumed = (long) buf.position();
            }
            if (consumed == 0) break;
            position += consumed;
        }
        // 更精确恢复：完整解析最后 segment
        recoverOffsetByFullScan(last, offset);
    }

    private void recoverOffsetByFullScan(LogSegment segment, long startOffset) throws IOException {
        long offset = segment.baseOffset();
        long pos = 0;
        while (pos < segment.size()) {
            int toRead = (int) Math.min(64 * 1024, segment.size() - pos);
            ByteBuffer buf = ByteBuffer.allocate(toRead);
            segment.logChannel().read(buf, pos);
            buf.flip();
            if (!buf.hasRemaining()) break;
            try {
                RecordBatch batch = RecordBatch.readFrom(buf);
                offset = batch.lastOffset() + 1;
                pos += batch.sizeInBytes();
            } catch (Exception e) {
                break;
            }
        }
        logEndOffset.set(offset);
    }

    public long append(RecordBatch batch) throws IOException {
        lock.lock();
        try {
            maybeRoll();
            activeSegment().append(batch);
            long nextOffset = batch.lastOffset() + 1;
            logEndOffset.set(nextOffset);
            return nextOffset;
        } finally {
            lock.unlock();
        }
    }

    public ByteBuffer read(long startOffset, int maxLength) throws IOException {
        lock.lock();
        try {
            LogSegment segment = findSegment(startOffset);
            if (segment == null) return null;
            return segment.read(startOffset, maxLength);
        } finally {
            lock.unlock();
        }
    }

    private LogSegment findSegment(long offset) {
        int lo = 0, hi = segments.size() - 1, result = -1;
        while (lo <= hi) {
            int mid = (lo + hi) >>> 1;
            if (segments.get(mid).baseOffset() <= offset) {
                result = mid;
                lo = mid + 1;
            } else {
                hi = mid - 1;
            }
        }
        return result >= 0 ? segments.get(result) : null;
    }

    private void maybeRoll() throws IOException {
        LogSegment active = activeSegment();
        long now = System.currentTimeMillis();
        if (active.isFull() || active.isExpired(now)) {
            log.info("Rolling log segment at offset {}", logEndOffset.get());
            active.flush();
            segments.add(new LogSegment(dir, logEndOffset.get(), config));
        }
    }

    private LogSegment activeSegment() {
        return segments.get(segments.size() - 1);
    }

    public long logEndOffset() { return logEndOffset.get(); }
    public int numberOfSegments() {
        lock.lock();
        try { return segments.size(); } finally { lock.unlock(); }
    }

    public void flush() throws IOException {
        lock.lock();
        try {
            for (LogSegment seg : segments) seg.flush();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void close() throws IOException {
        lock.lock();
        try {
            IOException first = null;
            for (LogSegment seg : segments) {
                try { seg.close(); } catch (IOException e) {
                    if (first == null) first = e;
                    else first.addSuppressed(e);
                }
            }
            if (first != null) throw first;
        } finally {
            lock.unlock();
        }
    }
}
