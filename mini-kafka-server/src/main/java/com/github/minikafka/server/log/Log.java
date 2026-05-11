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

/**
 * 单个 TopicPartition 的 Log，管理有序的 LogSegment 列表
 * 对齐 Kafka kafka.log.Log
 */
public final class Log implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(Log.class);

    private final File dir;
    private final LogConfig config;
    private final List<LogSegment> segments = new ArrayList<>();
    private final AtomicLong logEndOffset = new AtomicLong(0);

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

    private void recoverLogEndOffset() throws IOException {
        if (segments.isEmpty()) return;
        LogSegment last = activeSegment();
        long offset = last.baseOffset();
        ByteBuffer buf;
        while ((buf = last.read(offset, 1024 * 1024)) != null && buf.hasRemaining()) {
            try {
                RecordBatch batch = RecordBatch.readFrom(buf);
                offset = batch.lastOffset() + 1;
            } catch (Exception e) {
                break;
            }
        }
        logEndOffset.set(offset);
    }

    public long append(RecordBatch batch) throws IOException {
        maybeRoll();
        activeSegment().append(batch);
        long nextOffset = batch.lastOffset() + 1;
        logEndOffset.set(nextOffset);
        return nextOffset;
    }

    public ByteBuffer read(long startOffset, int maxLength) throws IOException {
        LogSegment segment = findSegment(startOffset);
        if (segment == null) return null;
        return segment.read(startOffset, maxLength);
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
    public int numberOfSegments() { return segments.size(); }

    public void flush() throws IOException {
        for (LogSegment seg : segments) seg.flush();
    }

    @Override
    public void close() throws IOException {
        for (LogSegment seg : segments) seg.close();
    }
}
