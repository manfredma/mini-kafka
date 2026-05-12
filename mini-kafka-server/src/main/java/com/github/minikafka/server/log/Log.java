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
 * 单个 TopicPartition 的持久化日志，管理有序的 {@link LogSegment} 列表。
 * <p>
 * 对应 Kafka 原版 {@code kafka.log.Log}（大幅简化版）。
 * 每个 Log 对应磁盘上一个目录（如 {@code /data/topic-0/}），目录内包含若干对
 * .log + .index 文件（每对对应一个 LogSegment）。
 * </p>
 * <p>
 * 线程安全性：{@link #append}、{@link #read}、{@link #maybeRoll}、{@link #findSegment}
 * 均在 {@link ReentrantLock} 保护下执行，可安全跨线程调用。
 * </p>
 * <p>
 * 设计决策：
 * <ul>
 *   <li>logEndOffset 使用 {@link java.util.concurrent.atomic.AtomicLong} 存储，
 *       允许在不持锁时快速读取（如监控场景）。</li>
 *   <li>重启时通过 {@link #recoverLogEndOffset()} 扫描最后一个 Segment 恢复 LEO，
 *       无需依赖外部 checkpoint 文件。</li>
 * </ul>
 * </p>
 */
public final class Log implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(Log.class);

    private final File dir;
    private final LogConfig config;
    private final List<LogSegment> segments = new ArrayList<>();
    private final AtomicLong logEndOffset = new AtomicLong(0);
    private final ReentrantLock lock = new ReentrantLock();

    /**
     * 打开或创建指定目录下的 Log。
     * <p>
     * 若目录为空（首次创建），自动生成 baseOffset=0 的初始 Segment；
     * 若目录已有 .log 文件（重启场景），按文件名排序加载所有 Segment，
     * 并调用 {@link #recoverLogEndOffset()} 恢复 LEO。
     * </p>
     *
     * @param dir    Log 数据目录，不存在时自动创建
     * @param config 日志配置
     * @throws IOException 目录创建或 Segment 加载失败时抛出
     */
    public Log(File dir, LogConfig config) throws IOException {
        this.dir = dir;
        this.config = config;
        dir.mkdirs();
        loadSegments();
    }

    /**
     * 扫描目录下所有 .log 文件，按 baseOffset 升序加载 Segment 列表。
     * 若目录为空，创建初始 Segment（baseOffset=0）。
     * 加载完成后调用 {@link #recoverLogEndOffset()} 恢复 LEO。
     *
     * @throws IOException Segment 加载失败时抛出
     */
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
     * 重启恢复：通过完整扫描最后一个 Segment 确定 Log End Offset（LEO）。
     * <p>
     * 分两阶段执行：
     * <ol>
     *   <li>粗扫阶段：以 1 MiB 为块读取 .log 文件，利用 position 游标跳过已解析区域，
     *       快速定位到末尾附近，避免对整个文件做完整解析。</li>
     *   <li>精扫阶段：调用 {@link #recoverOffsetByFullScan} 对最后 Segment 做完整
     *       {@link com.github.minikafka.common.record.RecordBatch#readFrom} 解析，
     *       精确恢复 LEO = lastBatch.lastOffset() + 1。</li>
     * </ol>
     * </p>
     *
     * @throws IOException 读取文件失败时抛出
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

    /**
     * 对指定 Segment 做完整 RecordBatch 解析扫描，精确恢复 logEndOffset。
     * <p>
     * 遇到无法解析的字节（文件截断或损坏）时停止扫描，以最后成功解析的 batch 为准。
     * 最终通过 {@link #logEndOffset} 原子更新 LEO。
     * </p>
     *
     * @param segment     要扫描的 Segment
     * @param startOffset 粗扫阶段估算的起始 offset（当前未使用，保留供将来优化）
     * @throws IOException 读取文件失败时抛出
     */
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

    /**
     * 将 RecordBatch 追加到当前活跃 Segment，并更新 logEndOffset。
     * <p>
     * 追加前先调用 {@link #maybeRoll()} 判断是否需要滚动新 Segment。
     * 整个操作在 {@link ReentrantLock} 保护下执行，保证线程安全。
     * </p>
     *
     * @param batch 要追加的消息批次，不得为 {@code null}；
     *              batch.baseOffset 应等于当前 logEndOffset（由调用方保证）
     * @return 追加后的 logEndOffset（即 batch.lastOffset() + 1）
     * @throws IOException 写文件失败时抛出
     */
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

    /**
     * 从指定 offset 开始读取最多 maxLength 字节的消息数据。
     * <p>
     * 先通过 {@link #findSegment(long)} 定位包含 startOffset 的 Segment，
     * 再委托 {@link LogSegment#read(long, int)} 完成实际读取。
     * 整个操作在 {@link ReentrantLock} 保护下执行。
     * </p>
     *
     * @param startOffset 读取起始 offset（包含），必须 >= 0
     * @param maxLength   最多读取的字节数，正整数
     * @return 包含从 startOffset 开始的消息数据的 ByteBuffer（position=0，limit=实际数据量）；
     *         若 startOffset 超出范围或对应 Segment 无数据，返回 {@code null}
     * @throws IOException 读文件失败时抛出
     */
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

    /**
     * 二分查找 baseOffset &lt;= offset 的最大 Segment。
     * <p>
     * 由于 Segment 列表按 baseOffset 升序排列，二分查找时间复杂度为 O(log n)。
     * 必须在持锁状态下调用。
     * </p>
     *
     * @param offset 目标 offset
     * @return baseOffset 最大且不超过 offset 的 Segment；
     *         若所有 Segment 的 baseOffset 都大于 offset，返回 {@code null}
     */
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

    /**
     * 判断当前活跃 Segment 是否需要滚动，若是则 flush 当前 Segment 并创建新 Segment。
     * <p>
     * 滚动条件（满足其一即滚动）：
     * <ul>
     *   <li>活跃 Segment 已达字节上限（{@link LogSegment#isFull()}）</li>
     *   <li>活跃 Segment 已超过最大存活时长（{@link LogSegment#isExpired(long)}）</li>
     * </ul>
     * 新 Segment 的 baseOffset 为当前 logEndOffset。
     * 必须在持锁状态下调用。
     * </p>
     *
     * @throws IOException Segment flush 或创建失败时抛出
     */
    private void maybeRoll() throws IOException {
        LogSegment active = activeSegment();
        long now = System.currentTimeMillis();
        if (active.isFull() || active.isExpired(now)) {
            log.info("Rolling log segment at offset {}", logEndOffset.get());
            active.flush();
            segments.add(new LogSegment(dir, logEndOffset.get(), config));
        }
    }

    /**
     * 返回当前活跃（最新）的 Segment，即列表末尾的 Segment。
     * 必须在持锁状态下调用。
     *
     * @return 当前活跃 {@link LogSegment}
     */
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
