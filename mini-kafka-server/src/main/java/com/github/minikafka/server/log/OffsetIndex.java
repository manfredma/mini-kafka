package com.github.minikafka.server.log;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 * 稀疏 offset 索引，每条 entry = (relativeOffset:4bytes, position:4bytes)，共 8 字节。
 * <p>
 * 对应 Kafka 原版 {@code kafka.log.OffsetIndex}。
 * 索引文件通过 {@link MappedByteBuffer} 内存映射，追加和查找均直接操作 mmap，
 * 避免用户态/内核态拷贝。
 * </p>
 * <p>
 * 设计决策：
 * <ul>
 *   <li>relativeOffset 存储相对于 baseOffset 的差值（4 字节），限制单 Segment 最大 offset 跨度
 *       不超过 {@link Integer#MAX_VALUE}（约 21 亿条消息）。</li>
 *   <li>索引为稀疏索引，不是每条消息都有对应条目，由 {@code indexIntervalBytes} 控制密度。</li>
 *   <li>构造时自动区分新文件（entries=0）和重启恢复（调用 {@link #recoverEntries()}）。</li>
 *   <li>非线程安全，外部调用方（LogSegment）负责同步。</li>
 * </ul>
 * </p>
 */
public final class OffsetIndex implements Closeable {

    private static final int ENTRY_SIZE = 8;

    private final long baseOffset;
    private final MappedByteBuffer mmap;
    private final int maxEntries;
    private int entries;

    /**
     * 打开或创建索引文件，并完成内存映射初始化。
     * <p>
     * 若文件已存在且有内容（重启场景），则调用 {@link #recoverEntries()} 恢复 entries 计数；
     * 若文件不存在或为空（新建场景），entries 从 0 开始。
     * </p>
     *
     * @param file         索引文件路径（.index 后缀），不存在时自动创建
     * @param baseOffset   所属 LogSegment 的起始 offset，用于计算 relativeOffset
     * @param maxIndexSize 索引文件的最大字节数，同时决定 mmap 映射大小和最大条目数
     * @throws IOException 文件 IO 或内存映射失败时抛出
     */
    public OffsetIndex(File file, long baseOffset, int maxIndexSize) throws IOException {
        this.baseOffset = baseOffset;
        this.maxEntries = maxIndexSize / ENTRY_SIZE;
        // 在 setLength 之前记录文件是否已有内容（用于区分新文件和重启恢复）
        long existingSize = file.exists() ? file.length() : 0L;
        try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
            raf.setLength(maxIndexSize);
            this.mmap = raf.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, maxIndexSize);
        }
        // 重启恢复：只有文件已有内容时才扫描，新文件直接从 0 开始
        this.entries = (existingSize > 0) ? recoverEntries() : 0;
    }

    /**
     * 扫描 mmap 恢复已有 entries 数量。
     * 索引的 position 字段严格单调递增（每个 batch 至少有若干字节）。
     * 找到第一个 position <= 前一条 position 的位置即为未写入区域的起点。
     * 唯一例外：第 0 条 position 合法为 0，单独处理。
     */
    private int recoverEntries() {
        if (maxEntries == 0) return 0;
        // 第 0 条特殊：position 合法为 0
        if (mmap.getInt(0) == 0 && mmap.getInt(4) == 0) {
            // (0,0) 的第 0 条：只有当后面还有有效条目时才算有效
            // 先假设有效，继续扫描
        }
        int count = 0;
        int prevFilePos = -1;
        while (count < maxEntries) {
            int mmapPos = count * ENTRY_SIZE;
            int filePosition = mmap.getInt(mmapPos + 4);
            if (count == 0) {
                // 第 0 条：position=0 是合法的（batch 从文件头写入），接受
                prevFilePos = 0;
                count++;
                continue;
            }
            // 后续条目：position 必须 > 前一条（严格递增，因为每条 batch 至少有几字节）
            if (filePosition <= prevFilePos) break;
            prevFilePos = filePosition;
            count++;
        }
        return count;
    }

    /**
     * 向索引末尾追加一条稀疏索引条目。
     * <p>
     * 将 offset 转换为相对于 baseOffset 的 relativeOffset（4 字节整数），
     * 与 filePosition 一起写入 mmap。
     * </p>
     *
     * @param offset   要索引的消息批次的绝对 offset（即该 RecordBatch 的 baseOffset）
     * @param position 该批次在 .log 文件中的起始字节位置（文件绝对偏移量）
     * @throws IllegalStateException    索引已满（entries >= maxEntries）时抛出
     * @throws IllegalArgumentException offset - baseOffset 超过 {@link Integer#MAX_VALUE} 时抛出，
     *                                  说明单 Segment 内 offset 跨度过大
     */
    public void append(long offset, int position) {
        if (entries >= maxEntries) {
            throw new IllegalStateException("Index is full: " + entries + " entries");
        }
        int relativeOffset = (int) (offset - baseOffset);
        if (offset - baseOffset > Integer.MAX_VALUE) {
            throw new IllegalArgumentException(
                "offset gap too large: offset=" + offset + " baseOffset=" + baseOffset);
        }
        mmap.position(entries * ENTRY_SIZE);
        mmap.putInt(relativeOffset);
        mmap.putInt(position);
        entries++;
    }

    /**
     * 二分查找，返回 relativeOffset &lt;= (targetOffset - baseOffset) 的最大索引条目对应的
     * 文件物理位置（filePosition）。
     * <p>
     * 由于是稀疏索引，返回的位置可能指向早于 targetOffset 的某个批次起始处，
     * 调用方（LogSegment.read）需继续顺序扫描跳过更早的批次。
     * </p>
     *
     * @param targetOffset 目标 offset（绝对值）
     * @return 最接近且不超过 targetOffset 的索引条目的文件位置；
     *         若索引为空（entries == 0），返回 0（从文件头开始读取）
     */
    public int lookup(long targetOffset) {
        if (entries == 0) return 0;
        int relTarget = (int) (targetOffset - baseOffset);
        int lo = 0, hi = entries - 1, result = 0;
        while (lo <= hi) {
            int mid = (lo + hi) >>> 1;
            int relOffset = mmap.getInt(mid * ENTRY_SIZE);
            int pos = mmap.getInt(mid * ENTRY_SIZE + 4);
            if (relOffset <= relTarget) {
                result = pos;
                lo = mid + 1;
            } else {
                hi = mid - 1;
            }
        }
        return result;
    }

    public int entries() { return entries; }

    /**
     * 将 mmap 中的修改强制刷盘。
     */
    public void flush() { mmap.force(); }

    /**
     * 刷盘并释放资源。等价于 {@link #flush()}，不关闭 mmap（由 JVM GC 回收）。
     */
    @Override
    public void close() { flush(); }
}
