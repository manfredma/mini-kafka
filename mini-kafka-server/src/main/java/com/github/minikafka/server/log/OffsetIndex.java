package com.github.minikafka.server.log;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 * 稀疏 offset 索引，每条 entry = (relativeOffset:4bytes, position:4bytes)
 * 对齐 Kafka OffsetIndex
 */
public final class OffsetIndex implements Closeable {

    private static final int ENTRY_SIZE = 8;

    private final long baseOffset;
    private final MappedByteBuffer mmap;
    private final int maxEntries;
    private int entries;

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

    public void flush() { mmap.force(); }

    @Override
    public void close() { flush(); }
}
