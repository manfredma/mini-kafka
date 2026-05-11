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
    private int entries;

    public OffsetIndex(File file, long baseOffset, int maxIndexSize) throws IOException {
        this.baseOffset = baseOffset;
        try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
            raf.setLength(maxIndexSize);
            this.mmap = raf.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, maxIndexSize);
        }
        this.entries = 0;
    }

    public void append(long offset, int position) {
        int relativeOffset = (int) (offset - baseOffset);
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
