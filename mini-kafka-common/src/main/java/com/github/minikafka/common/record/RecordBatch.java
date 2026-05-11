package com.github.minikafka.common.record;

import com.github.minikafka.common.network.ByteBufferUtils;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.zip.CRC32;

/**
 * Kafka magic=2 RecordBatch 格式（简化版）
 *
 * 帧布局：
 *   8  baseOffset
 *   4  batchLength
 *   4  partitionLeaderEpoch (固定0)
 *   1  magic (固定2)
 *   4  crc
 *   2  attributes
 *   4  lastOffsetDelta
 *   8  firstTimestamp
 *   8  maxTimestamp
 *   8  producerId (固定-1)
 *   2  producerEpoch (固定-1)
 *   4  baseSequence (固定-1)
 *   4  recordCount
 *   [records]
 */
public final class RecordBatch {

    // partitionLeaderEpoch(4)+magic(1)+crc(4)+attributes(2)+lastOffsetDelta(4)
    // +firstTimestamp(8)+maxTimestamp(8)+producerId(8)+producerEpoch(2)+baseSequence(4)+recordCount(4)
    private static final int BATCH_OVERHEAD = 4 + 1 + 4 + 2 + 4 + 8 + 8 + 8 + 2 + 4 + 4;

    private final long baseOffset;
    private final List<Record> records;

    public RecordBatch(long baseOffset, List<Record> records) {
        this.baseOffset = baseOffset;
        this.records = Collections.unmodifiableList(new ArrayList<>(records));
    }

    public long baseOffset() { return baseOffset; }
    public List<Record> records() { return records; }
    public long lastOffset() { return baseOffset + records.size() - 1; }

    public int sizeInBytes() {
        return 8 + 4 + BATCH_OVERHEAD + recordsSize();
    }

    private int recordsSize() {
        int size = 0;
        for (Record r : records) size += recordSize(r);
        return size;
    }

    private int recordSize(Record r) {
        return 4 + 1 + 8 + 4
            + ByteBufferUtils.sizeOfBytes(r.key())
            + ByteBufferUtils.sizeOfBytes(r.value());
    }

    public void writeTo(ByteBuffer buffer) {
        long firstTimestamp = records.isEmpty() ? 0L : records.get(0).timestamp();
        long maxTimestamp = records.isEmpty() ? 0L : records.get(records.size() - 1).timestamp();
        int lastOffsetDelta = records.isEmpty() ? 0 : records.size() - 1;

        buffer.putLong(baseOffset);
        int batchLengthPos = buffer.position();
        buffer.putInt(0);
        int batchStart = buffer.position();

        buffer.putInt(0);
        buffer.put((byte) 2);

        int crcPos = buffer.position();
        buffer.putInt(0);

        int crcStart = buffer.position();
        buffer.putShort((short) 0);
        buffer.putInt(lastOffsetDelta);
        buffer.putLong(firstTimestamp);
        buffer.putLong(maxTimestamp);
        buffer.putLong(-1L);
        buffer.putShort((short) -1);
        buffer.putInt(-1);
        buffer.putInt(records.size());

        for (int i = 0; i < records.size(); i++) {
            Record r = records.get(i);
            int rSize = recordSize(r) - 4;
            buffer.putInt(rSize);
            buffer.put((byte) 0);
            buffer.putLong(r.timestamp() - firstTimestamp);
            buffer.putInt(i);
            ByteBufferUtils.writeBytes(buffer, r.key());
            ByteBufferUtils.writeBytes(buffer, r.value());
        }

        int end = buffer.position();
        buffer.putInt(batchLengthPos, end - batchStart);

        byte[] crcData = new byte[end - crcStart];
        int savedPos = buffer.position();
        buffer.position(crcStart);
        buffer.get(crcData);
        buffer.position(savedPos);

        CRC32 crc32 = new CRC32();
        crc32.update(crcData);
        buffer.putInt(crcPos, (int) crc32.getValue());
    }

    public static RecordBatch readFrom(ByteBuffer buffer) {
        long baseOffset = buffer.getLong();
        buffer.getInt(); // batchLength
        buffer.getInt(); // partitionLeaderEpoch
        buffer.get();    // magic
        buffer.getInt(); // crc
        buffer.getShort(); // attributes
        buffer.getInt(); // lastOffsetDelta
        long firstTimestamp = buffer.getLong();
        buffer.getLong(); // maxTimestamp
        buffer.getLong(); // producerId
        buffer.getShort(); // producerEpoch
        buffer.getInt(); // baseSequence
        int recordCount = buffer.getInt();

        List<Record> records = new ArrayList<>(recordCount);
        for (int i = 0; i < recordCount; i++) {
            buffer.getInt(); // length
            buffer.get();    // attributes
            long timestampDelta = buffer.getLong();
            int offsetDelta = buffer.getInt();
            byte[] key = ByteBufferUtils.readBytes(buffer);
            byte[] value = ByteBufferUtils.readBytes(buffer);
            records.add(new Record(baseOffset + offsetDelta, firstTimestamp + timestampDelta, key, value));
        }

        return new RecordBatch(baseOffset, records);
    }
}
