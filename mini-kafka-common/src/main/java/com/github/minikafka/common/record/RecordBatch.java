package com.github.minikafka.common.record;

import com.github.minikafka.common.network.ByteBufferUtils;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.zip.CRC32;

/**
 * Kafka magic=2 消息批次，对应 Kafka 原版 {@code org.apache.kafka.common.record.RecordBatch}
 * 及其默认实现 {@code DefaultRecordBatch}。
 *
 * <p>一个 RecordBatch 是 Kafka 存储和网络传输的基本单元，包含一组连续 offset 的消息记录。
 *
 * <p>帧布局（字节顺序）：
 * <pre>
 *   8  baseOffset               — 批次中第一条消息的绝对 offset
 *   4  batchLength              — 从 partitionLeaderEpoch 到末尾的字节数
 *   4  partitionLeaderEpoch     — 固定 0（mini-kafka 不支持 leader epoch）
 *   1  magic                    — 固定 2
 *   4  crc                      — CRC32C 校验，覆盖从 attributes 到末尾的所有字节
 *   2  attributes               — 固定 0（无压缩、无事务、无控制消息）
 *   4  lastOffsetDelta          — 最后一条消息相对 baseOffset 的 delta，即 records.size()-1
 *   8  firstTimestamp           — 第一条消息的时间戳
 *   8  maxTimestamp             — 批次中最大的时间戳
 *   8  producerId               — 固定 -1（mini-kafka 不支持幂等生产者）
 *   2  producerEpoch            — 固定 -1
 *   4  baseSequence             — 固定 -1
 *   4  recordCount              — 批次中消息数量
 *   [records]                   — 各条 Record 的序列化内容
 * </pre>
 *
 * <p>关键设计决策：
 * <ul>
 *   <li>records 列表在构造时做防御性拷贝并包装为不可修改视图，保证对象不可变。</li>
 *   <li>CRC 校验范围从 {@code attributes} 字段开始（不含 CRC 字段本身），与 Kafka 规范一致。</li>
 *   <li>maxTimestamp 通过遍历所有 record 取最大值，不假设消息按时间戳升序排列。</li>
 *   <li>每条 Record 的 {@code timestampDelta} 和 {@code offsetDelta} 在序列化时动态计算，
 *       不存储在 Record 对象中，以保持 Record 持有绝对值的语义。</li>
 * </ul>
 */
public final class RecordBatch {

    // partitionLeaderEpoch(4)+magic(1)+crc(4)+attributes(2)+lastOffsetDelta(4)
    // +firstTimestamp(8)+maxTimestamp(8)+producerId(8)+producerEpoch(2)+baseSequence(4)+recordCount(4)
    private static final int BATCH_OVERHEAD = 4 + 1 + 4 + 2 + 4 + 8 + 8 + 8 + 2 + 4 + 4;

    private final long baseOffset;
    private final List<Record> records;

    /**
     * 构造一个消息批次。
     *
     * @param baseOffset 批次中第一条消息的绝对 offset
     * @param records    批次包含的消息列表，构造时做防御性拷贝，不允许为 null，但允许为空列表
     */
    public RecordBatch(long baseOffset, List<Record> records) {
        this.baseOffset = baseOffset;
        this.records = Collections.unmodifiableList(new ArrayList<>(records));
    }

    public long baseOffset() { return baseOffset; }
    public List<Record> records() { return records; }

    /**
     * 返回批次中最后一条消息的绝对 offset。
     *
     * @return {@code baseOffset + records.size() - 1}
     * @throws IllegalStateException 当批次为空（无任何消息）时抛出
     */
    public long lastOffset() {
        if (records.isEmpty()) throw new IllegalStateException("RecordBatch has no records");
        return baseOffset + records.size() - 1;
    }

    /**
     * 返回此批次序列化后的总字节数，包含 baseOffset(8) 和 batchLength(4) 字段本身。
     *
     * <p>等价于调用 {@link #writeTo(ByteBuffer)} 后 position 的增量，
     * 可用于预分配 ByteBuffer 容量。
     *
     * @return 序列化总字节数，始终大于 0
     */
    public int sizeInBytes() {
        return 8 + 4 + BATCH_OVERHEAD + recordsSize();
    }

    /**
     * 计算所有 Record 序列化后的总字节数（不含 baseOffset 和 batchLength 字段）。
     */
    private int recordsSize() {
        int size = 0;
        for (Record r : records) size += recordSize(r);
        return size;
    }

    /**
     * 计算单条 Record 序列化后的字节数，包含 4 字节长度前缀。
     *
     * <p>Record 帧格式：length(4) + attributes(1) + timestampDelta(8) + offsetDelta(4)
     * + key(NULLABLE_BYTES) + value(NULLABLE_BYTES)。
     *
     * @param r 待计算的消息记录
     * @return 该 Record 序列化字节数（含 4 字节长度前缀）
     */
    private int recordSize(Record r) {
        return 4 + 1 + 8 + 4
            + ByteBufferUtils.sizeOfBytes(r.key())
            + ByteBufferUtils.sizeOfBytes(r.value());
    }

    /**
     * 将此批次按 Kafka magic=2 格式序列化写入 {@code buffer}，包含 CRC 计算与回填。
     *
     * <p>写入过程分三步：
     * <ol>
     *   <li>写入 baseOffset 和占位的 batchLength（后续回填）。</li>
     *   <li>写入批次头部固定字段和所有 Record，CRC 字段先写占位 0。</li>
     *   <li>回填 batchLength 和 CRC32 校验值。</li>
     * </ol>
     *
     * <p>调用方须确保 {@code buffer} 剩余容量不小于 {@link #sizeInBytes()} 字节。
     *
     * @param buffer 目标 ByteBuffer，写入后 position 向后移动 {@link #sizeInBytes()} 个字节
     */
    public void writeTo(ByteBuffer buffer) {
        long firstTimestamp = records.isEmpty() ? 0L : records.get(0).timestamp();
        // maxTimestamp 取所有 record 中的最大值，不能假设按时间戳升序
        long maxTimestamp = 0L;
        for (Record r : records) if (r.timestamp() > maxTimestamp) maxTimestamp = r.timestamp();
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

    /**
     * 从 {@code buffer} 当前位置反序列化出一个 {@code RecordBatch}。
     *
     * <p>读取过程中会跳过若干固定字段（batchLength、partitionLeaderEpoch、magic、crc、
     * attributes、lastOffsetDelta、maxTimestamp、producerId、producerEpoch、baseSequence），
     * 不对 CRC 进行校验（mini-kafka 简化实现）。
     *
     * <p>每条 Record 的绝对 offset 和绝对时间戳由 baseOffset/firstTimestamp 加上各自 delta 还原。
     *
     * @param buffer 包含批次字节的 ByteBuffer，position 需指向 baseOffset 字段起始位置；
     *               读取后 position 移动到批次末尾
     * @return 反序列化得到的 {@code RecordBatch}，保证非 null
     */
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
