package com.github.minikafka.common.record;

import org.junit.Test;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import static org.junit.Assert.*;

public class RecordBatchTest {

    @Test
    public void testWriteAndReadSingleRecord() {
        List<Record> records = Arrays.asList(
            new Record(0, 1000L, "key1".getBytes(), "value1".getBytes())
        );
        RecordBatch batch = new RecordBatch(0L, records);

        ByteBuffer buf = ByteBuffer.allocate(batch.sizeInBytes());
        batch.writeTo(buf);
        buf.flip();

        RecordBatch decoded = RecordBatch.readFrom(buf);
        assertEquals(0L, decoded.baseOffset());
        assertEquals(1, decoded.records().size());

        Record r = decoded.records().get(0);
        assertArrayEquals("key1".getBytes(), r.key());
        assertArrayEquals("value1".getBytes(), r.value());
        assertEquals(1000L, r.timestamp());
    }

    @Test
    public void testWriteAndReadMultipleRecords() {
        List<Record> records = Arrays.asList(
            new Record(0, 1000L, null, "msg1".getBytes()),
            new Record(1, 1001L, null, "msg2".getBytes()),
            new Record(2, 1002L, null, "msg3".getBytes())
        );
        RecordBatch batch = new RecordBatch(100L, records);

        ByteBuffer buf = ByteBuffer.allocate(batch.sizeInBytes());
        batch.writeTo(buf);
        buf.flip();

        RecordBatch decoded = RecordBatch.readFrom(buf);
        assertEquals(100L, decoded.baseOffset());
        assertEquals(3, decoded.records().size());
        assertArrayEquals("msg3".getBytes(), decoded.records().get(2).value());
    }

    @Test
    public void testLastOffset() {
        List<Record> records = Arrays.asList(
            new Record(0, 1000L, null, "a".getBytes()),
            new Record(1, 1001L, null, "b".getBytes())
        );
        RecordBatch batch = new RecordBatch(5L, records);
        assertEquals(6L, batch.lastOffset());
    }
}
