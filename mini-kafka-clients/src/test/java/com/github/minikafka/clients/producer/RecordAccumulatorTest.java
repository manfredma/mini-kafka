package com.github.minikafka.clients.producer;

import org.junit.Test;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import static org.junit.Assert.*;

public class RecordAccumulatorTest {

    @Test
    public void testAppendCreatesBatch() throws Exception {
        RecordAccumulator acc = new RecordAccumulator(16384, 0L);
        CompletableFuture<RecordMetadata> future = acc.append(
            "test", 0, "key".getBytes(), "value".getBytes(), System.currentTimeMillis()
        );
        assertNotNull(future);
        List<ProducerBatch> ready = acc.drain("test", 0);
        assertFalse(ready.isEmpty());
    }

    @Test
    public void testMultipleRecordsSameBatch() throws Exception {
        RecordAccumulator acc = new RecordAccumulator(16384, 100L);
        acc.append("t", 0, null, "v1".getBytes(), System.currentTimeMillis());
        acc.append("t", 0, null, "v2".getBytes(), System.currentTimeMillis());
        acc.append("t", 0, null, "v3".getBytes(), System.currentTimeMillis());

        List<ProducerBatch> ready = acc.drain("t", 0);
        assertTrue(ready.isEmpty());
    }

    @Test
    public void testBatchReadyWhenFull() throws Exception {
        RecordAccumulator acc = new RecordAccumulator(1, 10000L);
        acc.append("t", 0, null, "value".getBytes(), System.currentTimeMillis());
        List<ProducerBatch> ready = acc.drain("t", 0);
        assertFalse(ready.isEmpty());
    }
}
