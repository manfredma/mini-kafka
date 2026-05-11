package com.github.minikafka.server.log;

import com.github.minikafka.common.record.Record;
import com.github.minikafka.common.record.RecordBatch;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import static org.junit.Assert.*;

public class LogTest {

    private File dir;
    private Log log;

    @Before
    public void setUp() throws IOException {
        dir = createTempDir();
        log = new Log(dir, LogConfig.testConfig(512));
    }

    @After
    public void tearDown() throws IOException {
        log.close();
        deleteDir(dir);
    }

    @Test
    public void testAppendAndNextOffset() throws IOException {
        assertEquals(0L, log.logEndOffset());

        List<Record> records = Arrays.asList(
            new Record(0, System.currentTimeMillis(), null, "msg1".getBytes()),
            new Record(1, System.currentTimeMillis(), null, "msg2".getBytes())
        );
        long nextOffset = log.append(new RecordBatch(0L, records));
        assertEquals(2L, nextOffset);
        assertEquals(2L, log.logEndOffset());
    }

    @Test
    public void testReadBack() throws IOException {
        List<Record> records = Collections.singletonList(
            new Record(0, System.currentTimeMillis(), "k".getBytes(), "hello".getBytes())
        );
        log.append(new RecordBatch(0L, records));

        ByteBuffer result = log.read(0L, 4096);
        assertNotNull(result);
        RecordBatch decoded = RecordBatch.readFrom(result);
        assertArrayEquals("hello".getBytes(), decoded.records().get(0).value());
    }

    @Test
    public void testSegmentRolling() throws IOException {
        int count = 0;
        while (log.numberOfSegments() < 2 && count < 1000) {
            byte[] value = new byte[64];
            List<Record> records = Collections.singletonList(
                new Record(count, System.currentTimeMillis(), null, value)
            );
            log.append(new RecordBatch(count, records));
            count++;
        }
        assertTrue("Should have rolled to a new segment", log.numberOfSegments() >= 2);
    }

    private File createTempDir() throws IOException {
        File f = File.createTempFile("log-test", "");
        f.delete();
        f.mkdirs();
        return f;
    }

    private void deleteDir(File dir) {
        if (dir.isDirectory()) {
            for (File f : dir.listFiles()) f.delete();
        }
        dir.delete();
    }
}
