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

public class LogSegmentTest {

    private File dir;
    private LogSegment segment;

    @Before
    public void setUp() throws IOException {
        dir = createTempDir();
        segment = new LogSegment(dir, 0L, LogConfig.testConfig(1024 * 1024));
    }

    @After
    public void tearDown() throws IOException {
        segment.close();
        deleteDir(dir);
    }

    @Test
    public void testAppendAndRead() throws IOException {
        List<Record> records = Arrays.asList(
            new Record(0, System.currentTimeMillis(), "k1".getBytes(), "v1".getBytes())
        );
        RecordBatch batch = new RecordBatch(0L, records);
        segment.append(batch);

        ByteBuffer result = segment.read(0L, 4096);
        assertNotNull(result);
        RecordBatch decoded = RecordBatch.readFrom(result);
        assertEquals(0L, decoded.baseOffset());
        assertArrayEquals("v1".getBytes(), decoded.records().get(0).value());
    }

    @Test
    public void testSize() throws IOException {
        assertEquals(0, segment.size());
        List<Record> records = Collections.singletonList(
            new Record(0, System.currentTimeMillis(), null, "hello".getBytes())
        );
        segment.append(new RecordBatch(0L, records));
        assertTrue(segment.size() > 0);
    }

    @Test
    public void testBaseOffset() {
        assertEquals(0L, segment.baseOffset());
    }

    private File createTempDir() throws IOException {
        File f = File.createTempFile("log-segment-test", "");
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
