package com.github.minikafka.server.log;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import java.io.File;
import java.io.IOException;
import static org.junit.Assert.*;

public class OffsetIndexTest {

    private File indexFile;
    private OffsetIndex index;

    @Before
    public void setUp() throws IOException {
        indexFile = File.createTempFile("test", ".index");
        indexFile.deleteOnExit();
        index = new OffsetIndex(indexFile, 0L, 1024 * 1024);
    }

    @After
    public void tearDown() throws IOException {
        index.close();
    }

    @Test
    public void testAppendAndLookup() throws IOException {
        index.append(0, 0);
        index.append(10, 500);
        index.append(20, 1200);

        assertEquals(500, index.lookup(10));
        assertEquals(500, index.lookup(15));
        assertEquals(0, index.lookup(5));
    }

    @Test
    public void testEntries() throws IOException {
        assertEquals(0, index.entries());
        index.append(0, 0);
        index.append(10, 500);
        assertEquals(2, index.entries());
    }

    @Test
    public void testEmpty() throws IOException {
        assertEquals(0, index.lookup(100));
    }
}
