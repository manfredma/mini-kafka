package com.github.minikafka.server.log;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import java.io.File;
import java.io.IOException;
import static org.junit.Assert.*;

/**
 * 验证 OffsetIndex 重启后 entries 正确恢复
 */
public class OffsetIndexRestartTest {

    private File indexFile;

    @Before
    public void setUp() throws IOException {
        indexFile = File.createTempFile("restart", ".index");
        indexFile.deleteOnExit();
    }

    @Test
    public void testEntriesRestoredAfterReopen() throws IOException {
        // 第一次打开，写入 3 条索引
        try (OffsetIndex index = new OffsetIndex(indexFile, 0L, 1024 * 1024)) {
            index.append(0, 0);
            index.append(100, 5000);
            index.append(200, 10000);
            assertEquals(3, index.entries());
        }

        // 重新打开，entries 应恢复为 3
        try (OffsetIndex index = new OffsetIndex(indexFile, 0L, 1024 * 1024)) {
            assertEquals("重启后 entries 应恢复为 3", 3, index.entries());
            // 查找应仍然正确
            assertEquals(5000, index.lookup(100));
            assertEquals(5000, index.lookup(150));
            assertEquals(10000, index.lookup(200));
        }
    }

    @Test
    public void testAppendAfterReopenContinuesFromCorrectPosition() throws IOException {
        try (OffsetIndex index = new OffsetIndex(indexFile, 0L, 1024 * 1024)) {
            index.append(0, 0);
            index.append(10, 512);
        }

        try (OffsetIndex index = new OffsetIndex(indexFile, 0L, 1024 * 1024)) {
            assertEquals(2, index.entries());
            // 追加新条目不应覆盖已有条目
            index.append(20, 1024);
            assertEquals(3, index.entries());
            assertEquals(512, index.lookup(10));
            assertEquals(1024, index.lookup(20));
        }
    }

    @Test
    public void testNewFileStartsWithZeroEntries() throws IOException {
        File newFile = File.createTempFile("new-index", ".index");
        newFile.deleteOnExit();
        try (OffsetIndex index = new OffsetIndex(newFile, 0L, 1024 * 1024)) {
            assertEquals(0, index.entries());
        }
    }

    @After
    public void tearDown() {
        if (indexFile != null) indexFile.delete();
    }
}
