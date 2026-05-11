package com.github.minikafka.server.log;

import com.github.minikafka.common.record.Record;
import com.github.minikafka.common.record.RecordBatch;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import static org.junit.Assert.*;

/**
 * 验证 Log 重启恢复和并发 append 安全性
 */
public class LogRestartTest {

    private File dir;

    @Before
    public void setUp() throws IOException {
        dir = createTempDir();
    }

    @After
    public void tearDown() {
        deleteDir(dir);
    }

    @Test
    public void testLogEndOffsetRestoredAfterRestart() throws IOException {
        // 写入 5 条消息，关闭
        try (Log log = new Log(dir, LogConfig.defaultConfig())) {
            for (int i = 0; i < 5; i++) {
                log.append(new RecordBatch(i, Collections.singletonList(
                    new Record(i, System.currentTimeMillis(), null, ("msg-" + i).getBytes())
                )));
            }
            assertEquals(5L, log.logEndOffset());
        }

        // 重新打开，logEndOffset 应恢复
        try (Log log = new Log(dir, LogConfig.defaultConfig())) {
            assertEquals("重启后 logEndOffset 应恢复为 5", 5L, log.logEndOffset());

            // 继续追加，offset 应连续
            long next = log.append(new RecordBatch(5L, Collections.singletonList(
                new Record(5L, System.currentTimeMillis(), null, "msg-5".getBytes())
            )));
            assertEquals(6L, next);
            assertEquals(6L, log.logEndOffset());
        }
    }

    @Test
    public void testReadAfterRestartReturnsCorrectData() throws IOException {
        try (Log log = new Log(dir, LogConfig.defaultConfig())) {
            log.append(new RecordBatch(0L, Collections.singletonList(
                new Record(0, System.currentTimeMillis(), "k".getBytes(), "hello-world".getBytes())
            )));
        }

        try (Log log = new Log(dir, LogConfig.defaultConfig())) {
            ByteBuffer buf = log.read(0L, 65536);
            assertNotNull("重启后应能读到数据", buf);
            RecordBatch batch = RecordBatch.readFrom(buf);
            assertArrayEquals("hello-world".getBytes(), batch.records().get(0).value());
        }
    }

    @Test
    public void testConcurrentAppendNoException() throws Exception {
        Log log = new Log(dir, LogConfig.defaultConfig());
        int threads = 4;
        int messagesPerThread = 50;
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);
        AtomicInteger errors = new AtomicInteger(0);
        // 用原子计数器分配全局唯一 baseOffset，避免并发读 logEndOffset 竞态
        java.util.concurrent.atomic.AtomicLong offsetCounter = new java.util.concurrent.atomic.AtomicLong(0);

        ExecutorService pool = Executors.newFixedThreadPool(threads);
        for (int t = 0; t < threads; t++) {
            final int threadId = t;
            pool.submit(() -> {
                try {
                    start.await();
                    for (int i = 0; i < messagesPerThread; i++) {
                        long baseOffset = offsetCounter.getAndIncrement();
                        log.append(new RecordBatch(baseOffset, Collections.singletonList(
                            new Record(baseOffset, System.currentTimeMillis(), null,
                                ("t" + threadId + "-m" + i).getBytes())
                        )));
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    errors.incrementAndGet();
                } finally {
                    done.countDown();
                }
            });
        }

        start.countDown();
        assertTrue("并发 append 应在 10 秒内完成", done.await(10, TimeUnit.SECONDS));
        pool.shutdown();
        log.close();

        assertEquals("并发 append 不应有异常", 0, errors.get());
        // logEndOffset = 最大写入 offset + 1，并发下只要不抛异常且值合理即可
        assertTrue("logEndOffset 应大于 0", log.logEndOffset() > 0);
    }

    private File createTempDir() throws IOException {
        File f = File.createTempFile("log-restart-test", "");
        f.delete();
        f.mkdirs();
        return f;
    }

    private void deleteDir(File dir) {
        if (dir == null) return;
        if (dir.isDirectory()) {
            for (File f : dir.listFiles()) f.delete();
        }
        dir.delete();
    }
}
