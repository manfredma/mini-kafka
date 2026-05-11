package com.github.minikafka.common.network;

import org.junit.Test;
import java.nio.ByteBuffer;
import static org.junit.Assert.*;

public class ByteBufferUtilsTest {

    @Test
    public void testWriteAndReadString() {
        ByteBuffer buf = ByteBuffer.allocate(64);
        ByteBufferUtils.writeString(buf, "hello");
        buf.flip();
        assertEquals("hello", ByteBufferUtils.readString(buf));
    }

    @Test
    public void testWriteAndReadNullString() {
        ByteBuffer buf = ByteBuffer.allocate(64);
        ByteBufferUtils.writeString(buf, null);
        buf.flip();
        assertNull(ByteBufferUtils.readString(buf));
    }

    @Test
    public void testWriteAndReadBytes() {
        ByteBuffer buf = ByteBuffer.allocate(64);
        byte[] data = {1, 2, 3, 4};
        ByteBufferUtils.writeBytes(buf, data);
        buf.flip();
        assertArrayEquals(data, ByteBufferUtils.readBytes(buf));
    }

    @Test
    public void testWriteAndReadNullBytes() {
        ByteBuffer buf = ByteBuffer.allocate(64);
        ByteBufferUtils.writeBytes(buf, null);
        buf.flip();
        assertNull(ByteBufferUtils.readBytes(buf));
    }

    @Test
    public void testSizeOfString() {
        assertEquals(2 + 5, ByteBufferUtils.sizeOfString("hello"));
        assertEquals(2, ByteBufferUtils.sizeOfString(null));
    }
}
