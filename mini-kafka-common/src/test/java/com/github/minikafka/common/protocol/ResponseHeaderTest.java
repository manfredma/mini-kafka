package com.github.minikafka.common.protocol;

import org.junit.Test;
import java.nio.ByteBuffer;
import static org.junit.Assert.*;

public class ResponseHeaderTest {

    @Test
    public void testSerializeDeserialize() {
        ResponseHeader header = new ResponseHeader(42);
        ByteBuffer buf = ByteBuffer.allocate(ResponseHeader.SIZE);
        header.writeTo(buf);
        buf.flip();
        ResponseHeader decoded = ResponseHeader.parse(buf);
        assertEquals(42, decoded.correlationId());
    }

    @Test
    public void testSize() {
        assertEquals(4, ResponseHeader.SIZE);
    }
}
