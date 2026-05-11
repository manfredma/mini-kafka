package com.github.minikafka.common.protocol;

import org.junit.Test;
import java.nio.ByteBuffer;
import static org.junit.Assert.*;

public class RequestHeaderTest {

    @Test
    public void testSerializeDeserialize() {
        RequestHeader header = new RequestHeader(ApiKeys.PRODUCE, (short) 0, 42, "test-client");
        ByteBuffer buf = ByteBuffer.allocate(header.sizeOf());
        header.writeTo(buf);
        buf.flip();
        RequestHeader decoded = RequestHeader.parse(buf);
        assertEquals(ApiKeys.PRODUCE.id, decoded.apiKey());
        assertEquals(0, decoded.apiVersion());
        assertEquals(42, decoded.correlationId());
        assertEquals("test-client", decoded.clientId());
    }

    @Test
    public void testSizeOf() {
        RequestHeader header = new RequestHeader(ApiKeys.FETCH, (short) 1, 1, "client");
        // 2(apiKey) + 2(apiVersion) + 4(correlationId) + 2(clientIdLen) + 6(clientId)
        assertEquals(16, header.sizeOf());
    }
}
