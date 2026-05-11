package com.github.minikafka.server.integration;

import com.github.minikafka.server.KafkaConfig;
import com.github.minikafka.server.KafkaServer;
import com.github.minikafka.common.network.ByteBufferUtils;
import com.github.minikafka.common.protocol.*;
import com.github.minikafka.common.record.*;
import org.junit.*;
import java.io.*;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.*;
import static org.junit.Assert.*;

/**
 * 启动内嵌 Broker，通过 TCP 发送原始协议请求，验证 Produce/Fetch/Metadata/CreateTopics
 */
public class BrokerIntegrationTest {

    private static KafkaServer server;
    private static final int PORT = 19092;

    @BeforeClass
    public static void startBroker() throws Exception {
        Properties props = new Properties();
        props.setProperty("port", String.valueOf(PORT));
        props.setProperty("log.dirs", System.getProperty("java.io.tmpdir") + "/mini-kafka-it-" + System.currentTimeMillis());
        server = new KafkaServer(new KafkaConfig(props));
        server.startup();
        Thread.sleep(300);
    }

    @AfterClass
    public static void stopBroker() throws Exception {
        if (server != null) server.shutdown();
    }

    @Test
    public void testCreateTopicAndProduceAndFetch() throws Exception {
        sendCreateTopics("integration-test", 1);
        Thread.sleep(100);

        List<Record> records = Arrays.asList(
            new Record(0, System.currentTimeMillis(), "key1".getBytes(), "value1".getBytes())
        );
        RecordBatch batch = new RecordBatch(0L, records);
        long baseOffset = sendProduce("integration-test", 0, batch);
        assertEquals(0L, baseOffset);

        byte[] fetched = sendFetch("integration-test", 0, 0L, 1024 * 1024);
        assertNotNull(fetched);
        assertTrue(fetched.length > 0);
        RecordBatch fetchedBatch = RecordBatch.readFrom(ByteBuffer.wrap(fetched));
        assertEquals(1, fetchedBatch.records().size());
        assertArrayEquals("value1".getBytes(), fetchedBatch.records().get(0).value());
    }

    @Test
    public void testMetadata() throws Exception {
        sendCreateTopics("meta-test", 2);
        Thread.sleep(100);
        int partitions = sendMetadataGetPartitions("meta-test");
        assertEquals(2, partitions);
    }

    private void sendCreateTopics(String topic, int partitions) throws Exception {
        int bodySize = 4 + ByteBufferUtils.sizeOfString(topic) + 4;
        ByteBuffer body = ByteBuffer.allocate(bodySize);
        body.putInt(1);
        ByteBufferUtils.writeString(body, topic);
        body.putInt(partitions);
        body.flip();
        sendRequest(ApiKeys.CREATE_TOPICS, 1, body);
    }

    private long sendProduce(String topic, int partition, RecordBatch batch) throws Exception {
        int batchSize = batch.sizeInBytes();
        ByteBuffer batchBuf = ByteBuffer.allocate(batchSize);
        batch.writeTo(batchBuf);
        batchBuf.flip();

        int bodySize = 4 + ByteBufferUtils.sizeOfString(topic) + 4 + 4 + 4 + batchSize;
        ByteBuffer body = ByteBuffer.allocate(bodySize);
        body.putInt(1);
        ByteBufferUtils.writeString(body, topic);
        body.putInt(1);
        body.putInt(partition);
        body.putInt(batchSize);
        body.put(batchBuf);
        body.flip();

        ByteBuffer response = sendRequest(ApiKeys.PRODUCE, 2, body);
        response.getInt(); // topicCount
        ByteBufferUtils.readString(response); // topic
        response.getInt(); // partCount
        response.getInt(); // partition
        response.getShort(); // errorCode
        return response.getLong();
    }

    private byte[] sendFetch(String topic, int partition, long offset, int maxBytes) throws Exception {
        int bodySize = 4 + 4 + 4 + 4 + ByteBufferUtils.sizeOfString(topic) + 4 + 4 + 8 + 4;
        ByteBuffer body = ByteBuffer.allocate(bodySize);
        body.putInt(-1); body.putInt(100); body.putInt(0); body.putInt(1);
        ByteBufferUtils.writeString(body, topic);
        body.putInt(1);
        body.putInt(partition);
        body.putLong(offset);
        body.putInt(maxBytes);
        body.flip();

        ByteBuffer response = sendRequest(ApiKeys.FETCH, 3, body);
        response.getInt(); // topicCount
        ByteBufferUtils.readString(response); // topic
        response.getInt(); // partCount
        response.getInt(); // partition
        short errorCode = response.getShort();
        assertEquals(0, errorCode);
        response.getLong(); // highWatermark
        int recordSize = response.getInt();
        if (recordSize == 0) return null;
        byte[] records = new byte[recordSize];
        response.get(records);
        return records;
    }

    private int sendMetadataGetPartitions(String topic) throws Exception {
        int bodySize = 4 + ByteBufferUtils.sizeOfString(topic);
        ByteBuffer body = ByteBuffer.allocate(bodySize);
        body.putInt(1);
        ByteBufferUtils.writeString(body, topic);
        body.flip();

        ByteBuffer response = sendRequest(ApiKeys.METADATA, 4, body);
        int brokerCount = response.getInt();
        for (int i = 0; i < brokerCount; i++) {
            response.getInt(); ByteBufferUtils.readString(response); response.getInt();
        }
        response.getInt(); // topicCount
        ByteBufferUtils.readString(response); // topic
        response.getShort(); // errorCode
        return response.getInt();
    }

    private ByteBuffer sendRequest(ApiKeys apiKey, int correlationId, ByteBuffer body) throws Exception {
        RequestHeader header = new RequestHeader(apiKey, (short) 0, correlationId, "test");
        int totalSize = header.sizeOf() + body.remaining();
        ByteBuffer request = ByteBuffer.allocate(4 + totalSize);
        request.putInt(totalSize);
        header.writeTo(request);
        request.put(body);
        request.flip();

        try (Socket socket = new Socket("localhost", PORT)) {
            socket.setSoTimeout(5000);
            OutputStream out = socket.getOutputStream();
            InputStream in = socket.getInputStream();
            out.write(request.array(), request.position(), request.remaining());
            out.flush();

            byte[] sizeBuf = new byte[4];
            readFully(in, sizeBuf);
            int responseSize = ByteBuffer.wrap(sizeBuf).getInt();
            byte[] respBuf = new byte[responseSize];
            readFully(in, respBuf);
            ByteBuffer response = ByteBuffer.wrap(respBuf);
            response.getInt(); // correlationId
            return response;
        }
    }

    private void readFully(InputStream in, byte[] buf) throws IOException {
        int offset = 0;
        while (offset < buf.length) {
            int read = in.read(buf, offset, buf.length - offset);
            if (read < 0) throw new EOFException();
            offset += read;
        }
    }
}
