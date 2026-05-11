package com.github.minikafka.server.api;

import com.github.minikafka.common.network.ByteBufferUtils;
import com.github.minikafka.common.protocol.*;
import com.github.minikafka.common.record.RecordBatch;
import com.github.minikafka.server.controller.KafkaController;
import com.github.minikafka.server.coordinator.GroupCoordinator;
import com.github.minikafka.server.log.Log;
import com.github.minikafka.server.log.LogManager;
import com.github.minikafka.server.network.RequestChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * 请求分发器，处理所有 ApiKeys 对应的逻辑
 * 对齐 Kafka kafka.server.KafkaApis
 */
public final class KafkaApis {

    private static final Logger log = LoggerFactory.getLogger(KafkaApis.class);

    private final LogManager logManager;
    private final KafkaController controller;
    private final GroupCoordinator groupCoordinator;

    public KafkaApis(LogManager logManager, KafkaController controller,
                     GroupCoordinator groupCoordinator) {
        this.logManager = logManager;
        this.controller = controller;
        this.groupCoordinator = groupCoordinator;
    }

    public ByteBuffer handle(RequestChannel.Request request) {
        ByteBuffer buf = request.buffer.duplicate();
        RequestHeader header = RequestHeader.parse(buf);
        ApiKeys apiKey = ApiKeys.forId(header.apiKey());
        log.debug("Handling {} correlationId={}", apiKey.name, header.correlationId());

        ByteBuffer responseBody;
        try {
            switch (apiKey) {
                case PRODUCE:       responseBody = handleProduce(buf); break;
                case FETCH:         responseBody = handleFetch(buf); break;
                case METADATA:      responseBody = handleMetadata(buf); break;
                case CREATE_TOPICS: responseBody = handleCreateTopics(buf); break;
                case JOIN_GROUP:    responseBody = handleJoinGroup(buf); break;
                case SYNC_GROUP:    responseBody = handleSyncGroup(buf); break;
                case HEARTBEAT:     responseBody = handleHeartbeat(buf); break;
                case LEAVE_GROUP:   responseBody = handleLeaveGroup(buf); break;
                case OFFSET_COMMIT: responseBody = handleOffsetCommit(buf); break;
                case OFFSET_FETCH:  responseBody = handleOffsetFetch(buf); break;
                default:
                    log.warn("Unsupported api key: {}", apiKey);
                    responseBody = buildErrorResponse(Errors.UNKNOWN_SERVER_ERROR);
            }
        } catch (Exception e) {
            log.error("Error handling {}", apiKey, e);
            responseBody = buildErrorResponse(Errors.UNKNOWN_SERVER_ERROR);
        }

        return buildResponse(header.correlationId(), responseBody);
    }

    // PRODUCE: topicCount(4) [topic(string) partCount(4) [partition(4) batchSize(4) batch(bytes)]]
    private ByteBuffer handleProduce(ByteBuffer buf) throws IOException {
        int topicCount = buf.getInt();
        Map<String, Map<Integer, Long>> results = new LinkedHashMap<>();

        for (int t = 0; t < topicCount; t++) {
            String topic = ByteBufferUtils.readString(buf);
            int partCount = buf.getInt();
            Map<Integer, Long> partResults = new LinkedHashMap<>();
            for (int p = 0; p < partCount; p++) {
                int partition = buf.getInt();
                int batchSize = buf.getInt();
                ByteBuffer batchBuf = buf.slice();
                batchBuf.limit(batchSize);
                buf.position(buf.position() + batchSize);

                RecordBatch batch = RecordBatch.readFrom(batchBuf);
                Log partLog = logManager.getOrCreateLog(topic, partition);
                partLog.append(batch);
                partResults.put(partition, batch.baseOffset());
            }
            results.put(topic, partResults);
        }

        int size = 4;
        for (Map.Entry<String, Map<Integer, Long>> e : results.entrySet()) {
            size += ByteBufferUtils.sizeOfString(e.getKey()) + 4;
            size += e.getValue().size() * (4 + 2 + 8);
        }
        ByteBuffer resp = ByteBuffer.allocate(size);
        resp.putInt(results.size());
        for (Map.Entry<String, Map<Integer, Long>> e : results.entrySet()) {
            ByteBufferUtils.writeString(resp, e.getKey());
            resp.putInt(e.getValue().size());
            for (Map.Entry<Integer, Long> pe : e.getValue().entrySet()) {
                resp.putInt(pe.getKey());
                resp.putShort(Errors.NONE.code);
                resp.putLong(pe.getValue());
            }
        }
        resp.flip();
        return resp;
    }

    // FETCH: replicaId(4) maxWaitMs(4) minBytes(4) topicCount(4) [topic(string) partCount(4) [partition(4) fetchOffset(8) maxBytes(4)]]
    private ByteBuffer handleFetch(ByteBuffer buf) throws IOException {
        buf.getInt(); buf.getInt(); buf.getInt(); // replicaId, maxWaitMs, minBytes
        int topicCount = buf.getInt();

        ByteBuffer respBody = ByteBuffer.allocate(1024 * 1024);
        respBody.putInt(topicCount);

        for (int t = 0; t < topicCount; t++) {
            String topic = ByteBufferUtils.readString(buf);
            int partCount = buf.getInt();
            ByteBufferUtils.writeString(respBody, topic);
            respBody.putInt(partCount);

            for (int p = 0; p < partCount; p++) {
                int partition = buf.getInt();
                long fetchOffset = buf.getLong();
                int maxBytes = buf.getInt();

                respBody.putInt(partition);
                Log partLog = logManager.getLog(topic, partition);
                if (partLog == null) {
                    respBody.putShort(Errors.UNKNOWN_TOPIC_OR_PARTITION.code);
                    respBody.putLong(0L);
                    respBody.putInt(0);
                } else {
                    ByteBuffer records = partLog.read(fetchOffset, maxBytes);
                    respBody.putShort(Errors.NONE.code);
                    respBody.putLong(partLog.logEndOffset());
                    if (records != null && records.hasRemaining()) {
                        respBody.putInt(records.remaining());
                        respBody.put(records);
                    } else {
                        respBody.putInt(0);
                    }
                }
            }
        }
        respBody.flip();
        return respBody;
    }

    // METADATA: topicCount(4) [topic(string)]
    private ByteBuffer handleMetadata(ByteBuffer buf) {
        int topicCount = buf.getInt();
        List<String> requestedTopics = new ArrayList<>();
        for (int i = 0; i < topicCount; i++) requestedTopics.add(ByteBufferUtils.readString(buf));
        if (requestedTopics.isEmpty()) requestedTopics.addAll(controller.context().allTopics().keySet());

        ByteBuffer resp = ByteBuffer.allocate(4096);
        resp.putInt(1);
        resp.putInt(0);
        ByteBufferUtils.writeString(resp, "localhost");
        resp.putInt(9092);

        resp.putInt(requestedTopics.size());
        for (String topic : requestedTopics) {
            ByteBufferUtils.writeString(resp, topic);
            if (!controller.context().topicExists(topic)) {
                resp.putShort(Errors.UNKNOWN_TOPIC_OR_PARTITION.code);
                resp.putInt(0);
            } else {
                resp.putShort(Errors.NONE.code);
                int partitions = controller.context().partitionCount(topic);
                resp.putInt(partitions);
                for (int i = 0; i < partitions; i++) {
                    resp.putInt(i);
                    resp.putInt(0);
                    resp.putShort(Errors.NONE.code);
                }
            }
        }
        resp.flip();
        return resp;
    }

    // CREATE_TOPICS: topicCount(4) [topic(string) partitions(4)]
    private ByteBuffer handleCreateTopics(ByteBuffer buf) throws IOException {
        int topicCount = buf.getInt();
        Map<String, Short> results = new LinkedHashMap<>();
        for (int i = 0; i < topicCount; i++) {
            String topic = ByteBufferUtils.readString(buf);
            int partitions = buf.getInt();
            try {
                controller.createTopic(topic, partitions);
                results.put(topic, Errors.NONE.code);
            } catch (Exception e) {
                results.put(topic, Errors.UNKNOWN_SERVER_ERROR.code);
            }
        }
        int size = 4;
        for (String t : results.keySet()) size += ByteBufferUtils.sizeOfString(t) + 2;
        ByteBuffer resp = ByteBuffer.allocate(size);
        resp.putInt(results.size());
        for (Map.Entry<String, Short> e : results.entrySet()) {
            ByteBufferUtils.writeString(resp, e.getKey());
            resp.putShort(e.getValue());
        }
        resp.flip();
        return resp;
    }

    // JOIN_GROUP: groupId(string) sessionTimeoutMs(4) memberId(string) protocolType(string)
    private ByteBuffer handleJoinGroup(ByteBuffer buf) {
        String groupId = ByteBufferUtils.readString(buf);
        int sessionTimeoutMs = buf.getInt();
        String memberId = ByteBufferUtils.readString(buf);
        ByteBufferUtils.readString(buf); // protocolType

        GroupCoordinator.JoinResult result = groupCoordinator.handleJoinGroup(
            groupId, memberId, "client", sessionTimeoutMs, "range"
        );

        int size = 2 + 4
            + ByteBufferUtils.sizeOfString("range")
            + ByteBufferUtils.sizeOfString(result.leaderId)
            + ByteBufferUtils.sizeOfString(result.memberId) + 4;
        for (String m : result.members) size += ByteBufferUtils.sizeOfString(m);

        ByteBuffer resp = ByteBuffer.allocate(size);
        resp.putShort(Errors.NONE.code);
        resp.putInt(result.generationId);
        ByteBufferUtils.writeString(resp, "range");
        ByteBufferUtils.writeString(resp, result.leaderId);
        ByteBufferUtils.writeString(resp, result.memberId);
        resp.putInt(result.members.size());
        for (String m : result.members) ByteBufferUtils.writeString(resp, m);
        resp.flip();
        return resp;
    }

    // SYNC_GROUP: groupId(string) generationId(4) memberId(string) count(4) [memberId(string) assignment(bytes)]
    private ByteBuffer handleSyncGroup(ByteBuffer buf) {
        String groupId = ByteBufferUtils.readString(buf);
        int generationId = buf.getInt();
        String memberId = ByteBufferUtils.readString(buf);
        int count = buf.getInt();
        Map<String, byte[]> assignment = new HashMap<>();
        for (int i = 0; i < count; i++) {
            String mid = ByteBufferUtils.readString(buf);
            byte[] assign = ByteBufferUtils.readBytes(buf);
            assignment.put(mid, assign);
        }

        GroupCoordinator.SyncResult result = groupCoordinator.handleSyncGroup(
            groupId, generationId, memberId, assignment
        );

        ByteBuffer resp = ByteBuffer.allocate(2 + ByteBufferUtils.sizeOfBytes(result.assignment));
        resp.putShort(Errors.NONE.code);
        ByteBufferUtils.writeBytes(resp, result.assignment);
        resp.flip();
        return resp;
    }

    // HEARTBEAT: groupId(string) generationId(4) memberId(string)
    private ByteBuffer handleHeartbeat(ByteBuffer buf) {
        String groupId = ByteBufferUtils.readString(buf);
        int generationId = buf.getInt();
        String memberId = ByteBufferUtils.readString(buf);
        boolean ok = groupCoordinator.handleHeartbeat(groupId, generationId, memberId);
        ByteBuffer resp = ByteBuffer.allocate(2);
        resp.putShort(ok ? Errors.NONE.code : Errors.REBALANCE_IN_PROGRESS.code);
        resp.flip();
        return resp;
    }

    // LEAVE_GROUP: groupId(string) memberId(string)
    private ByteBuffer handleLeaveGroup(ByteBuffer buf) {
        String groupId = ByteBufferUtils.readString(buf);
        String memberId = ByteBufferUtils.readString(buf);
        groupCoordinator.handleLeaveGroup(groupId, memberId);
        ByteBuffer resp = ByteBuffer.allocate(2);
        resp.putShort(Errors.NONE.code);
        resp.flip();
        return resp;
    }

    // OFFSET_COMMIT: groupId(string) generationId(4) memberId(string) topicCount(4) [topic(string) partCount(4) [partition(4) offset(8)]]
    private ByteBuffer handleOffsetCommit(ByteBuffer buf) {
        String groupId = ByteBufferUtils.readString(buf);
        buf.getInt();
        ByteBufferUtils.readString(buf);
        int topicCount = buf.getInt();
        for (int t = 0; t < topicCount; t++) {
            String topic = ByteBufferUtils.readString(buf);
            int partCount = buf.getInt();
            for (int p = 0; p < partCount; p++) {
                int partition = buf.getInt();
                long offset = buf.getLong();
                groupCoordinator.commitOffset(groupId, topic, partition, offset);
            }
        }
        ByteBuffer resp = ByteBuffer.allocate(4);
        resp.putInt(0);
        resp.flip();
        return resp;
    }

    // OFFSET_FETCH: groupId(string) topicCount(4) [topic(string) partCount(4) [partition(4)]]
    private ByteBuffer handleOffsetFetch(ByteBuffer buf) {
        String groupId = ByteBufferUtils.readString(buf);
        int topicCount = buf.getInt();
        ByteBuffer resp = ByteBuffer.allocate(65536);
        resp.putInt(topicCount);
        for (int t = 0; t < topicCount; t++) {
            String topic = ByteBufferUtils.readString(buf);
            int partCount = buf.getInt();
            ByteBufferUtils.writeString(resp, topic);
            resp.putInt(partCount);
            for (int p = 0; p < partCount; p++) {
                int partition = buf.getInt();
                long offset = groupCoordinator.fetchOffset(groupId, topic, partition);
                resp.putInt(partition);
                resp.putLong(offset);
                resp.putShort(Errors.NONE.code);
            }
        }
        resp.flip();
        return resp;
    }

    private ByteBuffer buildErrorResponse(Errors error) {
        ByteBuffer buf = ByteBuffer.allocate(2);
        buf.putShort(error.code);
        buf.flip();
        return buf;
    }

    private ByteBuffer buildResponse(int correlationId, ByteBuffer body) {
        int totalSize = ResponseHeader.SIZE + body.remaining();
        ByteBuffer response = ByteBuffer.allocate(4 + totalSize);
        response.putInt(totalSize);
        new ResponseHeader(correlationId).writeTo(response);
        response.put(body);
        response.flip();
        return response;
    }
}
