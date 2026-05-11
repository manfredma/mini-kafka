package com.github.minikafka.clients.consumer;

import com.github.minikafka.clients.network.Metadata;
import com.github.minikafka.clients.network.NetworkClient;
import com.github.minikafka.common.network.ByteBufferUtils;
import com.github.minikafka.common.protocol.ApiKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * 客户端侧 Rebalance 协调器
 * 对齐 Kafka org.apache.kafka.clients.consumer.internals.ConsumerCoordinator
 */
public final class ConsumerCoordinator {

    private static final Logger log = LoggerFactory.getLogger(ConsumerCoordinator.class);

    private final String groupId;
    private final ConsumerConfig config;
    private final NetworkClient networkClient;
    private final Metadata metadata;
    private final SubscriptionState subscriptionState;

    private String memberId = "";
    private int generationId = -1;
    private boolean needsJoin = true;
    private long lastHeartbeatMs = 0;

    public ConsumerCoordinator(String groupId, ConsumerConfig config,
                                NetworkClient networkClient, Metadata metadata,
                                SubscriptionState subscriptionState) {
        this.groupId = groupId;
        this.config = config;
        this.networkClient = networkClient;
        this.metadata = metadata;
        this.subscriptionState = subscriptionState;
    }

    public void ensureActiveGroup() throws IOException {
        if (needsJoin) {
            joinGroup();
        } else {
            maybeHeartbeat();
        }
    }

    private void joinGroup() throws IOException {
        log.info("Joining group {}, memberId={}", groupId, memberId);

        ByteBuffer body = ByteBuffer.allocate(
            ByteBufferUtils.sizeOfString(groupId) + 4
            + ByteBufferUtils.sizeOfString(memberId)
            + ByteBufferUtils.sizeOfString("consumer")
        );
        ByteBufferUtils.writeString(body, groupId);
        body.putInt(config.sessionTimeoutMs);
        ByteBufferUtils.writeString(body, memberId);
        ByteBufferUtils.writeString(body, "consumer");
        body.flip();

        ByteBuffer resp = networkClient.sendSync(ApiKeys.JOIN_GROUP, body);
        resp.getShort(); // errorCode
        generationId = resp.getInt();
        ByteBufferUtils.readString(resp); // protocol
        String leaderId = ByteBufferUtils.readString(resp);
        memberId = ByteBufferUtils.readString(resp);
        int memberCount = resp.getInt();
        List<String> members = new ArrayList<>();
        for (int i = 0; i < memberCount; i++) members.add(ByteBufferUtils.readString(resp));

        log.info("Joined group {}, memberId={}, generation={}, leader={}",
            groupId, memberId, generationId, leaderId);

        syncGroup(leaderId, members);
        needsJoin = false;
        lastHeartbeatMs = System.currentTimeMillis();
    }

    private void syncGroup(String leaderId, List<String> members) throws IOException {
        Map<String, byte[]> assignment = new HashMap<>();
        if (memberId.equals(leaderId)) {
            assignment = rangeAssign(members);
        }

        int bodySize = ByteBufferUtils.sizeOfString(groupId) + 4
            + ByteBufferUtils.sizeOfString(memberId) + 4;
        for (Map.Entry<String, byte[]> e : assignment.entrySet()) {
            bodySize += ByteBufferUtils.sizeOfString(e.getKey()) + ByteBufferUtils.sizeOfBytes(e.getValue());
        }
        ByteBuffer body = ByteBuffer.allocate(bodySize);
        ByteBufferUtils.writeString(body, groupId);
        body.putInt(generationId);
        ByteBufferUtils.writeString(body, memberId);
        body.putInt(assignment.size());
        for (Map.Entry<String, byte[]> e : assignment.entrySet()) {
            ByteBufferUtils.writeString(body, e.getKey());
            ByteBufferUtils.writeBytes(body, e.getValue());
        }
        body.flip();

        ByteBuffer resp = networkClient.sendSync(ApiKeys.SYNC_GROUP, body);
        resp.getShort(); // errorCode
        byte[] myAssignment = ByteBufferUtils.readBytes(resp);

        if (myAssignment != null && myAssignment.length > 0) {
            parseAssignment(myAssignment);
        } else {
            assignAllSubscribed();
        }
        log.info("SyncGroup complete, assigned: {}", subscriptionState.assignedPartitions());
    }

    /**
     * RangeAssignor：将每个 topic 的 partition 均匀分配给 members。
     * 对齐 Kafka RangeAssignor：partitions < members 时，后面的 member 分配为空。
     */
    private Map<String, byte[]> rangeAssign(List<String> members) {
        Map<String, byte[]> result = new HashMap<>();
        for (String m : members) result.put(m, new byte[0]);

        for (String topic : subscriptionState.subscribedTopics()) {
            int partitions = metadata.partitionCount(topic);
            if (partitions == 0) continue;
            int membersCount = members.size();
            // RangeAssignor: numPartitionsPerConsumer = partitions / members
            // 前 remainder 个 consumer 多拿一个 partition
            int numPartitionsPerConsumer = partitions / membersCount;
            int remainder = partitions % membersCount;
            int start = 0;
            for (int i = 0; i < membersCount; i++) {
                int extra = (i < remainder) ? 1 : 0;
                int end = start + numPartitionsPerConsumer + extra;
                if (start < end) {
                    byte[] existing = result.get(members.get(i));
                    result.put(members.get(i), encodeAssignment(existing, topic, start, end));
                }
                // start < end 为 false 时（partitions < members），该 member 分配空列表，不写 encodeAssignment
                start = end;
            }
        }
        return result;
    }

    private byte[] encodeAssignment(byte[] existing, String topic, int start, int end) {
        int count = end - start;
        int existingLen = (existing == null) ? 0 : existing.length;
        int size = existingLen + ByteBufferUtils.sizeOfString(topic) + 4 + count * 4;
        ByteBuffer buf = ByteBuffer.allocate(size);
        if (existing != null && existing.length > 0) buf.put(existing);
        ByteBufferUtils.writeString(buf, topic);
        buf.putInt(count);
        for (int i = start; i < end; i++) buf.putInt(i);
        // 用 Arrays.copyOf 只取已写入部分，避免 buf.array() 含末尾未写入的 0 字节
        return java.util.Arrays.copyOf(buf.array(), buf.position());
    }

    private void parseAssignment(byte[] assignment) {
        ByteBuffer buf = ByteBuffer.wrap(assignment);
        Map<String, List<Integer>> topicPartitions = new HashMap<>();
        while (buf.remaining() >= 2) {
            String topic = ByteBufferUtils.readString(buf);
            if (topic == null || buf.remaining() < 4) break;
            int count = buf.getInt();
            List<Integer> partitions = new ArrayList<>();
            for (int i = 0; i < count && buf.remaining() >= 4; i++) partitions.add(buf.getInt());
            if (!partitions.isEmpty()) topicPartitions.put(topic, partitions);
        }
        if (!topicPartitions.isEmpty()) {
            subscriptionState.assignPartitions(topicPartitions);
        } else {
            assignAllSubscribed();
        }
    }

    private void assignAllSubscribed() {
        Map<String, List<Integer>> topicPartitions = new HashMap<>();
        for (String topic : subscriptionState.subscribedTopics()) {
            int partitions = Math.max(1, metadata.partitionCount(topic));
            List<Integer> parts = new ArrayList<>();
            for (int i = 0; i < partitions; i++) parts.add(i);
            topicPartitions.put(topic, parts);
        }
        subscriptionState.assignPartitions(topicPartitions);
    }

    private void maybeHeartbeat() throws IOException {
        long now = System.currentTimeMillis();
        if (now - lastHeartbeatMs < config.heartbeatIntervalMs) return;

        ByteBuffer body = ByteBuffer.allocate(
            ByteBufferUtils.sizeOfString(groupId) + 4 + ByteBufferUtils.sizeOfString(memberId)
        );
        ByteBufferUtils.writeString(body, groupId);
        body.putInt(generationId);
        ByteBufferUtils.writeString(body, memberId);
        body.flip();

        ByteBuffer resp = networkClient.sendSync(ApiKeys.HEARTBEAT, body);
        short errorCode = resp.getShort();
        if (errorCode != 0) {
            log.warn("Heartbeat failed, errorCode={}, will rejoin", errorCode);
            needsJoin = true;
        }
        lastHeartbeatMs = now;
    }

    public void commitOffset(String topic, int partition, long offset) throws IOException {
        ByteBuffer body = ByteBuffer.allocate(
            ByteBufferUtils.sizeOfString(groupId) + 4
            + ByteBufferUtils.sizeOfString(memberId) + 4
            + ByteBufferUtils.sizeOfString(topic) + 4 + 4 + 8
        );
        ByteBufferUtils.writeString(body, groupId);
        body.putInt(generationId);
        ByteBufferUtils.writeString(body, memberId);
        body.putInt(1);
        ByteBufferUtils.writeString(body, topic);
        body.putInt(1);
        body.putInt(partition);
        body.putLong(offset);
        body.flip();
        networkClient.sendSync(ApiKeys.OFFSET_COMMIT, body);
        subscriptionState.setCommitted(topic, partition, offset);
    }

    public void leaveGroup() throws IOException {
        ByteBuffer body = ByteBuffer.allocate(
            ByteBufferUtils.sizeOfString(groupId) + ByteBufferUtils.sizeOfString(memberId)
        );
        ByteBufferUtils.writeString(body, groupId);
        ByteBufferUtils.writeString(body, memberId);
        body.flip();
        networkClient.sendSync(ApiKeys.LEAVE_GROUP, body);
        needsJoin = true;
        memberId = "";
        generationId = -1;
    }
}
