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
 * 客户端侧 Consumer Group Rebalance 协调器，负责加入 Group、同步分区分配、发送心跳和提交 offset。
 *
 * <p>对齐 Kafka {@code org.apache.kafka.clients.consumer.internals.ConsumerCoordinator}（简化版）。
 *
 * <p>Rebalance 流程：
 * <ol>
 *   <li>{@link #ensureActiveGroup()} 检查是否需要重新加入 Group。</li>
 *   <li>{@link #joinGroup()} 发送 JoinGroupRequest，获取 generationId、memberId 和成员列表。</li>
 *   <li>{@link #syncGroup(String, List)} 中 leader 执行 {@link #rangeAssign} 分区分配，
 *       所有成员发送 SyncGroupRequest，接收各自的分区分配结果。</li>
 *   <li>分配结果通过 {@link #parseAssignment} 解析后写入 {@link SubscriptionState}。</li>
 * </ol>
 *
 * <p>线程安全性：本类不是线程安全的，应由单一 Consumer 主线程调用。
 */
public final class ConsumerCoordinator {

    private static final Logger log = LoggerFactory.getLogger(ConsumerCoordinator.class);

    private final String groupId;
    private final ConsumerConfig config;
    private final NetworkClient networkClient;
    private final Metadata metadata;
    private final SubscriptionState subscriptionState;

    /** 本 Consumer 在 Group 中的成员 ID，由 Coordinator 在 JoinGroup 响应中分配；初始为空字符串。 */
    private String memberId = "";
    /** 当前 Rebalance 轮次编号，每次 Rebalance 后递增；-1 表示尚未加入 Group。 */
    private int generationId = -1;
    /** 是否需要重新加入 Group（首次启动或心跳失败后置为 true）。 */
    private boolean needsJoin = true;
    /** 上次成功发送心跳的时间戳（毫秒），用于 {@link #maybeHeartbeat} 限流。 */
    private long lastHeartbeatMs = 0;

    /**
     * 构造 ConsumerCoordinator。
     *
     * @param groupId           Consumer Group ID
     * @param config            Consumer 配置（sessionTimeoutMs、heartbeatIntervalMs 等）
     * @param networkClient     网络客户端，用于发送协调请求
     * @param metadata          元数据缓存，用于获取 Topic 分区数
     * @param subscriptionState 订阅状态，Rebalance 后写入分区分配结果
     */
    public ConsumerCoordinator(String groupId, ConsumerConfig config,
                                NetworkClient networkClient, Metadata metadata,
                                SubscriptionState subscriptionState) {
        this.groupId = groupId;
        this.config = config;
        this.networkClient = networkClient;
        this.metadata = metadata;
        this.subscriptionState = subscriptionState;
    }

    /**
     * 确保本 Consumer 处于活跃的 Group 成员状态。
     *
     * <p>若 {@link #needsJoin} 为 true（首次启动或心跳失败），则触发完整的 Rebalance 流程
     * （joinGroup + syncGroup）；否则仅检查是否需要发送心跳。
     *
     * <p>每次 {@link KafkaConsumer#poll} 调用前都会调用此方法。
     *
     * @throws IOException 网络通信失败时抛出
     */
    public void ensureActiveGroup() throws IOException {
        if (needsJoin) {
            joinGroup();
        } else {
            maybeHeartbeat();
        }
    }

    /**
     * 发送 JoinGroupRequest，加入或重新加入 Consumer Group。
     *
     * <p>请求体格式：{@code [groupId][sessionTimeoutMs][memberId][protocolType="consumer"]}
     * <br>响应解析：errorCode、generationId、protocol、leaderId、memberId、members 列表。
     *
     * <p>加入成功后，若本 Consumer 是 leader（memberId == leaderId），则在
     * {@link #syncGroup} 中负责执行分区分配计算。
     *
     * @throws IOException 网络通信失败时抛出
     */
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

    /**
     * 发送 SyncGroupRequest，同步分区分配结果。
     *
     * <p>若本 Consumer 是 leader（{@code memberId.equals(leaderId)}），则先调用
     * {@link #rangeAssign} 计算分配方案，并在请求中携带所有成员的分配结果；
     * follower 发送空分配映射，等待 Coordinator 转发 leader 的计算结果。
     *
     * <p>响应中包含本 Consumer 被分配的分区字节流，由 {@link #parseAssignment} 解析后
     * 写入 {@link SubscriptionState}。
     *
     * @param leaderId 本轮 Rebalance 的 leader memberId
     * @param members  本轮 Rebalance 的所有成员 memberId 列表
     * @throws IOException 网络通信失败时抛出
     */
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
     * RangeAssignor 算法：将每个已订阅 Topic 的分区均匀分配给 Group 成员。
     *
     * <p>对齐 Kafka {@code org.apache.kafka.clients.consumer.RangeAssignor} 算法：
     * <ul>
     *   <li>每个 Topic 独立分配，不跨 Topic 平衡。</li>
     *   <li>分区按编号排序，成员按字典序排序（由 Coordinator 保证）。</li>
     *   <li>基本分配量 = partitions / members；前 {@code partitions % members} 个成员各多分配一个分区。</li>
     *   <li>当 partitions < members 时，后面的成员分配为空（不写入 encodeAssignment）。</li>
     * </ul>
     *
     * <p>仅由 leader Consumer 调用，follower 跳过此方法。
     *
     * @param members 本轮 Rebalance 的所有成员 memberId 列表（顺序由 Coordinator 决定）
     * @return memberId -> 序列化分配结果的映射，使用 {@link #encodeAssignment} 编码
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

    /**
     * 将一个 Topic 的分区范围 [start, end) 追加编码到已有分配字节流中。
     *
     * <p>编码格式（追加到 existing 末尾）：
     * {@code [string topic][4字节count][count * 4字节partitionId]}
     *
     * <p>使用 {@code Arrays.copyOf} 截取已写入部分，避免 {@code buf.array()} 返回含尾部零字节的完整数组。
     *
     * @param existing 已有的分配字节流（可为 null 或空数组，表示首次追加）
     * @param topic    要追加的 Topic 名称
     * @param start    分区范围起始编号（包含）
     * @param end      分区范围结束编号（不包含）
     * @return 追加后的完整分配字节流
     */
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

    /**
     * 解析 SyncGroup 响应中的分配字节流，更新 {@link SubscriptionState} 的分区分配。
     *
     * <p>解码格式（循环直到 buffer 耗尽）：
     * {@code [string topic][4字节count][count * 4字节partitionId]}
     *
     * <p>若解析结果为空（字节流格式异常或无分配），则回退到 {@link #assignAllSubscribed}。
     *
     * @param assignment SyncGroup 响应中本 Consumer 的分配字节流
     */
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

    /**
     * 兜底分配策略：将所有已订阅 Topic 的全部分区分配给本 Consumer。
     *
     * <p>在以下情况下调用：
     * <ul>
     *   <li>SyncGroup 响应中分配字节流为空（服务端未返回分配）。</li>
     *   <li>{@link #parseAssignment} 解析结果为空（格式异常）。</li>
     * </ul>
     * 分区数来自 {@link Metadata}，若 Topic 元数据不存在则默认分配 1 个分区。
     */
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

    /**
     * 按 heartbeatIntervalMs 间隔向 Coordinator 发送心跳，维持会话存活。
     *
     * <p>若距上次心跳时间未超过 {@link ConsumerConfig#heartbeatIntervalMs}，则直接返回（限流）。
     * 若心跳响应 errorCode 非零（如 REBALANCE_IN_PROGRESS），则将 {@link #needsJoin}
     * 置为 true，下次 {@link #ensureActiveGroup} 时触发重新加入。
     *
     * @throws IOException 网络通信失败时抛出
     */
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

    /**
     * 提交指定分区的 offset 到 Coordinator，并更新本地 {@link SubscriptionState} 的已提交记录。
     *
     * <p>请求体格式：{@code [groupId][generationId][memberId][topicCount=1][topic][partCount=1][partition][offset]}
     *
     * @param topic     Topic 名称
     * @param partition 分区编号
     * @param offset    要提交的 offset（通常为当前 position，即下一条待拉取消息的 offset）
     * @throws IOException 网络通信失败时抛出
     */
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

    /**
     * 发送 LeaveGroupRequest，主动退出 Consumer Group，触发服务端 Rebalance。
     *
     * <p>退出后重置本地状态：{@link #needsJoin} 置为 true，{@link #memberId} 清空，
     * {@link #generationId} 重置为 -1。
     * 通常由 {@link KafkaConsumer#close()} 在关闭前调用，以便 Group 尽快 Rebalance
     * 将分区分配给其他成员，而非等待 sessionTimeoutMs 超时。
     *
     * @throws IOException 网络通信失败时抛出（close 场景下通常忽略）
     */
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
