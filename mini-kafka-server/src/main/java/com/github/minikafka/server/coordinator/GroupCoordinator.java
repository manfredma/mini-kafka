package com.github.minikafka.server.coordinator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 服务端 GroupCoordinator，管理 Consumer Group 的 Rebalance 状态机及 offset 提交。
 * <p>
 * 对应 Kafka 原版 {@code kafka.coordinator.group.GroupCoordinator}（大幅简化版：顺序同步处理）。
 * 完整的 Rebalance 流程：
 * <ol>
 *   <li>客户端调用 JoinGroup → 服务端分配 memberId，状态机进入 CompletingRebalance，
 *       Leader 收到全量成员列表</li>
 *   <li>Leader 根据成员列表计算分配方案，调用 SyncGroup 提交 → 服务端将方案写入各成员，
 *       状态机进入 Stable</li>
 *   <li>各成员通过 SyncGroup 获取自己的分配结果</li>
 *   <li>Stable 状态下成员定期发送 Heartbeat 维持会话</li>
 *   <li>成员调用 LeaveGroup 或超时 → 触发新一轮 Rebalance</li>
 * </ol>
 * </p>
 * <p>
 * 线程安全性：所有公共方法均为 {@code synchronized}，保证状态机操作的原子性。
 * </p>
 */
public final class GroupCoordinator {

    private static final Logger log = LoggerFactory.getLogger(GroupCoordinator.class);

    private final Map<String, GroupMetadata> groups = new ConcurrentHashMap<>();
    private final int defaultSessionTimeoutMs;
    private final AtomicInteger memberIdCounter = new AtomicInteger(0);

    /**
     * 创建 GroupCoordinator。
     *
     * @param defaultSessionTimeoutMs 默认会话超时时间（毫秒），用于未指定超时的成员
     */
    public GroupCoordinator(int defaultSessionTimeoutMs) {
        this.defaultSessionTimeoutMs = defaultSessionTimeoutMs;
    }

    /**
     * JoinGroup 请求的处理结果，返回给客户端。
     */
    public static final class JoinResult {
        /** 服务端为该成员分配的 memberId（新成员由服务端生成，已有成员保持原值）。 */
        public final String memberId;
        /** 当前 Rebalance 轮次的 generationId，用于 Heartbeat 和 SyncGroup 的合法性校验。 */
        public final int generationId;
        /** 当前 Group 的 Leader memberId。 */
        public final String leaderId;
        /**
         * 全量成员 ID 列表：仅 Leader 成员收到非空列表，用于计算分配方案；
         * 普通成员收到空列表。
         */
        public final List<String> members;
        /** JoinGroup 处理后的 Group 状态（通常为 CompletingRebalance）。 */
        public final GroupMetadata.GroupState state;

        JoinResult(String memberId, int generationId, String leaderId,
                   List<String> members, GroupMetadata.GroupState state) {
            this.memberId = memberId;
            this.generationId = generationId;
            this.leaderId = leaderId;
            this.members = members;
            this.state = state;
        }
    }

    /**
     * SyncGroup 请求的处理结果，返回给客户端。
     */
    public static final class SyncResult {
        /**
         * 该成员的分区分配结果（序列化字节数组）。
         * Group 不存在时为 {@code null}；成员尚未收到分配时为空字节数组。
         */
        public final byte[] assignment;
        /** SyncGroup 处理后的 Group 状态。 */
        public final GroupMetadata.GroupState state;

        SyncResult(byte[] assignment, GroupMetadata.GroupState state) {
            this.assignment = assignment;
            this.state = state;
        }
    }

    /**
     * 处理 JoinGroup 请求，驱动状态机向 CompletingRebalance 转换。
     * <p>
     * 状态转换逻辑：
     * <ul>
     *   <li>若 Group 处于 Stable 或 Empty 状态，转入 PreparingRebalance 并递增 generationId</li>
     *   <li>将成员加入 Group（第一个成员自动成为 Leader）</li>
     *   <li>状态转入 CompletingRebalance，等待 Leader 提交分配方案</li>
     * </ul>
     * </p>
     *
     * @param groupId          Consumer Group ID
     * @param memberId         成员 ID；传入 {@code null} 或空字符串时，服务端自动生成新 ID
     * @param clientId         客户端 ID，用于生成 memberId 和日志
     * @param sessionTimeoutMs 该成员的会话超时时间（毫秒）
     * @param protocol         分配协议名称（如 "range"、"roundrobin"），当前未使用，保留供扩展
     * @return JoinGroup 处理结果，包含 memberId、generationId、leaderId 及成员列表
     */
    public synchronized JoinResult handleJoinGroup(
            String groupId, String memberId, String clientId,
            int sessionTimeoutMs, String protocol) {

        GroupMetadata group = groups.computeIfAbsent(groupId, GroupMetadata::new);

        if (memberId == null || memberId.isEmpty()) {
            memberId = clientId + "-" + memberIdCounter.incrementAndGet();
        }

        if (group.state() == GroupMetadata.GroupState.Stable
                || group.state() == GroupMetadata.GroupState.Empty) {
            group.setState(GroupMetadata.GroupState.PreparingRebalance);
            group.incrementGeneration();
        }

        MemberMetadata member = new MemberMetadata(memberId, clientId, sessionTimeoutMs);
        group.addMember(member);
        group.setState(GroupMetadata.GroupState.CompletingRebalance);

        List<String> memberList = Collections.emptyList();
        if (memberId.equals(group.leaderId())) {
            memberList = new ArrayList<>(group.members().keySet());
        }

        log.info("Member {} joined group {}, generation={}", memberId, groupId, group.generationId());
        return new JoinResult(memberId, group.generationId(), group.leaderId(), memberList, group.state());
    }

    /**
     * 处理 SyncGroup 请求：Leader 提交分配方案后，Group 状态转为 Stable。
     * <p>
     * 仅当请求方是 Leader 时，才将 assignment 写入各成员的 {@link MemberMetadata#assignment}，
     * 并将 Group 状态置为 Stable。普通成员调用此方法时直接返回自己的分配结果（可能为空）。
     * </p>
     *
     * @param groupId      Consumer Group ID
     * @param generationId 客户端持有的 generationId（当前未校验，保留供扩展）
     * @param memberId     发起请求的成员 ID
     * @param assignment   仅 Leader 传入非空 map（memberId → 分配结果字节数组）；普通成员传空 map
     * @return SyncGroup 处理结果，包含该成员的分配结果和当前 Group 状态；
     *         Group 不存在时 state 为 {@link GroupMetadata.GroupState#Dead}，assignment 为 {@code null}
     */
    public synchronized SyncResult handleSyncGroup(
            String groupId, int generationId, String memberId,
            Map<String, byte[]> assignment) {

        GroupMetadata group = groups.get(groupId);
        if (group == null) {
            return new SyncResult(null, GroupMetadata.GroupState.Dead);
        }

        if (memberId.equals(group.leaderId())) {
            for (Map.Entry<String, byte[]> e : assignment.entrySet()) {
                MemberMetadata m = group.members().get(e.getKey());
                if (m != null) m.assignment = e.getValue();
            }
            group.setState(GroupMetadata.GroupState.Stable);
            log.info("Group {} stabilized at generation {}", groupId, group.generationId());
        }

        byte[] myAssignment = new byte[0];
        MemberMetadata me = group.members().get(memberId);
        if (me != null && me.assignment != null) {
            myAssignment = me.assignment;
        }

        return new SyncResult(myAssignment, group.state());
    }

    /**
     * 处理 Heartbeat 请求，校验 generationId 和 Group 状态。
     * <p>
     * 心跳合法性校验：Group 必须存在、成员必须在 Group 中、generationId 必须匹配、
     * Group 必须处于 Stable 状态。
     * </p>
     *
     * @param groupId      Consumer Group ID
     * @param generationId 客户端持有的 generationId，必须与服务端当前值一致
     * @param memberId     发送心跳的成员 ID
     * @return {@code true} 表示心跳合法（Group Stable 且 generationId 匹配）；
     *         {@code false} 表示需要客户端重新发起 Rebalance
     */
    public synchronized boolean handleHeartbeat(String groupId, int generationId, String memberId) {
        GroupMetadata group = groups.get(groupId);
        if (group == null || !group.hasMember(memberId)) return false;
        if (group.generationId() != generationId) return false;
        return group.state() == GroupMetadata.GroupState.Stable;
    }

    /**
     * 处理 LeaveGroup 请求：移除成员并根据剩余成员数触发 Rebalance 或将 Group 置为 Empty。
     * <p>
     * 状态转换逻辑：
     * <ul>
     *   <li>移除后成员列表为空 → 状态转为 {@link GroupMetadata.GroupState#Empty}</li>
     *   <li>移除后仍有成员 → 状态转为 {@link GroupMetadata.GroupState#PreparingRebalance}，
     *       触发新一轮 Rebalance</li>
     * </ul>
     * </p>
     *
     * @param groupId  Consumer Group ID
     * @param memberId 要离开的成员 ID；Group 不存在或成员不在 Group 中时静默返回
     */
    public synchronized void handleLeaveGroup(String groupId, String memberId) {
        GroupMetadata group = groups.get(groupId);
        if (group == null) return;
        group.removeMember(memberId);
        if (group.members().isEmpty()) {
            group.setState(GroupMetadata.GroupState.Empty);
        } else {
            group.setState(GroupMetadata.GroupState.PreparingRebalance);
        }
        log.info("Member {} left group {}", memberId, groupId);
    }

    /**
     * 提交指定 Consumer Group 对 topic-partition 的消费 offset。
     * <p>
     * 若 Group 不存在，静默忽略（不创建 Group）。
     * </p>
     *
     * @param groupId   Consumer Group ID
     * @param topic     topic 名称
     * @param partition 分区编号
     * @param offset    要提交的 offset
     */
    public synchronized void commitOffset(String groupId, String topic, int partition, long offset) {
        GroupMetadata group = groups.get(groupId);
        if (group != null) group.commitOffset(topic, partition, offset);
    }

    /**
     * 查询指定 Consumer Group 对 topic-partition 的已提交 offset。
     *
     * @param groupId   Consumer Group ID
     * @param topic     topic 名称
     * @param partition 分区编号
     * @return 已提交的 offset；Group 不存在或从未提交过，返回 {@code -1L}
     */
    public synchronized long fetchOffset(String groupId, String topic, int partition) {
        GroupMetadata group = groups.get(groupId);
        return group == null ? -1L : group.fetchOffset(topic, partition);
    }

    /**
     * 按 groupId 查找 GroupMetadata，仅用于测试和监控，不做状态修改。
     *
     * @param groupId Consumer Group ID
     * @return 对应的 {@link GroupMetadata}；不存在时返回 {@code null}
     */
    public GroupMetadata getGroup(String groupId) {
        return groups.get(groupId);
    }
}
