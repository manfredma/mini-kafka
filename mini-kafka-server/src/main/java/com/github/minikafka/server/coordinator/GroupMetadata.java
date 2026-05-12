package com.github.minikafka.server.coordinator;

import java.util.*;

/**
 * Consumer Group 的完整元数据，包含状态机、成员列表和已提交 offset。
 * <p>
 * 对应 Kafka 原版 {@code kafka.coordinator.group.GroupMetadata}（精简版）。
 * 管理 Group 的 Rebalance 状态机流转、成员增删、Leader 选举以及 offset 存储。
 * </p>
 * <p>
 * 线程安全性：非线程安全，由外层 {@link GroupCoordinator} 的 {@code synchronized} 方法保护。
 * </p>
 */
public final class GroupMetadata {

    /**
     * Consumer Group 状态机的所有合法状态，与 Kafka 原版保持一致：
     * <ul>
     *   <li>{@code Empty} — 无成员，初始状态或所有成员离开后的状态</li>
     *   <li>{@code PreparingRebalance} — 触发 Rebalance，等待成员重新加入</li>
     *   <li>{@code CompletingRebalance} — 所有成员已加入，等待 Leader 提交分配方案</li>
     *   <li>{@code Stable} — Leader 已提交分配，Group 正常消费中</li>
     *   <li>{@code Dead} — Group 已被删除或不存在</li>
     * </ul>
     */
    public enum GroupState {
        Empty, PreparingRebalance, CompletingRebalance, Stable, Dead
    }

    private final String groupId;
    private GroupState state = GroupState.Empty;
    private int generationId = 0;
    private String leaderId;
    private final Map<String, MemberMetadata> members = new LinkedHashMap<>();
    private final Map<String, byte[]> committedOffsets = new HashMap<>();

    /**
     * 创建指定 groupId 的 GroupMetadata，初始状态为 {@link GroupState#Empty}，generationId=0。
     *
     * @param groupId Consumer Group 标识符，不得为 {@code null}
     */
    public GroupMetadata(String groupId) {
        this.groupId = groupId;
    }

    public String groupId() { return groupId; }
    public GroupState state() { return state; }
    public void setState(GroupState state) { this.state = state; }
    public int generationId() { return generationId; }
    public void incrementGeneration() { generationId++; }
    public String leaderId() { return leaderId; }
    public void setLeaderId(String leaderId) { this.leaderId = leaderId; }
    public Map<String, MemberMetadata> members() { return members; }

    /**
     * 将成员加入 Group。
     * <p>
     * 若当前成员列表为空（第一个加入的成员），该成员自动成为 Leader，
     * 负责在 SyncGroup 阶段提交分区分配方案。
     * 若 memberId 已存在，覆盖原有 MemberMetadata（重新加入场景）。
     * </p>
     *
     * @param member 要加入的成员元数据，不得为 {@code null}
     */
    public void addMember(MemberMetadata member) {
        if (members.isEmpty()) leaderId = member.memberId;
        members.put(member.memberId, member);
    }

    /**
     * 将指定成员从 Group 移除。
     * <p>
     * 若被移除的成员恰好是 Leader，则从剩余成员中选取第一个成员（插入顺序）作为新 Leader。
     * 若移除后成员列表为空，leaderId 保持原值（调用方应随后将状态置为 Empty）。
     * </p>
     *
     * @param memberId 要移除的成员 ID
     */
    public void removeMember(String memberId) {
        members.remove(memberId);
        if (memberId.equals(leaderId) && !members.isEmpty()) {
            leaderId = members.keySet().iterator().next();
        }
    }

    /**
     * 判断指定成员是否在 Group 中。
     *
     * @param memberId 成员 ID
     * @return {@code true} 表示成员存在
     */
    public boolean hasMember(String memberId) {
        return members.containsKey(memberId);
    }

    /**
     * 提交指定 topic-partition 的消费 offset。
     * <p>
     * offset 以大端序 8 字节数组形式存储（{@link #longToBytes(long)}），
     * key 格式为 {@code "topic-partition"}。
     * </p>
     *
     * @param topic     topic 名称
     * @param partition 分区编号
     * @param offset    已消费到的 offset（下次从该 offset 开始消费）
     */
    public void commitOffset(String topic, int partition, long offset) {
        committedOffsets.put(topic + "-" + partition, longToBytes(offset));
    }

    /**
     * 查询指定 topic-partition 的已提交 offset。
     *
     * @param topic     topic 名称
     * @param partition 分区编号
     * @return 已提交的 offset；若从未提交过，返回 {@code -1L}（表示从最早或最新位置开始消费）
     */
    public Long fetchOffset(String topic, int partition) {
        byte[] bytes = committedOffsets.get(topic + "-" + partition);
        return bytes == null ? -1L : bytesToLong(bytes);
    }

    /**
     * 将 long 值序列化为大端序 8 字节数组，用于 offset 的紧凑存储。
     *
     * @param v 要序列化的 long 值
     * @return 8 字节大端序字节数组
     */
    private byte[] longToBytes(long v) {
        return new byte[]{
            (byte)(v >> 56), (byte)(v >> 48), (byte)(v >> 40), (byte)(v >> 32),
            (byte)(v >> 24), (byte)(v >> 16), (byte)(v >> 8), (byte) v
        };
    }

    /**
     * 将大端序 8 字节数组反序列化为 long 值。
     *
     * @param b 8 字节大端序字节数组
     * @return 对应的 long 值
     */
    private long bytesToLong(byte[] b) {
        long v = 0;
        for (byte x : b) v = (v << 8) | (x & 0xFF);
        return v;
    }
}
