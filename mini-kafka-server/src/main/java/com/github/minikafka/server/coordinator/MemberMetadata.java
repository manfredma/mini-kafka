package com.github.minikafka.server.coordinator;

/**
 * Consumer Group 中单个成员的元数据。
 * <p>
 * 对应 Kafka 原版 {@code kafka.coordinator.group.MemberMetadata}（精简版）。
 * 保存成员标识、会话超时配置以及 SyncGroup 阶段由 Leader 下发的分区分配结果。
 * </p>
 * <p>
 * 线程安全性：非线程安全，由外层 {@link GroupCoordinator} 的 {@code synchronized} 方法保护。
 * </p>
 */
public final class MemberMetadata {
    /** 服务端为该成员生成的唯一标识符，格式为 {@code clientId + "-" + counter}。 */
    public final String memberId;
    /** 客户端自报的 clientId，用于生成 memberId 和日志可读性。 */
    public final String clientId;
    /**
     * 该成员的会话超时时间（毫秒）。
     * 超时未收到心跳则触发 Rebalance（mini-kafka 当前未实现超时检测，保留字段供扩展）。
     */
    public final int sessionTimeoutMs;
    /**
     * SyncGroup 阶段由 Leader 下发的分区分配结果（序列化字节数组）。
     * 初始为 {@code null}，在 {@link GroupCoordinator#handleSyncGroup} 中被赋值。
     * {@code null} 表示该成员尚未收到分配。
     */
    public byte[] assignment;

    /**
     * 构造成员元数据。
     *
     * @param memberId         服务端分配的成员 ID，不得为 {@code null}
     * @param clientId         客户端 ID，不得为 {@code null}
     * @param sessionTimeoutMs 会话超时时间（毫秒），正整数
     */
    public MemberMetadata(String memberId, String clientId, int sessionTimeoutMs) {
        this.memberId = memberId;
        this.clientId = clientId;
        this.sessionTimeoutMs = sessionTimeoutMs;
    }
}
