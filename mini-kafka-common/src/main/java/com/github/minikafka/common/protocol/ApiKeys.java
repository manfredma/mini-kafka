package com.github.minikafka.common.protocol;

/**
 * Kafka 协议 API 标识枚举，对应 Kafka 原版 {@code org.apache.kafka.common.protocol.ApiKeys}。
 *
 * <p>每个枚举常量持有一个 16 位数字 ID（与 Kafka 协议规范保持一致）和可读名称。
 * 当前仅收录 mini-kafka 实现所需的子集，未实现的 API 不在此枚举中出现。
 *
 * <p>关键设计决策：
 * <ul>
 *   <li>ID 使用 {@code short} 存储（Kafka 协议规定 API key 为 2 字节有符号整数）。</li>
 *   <li>构造参数接受 {@code int} 以避免调用处强制转型，内部截断为 {@code short}。</li>
 *   <li>{@link #forId(short)} 找不到时抛出异常而非返回 null，防止调用方忽略未知 API。</li>
 * </ul>
 */
public enum ApiKeys {
    PRODUCE(0, "Produce"),
    FETCH(1, "Fetch"),
    METADATA(3, "Metadata"),
    OFFSET_COMMIT(8, "OffsetCommit"),
    OFFSET_FETCH(9, "OffsetFetch"),
    JOIN_GROUP(11, "JoinGroup"),
    HEARTBEAT(12, "Heartbeat"),
    LEAVE_GROUP(13, "LeaveGroup"),
    SYNC_GROUP(14, "SyncGroup"),
    CREATE_TOPICS(19, "CreateTopics");

    /** Kafka 协议规范中的 API 数字标识，2 字节有符号整数。 */
    public final short id;

    /** API 的可读名称，与 Kafka 官方协议文档保持一致。 */
    public final String name;

    ApiKeys(int id, String name) {
        this.id = (short) id;
        this.name = name;
    }

    /**
     * 根据数字 ID 查找对应的 {@code ApiKeys} 枚举常量。
     *
     * @param id Kafka 协议帧中读取的 API key（2 字节有符号整数）
     * @return 与 {@code id} 匹配的枚举常量，保证非 null
     * @throws IllegalArgumentException 当 {@code id} 不对应任何已知 API 时抛出
     */
    public static ApiKeys forId(short id) {
        for (ApiKeys key : values()) {
            if (key.id == id) return key;
        }
        throw new IllegalArgumentException("Unknown api key: " + id);
    }
}
