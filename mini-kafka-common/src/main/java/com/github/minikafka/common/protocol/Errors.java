package com.github.minikafka.common.protocol;

/**
 * Kafka 协议错误码枚举，对应 Kafka 原版 {@code org.apache.kafka.common.protocol.Errors}。
 *
 * <p>每个枚举常量持有一个 16 位有符号错误码（与 Kafka 协议规范保持一致）和英文描述信息。
 * 当前仅收录 mini-kafka 实现所需的子集。
 *
 * <p>关键设计决策：
 * <ul>
 *   <li>错误码使用 {@code short} 存储（Kafka 协议规定 error code 为 2 字节有符号整数）。</li>
 *   <li>{@link #forCode(short)} 遇到未知错误码时返回 {@link #UNKNOWN_SERVER_ERROR} 而非抛出异常，
 *       与 Kafka 客户端的容错行为保持一致——未知错误视为服务端未知异常。</li>
 *   <li>{@link #NONE} 的 {@code code} 为 0，表示请求成功，无错误。</li>
 * </ul>
 */
public enum Errors {
    NONE(0, ""),
    UNKNOWN_SERVER_ERROR(-1, "The server experienced an unexpected error when processing the request."),
    UNKNOWN_TOPIC_OR_PARTITION(3, "This server does not host this topic-partition."),
    LEADER_NOT_AVAILABLE(5, "There is no leader for this topic-partition as we are in the middle of a leadership election."),
    REQUEST_TIMED_OUT(7, "The request timed out."),
    GROUP_LOAD_IN_PROGRESS(14, "The coordinator is loading and hence can't process requests for this group."),
    GROUP_COORDINATOR_NOT_AVAILABLE(15, "The group coordinator is not available."),
    NOT_COORDINATOR(16, "This is not the correct coordinator for this group."),
    INVALID_TOPIC_EXCEPTION(17, "The request attempted to perform an operation on an invalid topic."),
    REBALANCE_IN_PROGRESS(27, "The group is rebalancing, so a rejoin is needed."),
    UNKNOWN_MEMBER_ID(25, "The coordinator is not aware of this member."),
    ILLEGAL_GENERATION(22, "The provided generation id is not the current generation."),
    INCONSISTENT_GROUP_PROTOCOL(23, "The group member's supported protocols are incompatible with those of existing members.");

    /** Kafka 协议规范中的错误数字码，2 字节有符号整数；0 表示无错误，负数为通用错误。 */
    public final short code;

    /** 与 Kafka 官方协议文档保持一致的英文错误描述，{@link #NONE} 时为空字符串。 */
    public final String message;

    Errors(int code, String message) {
        this.code = (short) code;
        this.message = message;
    }

    /**
     * 根据数字错误码查找对应的 {@code Errors} 枚举常量。
     *
     * <p>与 {@link ApiKeys#forId(short)} 不同，此方法对未知错误码采用容错策略：
     * 返回 {@link #UNKNOWN_SERVER_ERROR} 而非抛出异常，以便调用方可以统一处理未知错误。
     *
     * @param code Kafka 协议帧中读取的错误码（2 字节有符号整数）
     * @return 与 {@code code} 匹配的枚举常量；未知错误码时返回 {@link #UNKNOWN_SERVER_ERROR}，保证非 null
     */
    public static Errors forCode(short code) {
        for (Errors e : values()) {
            if (e.code == code) return e;
        }
        return UNKNOWN_SERVER_ERROR;
    }
}
