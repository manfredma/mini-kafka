package com.github.minikafka.common.protocol;

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

    public final short code;
    public final String message;

    Errors(int code, String message) {
        this.code = (short) code;
        this.message = message;
    }

    public static Errors forCode(short code) {
        for (Errors e : values()) {
            if (e.code == code) return e;
        }
        return UNKNOWN_SERVER_ERROR;
    }
}
