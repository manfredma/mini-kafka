package com.github.minikafka.common.protocol;

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

    public final short id;
    public final String name;

    ApiKeys(int id, String name) {
        this.id = (short) id;
        this.name = name;
    }

    public static ApiKeys forId(short id) {
        for (ApiKeys key : values()) {
            if (key.id == id) return key;
        }
        throw new IllegalArgumentException("Unknown api key: " + id);
    }
}
