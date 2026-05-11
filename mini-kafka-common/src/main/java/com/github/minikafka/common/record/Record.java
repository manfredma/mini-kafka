package com.github.minikafka.common.record;

public final class Record {
    private final long offset;
    private final long timestamp;
    private final byte[] key;
    private final byte[] value;

    public Record(long offset, long timestamp, byte[] key, byte[] value) {
        this.offset = offset;
        this.timestamp = timestamp;
        this.key = key;
        this.value = value;
    }

    public long offset() { return offset; }
    public long timestamp() { return timestamp; }
    public byte[] key() { return key; }
    public byte[] value() { return value; }
}
