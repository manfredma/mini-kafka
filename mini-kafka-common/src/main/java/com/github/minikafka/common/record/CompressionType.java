package com.github.minikafka.common.record;

public enum CompressionType {
    NONE(0, "none");

    public final int id;
    public final String name;

    CompressionType(int id, String name) {
        this.id = id;
        this.name = name;
    }
}
