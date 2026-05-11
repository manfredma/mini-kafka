package com.github.minikafka.server.coordinator;

public final class MemberMetadata {
    public final String memberId;
    public final String clientId;
    public final int sessionTimeoutMs;
    public byte[] assignment;

    public MemberMetadata(String memberId, String clientId, int sessionTimeoutMs) {
        this.memberId = memberId;
        this.clientId = clientId;
        this.sessionTimeoutMs = sessionTimeoutMs;
    }
}
