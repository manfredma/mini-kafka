package com.github.minikafka.server.coordinator;

import java.util.*;

public final class GroupMetadata {

    public enum GroupState {
        Empty, PreparingRebalance, CompletingRebalance, Stable, Dead
    }

    private final String groupId;
    private GroupState state = GroupState.Empty;
    private int generationId = 0;
    private String leaderId;
    private final Map<String, MemberMetadata> members = new LinkedHashMap<>();
    private final Map<String, byte[]> committedOffsets = new HashMap<>();

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

    public void addMember(MemberMetadata member) {
        if (members.isEmpty()) leaderId = member.memberId;
        members.put(member.memberId, member);
    }

    public void removeMember(String memberId) {
        members.remove(memberId);
        if (memberId.equals(leaderId) && !members.isEmpty()) {
            leaderId = members.keySet().iterator().next();
        }
    }

    public boolean hasMember(String memberId) {
        return members.containsKey(memberId);
    }

    public void commitOffset(String topic, int partition, long offset) {
        committedOffsets.put(topic + "-" + partition, longToBytes(offset));
    }

    public Long fetchOffset(String topic, int partition) {
        byte[] bytes = committedOffsets.get(topic + "-" + partition);
        return bytes == null ? -1L : bytesToLong(bytes);
    }

    private byte[] longToBytes(long v) {
        return new byte[]{
            (byte)(v >> 56), (byte)(v >> 48), (byte)(v >> 40), (byte)(v >> 32),
            (byte)(v >> 24), (byte)(v >> 16), (byte)(v >> 8), (byte) v
        };
    }

    private long bytesToLong(byte[] b) {
        long v = 0;
        for (byte x : b) v = (v << 8) | (x & 0xFF);
        return v;
    }
}
