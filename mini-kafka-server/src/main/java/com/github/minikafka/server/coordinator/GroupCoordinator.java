package com.github.minikafka.server.coordinator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 服务端 GroupCoordinator，管理 ConsumerGroup Rebalance 状态机
 * 对齐 Kafka kafka.coordinator.group.GroupCoordinator（简化版：顺序处理）
 */
public final class GroupCoordinator {

    private static final Logger log = LoggerFactory.getLogger(GroupCoordinator.class);

    private final Map<String, GroupMetadata> groups = new ConcurrentHashMap<>();
    private final int defaultSessionTimeoutMs;
    private final AtomicInteger memberIdCounter = new AtomicInteger(0);

    public GroupCoordinator(int defaultSessionTimeoutMs) {
        this.defaultSessionTimeoutMs = defaultSessionTimeoutMs;
    }

    public static final class JoinResult {
        public final String memberId;
        public final int generationId;
        public final String leaderId;
        public final List<String> members;
        public final GroupMetadata.GroupState state;

        JoinResult(String memberId, int generationId, String leaderId,
                   List<String> members, GroupMetadata.GroupState state) {
            this.memberId = memberId;
            this.generationId = generationId;
            this.leaderId = leaderId;
            this.members = members;
            this.state = state;
        }
    }

    public static final class SyncResult {
        public final byte[] assignment;
        public final GroupMetadata.GroupState state;

        SyncResult(byte[] assignment, GroupMetadata.GroupState state) {
            this.assignment = assignment;
            this.state = state;
        }
    }

    public synchronized JoinResult handleJoinGroup(
            String groupId, String memberId, String clientId,
            int sessionTimeoutMs, String protocol) {

        GroupMetadata group = groups.computeIfAbsent(groupId, GroupMetadata::new);

        if (memberId == null || memberId.isEmpty()) {
            memberId = clientId + "-" + memberIdCounter.incrementAndGet();
        }

        if (group.state() == GroupMetadata.GroupState.Stable
                || group.state() == GroupMetadata.GroupState.Empty) {
            group.setState(GroupMetadata.GroupState.PreparingRebalance);
            group.incrementGeneration();
        }

        MemberMetadata member = new MemberMetadata(memberId, clientId, sessionTimeoutMs);
        group.addMember(member);
        group.setState(GroupMetadata.GroupState.CompletingRebalance);

        List<String> memberList = Collections.emptyList();
        if (memberId.equals(group.leaderId())) {
            memberList = new ArrayList<>(group.members().keySet());
        }

        log.info("Member {} joined group {}, generation={}", memberId, groupId, group.generationId());
        return new JoinResult(memberId, group.generationId(), group.leaderId(), memberList, group.state());
    }

    public synchronized SyncResult handleSyncGroup(
            String groupId, int generationId, String memberId,
            Map<String, byte[]> assignment) {

        GroupMetadata group = groups.get(groupId);
        if (group == null) {
            return new SyncResult(null, GroupMetadata.GroupState.Dead);
        }

        if (memberId.equals(group.leaderId())) {
            for (Map.Entry<String, byte[]> e : assignment.entrySet()) {
                MemberMetadata m = group.members().get(e.getKey());
                if (m != null) m.assignment = e.getValue();
            }
            group.setState(GroupMetadata.GroupState.Stable);
            log.info("Group {} stabilized at generation {}", groupId, group.generationId());
        }

        byte[] myAssignment = new byte[0];
        MemberMetadata me = group.members().get(memberId);
        if (me != null && me.assignment != null) {
            myAssignment = me.assignment;
        }

        return new SyncResult(myAssignment, group.state());
    }

    public synchronized boolean handleHeartbeat(String groupId, int generationId, String memberId) {
        GroupMetadata group = groups.get(groupId);
        if (group == null || !group.hasMember(memberId)) return false;
        if (group.generationId() != generationId) return false;
        return group.state() == GroupMetadata.GroupState.Stable;
    }

    public synchronized void handleLeaveGroup(String groupId, String memberId) {
        GroupMetadata group = groups.get(groupId);
        if (group == null) return;
        group.removeMember(memberId);
        if (group.members().isEmpty()) {
            group.setState(GroupMetadata.GroupState.Empty);
        } else {
            group.setState(GroupMetadata.GroupState.PreparingRebalance);
        }
        log.info("Member {} left group {}", memberId, groupId);
    }

    public void commitOffset(String groupId, String topic, int partition, long offset) {
        GroupMetadata group = groups.get(groupId);
        if (group != null) group.commitOffset(topic, partition, offset);
    }

    public long fetchOffset(String groupId, String topic, int partition) {
        GroupMetadata group = groups.get(groupId);
        return group == null ? -1L : group.fetchOffset(topic, partition);
    }

    public GroupMetadata getGroup(String groupId) {
        return groups.get(groupId);
    }
}
