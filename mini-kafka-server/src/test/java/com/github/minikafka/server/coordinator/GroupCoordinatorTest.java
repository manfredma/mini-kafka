package com.github.minikafka.server.coordinator;

import org.junit.Before;
import org.junit.Test;
import java.util.HashMap;
import java.util.Map;
import static org.junit.Assert.*;

public class GroupCoordinatorTest {

    private GroupCoordinator coordinator;

    @Before
    public void setUp() {
        coordinator = new GroupCoordinator(30000);
    }

    @Test
    public void testJoinGroupCreatesGroup() {
        GroupCoordinator.JoinResult result = coordinator.handleJoinGroup(
            "test-group", "", "client-1", 30000, "range"
        );
        assertEquals(GroupMetadata.GroupState.CompletingRebalance, result.state);
        assertNotNull(result.memberId);
        assertFalse(result.memberId.isEmpty());
    }

    @Test
    public void testSecondMemberTriggersRebalance() {
        coordinator.handleJoinGroup("test-group", "", "client-1", 30000, "range");
        GroupCoordinator.JoinResult r2 = coordinator.handleJoinGroup(
            "test-group", "", "client-2", 30000, "range"
        );
        assertEquals(GroupMetadata.GroupState.CompletingRebalance, r2.state);
    }

    @Test
    public void testSyncGroupStabilizes() {
        GroupCoordinator.JoinResult r1 = coordinator.handleJoinGroup(
            "g1", "", "c1", 30000, "range"
        );
        Map<String, byte[]> assignment = new HashMap<>();
        assignment.put(r1.memberId, new byte[]{0});
        GroupCoordinator.SyncResult sync = coordinator.handleSyncGroup(
            "g1", r1.generationId, r1.memberId, assignment
        );
        assertEquals(GroupMetadata.GroupState.Stable, sync.state);
        assertNotNull(sync.assignment);
    }

    @Test
    public void testHeartbeat() {
        GroupCoordinator.JoinResult r = coordinator.handleJoinGroup(
            "hb-group", "", "c1", 30000, "range"
        );
        coordinator.handleSyncGroup("hb-group", r.generationId, r.memberId, new HashMap<>());
        boolean ok = coordinator.handleHeartbeat("hb-group", r.generationId, r.memberId);
        assertTrue(ok);
    }

    @Test
    public void testLeaveGroup() {
        GroupCoordinator.JoinResult r = coordinator.handleJoinGroup(
            "leave-group", "", "c1", 30000, "range"
        );
        coordinator.handleLeaveGroup("leave-group", r.memberId);
        GroupMetadata meta = coordinator.getGroup("leave-group");
        assertFalse(meta.hasMember(r.memberId));
    }
}
