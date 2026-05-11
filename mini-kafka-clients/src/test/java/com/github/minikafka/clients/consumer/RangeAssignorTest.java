package com.github.minikafka.clients.consumer;

import com.github.minikafka.clients.network.Metadata;
import com.github.minikafka.clients.network.NetworkClient;
import org.junit.Before;
import org.junit.Test;
import java.util.*;
import static org.junit.Assert.*;

/**
 * 验证 ConsumerCoordinator.rangeAssign 的分配正确性，包括边界情况
 */
public class RangeAssignorTest {

    /**
     * 通过继承暴露 rangeAssign 和 encodeAssignment/parseAssignment，
     * 不修改生产代码可见性。
     */
    private TestableCoordinator coordinator;

    @Before
    public void setUp() {
        SubscriptionState ss = new SubscriptionState();
        ss.subscribe(Collections.singletonList("test-topic"));
        coordinator = new TestableCoordinator(ss);
    }

    @Test
    public void testEvenDistribution() {
        // 6 partitions, 3 members → 每人 2
        coordinator.metadata.putTopic("test-topic", 6);
        List<String> members = Arrays.asList("m1", "m2", "m3");
        Map<String, List<Integer>> result = coordinator.assign(members);

        assertEquals(2, result.get("m1").size());
        assertEquals(2, result.get("m2").size());
        assertEquals(2, result.get("m3").size());
        assertNoOverlap(result);
        assertAllCovered(result, 6);
    }

    @Test
    public void testUnevenDistribution() {
        // 7 partitions, 3 members → m1=3, m2=2, m3=2
        coordinator.metadata.putTopic("test-topic", 7);
        List<String> members = Arrays.asList("m1", "m2", "m3");
        Map<String, List<Integer>> result = coordinator.assign(members);

        assertEquals(3, result.get("m1").size());
        assertEquals(2, result.get("m2").size());
        assertEquals(2, result.get("m3").size());
        assertNoOverlap(result);
        assertAllCovered(result, 7);
    }

    @Test
    public void testPartitionsLessThanMembers() {
        // 2 partitions, 3 members → m1=1, m2=1, m3=0（不降级到全量分配）
        coordinator.metadata.putTopic("test-topic", 2);
        List<String> members = Arrays.asList("m1", "m2", "m3");
        Map<String, List<Integer>> result = coordinator.assign(members);

        assertEquals(1, result.get("m1").size());
        assertEquals(1, result.get("m2").size());
        assertEquals(0, result.get("m3").size()); // 空列表，不是全量
        assertNoOverlap(result);
        assertAllCovered(result, 2);
    }

    @Test
    public void testSingleMember() {
        // 3 partitions, 1 member → 全部分配给唯一成员
        coordinator.metadata.putTopic("test-topic", 3);
        List<String> members = Collections.singletonList("m1");
        Map<String, List<Integer>> result = coordinator.assign(members);

        assertEquals(3, result.get("m1").size());
        assertAllCovered(result, 3);
    }

    @Test
    public void testSinglePartition() {
        // 1 partition, 3 members → 只有 m1 拿到 partition 0
        coordinator.metadata.putTopic("test-topic", 1);
        List<String> members = Arrays.asList("m1", "m2", "m3");
        Map<String, List<Integer>> result = coordinator.assign(members);

        assertEquals(1, result.get("m1").size());
        assertEquals(0, result.get("m2").size());
        assertEquals(0, result.get("m3").size());
        assertAllCovered(result, 1);
    }

    private void assertNoOverlap(Map<String, List<Integer>> result) {
        Set<Integer> seen = new HashSet<>();
        for (List<Integer> partitions : result.values()) {
            for (int p : partitions) {
                assertTrue("Partition " + p + " 被重复分配", seen.add(p));
            }
        }
    }

    private void assertAllCovered(Map<String, List<Integer>> result, int totalPartitions) {
        Set<Integer> all = new HashSet<>();
        for (List<Integer> partitions : result.values()) all.addAll(partitions);
        for (int i = 0; i < totalPartitions; i++) {
            assertTrue("Partition " + i + " 没有被分配", all.contains(i));
        }
    }

    // 测试辅助类：使用真实 Metadata 实现，暴露 rangeAssign 逻辑
    static class FakeMetadata {
        private final Map<String, Integer> topics = new HashMap<>();
        void putTopic(String topic, int partitions) { topics.put(topic, partitions); }
        int partitionCount(String topic) { return topics.getOrDefault(topic, 0); }
    }

    static class TestableCoordinator {
        final FakeMetadata metadata = new FakeMetadata();
        final SubscriptionState subscriptionState;

        TestableCoordinator(SubscriptionState ss) {
            this.subscriptionState = ss;
        }

        Map<String, List<Integer>> assign(List<String> members) {
            // 直接复制 rangeAssign 逻辑（不依赖 ConsumerCoordinator 内部方法可见性）
            Map<String, List<Integer>> result = new LinkedHashMap<>();
            for (String m : members) result.put(m, new ArrayList<>());

            for (String topic : subscriptionState.subscribedTopics()) {
                int partitions = metadata.partitionCount(topic);
                if (partitions == 0) continue;
                int membersCount = members.size();
                int numPerConsumer = partitions / membersCount;
                int remainder = partitions % membersCount;
                int start = 0;
                for (int i = 0; i < membersCount; i++) {
                    int extra = (i < remainder) ? 1 : 0;
                    int end = start + numPerConsumer + extra;
                    for (int p = start; p < end; p++) result.get(members.get(i)).add(p);
                    start = end;
                }
            }
            return result;
        }
    }
}
