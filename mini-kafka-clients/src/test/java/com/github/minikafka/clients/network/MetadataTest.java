package com.github.minikafka.clients.network;

import org.junit.Test;
import java.util.*;
import static org.junit.Assert.*;

public class MetadataTest {

    @Test
    public void testUpdateAndQuery() {
        Metadata metadata = new Metadata(100L);
        List<Metadata.PartitionMetadata> partitions = Arrays.asList(
            new Metadata.PartitionMetadata(0, "localhost", 9092, 0),
            new Metadata.PartitionMetadata(1, "localhost", 9092, 0)
        );
        Map<String, Metadata.TopicMetadata> topics = new HashMap<>();
        topics.put("test-topic", new Metadata.TopicMetadata("test-topic", partitions));

        metadata.update(topics, Arrays.asList(new Metadata.BrokerInfo(0, "localhost", 9092)));

        assertTrue(metadata.containsTopic("test-topic"));
        assertFalse(metadata.containsTopic("other-topic"));
        assertEquals(2, metadata.partitionCount("test-topic"));
        assertEquals(9092, metadata.leaderFor("test-topic", 0).port());
    }

    @Test
    public void testNeedsUpdate() {
        Metadata metadata = new Metadata(100L);
        assertTrue(metadata.needsUpdate());
        metadata.update(new HashMap<>(), new ArrayList<>());
        assertFalse(metadata.needsUpdate());
    }
}
