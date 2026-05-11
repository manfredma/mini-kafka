package com.github.minikafka.clients.consumer;

import java.util.*;

public final class ConsumerRecords implements Iterable<ConsumerRecord> {

    public static final ConsumerRecords EMPTY = new ConsumerRecords(Collections.emptyList());

    private final List<ConsumerRecord> records;

    public ConsumerRecords(List<ConsumerRecord> records) {
        this.records = Collections.unmodifiableList(new ArrayList<>(records));
    }

    public List<ConsumerRecord> records(String topic) {
        List<ConsumerRecord> result = new ArrayList<>();
        for (ConsumerRecord r : records) if (r.topic.equals(topic)) result.add(r);
        return result;
    }

    public boolean isEmpty() { return records.isEmpty(); }
    public int count() { return records.size(); }

    @Override
    public Iterator<ConsumerRecord> iterator() { return records.iterator(); }
}
