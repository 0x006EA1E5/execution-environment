package com.ocado.config.Channel;

import java.util.Objects;

public class KafkaChannel implements Channel {
    private final String topic;
    private final int partition;

    public KafkaChannel(String topic, int partition) {
        this.topic = topic;
        this.partition = partition;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        KafkaChannel that = (KafkaChannel) o;
        return partition == that.partition &&
                topic.equals(that.topic);
    }

    @Override
    public int hashCode() {
        return Objects.hash(topic, partition);
    }

    @Override
    public String toString() {
        return "KafkaChannel(" + topic + "/" + partition + ")";
    }
}
