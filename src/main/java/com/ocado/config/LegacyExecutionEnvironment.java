package com.ocado.config;

import com.ocado.config.Channel.Channel;
import com.ocado.config.Channel.KafkaChannel;
import com.ocado.config.eventtype.EventType;

import java.util.Collections;
import java.util.Set;

public class LegacyExecutionEnvironment implements ExecutionEnvironment {

    private Channel inputChannel;

    private Set<EventType> eventTypes;

    private Set<Object> queries;

    public LegacyExecutionEnvironment(Channel inputChannel) {
        this.inputChannel = inputChannel;
    }

    public LegacyExecutionEnvironment(Integer inputChannel) {
        this.inputChannel = new KafkaChannel("cepTopic", inputChannel);
    }

    public LegacyExecutionEnvironment() {
        inputChannel = new KafkaChannel("cepTopic", 1);
    };

    @Override
    public Set<EventType> getEventTypes() {
        return eventTypes != null ? eventTypes : Collections.emptySet();
    }

    public void setEventTypes(Set<EventType> eventTypes) {
        this.eventTypes = eventTypes;
    }

    @Override
    public Set<Object> getQueries() {
        return queries;
    }

    public void setQueries(Set<Object> queries) {
        this.queries = queries;
    }

    @Override
    public Channel getInputChannel() {
        return inputChannel;
    }

    public void setInputChannel(Channel inputChannel) {
        this.inputChannel = inputChannel;
    }


}
