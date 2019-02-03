package com.ocado.config;

import com.ocado.config.Channel.Channel;
import com.ocado.config.eventtype.EventType;


import java.util.Set;

public interface ExecutionEnvironment {

    Set<EventType> getEventTypes();

    Set<Object> getQueries();

    Channel getInputChannel();


}
