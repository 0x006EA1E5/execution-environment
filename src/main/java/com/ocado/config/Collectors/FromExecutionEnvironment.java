package com.ocado.config.Collectors;

import com.ocado.config.Channel.Channel;
import com.ocado.config.ExecutionEnvironment;
import com.ocado.config.eventtype.EventType;

import java.util.Collections;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.*;
import java.util.stream.Collector;

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toSet;

public class FromExecutionEnvironment {

    private static final EnvToEventTypeMap ENV_TO_EVENT_TYPE_MAP = new EnvToEventTypeMap();
    private static final EnvToChannelEnvMap ENV_TO_CHANNEL_ENV_MAP = new EnvToChannelEnvMap();

    public static EnvToEventTypeMap toEnvToEventTypeMap() {
        return ENV_TO_EVENT_TYPE_MAP;
    }

//    public static EnvToChannelEnvMap toEnvToChannelEnvMap_() {
//        return ENV_TO_CHANNEL_ENV_MAP;
//    }
    public static Collector<ExecutionEnvironment, ?, Map<Channel, Set<ExecutionEnvironment>>> toEnvToChannelEnvMap() {
        return groupingBy(ExecutionEnvironment::getInputChannel, toSet());
    }

    public static class EnvToEventTypeMap implements Collector<ExecutionEnvironment, Map<EventType, Set<ExecutionEnvironment>>, Map<EventType, Set<ExecutionEnvironment>>> {



        private EnvToEventTypeMap() {}

        @Override
        public Supplier<Map<EventType, Set<ExecutionEnvironment>>> supplier() {
            return ConcurrentHashMap::new;
        }

        @Override
        public BiConsumer<Map<EventType, Set<ExecutionEnvironment>>, ExecutionEnvironment> accumulator() {
            return (map, executionEnvironment) -> {
                accumulatorFn().apply(map, executionEnvironment);
            };
        }

        public BiFunction<Map<EventType, Set<ExecutionEnvironment>>, ExecutionEnvironment, Map<EventType, Set<ExecutionEnvironment>>> accumulatorFn() {
            return (map, executionEnvironment) ->
            {
                 executionEnvironment
                        .getEventTypes()
                        .forEach(eventType -> {
                            map.compute(eventType, (key, value) -> {
                                if (value == null)
                                    value = ConcurrentHashMap.newKeySet();
                                value.add(executionEnvironment);
                                return value;
                            });
                        });
                 return map;
            };
        }

        @Override
        public BinaryOperator<Map<EventType, Set<ExecutionEnvironment>>> combiner() {
            return (mapA, mapB) -> {

                mapB.forEach((keyInB, valueInB) -> {
                    if (valueInB != null) {
                        mapA.merge(keyInB, valueInB, (s1, s2) -> {
                            s1.addAll(s2);
                            return s1;
                        });
                    }
                });
                return mapA;
            };
        }

        @Override
        public Function<Map<EventType, Set<ExecutionEnvironment>>, Map<EventType, Set<ExecutionEnvironment>>> finisher() {
            return Function.identity();
        }

        @Override
        public Set<Characteristics> characteristics() {
            return Collections.unmodifiableSet(EnumSet.of(
                    Collector.Characteristics.IDENTITY_FINISH,
                    Characteristics.UNORDERED,
                    Characteristics.CONCURRENT
            ));
        }
    }
    public static class EnvToChannelEnvMap implements Collector<ExecutionEnvironment, Map<Channel, Set<ExecutionEnvironment>>, Map<Channel, Set<ExecutionEnvironment>>> {

        private EnvToChannelEnvMap() {}

        @Override
        public Supplier<Map<Channel, Set<ExecutionEnvironment>>> supplier() {
            return ConcurrentHashMap::new;
        }

        @Override
        public BiConsumer<Map<Channel, Set<ExecutionEnvironment>>, ExecutionEnvironment> accumulator() {
            return (map, executionEnvironment) -> {
                Channel inputChannel = executionEnvironment.getInputChannel();
//                map.putIfAbsent(inputChannel, ConcurrentHashMap.newKeySet());
                map.compute(inputChannel, (key, value)-> {
                    if (value == null)
                        value = ConcurrentHashMap.newKeySet();
                    value.add(executionEnvironment);
                    return value;
                });
            };
        }

        @Override
        public BinaryOperator<Map<Channel, Set<ExecutionEnvironment>>> combiner() {
            return (a, b) -> {

                b.forEach((keyInB, valueInB) -> a.merge(keyInB, valueInB, (s1, s2) -> {
                    s1.addAll(s2);
                    return s1;
                }));

//            a.merge();
                return a;
            };
        }

        @Override
        public Function<Map<Channel, Set<ExecutionEnvironment>>, Map<Channel, Set<ExecutionEnvironment>>> finisher() {
            return Function.identity();
        }

        @Override
        public Set<Characteristics> characteristics() {
            return Collections.unmodifiableSet(EnumSet.of(
                    Collector.Characteristics.IDENTITY_FINISH,
                    Characteristics.UNORDERED,
                    Characteristics.CONCURRENT
            ));
        }
    }
}
