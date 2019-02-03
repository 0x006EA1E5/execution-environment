package com.ocado.config.Collectors;

import com.ocado.config.Channel.Channel;
import com.ocado.config.ExecutionEnvironment;
import com.ocado.config.eventtype.EventType;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.*;
import java.util.stream.Collector;

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toSet;

public class FromExecutionEnvironment {


//    private static final EnvToChannelEnvMap ENV_TO_CHANNEL_ENV_MAP = new EnvToChannelEnvMap();

    public static ConcurrentEnvToEventTypeMap toConcurrentEnvToEventTypeMap() {
        return new ConcurrentEnvToEventTypeMap();
    }
    public static EnvToEventTypeMap toEnvToEventTypeMap() {
        return new EnvToEventTypeMap();
    }

//    public static EnvToChannelEnvMap toEnvToChannelEnvMap_() {
//        return ENV_TO_CHANNEL_ENV_MAP;
//    }
    public static Collector<ExecutionEnvironment, ?, Map<Channel, Set<ExecutionEnvironment>>> toEnvToChannelEnvMap() {
        return groupingBy(ExecutionEnvironment::getInputChannel, toSet());
    }

    public static class ConcurrentEnvToEventTypeMap extends AbstractEnvToEventTypeMap {
        @Override
        protected Set<ExecutionEnvironment> getNewSet(){
            return ConcurrentHashMap.newKeySet();
        }

        @Override
        protected  Map<EventType, Set<ExecutionEnvironment>> getNewMap() {
            return new ConcurrentHashMap<>();
        }
        @Override
        public Function<Map<EventType, Set<ExecutionEnvironment>>, Map<EventType, Set<ExecutionEnvironment>>> finisher() {
            return Function.identity();
        }
    }
    public static class EnvToEventTypeMap extends AbstractEnvToEventTypeMap {

        @Override
        protected Set<ExecutionEnvironment> getNewSet(){
            return new HashSet<>();
        }
        @Override
        protected  Map<EventType, Set<ExecutionEnvironment>> getNewMap() {
            return new HashMap<>();
        }
        @Override
        public Function<Map<EventType, Set<ExecutionEnvironment>>, Map<EventType, Set<ExecutionEnvironment>>> finisher() {
            return Collections::unmodifiableMap;
        }
    }
    public static abstract class AbstractEnvToEventTypeMap implements Collector<ExecutionEnvironment, Map<EventType, Set<ExecutionEnvironment>>, Map<EventType, Set<ExecutionEnvironment>>> {


        protected abstract Set<ExecutionEnvironment> getNewSet();
        protected abstract Map<EventType, Set<ExecutionEnvironment>> getNewMap();

        private AbstractEnvToEventTypeMap() {}

        @Override
        public Supplier<Map<EventType, Set<ExecutionEnvironment>>> supplier() {
            return this::getNewMap;
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
                                    value = getNewSet();
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
        public Set<Characteristics> characteristics() {
            return Collections.unmodifiableSet(EnumSet.of(
                    Collector.Characteristics.IDENTITY_FINISH,
                    Characteristics.UNORDERED,
                    Characteristics.CONCURRENT
            ));
        }
    }
//    public static class EnvToChannelEnvMap implements Collector<ExecutionEnvironment, Map<Channel, Set<ExecutionEnvironment>>, Map<Channel, Set<ExecutionEnvironment>>> {
//
//        private EnvToChannelEnvMap() {}
//
//        @Override
//        public Supplier<Map<Channel, Set<ExecutionEnvironment>>> supplier() {
//            return ConcurrentHashMap::new;
//        }
//
//        @Override
//        public BiConsumer<Map<Channel, Set<ExecutionEnvironment>>, ExecutionEnvironment> accumulator() {
//            return (map, executionEnvironment) -> {
//                Channel inputChannel = executionEnvironment.getInputChannel();
////                map.putIfAbsent(inputChannel, ConcurrentHashMap.newKeySet());
//                map.compute(inputChannel, (key, value)-> {
//                    if (value == null)
//                        value = ConcurrentHashMap.newKeySet();
//                    value.add(executionEnvironment);
//                    return value;
//                });
//            };
//        }
//
//        @Override
//        public BinaryOperator<Map<Channel, Set<ExecutionEnvironment>>> combiner() {
//            return (a, b) -> {
//
//                b.forEach((keyInB, valueInB) -> a.merge(keyInB, valueInB, (s1, s2) -> {
//                    s1.addAll(s2);
//                    return s1;
//                }));
//
////            a.merge();
//                return a;
//            };
//        }
//
//        @Override
//        public Function<Map<Channel, Set<ExecutionEnvironment>>, Map<Channel, Set<ExecutionEnvironment>>> finisher() {
//            return Function.identity();
//        }
//
//        @Override
//        public Set<Characteristics> characteristics() {
//            return Collections.unmodifiableSet(EnumSet.of(
//                    Collector.Characteristics.IDENTITY_FINISH,
//                    Characteristics.UNORDERED,
//                    Characteristics.CONCURRENT
//            ));
//        }
//    }
}
