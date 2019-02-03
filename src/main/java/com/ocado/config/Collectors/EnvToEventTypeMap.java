package com.ocado.config.Collectors;

import com.ocado.config.ExecutionEnvironment;
import com.ocado.config.eventtype.EventType;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;

public class EnvToEventTypeMap implements Collector<ExecutionEnvironment, Map<EventType, Set<ExecutionEnvironment>>, Map<EventType, Set<ExecutionEnvironment>>> {

    private static final EnvToEventTypeMap instance = new EnvToEventTypeMap();

    public static EnvToEventTypeMap instance() {
        return instance;
    }

    public EnvToEventTypeMap() {}

    @Override
    public Supplier<Map<EventType, Set<ExecutionEnvironment>>> supplier() {
        return ConcurrentHashMap::new;
    }

    @Override
    public BiConsumer<Map<EventType, Set<ExecutionEnvironment>>, ExecutionEnvironment> accumulator() {
        return (map, executionEnvironment) ->
            executionEnvironment
                .getEventTypes()
                .forEach(eventType -> {
//                    map.merge(eventType, executionEnvironment, (s1, s2) -> {
//                        s1.addAll(s2);
//                        return s1;
//                    });
                    map.putIfAbsent(eventType, ConcurrentHashMap.newKeySet());
                        map.computeIfPresent(eventType, (key, value) -> {
                            value.add(executionEnvironment);
                            return value;

                        });
                });

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
    //                if (valueInB != null) {
    //                    mapA.putIfAbsent(keyInB, new HashSet<>());
    //                    mapA.computeIfPresent(keyInB, (keyInA, valueInA) -> {
    //                        valueInA.addAll(valueInB);
    //                        return valueInA;
    //                    });
    //                }
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
