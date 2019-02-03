package com.ocado.config.Collectors;

import com.ocado.config.Channel.Channel;
import com.ocado.config.ExecutionEnvironment;

import java.util.Collections;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;

public class EnvToChannelEnvMap implements Collector<ExecutionEnvironment, Map<Channel, Set<ExecutionEnvironment>>, Map<Channel, Set<ExecutionEnvironment>>> {
    @Override
    public Supplier<Map<Channel, Set<ExecutionEnvironment>>> supplier() {
        return ConcurrentHashMap::new;
    }

    @Override
    public BiConsumer<Map<Channel, Set<ExecutionEnvironment>>, ExecutionEnvironment> accumulator() {
        return (map, executionEnvironment) -> {
            Channel inputChannel = executionEnvironment.getInputChannel();
            map.putIfAbsent(inputChannel, ConcurrentHashMap.newKeySet());
            map.computeIfPresent(inputChannel, (key, value)-> {
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
