package com.ocado.config;

import com.ocado.config.Channel.Channel;
import com.ocado.config.eventtype.EventType;
import com.ocado.config.eventtype.LegacyEventType;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collector;
import java.util.stream.IntStream;

import static com.ocado.config.Collectors.FromExecutionEnvironment.*;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toSet;
import static org.junit.jupiter.api.Assertions.*;

class ExecutionEnvironmentTest {

    private static final LegacyEventType LEGACY_A = new LegacyEventType("A");
    private static final LegacyEventType LEGACY_B = new LegacyEventType("B");
    private static final LegacyEventType LEGACY_C = new LegacyEventType("C");
    private static final LegacyEventType LEGACY_D = new LegacyEventType("D");

    @Test
    void testReducer() {
        LegacyExecutionEnvironment legacyExecutionEnvironment = new LegacyExecutionEnvironment();

        HashSet<EventType> eventTypes = new HashSet<>(Arrays.asList(LEGACY_A));
        legacyExecutionEnvironment.setEventTypes(eventTypes);
        HashMap<EventType, Set<ExecutionEnvironment>> eventTypeSetHashMap = new HashMap<>();
        toEnvToEventTypeMap().accumulator().accept(eventTypeSetHashMap, legacyExecutionEnvironment);
        assertTrue(eventTypeSetHashMap.containsKey(LEGACY_A));

        eventTypeSetHashMap.get(LEGACY_A).forEach(env -> assertEquals(legacyExecutionEnvironment, env));


    }

    static Set<ExecutionEnvironment> genExecutionEnvoronments(int envCount, int eventCount) {
        Set<ExecutionEnvironment> executionEnvironments = new HashSet<>();
        for (int i = 0; i < envCount; i++) {
            executionEnvironments.add(genExecutionEnvoronment(i, eventCount));
        }
        return executionEnvironments;
    }

    static ExecutionEnvironment genExecutionEnvoronment(int envid, int eventCount) {
        Random random = new Random();
        Set<EventType> eventTypes = new HashSet<>();
        for (int i = 0; i < eventCount; i++) {
            eventTypes.add(new LegacyEventType(""+ random.nextInt()));
        }
        LegacyExecutionEnvironment executionEnvoronment = new LegacyExecutionEnvironment(envid);
        executionEnvoronment.setEventTypes(eventTypes);
        return executionEnvoronment;
    }

        @Test
    void combiner1() {
        Map<EventType, Set<ExecutionEnvironment>> a = new HashMap<>();
        LegacyExecutionEnvironment legacyExecutionEnvironment1 = new LegacyExecutionEnvironment(1);
        LegacyExecutionEnvironment legacyExecutionEnvironment2 = new LegacyExecutionEnvironment(2);
        LegacyExecutionEnvironment legacyExecutionEnvironment3 = new LegacyExecutionEnvironment(3);
        a.put(LEGACY_A, new HashSet<>(Arrays.asList(legacyExecutionEnvironment1)));
        a.put(LEGACY_B, new HashSet<>(Arrays.asList(legacyExecutionEnvironment1, legacyExecutionEnvironment2)));
        a.put(LEGACY_C, null);
        a.put(LEGACY_D, null);

        Map<EventType, Set<ExecutionEnvironment>> b = new HashMap<>();
        b.put(LEGACY_B, new HashSet<>(Arrays.asList(legacyExecutionEnvironment2)));
        b.put(LEGACY_C, new HashSet<>(Arrays.asList(legacyExecutionEnvironment3)));
        b.put(LEGACY_D, null);


        toEnvToEventTypeMap().combiner().apply(a, b);
        System.out.println(a);
        assertTrue(a.get(LEGACY_A).containsAll(Arrays.asList(legacyExecutionEnvironment1)));
        assertEquals(1, a.get(LEGACY_A).size());
        assertTrue(a.get(LEGACY_B).containsAll(Arrays.asList(legacyExecutionEnvironment1, legacyExecutionEnvironment2)));
        assertEquals(2, a.get(LEGACY_B).size());
        assertTrue(a.get(LEGACY_C).containsAll(Arrays.asList(legacyExecutionEnvironment3)));
        assertEquals(1, a.get(LEGACY_C).size());
        assertNull(a.get(LEGACY_D));
    }


    @Test
    void combinerTimer() {
        Map<EventType, Set<ExecutionEnvironment>> a = new HashMap<>();
        LegacyExecutionEnvironment legacyExecutionEnvironment1 = new LegacyExecutionEnvironment(1);
        LegacyExecutionEnvironment legacyExecutionEnvironment2 = new LegacyExecutionEnvironment(2);
        LegacyExecutionEnvironment legacyExecutionEnvironment3 = new LegacyExecutionEnvironment(3);
        a.put(LEGACY_A, new HashSet<>(Arrays.asList(legacyExecutionEnvironment1)));
        a.put(LEGACY_B, new HashSet<>(Arrays.asList(legacyExecutionEnvironment1, legacyExecutionEnvironment2)));
        a.put(LEGACY_C, null);
        a.put(LEGACY_D, null);

        Map<EventType, Set<ExecutionEnvironment>> b = new HashMap<>();
        b.put(LEGACY_B, new HashSet<>(Arrays.asList(legacyExecutionEnvironment2)));
        b.put(LEGACY_C, new HashSet<>(Arrays.asList(legacyExecutionEnvironment3)));
        b.put(LEGACY_D, null);

        assertTimeout(Duration.ofMillis(500), () -> {
            IntStream.of(1000000)
                    .forEach(i -> toEnvToEventTypeMap().combiner().apply(a, b));

        });
    }

    @Test
    void reducerTimer() {
        Set<ExecutionEnvironment> executionEnvironments = ExecutionEnvironmentTest.genExecutionEnvoronments(20, 20);

        int timeout = 500;
        final int runs = 1000000;
        assertTimeout(Duration.ofMillis(timeout), () ->
            IntStream.of(runs).forEach(i ->
                    executionEnvironments.stream()
                            //.reduce(new HashMap<>(), accumulator, combinerTwoMapsOfSetsOfInts)
                    .collect(toEnvToEventTypeMap())
            )

        );
        System.out.println("Completed on avg under " + (double) timeout / runs + "ms");
        System.out.println("Can do more than " +  (int)(1000d / timeout * runs) + " runs/sec");

    }

    @Test
    void reducerTimerCalc() {

        final int runs = 10000;
        long currentTimeMillis = System.currentTimeMillis();
        Set<ExecutionEnvironment> executionEnvironments = ExecutionEnvironmentTest.genExecutionEnvoronments(20, 20);
        for (int i = 0; i < runs; i++) {
            Map<EventType, Set<ExecutionEnvironment>> collect = executionEnvironments.stream()
                        .reduce(new HashMap<>(), toEnvToEventTypeMap().accumulatorFn(), toEnvToEventTypeMap().combiner());
//                    .collect(toEnvToEventTypeMap());
//                    .collect(toConcurrentEnvToEventTypeMap());
//            if (collect.size() != 400)
//                fail();
        }

        long runTime = System.currentTimeMillis() - currentTimeMillis;

        System.out.println("Completed on avg under " + (double) runTime / runs + "ms");
        System.out.println("Did ≈" +  (int)(1000d / runTime * runs) + " runs/sec");

    }

    @Test
    void reducerLarge() {
        Set<ExecutionEnvironment> executionEnvironments = ExecutionEnvironmentTest.genExecutionEnvoronments(12, 12);
        Map<EventType, Set<ExecutionEnvironment>> reduce = executionEnvironments.stream()
//                .reduce(new HashMap<>(), accumulator, combinerTwoMapsOfSetsOfInts);
                .collect(toEnvToEventTypeMap());
        System.out.println(reduce);
    }

    @Test
    void collectorMapsTwoEnvs() {

        Set<ExecutionEnvironment> executionEnvironments = new HashSet<>();


        Set<EventType> eventTypes1 = new HashSet<>();
        eventTypes1.add(LEGACY_A);
        eventTypes1.add(LEGACY_B);
        eventTypes1.add(LEGACY_C);

        LegacyExecutionEnvironment legacyExecutionEnvironment1 = new LegacyExecutionEnvironment(1);
        legacyExecutionEnvironment1.setEventTypes(eventTypes1);

        executionEnvironments.add(legacyExecutionEnvironment1);


        Set<EventType> eventTypes2 = new HashSet<>();
        eventTypes2.add(LEGACY_B);
        eventTypes2.add(LEGACY_C);
        eventTypes2.add(LEGACY_D);

        LegacyExecutionEnvironment legacyExecutionEnvironment2 = new LegacyExecutionEnvironment(2);
        legacyExecutionEnvironment2.setEventTypes(eventTypes2);

        executionEnvironments.add(legacyExecutionEnvironment2);

        Map<EventType, Set<ExecutionEnvironment>> eventTypeEnvMap =
                executionEnvironments.stream().collect(toEnvToEventTypeMap());

        assertEquals(4, eventTypeEnvMap.size());

        assertTrue(eventTypeEnvMap.containsKey(LEGACY_A));


        assertEquals(1, eventTypeEnvMap.get(LEGACY_A).size());
        assertTrue(eventTypeEnvMap.get(LEGACY_A).contains(legacyExecutionEnvironment1));

        assertEquals(2, eventTypeEnvMap.get(LEGACY_B).size());
        assertTrue(eventTypeEnvMap.get(LEGACY_B).contains(legacyExecutionEnvironment1));
        assertTrue(eventTypeEnvMap.get(LEGACY_B).contains(legacyExecutionEnvironment2));

        assertEquals(2, eventTypeEnvMap.get(LEGACY_C).size());
        assertTrue(eventTypeEnvMap.get(LEGACY_C).contains(legacyExecutionEnvironment1));
        assertTrue(eventTypeEnvMap.get(LEGACY_C).contains(legacyExecutionEnvironment2));

        assertEquals(1, eventTypeEnvMap.get(LEGACY_D).size());
        assertTrue(eventTypeEnvMap.get(LEGACY_D).contains(legacyExecutionEnvironment2));


    }

    @Test
    void groupBy() {
        Set<ExecutionEnvironment> executionEnvironments = ExecutionEnvironmentTest.genExecutionEnvoronments(12, 12);
        Collector<ExecutionEnvironment, ?, Map<Channel, Set<ExecutionEnvironment>>> executionEnvironmentMapCollector = groupingBy(ExecutionEnvironment::getInputChannel, toSet());
        Map<Channel, Set<ExecutionEnvironment>> collect = executionEnvironments.stream()
                .collect(toEnvToChannelEnvMap());

        System.out.println(collect);


    }
}