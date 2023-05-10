/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious;

import org.apache.kafka.common.protocol.ApiKeys;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.infra.Blackhole;

import io.kroxylicious.proxy.filter.FilterInvoker;
import io.kroxylicious.proxy.filter.FilterInvokers;

public class MyBenchmark {

    @org.openjdk.jmh.annotations.State(Scope.Benchmark)
    public static class BenchState {
        volatile FilterInvoker instanceOfInvokerFilterHasOneInterface = FilterInvokers.instanceOfInvoker(new OneInterfaceFilter());
        volatile FilterInvoker instanceOfInvokerFilterHasTwoInterfaces = FilterInvokers.instanceOfInvoker(new TwoInterfaceFilter());
        volatile FilterInvoker instanceOfInvokerFilterHasFourInterfaces = FilterInvokers.instanceOfInvoker(new FourInterfaceFilter());
        volatile FilterInvoker instanceOfInvokerFilterHasEightInterfaces = FilterInvokers.instanceOfInvoker(new EightInterfaceFilter());
        volatile FilterInvoker mapInvokerFilterHasOneInterface = FilterInvokers.mapInvoker(new OneInterfaceFilter());
        volatile FilterInvoker mapInvokerFilterHasTwoInterfaces = FilterInvokers.mapInvoker(new TwoInterfaceFilter());
        volatile FilterInvoker mapInvokerFilterHasFourInterfaces = FilterInvokers.mapInvoker(new FourInterfaceFilter());
        volatile FilterInvoker mapInvokerFilterHasEightInterfaces = FilterInvokers.mapInvoker(new EightInterfaceFilter());
        volatile FilterInvoker fieldInvokerFilterHasOneInterface = FilterInvokers.fieldInvoker(new OneInterfaceFilter());
        volatile FilterInvoker fieldInvokerFilterHasTwoInterfaces = FilterInvokers.fieldInvoker(new TwoInterfaceFilter());
        volatile FilterInvoker fieldInvokerFilterHasFourInterfaces = FilterInvokers.fieldInvoker(new FourInterfaceFilter());
        volatile FilterInvoker fieldInvokerFilterHasEightInterfaces = FilterInvokers.fieldInvoker(new EightInterfaceFilter());
        volatile FilterInvoker arrayInvokerFilterHasOneInterface = FilterInvokers.arrayInvoker(new OneInterfaceFilter());
        volatile FilterInvoker arrayInvokerFilterHasTwoInterfaces = FilterInvokers.arrayInvoker(new TwoInterfaceFilter());
        volatile FilterInvoker arrayInvokerFilterHasFourInterfaces = FilterInvokers.arrayInvoker(new FourInterfaceFilter());
        volatile FilterInvoker arrayInvokerFilterHasEightInterfaces = FilterInvokers.arrayInvoker(new EightInterfaceFilter());
    }

    @Benchmark
    public void testInstanceOfInvokerFilterHasOneInterface(BenchState state, Blackhole blackhole) {
        invoke(blackhole, state.instanceOfInvokerFilterHasOneInterface);
    }

    @Benchmark
    public void testInstanceOfInvokerFilterHasTwoInterfaces(BenchState state, Blackhole blackhole) {
        invoke(blackhole, state.instanceOfInvokerFilterHasTwoInterfaces);
    }

    @Benchmark
    public void testInstanceOfInvokerFilterHasFourInterfaces(BenchState state, Blackhole blackhole) {
        invoke(blackhole, state.instanceOfInvokerFilterHasFourInterfaces);
    }

    @Benchmark
    public void testInstanceOfInvokerFilterHasEightInterfaces(BenchState state, Blackhole blackhole) {
        invoke(blackhole, state.instanceOfInvokerFilterHasEightInterfaces);
    }

    @Benchmark
    public void testArrayInvokerFilterHasOneInterface(BenchState state, Blackhole blackhole) {
        invoke(blackhole, state.arrayInvokerFilterHasOneInterface);
    }

    @Benchmark
    public void testArrayInvokerFilterHasTwoInterfaces(BenchState state, Blackhole blackhole) {
        invoke(blackhole, state.arrayInvokerFilterHasTwoInterfaces);
    }

    @Benchmark
    public void testArrayInvokerFilterHasFourInterfaces(BenchState state, Blackhole blackhole) {
        invoke(blackhole, state.arrayInvokerFilterHasFourInterfaces);
    }

    @Benchmark
    public void testArrayInvokerFilterHasEightInterfaces(BenchState state, Blackhole blackhole) {
        invoke(blackhole, state.arrayInvokerFilterHasEightInterfaces);
    }

    @Benchmark
    public void testMapInvokerFilterHasOneInterface(BenchState state, Blackhole blackhole) {
        invoke(blackhole, state.mapInvokerFilterHasOneInterface);
    }

    @Benchmark
    public void testMapInvokerFilterHasTwoInterfaces(BenchState state, Blackhole blackhole) {
        invoke(blackhole, state.mapInvokerFilterHasTwoInterfaces);
    }

    @Benchmark
    public void testMapInvokerFilterHasFourInterfaces(BenchState state, Blackhole blackhole) {
        invoke(blackhole, state.mapInvokerFilterHasFourInterfaces);
    }

    @Benchmark
    public void testMapInvokerFilterHasEightInterfaces(BenchState state, Blackhole blackhole) {
        invoke(blackhole, state.mapInvokerFilterHasEightInterfaces);
    }

    @Benchmark
    public void testFieldInvokerFilterHasOneInterface(BenchState state, Blackhole blackhole) {
        invoke(blackhole, state.fieldInvokerFilterHasOneInterface);
    }

    @Benchmark
    public void testFieldInvokerFilterHasTwoInterfaces(BenchState state, Blackhole blackhole) {
        invoke(blackhole, state.fieldInvokerFilterHasTwoInterfaces);
    }

    @Benchmark
    public void testFieldInvokerFilterHasFourInterfaces(BenchState state, Blackhole blackhole) {
        invoke(blackhole, state.fieldInvokerFilterHasFourInterfaces);
    }

    @Benchmark
    public void testFieldInvokerFilterHasEightInterfaces(BenchState state, Blackhole blackhole) {
        invoke(blackhole, state.fieldInvokerFilterHasEightInterfaces);
    }

    private static void invoke(Blackhole blackhole, FilterInvoker filter) {
        blackhole.consume(filter.shouldHandleRequest(ApiKeys.PRODUCE, ApiKeys.PRODUCE.latestVersion()));
        blackhole.consume(filter.shouldHandleRequest(ApiKeys.FETCH, ApiKeys.FETCH.latestVersion()));
        blackhole.consume(filter.shouldHandleResponse(ApiKeys.PRODUCE, ApiKeys.PRODUCE.latestVersion()));
        blackhole.consume(filter.shouldHandleResponse(ApiKeys.FETCH, ApiKeys.FETCH.latestVersion()));
    }

}
