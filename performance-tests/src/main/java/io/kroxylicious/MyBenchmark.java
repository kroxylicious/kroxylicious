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
        volatile FilterInvoker instanceOfInvokerFilterHasTwoInterfaces = FilterInvokers.instanceOfInvoker(new TwoInterfaceFilter());
        volatile FilterInvoker instanceOfInvokerFilterHasFourInterfaces = FilterInvokers.instanceOfInvoker(new FourInterfaceFilter());
        volatile FilterInvoker instanceOfInvokerFilterHasEightInterfaces = FilterInvokers.instanceOfInvoker(new EightInterfaceFilter());
        volatile FilterInvoker arrayInvokerFilterHasTwoInterfaces = FilterInvokers.arrayInvoker(new TwoInterfaceFilter());
        volatile FilterInvoker arrayInvokerFilterHasFourInterfaces = FilterInvokers.arrayInvoker(new FourInterfaceFilter());
        volatile FilterInvoker arrayInvokerFilterHasEightInterfaces = FilterInvokers.arrayInvoker(new EightInterfaceFilter());
        volatile FilterInvoker instanceOfInvokerFilterHasOneInterface = FilterInvokers.instanceOfInvoker(new OneInterfaceFilter());
        volatile FilterInvoker arrayInvokerFilterHasOneInterface = FilterInvokers.arrayInvoker(new OneInterfaceFilter());
    }

    @Benchmark
    public void testInstanceOfInvokerFilterHasTwoInterfaces(BenchState state, Blackhole blackhole) {
        invoke(blackhole, state.instanceOfInvokerFilterHasTwoInterfaces);
    }

    @Benchmark
    public void testArrayInvokerFilterHasTwoInterfaces(BenchState state, Blackhole blackhole) {
        invoke(blackhole, state.arrayInvokerFilterHasTwoInterfaces);
    }

    @Benchmark
    public void testInstanceOfInvokerFilterHasFourInterfaces(BenchState state, Blackhole blackhole) {
        invoke(blackhole, state.instanceOfInvokerFilterHasFourInterfaces);
    }

    @Benchmark
    public void testArrayInvokerFilterHasFourInterfaces(BenchState state, Blackhole blackhole) {
        invoke(blackhole, state.arrayInvokerFilterHasFourInterfaces);
    }

    @Benchmark
    public void testInstanceOfInvokerFilterHasEightInterfaces(BenchState state, Blackhole blackhole) {
        invoke(blackhole, state.instanceOfInvokerFilterHasEightInterfaces);
    }

    @Benchmark
    public void testArrayInvokerFilterHasEightInterfaces(BenchState state, Blackhole blackhole) {
        invoke(blackhole, state.arrayInvokerFilterHasEightInterfaces);
    }

    @Benchmark
    public void testInstanceOfInvokerFilterHasOneInterface(BenchState state, Blackhole blackhole) {
        invoke(blackhole, state.instanceOfInvokerFilterHasOneInterface);
    }

    @Benchmark
    public void testArrayInvokerFilterHasOneInterface(BenchState state, Blackhole blackhole) {
        invoke(blackhole, state.arrayInvokerFilterHasOneInterface);
    }

    private static void invoke(Blackhole blackhole, FilterInvoker filter) {
        blackhole.consume(filter.shouldHandleRequest(ApiKeys.PRODUCE, ApiKeys.PRODUCE.latestVersion()));
        blackhole.consume(filter.shouldHandleRequest(ApiKeys.FETCH, ApiKeys.FETCH.latestVersion()));
        blackhole.consume(filter.shouldHandleResponse(ApiKeys.PRODUCE, ApiKeys.PRODUCE.latestVersion()));
        blackhole.consume(filter.shouldHandleResponse(ApiKeys.FETCH, ApiKeys.FETCH.latestVersion()));
    }

}
