/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.microbenchmarks;

import org.apache.kafka.common.protocol.ApiKeys;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.infra.Blackhole;

import io.kroxylicious.microbenchmarks.filters.EightInterfaceFilter;
import io.kroxylicious.microbenchmarks.filters.FourInterfaceFilter;
import io.kroxylicious.microbenchmarks.filters.OneInterfaceFilter;
import io.kroxylicious.microbenchmarks.filters.TwoInterfaceFilter;
import io.kroxylicious.proxy.filter.FilterInvoker;
import io.kroxylicious.proxy.filter.FilterInvokers;

public class InvokerBenchmark {

    @org.openjdk.jmh.annotations.State(Scope.Benchmark)
    public static class BenchState {
        final FilterInvoker arrayInvokerFilter2HasOneInterface = FilterInvokers.arrayInvoker(new OneInterfaceFilter());
        final FilterInvoker arrayInvokerFilter2HasTwoInterfaces = FilterInvokers.arrayInvoker(new TwoInterfaceFilter());
        final FilterInvoker arrayInvokerFilter2HasFourInterfaces = FilterInvokers.arrayInvoker(new FourInterfaceFilter());
        final FilterInvoker arrayInvokerFilter2HasEightInterfaces = FilterInvokers.arrayInvoker(new EightInterfaceFilter());
    }

    @Benchmark
    public void testArrayInvokerFilter2HasOneInterface(BenchState state, Blackhole blackhole) {
        invoke(blackhole, state.arrayInvokerFilter2HasOneInterface);
    }

    @Benchmark
    public void testArrayInvokerFilter2HasTwoInterfaces(BenchState state, Blackhole blackhole) {
        invoke(blackhole, state.arrayInvokerFilter2HasTwoInterfaces);
    }

    @Benchmark
    public void testArrayInvokerFilter2HasFourInterfaces(BenchState state, Blackhole blackhole) {
        invoke(blackhole, state.arrayInvokerFilter2HasFourInterfaces);
    }

    @Benchmark
    public void testArrayInvokerFilter2HasEightInterfaces(BenchState state, Blackhole blackhole) {
        invoke(blackhole, state.arrayInvokerFilter2HasEightInterfaces);
    }

    private static void invoke(Blackhole blackhole, FilterInvoker filter) {
        blackhole.consume(filter.shouldHandleRequest(ApiKeys.PRODUCE, ApiKeys.PRODUCE.latestVersion()));
        blackhole.consume(filter.shouldHandleRequest(ApiKeys.FETCH, ApiKeys.FETCH.latestVersion()));
        blackhole.consume(filter.shouldHandleResponse(ApiKeys.PRODUCE, ApiKeys.PRODUCE.latestVersion()));
        blackhole.consume(filter.shouldHandleResponse(ApiKeys.FETCH, ApiKeys.FETCH.latestVersion()));
    }

}
