/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.benchmarking.jmh;

import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.protocol.ApiKeys;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import io.kroxylicious.benchmarking.jmh.filters.EightInterfaceFilter;
import io.kroxylicious.benchmarking.jmh.filters.FourInterfaceFilter;
import io.kroxylicious.benchmarking.jmh.filters.OneInterfaceFilter;
import io.kroxylicious.benchmarking.jmh.filters.TwoInterfaceFilter;
import io.kroxylicious.proxy.internal.filter.FilterInvoker;
import io.kroxylicious.proxy.internal.filter.FilterInvokers;

@Fork(value = 2, jvmArgsAppend = "-XX:LoopUnrollLimit=1")
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 500, timeUnit = TimeUnit.MILLISECONDS)
public class InvokerBenchmark {

    @org.openjdk.jmh.annotations.State(Scope.Benchmark)
    public static class BenchState {
        final FilterInvoker arrayInvokerHasOneInterface = FilterInvokers.arrayInvoker(new OneInterfaceFilter());
        final FilterInvoker arrayInvokerHasTwoInterfaces = FilterInvokers.arrayInvoker(new TwoInterfaceFilter());
        final FilterInvoker arrayInvokerHasFourInterfaces = FilterInvokers.arrayInvoker(new FourInterfaceFilter());
        final FilterInvoker arrayInvokerHasEightInterfaces = FilterInvokers.arrayInvoker(new EightInterfaceFilter());
    }

    @Benchmark
    public void testArrayInvokerHasOneInterface(BenchState state, Blackhole blackhole) {
        invoke(blackhole, state.arrayInvokerHasOneInterface);
    }

    @Benchmark
    public void testArrayInvokerHasTwoInterfaces(BenchState state, Blackhole blackhole) {
        invoke(blackhole, state.arrayInvokerHasTwoInterfaces);
    }

    @Benchmark
    public void testArrayInvokerHasFourInterfaces(BenchState state, Blackhole blackhole) {
        invoke(blackhole, state.arrayInvokerHasFourInterfaces);
    }

    @Benchmark
    public void testArrayInvokerHasEightInterfaces(BenchState state, Blackhole blackhole) {
        invoke(blackhole, state.arrayInvokerHasEightInterfaces);
    }

    private static void invoke(Blackhole blackhole, FilterInvoker filter) {
        blackhole.consume(filter.shouldHandleRequest(ApiKeys.PRODUCE, ApiKeys.PRODUCE.latestVersion()));
        blackhole.consume(filter.shouldHandleRequest(ApiKeys.FETCH, ApiKeys.FETCH.latestVersion()));
        blackhole.consume(filter.shouldHandleResponse(ApiKeys.PRODUCE, ApiKeys.PRODUCE.latestVersion()));
        blackhole.consume(filter.shouldHandleResponse(ApiKeys.FETCH, ApiKeys.FETCH.latestVersion()));
    }

}
