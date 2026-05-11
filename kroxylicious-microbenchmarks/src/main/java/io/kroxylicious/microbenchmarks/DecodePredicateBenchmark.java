/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.microbenchmarks;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.protocol.ApiKeys;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import io.kroxylicious.microbenchmarks.filters.FourInterfaceFilter0;
import io.kroxylicious.microbenchmarks.filters.FourInterfaceFilter1;
import io.kroxylicious.microbenchmarks.filters.FourInterfaceFilter2;
import io.kroxylicious.microbenchmarks.filters.FourInterfaceFilter3;
import io.kroxylicious.proxy.filter.Filter;
import io.kroxylicious.proxy.internal.codec.DecodePredicate;
import io.kroxylicious.proxy.internal.filter.FilterAndInvoker;
import io.kroxylicious.proxy.internal.filter.FilterInvokers;

// try hard to make shouldHandleXYZ to observe different receivers concrete types, saving unrolling to bias a specific call-site to a specific concrete type
@Fork(value = 2, jvmArgsAppend = "-XX:LoopUnrollLimit=1")
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 500, timeUnit = TimeUnit.MILLISECONDS)
public class DecodePredicateBenchmark {

    @State(Scope.Benchmark)
    public static class BenchState {
        DecodePredicate predicate;
        ApiKeys[] keys;

        @Setup
        public void init() {
            Filter[] filters = {
                    new FourInterfaceFilter0(),
                    new FourInterfaceFilter1(),
                    new FourInterfaceFilter2(),
                    new FourInterfaceFilter3(),
                    new FourInterfaceFilter0(),
                    new FourInterfaceFilter1(),
                    new FourInterfaceFilter2(),
                    new FourInterfaceFilter3()
            };
            List<FilterAndInvoker> filterAndInvokers = Arrays.stream(filters)
                    .map(f -> new FilterAndInvoker(f.getClass().getSimpleName(), f, FilterInvokers.arrayInvoker(f)))
                    .toList();
            predicate = DecodePredicate.forFilters(filterAndInvokers);
            keys = ApiKeys.values();
        }
    }

    @Benchmark
    public void testShouldDecodeRequest(BenchState state, Blackhole blackhole) {
        for (ApiKeys key : state.keys) {
            blackhole.consume(state.predicate.shouldDecodeRequest(key, key.latestVersion()));
        }
    }

    @Benchmark
    public void testShouldDecodeResponse(BenchState state, Blackhole blackhole) {
        for (ApiKeys key : state.keys) {
            blackhole.consume(state.predicate.shouldDecodeResponse(key, key.latestVersion()));
        }
    }
}
