/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.it.testplugins;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

import io.kroxylicious.kafka.common.message.ProduceRequestData;
import io.kroxylicious.kafka.common.message.RequestHeaderData;

import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.ProduceRequestFilter;
import io.kroxylicious.proxy.filter.RequestFilterResult;

/**
 * A filter that counts Produce requests only. Unlike {@link RequestCountingFilter}
 * which implements {@link io.kroxylicious.proxy.filter.RequestFilter} (all API keys),
 * this implements {@link ProduceRequestFilter} so it only requires Produce requests
 * to be decoded. Used to test that nested router filters with specific decode
 * requirements are correctly propagated to the {@code DecodePredicate}.
 */
public class ProduceCountingFilter implements ProduceRequestFilter {

    private static final ConcurrentHashMap<String, LongAdder> COUNTERS = new ConcurrentHashMap<>();

    public static long countFor(String id) {
        var adder = COUNTERS.get(id);
        return adder == null ? 0 : adder.longValue();
    }

    public static void reset(String id) {
        COUNTERS.remove(id);
    }

    private final String counterId;

    ProduceCountingFilter(String counterId) {
        this.counterId = counterId;
    }

    @Override
    public CompletionStage<RequestFilterResult> onProduceRequest(short apiVersion, RequestHeaderData header, ProduceRequestData request,
                                                                 FilterContext context) {
        COUNTERS.computeIfAbsent(counterId, k -> new LongAdder()).increment();
        return context.forwardRequest(header, request);
    }
}
