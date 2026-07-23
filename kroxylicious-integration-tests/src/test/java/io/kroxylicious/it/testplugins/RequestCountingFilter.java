/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.it.testplugins;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;

import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.RequestFilter;
import io.kroxylicious.proxy.filter.RequestFilterResult;

/**
 * A filter that counts the number of times it is invoked per API key, using static state
 * keyed by a caller-supplied ID so that the test and the filter instance running inside the
 * proxy can share data without a network boundary.
 */
public class RequestCountingFilter implements RequestFilter {

    private static final ConcurrentHashMap<String, ConcurrentHashMap<ApiKeys, LongAdder>> COUNTERS = new ConcurrentHashMap<>();

    public static long countFor(String id, ApiKeys apiKey) {
        var byKey = COUNTERS.get(id);
        if (byKey == null) {
            return 0;
        }
        var adder = byKey.get(apiKey);
        return adder == null ? 0 : adder.longValue();
    }

    public static void reset(String id) {
        COUNTERS.remove(id);
    }

    private final String counterId;

    RequestCountingFilter(String counterId) {
        this.counterId = counterId;
    }

    @Override
    public CompletionStage<RequestFilterResult> onRequest(ApiKeys apiKey, short apiVersion, RequestHeaderData header, ApiMessage body, FilterContext context) {
        COUNTERS.computeIfAbsent(counterId, k -> new ConcurrentHashMap<>())
                .computeIfAbsent(apiKey, k -> new LongAdder())
                .increment();
        return context.forwardRequest(header, body);
    }
}
