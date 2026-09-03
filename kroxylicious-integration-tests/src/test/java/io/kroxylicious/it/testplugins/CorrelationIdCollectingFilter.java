/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.it.testplugins;

import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.kroxylicious.kafka.common.message.ListGroupsRequestData;
import io.kroxylicious.kafka.common.message.ListGroupsResponseData;
import io.kroxylicious.kafka.common.message.RequestHeaderData;
import io.kroxylicious.kafka.common.protocol.ApiKeys;
import io.kroxylicious.kafka.common.protocol.ApiMessage;
import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.RequestFilter;
import io.kroxylicious.proxy.filter.RequestFilterResult;

/**
 * Simulates a filter that keys its own bookkeeping off the correlation id the proxy assigns to an
 * out-of-band request - a pattern some real filters legitimately use. For a configured trigger api
 * key, issues an out-of-band {@code LIST_GROUPS} request and records the correlation id the proxy
 * assigned to it (visible on the header passed to {@code sendRequest} once the call returns) against
 * a caller-supplied collector id, using static state so the test can inspect it directly. Used to
 * prove that two concurrent out-of-band requests on one connection are always assigned distinct
 * correlation ids.
 */
public class CorrelationIdCollectingFilter implements RequestFilter {

    private static final ConcurrentHashMap<String, Set<Integer>> OBSERVED = new ConcurrentHashMap<>();

    public static Set<Integer> observedFor(String collectorId) {
        return OBSERVED.getOrDefault(collectorId, Set.of());
    }

    public static void reset(String collectorId) {
        OBSERVED.remove(collectorId);
    }

    private final Config config;

    public CorrelationIdCollectingFilter(Config config) {
        this.config = config;
    }

    public record Config(String collectorId, ApiKeys keyToTrigger) {
        @JsonCreator
        public Config(@JsonProperty(value = "collectorId", required = true) String collectorId,
                      @JsonProperty(value = "keyToTrigger", required = true) ApiKeys keyToTrigger) {
            this.collectorId = collectorId;
            this.keyToTrigger = keyToTrigger;
        }
    }

    @Override
    public CompletionStage<RequestFilterResult> onRequest(ApiKeys apiKey, short apiVersion, RequestHeaderData header, ApiMessage request, FilterContext context) {
        if (apiKey != config.keyToTrigger()) {
            return context.forwardRequest(header, request);
        }
        var oobRequest = new ListGroupsRequestData();
        var oobHeader = new RequestHeaderData().setRequestApiVersion(oobRequest.highestSupportedVersion());
        CompletionStage<ListGroupsResponseData> stage = context.sendRequest(oobHeader, oobRequest);
        OBSERVED.computeIfAbsent(config.collectorId(), k -> ConcurrentHashMap.newKeySet()).add(oobHeader.correlationId());
        return stage.thenCompose(ignored -> context.forwardRequest(header, request));
    }
}
