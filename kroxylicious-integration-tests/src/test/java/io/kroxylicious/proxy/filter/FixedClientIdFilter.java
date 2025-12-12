/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter;

import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;

public class FixedClientIdFilter implements RequestFilter, ResponseFilter {

    private final String clientId;

    FixedClientIdFilter(FixedClientIdFilterConfig config) {
        this.clientId = config.clientId();
    }

    @Override
    public CompletionStage<RequestFilterResult> onRequest(ApiKeys apiKey, short apiVersion, RequestHeaderData header, ApiMessage request, FilterContext context) {
        header.setClientId(clientId);
        return context.forwardRequest(header, request);
    }

    @Override
    public CompletionStage<ResponseFilterResult> onResponse(ApiKeys apiKey, short apiVersion, ResponseHeaderData header, ApiMessage response, FilterContext context) {
        return context.forwardResponse(header, response);
    }

    public record FixedClientIdFilterConfig(String clientId) {

    }
}
