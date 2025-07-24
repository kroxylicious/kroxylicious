/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.message.ApiVersionsRequestData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.types.RawTaggedField;

public class GenericResponseSpecificRequestFilter implements ResponseFilter, ApiVersionsRequestFilter {
    public static final int CLIENT_ID_TAG = 100;
    Map<Integer, String> clientIdPerCorrelationId = new HashMap<>();

    @Override
    public CompletionStage<RequestFilterResult> onApiVersionsRequest(short apiVersion, RequestHeaderData header, ApiVersionsRequestData request, FilterContext context) {
        String clientId = header.clientId();
        clientIdPerCorrelationId.put(header.correlationId(), clientId);
        return context.forwardRequest(header, request);
    }

    @Override
    public CompletionStage<ResponseFilterResult> onResponse(ApiKeys apiKey, ResponseHeaderData header, ApiMessage response, FilterContext context) {
        String requestClientId = clientIdPerCorrelationId.remove(header.correlationId());
        response.unknownTaggedFields().add(new RawTaggedField(CLIENT_ID_TAG, requestClientId.getBytes(StandardCharsets.UTF_8)));
        return context.forwardResponse(header, response);
    }
}
