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

import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.types.RawTaggedField;

public class GenericRequestSpecificResponseFilter implements RequestFilter, ApiVersionsResponseFilter {

    public static final int CLIENT_ID_TAG = 99;
    Map<Integer, String> clientIdPerCorrelationId = new HashMap<>();

    @Override
    public CompletionStage<RequestFilterResult> onRequest(ApiKeys apiKey, RequestHeaderData header, ApiMessage request, FilterContext context) {
        String clientId = header.clientId();
        clientIdPerCorrelationId.put(header.correlationId(), clientId);
        return context.forwardRequest(header, request);
    }

    @Override
    public CompletionStage<ResponseFilterResult> onApiVersionsResponse(short apiVersion, ResponseHeaderData header, ApiVersionsResponseData response,
                                                                       FilterContext context) {
        String requestClientId = clientIdPerCorrelationId.remove(header.correlationId());
        response.unknownTaggedFields().add(new RawTaggedField(CLIENT_ID_TAG, requestClientId.getBytes(StandardCharsets.UTF_8)));
        return context.forwardResponse(header, response);
    }
}
