/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.filter;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletionStage;

import io.kroxylicious.proxy.filter.ApiVersionsRequestFilter;

import io.kroxylicious.proxy.filter.RequestFilterResult;

import org.apache.kafka.common.message.ApiVersionsRequestData;
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;

import io.kroxylicious.proxy.filter.ApiVersionsResponseFilter;
import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.ResponseFilterResult;
import io.kroxylicious.proxy.internal.ApiVersionsServiceImpl;

import org.apache.kafka.common.protocol.Errors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.kroxylicious.proxy.internal.codec.KafkaRequestDecoder.UNSUPPORTED_VERSION_REMOVE_BEFORE_FORWARD;

/**
 * Changes an API_VERSIONS response so that a client sees the intersection of supported version ranges for each
 * API key. This is an intrinsic part of correctly acting as a proxy.
 */
public class ApiVersionsIntersectFilter implements ApiVersionsRequestFilter, ApiVersionsResponseFilter {
    private final ApiVersionsServiceImpl apiVersionsService;
    private final Set<Integer> unsupportedApiVersionsRequest = new HashSet<>();
    private static final Logger LOGGER = LoggerFactory.getLogger(ApiVersionsIntersectFilter.class);

    public ApiVersionsIntersectFilter(ApiVersionsServiceImpl service) {
        this.apiVersionsService = service;
    }


    @Override
    public CompletionStage<ResponseFilterResult> onApiVersionsResponse(short apiVersion, ResponseHeaderData header, ApiVersionsResponseData data,
                                                                       FilterContext context) {
        if(unsupportedApiVersionsRequest.contains(header.correlationId())) {
            LOGGER.info("setting unsupported error response version for {}", header.correlationId());
            data.setErrorCode(Errors.UNSUPPORTED_VERSION.code());
            unsupportedApiVersionsRequest.remove(header.correlationId());
        }
        apiVersionsService.updateVersions(context.channelDescriptor(), data);
        return context.forwardResponse(header, data);
    }

    @Override
    public CompletionStage<RequestFilterResult> onApiVersionsRequest(short apiVersion, RequestHeaderData header, ApiVersionsRequestData request, FilterContext context) {
        if(UNSUPPORTED_VERSION_REMOVE_BEFORE_FORWARD.equals(request.clientSoftwareName())) {
            LOGGER.info("marking {} as an unsupported request", header.correlationId());
            unsupportedApiVersionsRequest.add(header.correlationId());
            request.setClientSoftwareName("");
        }
        return context.forwardRequest(header, request);
    }
}
