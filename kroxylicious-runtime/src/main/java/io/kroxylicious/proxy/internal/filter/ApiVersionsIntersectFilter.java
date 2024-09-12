/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.filter;

import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.message.ResponseHeaderData;

import io.kroxylicious.proxy.filter.ApiVersionsResponseFilter;
import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.ResponseFilterResult;
import io.kroxylicious.proxy.internal.ApiVersionsServiceImpl;

/**
 * Changes an API_VERSIONS response so that a client sees the intersection of supported version ranges for each
 * API key. This is an intrinsic part of correctly acting as a proxy.
 */
public class ApiVersionsIntersectFilter implements ApiVersionsResponseFilter {
    private final ApiVersionsServiceImpl apiVersionsService;

    public ApiVersionsIntersectFilter(ApiVersionsServiceImpl service) {
        this.apiVersionsService = service;
    }

    @Override
    public CompletionStage<ResponseFilterResult> onApiVersionsResponse(
            short apiVersion,
            ResponseHeaderData header,
            ApiVersionsResponseData data,
            FilterContext context
    ) {
        apiVersionsService.updateVersions(context.channelDescriptor(), data);
        return context.forwardResponse(header, data);
    }
}
