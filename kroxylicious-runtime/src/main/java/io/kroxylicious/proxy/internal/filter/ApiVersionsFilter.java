/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.filter;

import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.message.ApiVersionsRequestData;
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;

import io.kroxylicious.proxy.filter.ApiVersionsRequestFilter;
import io.kroxylicious.proxy.filter.ApiVersionsResponseFilter;
import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.RequestFilterResult;
import io.kroxylicious.proxy.filter.ResponseFilterResult;

import org.apache.kafka.common.protocol.Errors;

/**
 * TODO
 */
public class ApiVersionsFilter implements ApiVersionsRequestFilter, ApiVersionsResponseFilter {

    boolean unsupportedResponseRequired = false;

    @Override
    public CompletionStage<RequestFilterResult> onApiVersionsRequest(short apiVersion, RequestHeaderData header, ApiVersionsRequestData request, FilterContext context) {
        // if client is using a codec that doesn't support a ApiVersions version known to
        // us, clamp it to the highest/lowest we can do.
        clampRequestApiVersionIfNecessary(header);
        return context.forwardRequest(header, request);
    }

    @Override
    public CompletionStage<ResponseFilterResult> onApiVersionsResponse(short apiVersion, ResponseHeaderData header, ApiVersionsResponseData response,
                                                                       FilterContext context) {
        if (unsupportedResponseRequired) {
            // if the proxy was unable to support the ApiVersion requested by the client,
            // send an UNSUPPORTED_VERSION response.  That will cause the client to negotiate.
            response.setErrorCode(Errors.UNSUPPORTED_VERSION.code());
        }
        return context.forwardResponse(header, response);
    }

    private void clampRequestApiVersionIfNecessary(RequestHeaderData header) {
        var apiVersions = ApiKeys.API_VERSIONS;
        if (header.requestApiVersion() > apiVersions.latestVersion()) {
            unsupportedResponseRequired = true;
            header.setRequestApiVersion(apiVersions.latestVersion());
        }
        if (header.requestApiVersion() < apiVersions.oldestVersion()) {
            unsupportedResponseRequired = true;
            header.setRequestApiVersion(apiVersions.oldestVersion());
        }
    }
}
