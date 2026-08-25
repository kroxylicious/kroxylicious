/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.filter.impl;

import java.util.Objects;
import java.util.concurrent.CompletionStage;

import io.kroxylicious.kafka.common.message.ApiVersionsRequestData;
import io.kroxylicious.kafka.common.message.ApiVersionsResponseData;
import io.kroxylicious.kafka.common.message.RequestHeaderData;
import io.kroxylicious.kafka.common.protocol.ApiKeys;
import io.kroxylicious.kafka.common.protocol.Errors;
import io.kroxylicious.kafka.common.protocol.ObjectSerializationCache;
import io.kroxylicious.kafka.common.protocol.Writable;

import io.kroxylicious.proxy.filter.ApiVersionsRequestFilter;
import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.RequestFilterResult;
import io.kroxylicious.proxy.frame.DecodedRequestFrame;
import io.kroxylicious.proxy.internal.ApiVersionsServiceImpl;

/**
 * Filter that short-circuits ApiVersions requests received at an api version higher than the
 * proxy supports, responding directly with {@code UNSUPPORTED_VERSION} and the versions the
 * proxy does support, without forwarding to any broker.
 */
public class ApiVersionsDowngradeFilter implements ApiVersionsRequestFilter {

    private final ApiVersionsServiceImpl apiVersionsService;

    /**
     * Creates the filter.
     *
     * @param apiVersionsService the service that knows the api versions supported by the proxy
     */
    public ApiVersionsDowngradeFilter(ApiVersionsServiceImpl apiVersionsService) {
        this.apiVersionsService = Objects.requireNonNull(apiVersionsService);
    }

    /**
     * This subclass is used when we receive an ApiVersions request at an api version higher
     * than the proxy supports. It should be handled internally with a short-circuit response
     * and not forwarded to any broker. Only the ApiVersionsDowngradeFilter should be exposed to
     * this type.
     */
    private static class DowngradeApiVersionsRequestData extends ApiVersionsRequestData {

        private DowngradeApiVersionsRequestData() {
            super();
        }

        @Override
        public void write(Writable writable, ObjectSerializationCache cache, short version) {
            throw new UnsupportedOperationException("DowngradeApiVersionsRequestData is read-only");
        }

        @Override
        public int size(ObjectSerializationCache cache, short version) {
            throw new UnsupportedOperationException("DowngradeApiVersionsRequestData is read-only");
        }
    }

    /**
     * This subclass is used when we receive an ApiVersions request at an api version higher
     * than the proxy supports. It should be handled internally with a short-circuit response
     * and not forwarded further to any broker. Only the ApiVersionsDowngradeFilter should be exposed to
     * this type.
     */
    private static class DowngradeRequestHeaderData extends RequestHeaderData {

        private DowngradeRequestHeaderData() {
            super();
        }

        @Override
        public void write(Writable writable, ObjectSerializationCache cache, short version) {
            throw new UnsupportedOperationException("DowngradeRequestHeaderData is read-only");
        }

        @Override
        public int size(ObjectSerializationCache cache, short version) {
            throw new UnsupportedOperationException("DowngradeRequestHeaderData is read-only");
        }
    }

    private static RequestHeaderData apiVersionsRequestDowngradeHeader(int correlationId) {
        return new DowngradeRequestHeaderData()
                .setCorrelationId(correlationId)
                .setRequestApiKey(ApiKeys.API_VERSIONS.id)
                .setRequestApiVersion((short) 0);
    }

    /**
     * Creates the synthetic ApiVersions request frame used to trigger the downgrade response.
     *
     * @param correlationId the correlation id of the client's original ApiVersions request
     * @return the synthetic request frame
     */
    public static DecodedRequestFrame<ApiVersionsRequestData> downgradeApiVersionsFrame(int correlationId) {
        RequestHeaderData requestHeaderData = apiVersionsRequestDowngradeHeader(correlationId);
        return new DecodedRequestFrame<>(
                requestHeaderData.requestApiVersion(), correlationId, true, requestHeaderData, new DowngradeApiVersionsRequestData());
    }

    @Override
    public CompletionStage<RequestFilterResult> onApiVersionsRequest(short apiVersion, RequestHeaderData header, ApiVersionsRequestData request, FilterContext context) {
        if (request instanceof DowngradeApiVersionsRequestData) {
            ApiVersionsResponseData.ApiVersionCollection collection = new ApiVersionsResponseData.ApiVersionCollection();
            ApiKeys apiVersions = ApiKeys.API_VERSIONS;
            ApiVersionsResponseData.ApiVersion version = new ApiVersionsResponseData.ApiVersion()
                    .setApiKey(apiVersions.id)
                    .setMinVersion(apiVersions.oldestVersion())
                    .setMaxVersion(apiVersionsService.latestVersion(apiVersions));
            collection.add(version);
            ApiVersionsResponseData message = new ApiVersionsResponseData()
                    .setApiKeys(collection)
                    .setErrorCode(Errors.UNSUPPORTED_VERSION.code());
            return context.requestFilterResultBuilder().shortCircuitResponse(message).completed();
        }
        return context.forwardRequest(header, request);
    }
}
