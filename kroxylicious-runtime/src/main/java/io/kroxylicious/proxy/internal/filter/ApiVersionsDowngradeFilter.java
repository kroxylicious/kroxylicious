/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.filter;

import java.util.Objects;
import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.message.ApiVersionsRequestData;
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.common.protocol.Writable;

import io.kroxylicious.proxy.filter.ApiVersionsRequestFilter;
import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.RequestFilterResult;
import io.kroxylicious.proxy.frame.DecodedRequestFrame;
import io.kroxylicious.proxy.internal.ApiVersionsServiceImpl;

import edu.umd.cs.findbugs.annotations.NonNull;

public class ApiVersionsDowngradeFilter implements ApiVersionsRequestFilter {

    private final ApiVersionsServiceImpl apiVersionsService;

    public ApiVersionsDowngradeFilter(@NonNull ApiVersionsServiceImpl apiVersionsService) {
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

    public static DecodedRequestFrame<ApiVersionsRequestData> downgradeApiVersionsFrame(int correlationId) {
        RequestHeaderData requestHeaderData = apiVersionsRequestDowngradeHeader(correlationId);
        return new DecodedRequestFrame<>(
                requestHeaderData.requestApiVersion(), correlationId, true, requestHeaderData, new DowngradeApiVersionsRequestData(), -1 /* KW FIXME */);
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
