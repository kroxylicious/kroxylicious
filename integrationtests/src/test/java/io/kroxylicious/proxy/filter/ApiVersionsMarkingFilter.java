/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter;

import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.types.RawTaggedField;

import io.kroxylicious.proxy.ApiVersionsService;

import static java.nio.charset.StandardCharsets.UTF_8;

public class ApiVersionsMarkingFilter implements RequestFilter {

    public static final int INTERSECTED_API_VERSION_RANGE_TAG = 89;
    public static final int UPSTREAM_API_VERSION_RANGE_TAG = 90;

    @Override
    public CompletionStage<RequestFilterResult> onRequest(ApiKeys apiKey, RequestHeaderData header, ApiMessage request, FilterContext context) {
        return context.getApiVersionsService().getApiVersionRanges(apiKey).thenCompose(apiVersionRanges -> {
            if (apiVersionRanges.isEmpty()) {
                return context.requestFilterResultBuilder().withCloseConnection().completed();
            }
            ApiVersionsService.ApiVersionRanges versionRanges = apiVersionRanges.get();
            ApiVersionsResponseData.ApiVersion intersected = versionRanges.intersected();
            request.unknownTaggedFields()
                    .add(new RawTaggedField(INTERSECTED_API_VERSION_RANGE_TAG, (intersected.minVersion() + "-" + intersected.maxVersion()).getBytes(
                            UTF_8)));
            ApiVersionsResponseData.ApiVersion upstream = versionRanges.upstream();
            request.unknownTaggedFields().add(new RawTaggedField(UPSTREAM_API_VERSION_RANGE_TAG, (upstream.minVersion() + "-" + upstream.maxVersion()).getBytes(
                    UTF_8)));
            return context.forwardRequest(header, request);
        });
    }

}
