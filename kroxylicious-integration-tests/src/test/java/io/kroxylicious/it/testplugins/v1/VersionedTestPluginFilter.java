/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.it.testplugins.v1;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.types.RawTaggedField;

import io.kroxylicious.proxy.filter.ApiVersionsResponseFilter;
import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.ResponseFilterResult;

/**
 * Filter that adds a tagged field to request headers with the v1 message.
 */
public class VersionedTestPluginFilter implements ApiVersionsResponseFilter {

    private final String message;
    private final int tag;

    public VersionedTestPluginFilter(String message, int tag) {
        this.message = message;
        this.tag = tag;
    }

    @Override
    public CompletionStage<ResponseFilterResult> onApiVersionsResponse(short apiVersion, ResponseHeaderData header, ApiVersionsResponseData response,
                                                                       FilterContext context) {
        response.unknownTaggedFields().add(new RawTaggedField(tag, message.getBytes(StandardCharsets.UTF_8)));
        return context.forwardResponse(header, response);
    }
}
