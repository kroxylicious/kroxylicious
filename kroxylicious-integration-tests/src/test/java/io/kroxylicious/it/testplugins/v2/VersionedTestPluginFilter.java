/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.it.testplugins.v2;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.types.RawTaggedField;

import io.kroxylicious.proxy.filter.ApiVersionsResponseFilter;
import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.ResponseFilterResult;

/**
 * Filter that adds a tagged field to request headers with the v2 integer value.
 */
public class VersionedTestPluginFilter implements ApiVersionsResponseFilter {

    private final int tagValue;
    private final int tag;

    public VersionedTestPluginFilter(int tagValue, int tag) {
        this.tagValue = tagValue;
        this.tag = tag;
    }

    @Override
    public CompletionStage<ResponseFilterResult> onApiVersionsResponse(short apiVersion, ResponseHeaderData header, ApiVersionsResponseData response,
                                                                       FilterContext context) {
        ByteBuffer buffer = ByteBuffer.allocate(4);
        buffer.putInt(tagValue);
        response.unknownTaggedFields().add(new RawTaggedField(tag, buffer.array()));
        return context.forwardResponse(header, response);
    }
}
