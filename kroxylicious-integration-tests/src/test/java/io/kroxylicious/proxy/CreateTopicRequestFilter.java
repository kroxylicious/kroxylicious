/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy;

import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.message.CreateTopicsRequestData;
import org.apache.kafka.common.message.RequestHeaderData;

import io.kroxylicious.proxy.filter.CreateTopicsRequestFilter;
import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.RequestFilterResult;

class CreateTopicRequestFilter implements CreateTopicsRequestFilter {

    @Override
    public CompletionStage<RequestFilterResult> onCreateTopicsRequest(short apiVersion, RequestHeaderData header, CreateTopicsRequestData request,
                                                                      FilterContext context) {
        return context.forwardRequest(header, request);
    }
}
