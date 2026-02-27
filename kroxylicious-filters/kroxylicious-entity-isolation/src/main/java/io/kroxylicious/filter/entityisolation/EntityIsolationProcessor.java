/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.entityisolation;

import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiMessage;

import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.RequestFilterResult;
import io.kroxylicious.proxy.filter.ResponseFilterResult;

interface EntityIsolationProcessor<Q extends ApiMessage, S extends ApiMessage> {

    default boolean shouldHandleRequest(short apiVersion) {
        return false;
    }

    default CompletionStage<RequestFilterResult> onRequest(RequestHeaderData header,
                                                           short apiVersion,
                                                           Q request,
                                                           FilterContext context,
                                                           MapperContext mapperContext) {

        return context.forwardRequest(header, request);
    }

    default boolean shouldHandleResponse(short apiVersion) {
        return false;
    }

    default CompletionStage<ResponseFilterResult> onResponse(ResponseHeaderData header,
                                                             short apiVersion,
                                                             S response,
                                                             FilterContext context,
                                                             MapperContext mapperContext) {
        return context.forwardResponse(header, response);
    }

}