/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.entityisolation;

import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiMessage;

import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.ResponseFilterResult;

interface ResponseEntityIsolationProcessor<Q extends ApiMessage, S extends ApiMessage> {

    boolean shouldHandleRequest(short apiVersion);

    default CompletionStage<ResponseFilterResult> onResponse(ResponseHeaderData header,
                                                             short apiVersion,
                                                             S response,
                                                             FilterContext context,
                                                             EntityIsolationFilter entityIsolationFilter) {
        return context.forwardResponse(header, response);
    }

}