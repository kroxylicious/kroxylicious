/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.authorization;

import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiMessage;

import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.RequestFilterResult;
import io.kroxylicious.proxy.filter.ResponseFilterResult;

public abstract class ApiEnforcement<Q extends ApiMessage, S extends ApiMessage> {

    abstract short minSupportedVersion();

    abstract short maxSupportedVersion();

    abstract CompletionStage<RequestFilterResult> onRequest(RequestHeaderData header,
                                                            Q request,
                                                            FilterContext context,
                                                            AuthorizationFilter authorizationFilter);

    CompletionStage<ResponseFilterResult> onResponse(ResponseHeaderData header,
                                                     S response,
                                                     FilterContext context,
                                                     AuthorizationFilter authorizationFilter) {
        return context.forwardResponse(header, authorizationFilter.popAndApplyInflightState(header, response));
    }

}
