/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter;

import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;

record RequestResponseInvoker(
        RequestFilter requestFilter,
        ResponseFilter responseFilter
) implements FilterInvoker {

    @Override
    public CompletionStage<RequestFilterResult> onRequest(ApiKeys apiKey, short apiVersion, RequestHeaderData header, ApiMessage body, FilterContext filterContext) {
        return requestFilter.onRequest(apiKey, header, body, filterContext);
    }

    @Override
    public CompletionStage<ResponseFilterResult> onResponse(ApiKeys apiKey, short apiVersion, ResponseHeaderData header, ApiMessage body, FilterContext filterContext) {
        return responseFilter.onResponse(apiKey, header, body, filterContext);
    }

    @Override
    public boolean shouldHandleRequest(ApiKeys apiKey, short apiVersion) {
        return requestFilter.shouldHandleRequest(apiKey, apiVersion);
    }

    @Override
    public boolean shouldHandleResponse(ApiKeys apiKey, short apiVersion) {
        return responseFilter.shouldHandleResponse(apiKey, apiVersion);
    }

}
