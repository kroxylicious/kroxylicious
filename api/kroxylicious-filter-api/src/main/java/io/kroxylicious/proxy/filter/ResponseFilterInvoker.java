/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter;

import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;

record ResponseFilterInvoker(ResponseFilter filter) implements FilterInvoker {

    @Override
    public void onResponse(ApiKeys apiKey, short apiVersion, ResponseHeaderData header, ApiMessage body, KrpcFilterContext filterContext) {
        filter.onResponse(apiKey, header, body, filterContext);
    }

    @Override
    public boolean shouldHandleResponse(ApiKeys apiKey, short apiVersion) {
        return filter.shouldHandleResponse(apiKey, apiVersion);
    }

}
