/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter;

import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;

import io.kroxylicious.proxy.config.BaseConfig;

public class ClientIdPrefixingFilter implements RequestFilter, ResponseFilter {

    private final String prefix;

    public static class ClientIdPrefixingFilterConfig extends BaseConfig {

        private final String clientId;

        public ClientIdPrefixingFilterConfig(String clientId) {
            this.clientId = clientId;
        }

        public String getClientId() {
            return clientId;
        }
    }

    ClientIdPrefixingFilter(ClientIdPrefixingFilterConfig config) {
        this.prefix = config.getClientId();
    }

    @Override
    public boolean shouldHandleRequest(ApiKeys apiKey, short apiVersion) {
        return true;
    }

    @Override
    public boolean shouldHandleResponse(ApiKeys apiKey, short apiVersion) {
        return true;
    }

    @Override
    public void onRequest(ApiKeys apiKey, RequestHeaderData header, ApiMessage body, KrpcFilterContext filterContext) {
        header.setClientId(prefix + header.clientId());
        filterContext.forwardRequest(header, body);
    }

    @Override
    public void onResponse(ApiKeys apiKey, ResponseHeaderData header, ApiMessage body, KrpcFilterContext filterContext) {
        filterContext.forwardResponse(header, body);
    }
}
