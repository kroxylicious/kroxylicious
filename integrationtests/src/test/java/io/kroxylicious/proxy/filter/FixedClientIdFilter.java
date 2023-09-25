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

public class FixedClientIdFilter implements RequestFilter, ResponseFilter {

    private final String clientId;

    public static class FixedClientIdFilterConfig {

        private final String clientId;

        public FixedClientIdFilterConfig(String clientId) {
            this.clientId = clientId;
        }

        public String getClientId() {
            return clientId;
        }
    }

    FixedClientIdFilter(FixedClientIdFilterConfig config) {
        this.clientId = config.getClientId();
    }

    @Override
    public CompletionStage<RequestFilterResult> onRequest(ApiKeys apiKey, RequestHeaderData header, ApiMessage request, FilterContext context) {
        header.setClientId(clientId);
        return context.forwardRequest(header, request);
    }

    @Override
    public CompletionStage<ResponseFilterResult> onResponse(ApiKeys apiKey, ResponseHeaderData header, ApiMessage response, FilterContext context) {
        return context.forwardResponse(header, response);
    }

    public static class Contributor implements FilterContributor<FixedClientIdFilterConfig> {
        @Override
        public String getTypeName() {
            return "FixedClientId";
        }

        @Override
        public Class<FixedClientIdFilterConfig> getConfigType() {
            return FixedClientIdFilterConfig.class;
        }

        @Override
        public boolean requiresConfiguration() {
            return true;
        }

        @Override
        public Filter getInstance(FilterConstructContext<FixedClientIdFilterConfig> context) {
            return new FixedClientIdFilter(context.getConfig());
        }
    }
}
