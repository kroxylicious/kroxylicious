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

import io.kroxylicious.proxy.plugin.PluginConfigType;
import io.kroxylicious.proxy.plugin.Plugins;

@PluginConfigType(FixedClientIdFilterFactory.Config.class)
public class FixedClientIdFilterFactory implements FilterFactory<FixedClientIdFilterFactory.Config, FixedClientIdFilterFactory.Config> {

    @Override
    public Config initialize(FilterFactoryContext context, Config config) {
        return Plugins.requireConfig(this, config);
    }

    @Override
    public Filter createFilter(FilterFactoryContext context, Config configuration) {
        return new Filter(configuration);
    }

    public static class Filter implements RequestFilter, ResponseFilter {

        private final String clientId;

        Filter(Config config) {
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

    }

    public static class Config {

        private final String clientId;

        public Config(String clientId) {
            this.clientId = clientId;
        }

        public String getClientId() {
            return clientId;
        }
    }
}
