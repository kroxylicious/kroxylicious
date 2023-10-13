/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter;

import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.jetbrains.annotations.NotNull;

import io.kroxylicious.proxy.plugin.PluginConfigType;
import io.kroxylicious.proxy.plugin.Plugins;

@PluginConfigType(CompositePrefixingFixedClientIdFilterConfig.class)
public class CompositePrefixingFixedClientIdFilterFactory
        implements FilterFactory<CompositePrefixingFixedClientIdFilterConfig, CompositePrefixingFixedClientIdFilterConfig> {

    @Override
    public CompositePrefixingFixedClientIdFilterConfig initialize(FilterFactoryContext context, CompositePrefixingFixedClientIdFilterConfig config) {
        return Plugins.requireConfig(this, config);
    }

    @NotNull
    @Override
    public CompositePrefixingFixedClientIdFilter createFilter(FilterFactoryContext context,
                                                              CompositePrefixingFixedClientIdFilterConfig configuration) {
        return new CompositePrefixingFixedClientIdFilter(configuration);
    }

    static class PrefixingFilter implements RequestFilter {
        private final CompositePrefixingFixedClientIdFilter filter;

        PrefixingFilter(CompositePrefixingFixedClientIdFilter filter) {
            this.filter = filter;
        }

        @Override
        public CompletionStage<RequestFilterResult> onRequest(ApiKeys apiKey, RequestHeaderData header, ApiMessage request, FilterContext context) {
            header.setClientId(filter.config().prefix() + header.clientId());
            return context.forwardRequest(header, request);
        }
    }
}
