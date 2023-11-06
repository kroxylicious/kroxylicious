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

import io.kroxylicious.proxy.plugin.Plugin;
import io.kroxylicious.proxy.plugin.Plugins;

import edu.umd.cs.findbugs.annotations.NonNull;

@Plugin(configType = CompositePrefixingFixedClientIdFilter.CompositePrefixingFixedClientIdFilterConfig.class)
public class CompositePrefixingFixedClientIdFilterFactory
        implements
        FilterFactory<CompositePrefixingFixedClientIdFilter.CompositePrefixingFixedClientIdFilterConfig, CompositePrefixingFixedClientIdFilter.CompositePrefixingFixedClientIdFilterConfig> {

    @Override
    public CompositePrefixingFixedClientIdFilter.CompositePrefixingFixedClientIdFilterConfig initialize(FilterFactoryContext context,
                                                                                                        CompositePrefixingFixedClientIdFilter.CompositePrefixingFixedClientIdFilterConfig config) {
        return Plugins.requireConfig(this, config);
    }

    @NonNull
    @Override
    public CompositePrefixingFixedClientIdFilter createFilter(FilterFactoryContext context,
                                                              CompositePrefixingFixedClientIdFilter.CompositePrefixingFixedClientIdFilterConfig configuration) {
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
