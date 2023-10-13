/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter;

import java.util.List;
import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.jetbrains.annotations.NotNull;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.kroxylicious.proxy.plugin.PluginConfigType;
import io.kroxylicious.proxy.plugin.Plugins;

@PluginConfigType(CompositePrefixingFixedClientIdFilterFactory.Config.class)
public class CompositePrefixingFixedClientIdFilterFactory
        implements FilterFactory<CompositePrefixingFixedClientIdFilterFactory.Config, CompositePrefixingFixedClientIdFilterFactory.Config> {

    @Override
    public CompositePrefixingFixedClientIdFilterFactory.Config initialize(FilterFactoryContext context, Config config) {
        return Plugins.requireConfig(this, config);
    }

    @NotNull
    @Override
    public Filter createFilter(FilterFactoryContext context,
                               CompositePrefixingFixedClientIdFilterFactory.Config configuration) {
        return new Filter(configuration);
    }

    public class Filter implements CompositeFilter {

        private final Config config;

        public Filter(Config config) {
            this.config = config;
        }

        @Override
        public List<io.kroxylicious.proxy.filter.Filter> getFilters() {
            FixedClientIdFilterFactory.Filter clientIdFilter = new FixedClientIdFilterFactory.Filter(new FixedClientIdFilterFactory.Config(config.clientId));
            return List.of(clientIdFilter, new CompositePrefixingFixedClientIdFilterFactory.PrefixingFilter(this));
        }

    }

    public static class Config {
        private final String prefix;
        private final String clientId;

        @JsonCreator
        public Config(@JsonProperty("prefix") String prefix, @JsonProperty("clientId") String clientId) {
            this.prefix = prefix;
            this.clientId = clientId;
        }
    }

    private class PrefixingFilter implements RequestFilter {
        private final Filter filter;

        PrefixingFilter(Filter filter) {
            this.filter = filter;
        }

        @Override
        public CompletionStage<RequestFilterResult> onRequest(ApiKeys apiKey, RequestHeaderData header, ApiMessage request, FilterContext context) {
            header.setClientId(filter.config.prefix + header.clientId());
            return context.forwardRequest(header, request);
        }
    }
}
