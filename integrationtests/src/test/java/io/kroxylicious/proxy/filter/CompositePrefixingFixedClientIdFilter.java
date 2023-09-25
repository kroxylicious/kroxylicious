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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class CompositePrefixingFixedClientIdFilter implements CompositeFilter {

    private final CompositePrefixingFixedClientIdFilterConfig config;

    public CompositePrefixingFixedClientIdFilter(CompositePrefixingFixedClientIdFilterConfig config) {
        this.config = config;
    }

    private class PrefixingFilter implements RequestFilter {
        @Override
        public CompletionStage<RequestFilterResult> onRequest(ApiKeys apiKey, RequestHeaderData header, ApiMessage request, FilterContext context) {
            header.setClientId(config.prefix + header.clientId());
            return context.forwardRequest(header, request);
        }
    }

    @Override
    public List<Filter> getFilters() {
        FixedClientIdFilter clientIdFilter = new FixedClientIdFilter(new FixedClientIdFilter.FixedClientIdFilterConfig(config.clientId));
        return List.of(clientIdFilter, new PrefixingFilter());
    }

    public static class CompositePrefixingFixedClientIdFilterConfig {
        private final String prefix;
        private final String clientId;

        @JsonCreator
        public CompositePrefixingFixedClientIdFilterConfig(@JsonProperty("prefix") String prefix, @JsonProperty("clientId") String clientId) {
            this.prefix = prefix;
            this.clientId = clientId;
        }
    }

    public static class Contributor implements FilterContributor<CompositePrefixingFixedClientIdFilterConfig> {

        @Override
        public String getTypeName() {
            return "CompositePrefixingFixedClientId";
        }

        @Override
        public Class<CompositePrefixingFixedClientIdFilterConfig> getConfigType() {
            return CompositePrefixingFixedClientIdFilterConfig.class;
        }

        @Override
        public boolean requiresConfiguration() {
            return true;
        }

        @Override
        public Filter getInstance(FilterConstructContext<CompositePrefixingFixedClientIdFilterConfig> context) {
            return new CompositePrefixingFixedClientIdFilter(context.getConfig());
        }
    }
}
