/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.filter;

import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;

import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.FilterFactory;
import io.kroxylicious.proxy.filter.FilterFactoryContext;
import io.kroxylicious.proxy.filter.RequestFilter;
import io.kroxylicious.proxy.filter.RequestFilterResult;
import io.kroxylicious.proxy.plugin.Plugin;
import io.kroxylicious.proxy.plugin.Plugins;

@Plugin(configType = ExampleConfig.class)
public class RequiresConfigFactory implements FilterFactory<ExampleConfig, ExampleConfig> {

    @Override
    public ExampleConfig initialize(FilterFactoryContext context, ExampleConfig config) {
        return Plugins.requireConfig(this, config);
    }

    @Override
    public Filter createFilter(FilterFactoryContext context, ExampleConfig configuration) {
        return new Filter(context, configuration, this.getClass());
    }

    public static class Filter implements RequestFilter, TestFilter {
        private final FilterFactoryContext context;
        private final ExampleConfig exampleConfig;
        private final Class<? extends FilterFactory> contributorClass;

        public Filter(FilterFactoryContext context, ExampleConfig exampleConfig, Class<? extends FilterFactory> contributorClass) {
            this.context = context;
            this.exampleConfig = exampleConfig;
            this.contributorClass = contributorClass;
        }

        @Override
        public CompletionStage<RequestFilterResult> onRequest(ApiKeys apiKey, RequestHeaderData header, ApiMessage request, FilterContext context) {
            throw new IllegalStateException("not implemented!");
        }

        public FilterFactoryContext getContext() {
            return context;
        }

        public ExampleConfig getExampleConfig() {
            return exampleConfig;
        }

        public Class<? extends FilterFactory> getContributorClass() {
            return contributorClass;
        }

    }
}
