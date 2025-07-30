/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.filter;

import java.util.Objects;
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

@Plugin(configType = FlakyConfig.class)
public class FlakyFactory implements FilterFactory<FlakyConfig, FlakyConfig> {

    private FlakyConfig config;

    @Override
    public FlakyConfig initialize(FilterFactoryContext context, FlakyConfig config) {
        Objects.requireNonNull(context);
        Objects.requireNonNull(config);
        this.config = config;
        if (config.initializeExceptionMsg() != null) {
            throw new RuntimeException(config.initializeExceptionMsg());
        }
        config.onInitialize();
        return config;
    }

    @Override
    public Filter createFilter(FilterFactoryContext context, FlakyConfig configuration) {
        Objects.requireNonNull(configuration);
        if (config.createExceptionMsg() != null) {
            throw new RuntimeException(config.createExceptionMsg());
        }
        return new Filter(context, configuration, this.getClass());
    }

    @Override
    public void close(FlakyConfig configuration) {
        Objects.requireNonNull(configuration);
        config.onClose();
        if (config.closeExceptionMsg() != null) {
            throw new RuntimeException(config.closeExceptionMsg());
        }
    }

    public static class Filter implements RequestFilter, TestFilter {
        private final FilterFactoryContext context;
        private final FlakyConfig exampleConfig;
        private final Class<? extends FilterFactory> contributorClass;

        public Filter(FilterFactoryContext context, FlakyConfig exampleConfig, Class<? extends FilterFactory> contributorClass) {
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
            return null;
        }

        public Class<? extends FilterFactory> getContributorClass() {
            return contributorClass;
        }

    }
}
