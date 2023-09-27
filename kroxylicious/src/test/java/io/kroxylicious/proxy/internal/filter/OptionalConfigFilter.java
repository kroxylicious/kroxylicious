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
import org.jetbrains.annotations.NotNull;

import io.kroxylicious.proxy.filter.Filter;
import io.kroxylicious.proxy.filter.FilterConstructContext;
import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.FilterFactory;
import io.kroxylicious.proxy.filter.RequestFilter;
import io.kroxylicious.proxy.filter.RequestFilterResult;

import edu.umd.cs.findbugs.annotations.NonNull;

public class OptionalConfigFilter implements RequestFilter {
    private final FilterConstructContext context;
    private final ExampleConfig exampleConfig;
    private final Class<? extends FilterFactory> contributorClass;

    public OptionalConfigFilter(FilterConstructContext context, ExampleConfig exampleConfig, Class<? extends FilterFactory> contributorClass) {
        this.context = context;
        this.exampleConfig = exampleConfig;
        this.contributorClass = contributorClass;
    }

    @Override
    public CompletionStage<RequestFilterResult> onRequest(ApiKeys apiKey, RequestHeaderData header, ApiMessage request, FilterContext context) {
        throw new IllegalStateException("not implemented!");
    }

    public FilterConstructContext getContext() {
        return context;
    }

    public ExampleConfig getExampleConfig() {
        return exampleConfig;
    }

    public Class<? extends FilterFactory> getContributorClass() {
        return contributorClass;
    }

    public static class OptionalConfigFactory implements FilterFactory<ExampleConfig> {

        @NotNull
        @Override
        public Class<? extends Filter> getServiceType() {
            return OptionalConfigFilter.class;
        }

        @NonNull
        @Override
        public Class<ExampleConfig> getConfigType() {
            return ExampleConfig.class;
        }

        @Override
        public void validateConfiguration(ExampleConfig config) {
            // any config object is valid
        }

        @Override
        public Filter createInstance(FilterConstructContext<ExampleConfig> context) {
            return new OptionalConfigFilter(context, context.getConfig(), this.getClass());
        }
    }
}
